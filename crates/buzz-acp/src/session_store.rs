//! W2 — the scope→session map, kept across a buzz-acp restart.
//!
//! `SessionState::sessions` lives in memory, so a restart loses every mapping and
//! the next event in a thread starts a cold session. The agent's transcript was
//! never lost — only our pointer to it — so persisting the pointer and calling
//! `session/load` is enough to make a restart invisible.
//!
//! Deliberately a small JSON file rather than a database: the map is tiny
//! (one line per live thread), a human needs to be able to read and delete it
//! during an incident, and a corrupt file must degrade to "start fresh" rather
//! than take the agent down.
//!
//! Scoped by agent pubkey so several bots on one host never share a map.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::scope::SessionScope;

/// Environment override for where the map lives. Defaults to
/// `$HOME/.local/state/buzz-acp/<pubkey>/sessions.json`.
const STATE_DIR_ENV: &str = "BUZZ_ACP_STATE_DIR";

/// What we remember about one resumable session.
///
/// `primed` records whether the session has already received its standing
/// context (`<base>` and the instruction sections). Legacy agents
/// (`protocol_version < 2`) receive it as a user message on the first
/// successful turn, so a session can exist without it — resuming such a session
/// as if it were primed would leave the agent with no instructions at all.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSession {
    pub id: String,
    #[serde(default)]
    pub primed: bool,
}

/// A resumable mapping from session scope to the agent's session.
#[derive(Debug, Default)]
pub struct SessionStore {
    path: Option<PathBuf>,
    entries: HashMap<String, StoredSession>,
}

/// `Conversation` and `Thread` flatten to distinct, greppable keys.
fn scope_key(scope: &SessionScope) -> String {
    match scope {
        SessionScope::Conversation { channel_id } => format!("conversation:{channel_id}"),
        SessionScope::Thread {
            channel_id,
            root_event_id,
        } => format!("thread:{channel_id}:{root_event_id}"),
    }
}

impl SessionStore {
    /// Open (or begin) the store for one agent. Never fails: an unusable path
    /// yields a store that remembers nothing, which is exactly today's behaviour.
    pub fn open(pubkey_hex: &str) -> Self {
        let Some(path) = Self::resolve_path(pubkey_hex) else {
            tracing::debug!(target: "acp::store", "no writable state dir; sessions will not persist");
            return Self::default();
        };
        let entries = Self::read(&path).unwrap_or_default();
        if !entries.is_empty() {
            tracing::info!(
                target: "acp::store",
                "loaded {} resumable session(s) from {}",
                entries.len(),
                path.display()
            );
        }
        Self {
            path: Some(path),
            entries,
        }
    }

    /// Open a store at an explicit path. Tests use this: sharing a process-wide
    /// env var makes them race, since `set_var` in one races `remove_var` in another.
    pub fn open_at(path: PathBuf) -> Self {
        let entries = Self::read(&path).unwrap_or_default();
        Self { path: Some(path), entries }
    }

    fn resolve_path(pubkey_hex: &str) -> Option<PathBuf> {
        let base = match std::env::var(STATE_DIR_ENV) {
            Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
            _ => {
                let home = std::env::var("HOME").ok()?;
                PathBuf::from(home).join(".local/state/buzz-acp")
            }
        };
        // A short pubkey prefix is enough to separate co-hosted agents and keeps
        // the path readable for a human during an incident.
        let dir = base.join(&pubkey_hex.get(..16).unwrap_or(pubkey_hex));
        if let Err(err) = std::fs::create_dir_all(&dir) {
            tracing::debug!(target: "acp::store", "cannot create {}: {err}", dir.display());
            return None;
        }
        Some(dir.join("sessions.json"))
    }

    fn read(path: &Path) -> Option<HashMap<String, StoredSession>> {
        let text = std::fs::read_to_string(path).ok()?;
        // A file written before `primed` existed holds bare strings. Read those
        // as "not primed": re-sending standing context wastes tokens, while
        // wrongly skipping it leaves an agent with no instructions.
        if let Ok(old) = serde_json::from_str::<HashMap<String, String>>(&text) {
            return Some(
                old.into_iter()
                    .map(|(k, id)| (k, StoredSession { id, primed: false }))
                    .collect(),
            );
        }
        match serde_json::from_str::<HashMap<String, StoredSession>>(&text) {
            Ok(map) => Some(map),
            Err(err) => {
                // Corrupt file: start clean rather than refuse to run.
                tracing::warn!(
                    target: "acp::store",
                    "ignoring unreadable session store {}: {err}", path.display()
                );
                None
            }
        }
    }

    /// What we know about this scope's session, if anything.
    pub fn get(&self, scope: &SessionScope) -> Option<&StoredSession> {
        self.entries.get(&scope_key(scope))
    }

    /// Remember a scope's session id. A write failure is logged, never fatal.
    pub fn insert(&mut self, scope: &SessionScope, session_id: &str) {
        let key = scope_key(scope);
        if self.entries.get(&key).map(|e| e.id.as_str()) == Some(session_id) {
            return; // unchanged — no write
        }
        self.entries.insert(
            key,
            StoredSession {
                id: session_id.to_owned(),
                primed: false,
            },
        );
        self.flush();
    }

    /// Record that this scope's session has received its standing context, so a
    /// resume does not send the whole block a second time.
    pub fn mark_primed(&mut self, scope: &SessionScope, session_id: &str) {
        let key = scope_key(scope);
        match self.entries.get_mut(&key) {
            Some(entry) if entry.id == session_id && !entry.primed => entry.primed = true,
            _ => return, // absent, a different session, or already recorded
        }
        self.flush();
    }

    /// Forget a scope, so the next turn starts a fresh session.
    pub fn remove(&mut self, scope: &SessionScope) {
        if self.entries.remove(&scope_key(scope)).is_some() {
            self.flush();
        }
    }

    /// Forget everything (agent exited, or every session was invalidated).
    pub fn clear(&mut self) {
        if !self.entries.is_empty() {
            self.entries.clear();
            self.flush();
        }
    }

    /// Write via a temp file + rename so a crash mid-write cannot leave a
    /// half-written map that would strand every thread on the next start.
    fn flush(&self) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        let write = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(serde_json::to_string_pretty(&self.entries)?.as_bytes())?;
            f.write_all(b"\n")?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)
        })();
        if let Err(err) = write {
            tracing::warn!(target: "acp::store", "could not persist sessions: {err}");
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn thread(root: &str) -> SessionScope {
        SessionScope::Thread { channel_id: Uuid::nil(), root_event_id: root.to_owned() }
    }

    /// Each test gets its own file, so these run in parallel without a shared env var.
    fn temp_store_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("buzz-store-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("sessions.json")
    }

    #[test]
    fn a_mapping_survives_reopening() {
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path.clone());
        store.insert(&thread("aa"), "sess-1");
        // A restart is exactly this: a new store over the same path.
        assert_eq!(SessionStore::open_at(path).get(&thread("aa")).map(|e| e.id.as_str()), Some("sess-1"));
    }

    #[test]
    fn scopes_do_not_collide() {
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path.clone());
        store.insert(&thread("aa"), "sess-a");
        store.insert(&thread("bb"), "sess-b");
        store.insert(&SessionScope::Conversation { channel_id: Uuid::nil() }, "sess-conv");
        let reopened = SessionStore::open_at(path);
        assert_eq!(reopened.get(&thread("aa")).map(|e| e.id.as_str()), Some("sess-a"));
        assert_eq!(reopened.get(&thread("bb")).map(|e| e.id.as_str()), Some("sess-b"));
        assert_eq!(
            reopened.get(&SessionScope::Conversation { channel_id: Uuid::nil() }).map(|e| e.id.as_str()),
            Some("sess-conv")
        );
    }

    #[test]
    fn agents_on_one_host_do_not_share_a_map() {
        let mut a = SessionStore::open_at(temp_store_path());
        a.insert(&thread("x"), "sess-a");
        assert_eq!(
            SessionStore::open_at(temp_store_path()).get(&thread("x")).map(|e| e.id.as_str()),
            None,
            "one agent must not see another's sessions"
        );
    }

    #[test]
    fn removal_is_persisted() {
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path.clone());
        store.insert(&thread("aa"), "sess-1");
        store.remove(&thread("aa"));
        assert_eq!(SessionStore::open_at(path).get(&thread("aa")).map(|e| e.id.as_str()), None);
    }

    #[test]
    fn clear_forgets_everything() {
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path.clone());
        store.insert(&thread("aa"), "sess-1");
        store.insert(&thread("bb"), "sess-2");
        store.clear();
        let reopened = SessionStore::open_at(path);
        assert_eq!(reopened.get(&thread("aa")).map(|e| e.id.as_str()), None);
        assert_eq!(reopened.get(&thread("bb")).map(|e| e.id.as_str()), None);
    }

    #[test]
    fn a_corrupt_file_degrades_to_empty_rather_than_failing() {
        let path = temp_store_path();
        std::fs::write(&path, "{ this is not json").unwrap();
        assert_eq!(SessionStore::open_at(path).get(&thread("aa")).map(|e| e.id.as_str()), None);
    }

    #[test]
    fn an_unwritable_location_never_panics() {
        let mut store = SessionStore::open_at(PathBuf::from("/proc/nope/sessions.json"));
        store.insert(&thread("aa"), "sess-1"); // must not panic
        assert_eq!(store.get(&thread("aa")).map(|e| e.id.as_str()), Some("sess-1")); // in memory for this run
    }

    #[test]
    fn keys_are_greppable_and_distinct() {
        assert_eq!(
            scope_key(&SessionScope::Conversation { channel_id: Uuid::nil() }),
            "conversation:00000000-0000-0000-0000-000000000000"
        );
        assert!(scope_key(&thread("beef")).starts_with("thread:"));
        assert!(scope_key(&thread("beef")).ends_with(":beef"));
    }

    #[test]
    fn a_new_session_is_not_primed_and_priming_persists() {
        // The bug this guards: a resumed session whose primed flag was lost
        // re-sent its whole <base> block on the next turn, which showed up as a
        // repeated instruction header in the client after every restart.
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path.clone());
        store.insert(&thread("aa"), "sess-1");
        assert!(!store.get(&thread("aa")).unwrap().primed, "new sessions start unprimed");

        store.mark_primed(&thread("aa"), "sess-1");
        assert!(SessionStore::open_at(path).get(&thread("aa")).unwrap().primed);
    }

    #[test]
    fn priming_a_different_session_is_ignored() {
        // A stale session id must never mark the current one as primed.
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path);
        store.insert(&thread("aa"), "sess-new");
        store.mark_primed(&thread("aa"), "sess-old");
        assert!(!store.get(&thread("aa")).unwrap().primed);
    }

    #[test]
    fn replacing_a_session_clears_its_primed_flag() {
        // A fresh session has not received standing context, whatever the old one had.
        let path = temp_store_path();
        let mut store = SessionStore::open_at(path);
        store.insert(&thread("aa"), "sess-1");
        store.mark_primed(&thread("aa"), "sess-1");
        store.insert(&thread("aa"), "sess-2");
        assert!(!store.get(&thread("aa")).unwrap().primed);
    }

    #[test]
    fn a_pre_primed_file_reads_as_unprimed() {
        // Files written before `primed` existed hold bare strings. Re-sending
        // standing context wastes tokens; wrongly skipping it strands an agent
        // with no instructions, so the old format must read as NOT primed.
        let path = temp_store_path();
        std::fs::write(&path, r#"{"thread:00000000-0000-0000-0000-000000000000:aa":"sess-old"}"#).unwrap();
        let store = SessionStore::open_at(path);
        let entry = store.get(&thread("aa")).expect("old format must still load");
        assert_eq!(entry.id, "sess-old");
        assert!(!entry.primed);
    }
}
