//! Durable bridge routing and input receipts, independent of an execution connection.
//!
//! A transport failure does not prove that an accepted command failed. Inputs in
//! `submitting` or `accepted` remain queryable and are never returned as queued.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::scope::SessionScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Binding {
    pub session_id: String,
    pub primed: bool,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct StoredInput {
    pub event_id: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub request_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PendingControl {
    pub event_id: String,
    pub session_id: Option<String>,
    pub request_keys: Vec<String>,
}

/// Clones share a connection; independently opened handles serialize through SQLite.
#[derive(Debug, Clone)]
pub(crate) struct BridgeState {
    connection: Arc<Mutex<Connection>>,
    namespace: String,
}

fn scope_key(scope: &SessionScope) -> String {
    match scope {
        SessionScope::Conversation { channel_id } => format!("conversation:{channel_id}"),
        SessionScope::Thread {
            channel_id,
            root_event_id,
        } => format!("thread:{channel_id}:{root_event_id}"),
    }
}

impl BridgeState {
    /// Open durable state. Corruption or an unwritable location is an error, never amnesia.
    pub fn open(path: &Path, community: &str, agent_pubkey: &str) -> Result<Self> {
        ensure!(
            agent_pubkey.len() == 64 && agent_pubkey.bytes().all(|c| c.is_ascii_hexdigit()),
            "bridge state requires a full agent public key"
        );
        let mut origin = url::Url::parse(community).context("invalid bridge community URL")?;
        let scheme = match origin.scheme() {
            "ws" | "http" => "http",
            "wss" | "https" => "https",
            _ => bail!("unsupported community URL scheme"),
        };
        origin
            .set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("invalid community scheme"))?;
        ensure!(origin.host_str().is_some(), "community URL has no host");
        let namespace = serde_json::to_string(&(
            origin.origin().ascii_serialization(),
            agent_pubkey.to_ascii_lowercase(),
        ))?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).context("create bridge state directory")?;
        }
        let connection = Connection::open(path).context("open bridge state")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(include_str!("bridge_state/schema.sql"))?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            namespace,
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow::anyhow!("bridge state lock poisoned"))
    }

    /// Retain a replay floor across downtime. Receipt dedup makes overlapping replay safe.
    pub fn replay_floor(&self, initial: u64) -> Result<u64> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR IGNORE INTO relay_progress(namespace,replay_floor) VALUES (?,?)",
            params![self.namespace, initial],
        )?;
        Ok(conn.query_row(
            "SELECT replay_floor FROM relay_progress WHERE namespace=?",
            [&self.namespace],
            |row| row.get(0),
        )?)
    }

    pub fn scope_blocked(&self, scope: &SessionScope) -> Result<bool> {
        Ok(self.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM inputs
            WHERE namespace=?1 AND scope=?2 AND status IN ('submitting','accepted','uncertain'))
            OR EXISTS(SELECT 1 FROM controls WHERE namespace=?1 AND scope=?2 AND status='pending')",
            params![self.namespace, scope_key(scope)],
            |row| row.get(0),
        )?)
    }

    pub fn claim_control(&self, event_id: &str) -> Result<bool> {
        Ok(self.connection()?.execute(
            "INSERT OR IGNORE INTO controls(namespace,event_id,status) VALUES (?,?,'observed')",
            params![self.namespace, event_id],
        )? == 1)
    }

    pub fn record_cancel(&self, scope: &SessionScope, event_id: &str) -> Result<bool> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if tx
            .query_row(
                "SELECT 1 FROM controls WHERE namespace=? AND event_id=?",
                params![self.namespace, event_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Ok(false);
        }
        let key = scope_key(scope);
        let session_id: Option<String> = tx
            .query_row(
                "SELECT session_id FROM bindings WHERE namespace=? AND scope=?",
                params![self.namespace, key],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let request_keys = {
            let mut query = tx.prepare(
                "SELECT DISTINCT request_key FROM inputs
                WHERE namespace=? AND scope=? AND status IN ('submitting','accepted','uncertain')
                AND request_key IS NOT NULL ORDER BY request_key",
            )?;
            let rows =
                query.query_map(params![self.namespace, key], |row| row.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let payload = PendingControl {
            event_id: event_id.into(),
            session_id,
            request_keys,
        };
        tx.execute(
            "INSERT INTO controls(namespace,event_id,scope,payload) VALUES (?,?,?,?)",
            params![
                self.namespace,
                event_id,
                key,
                serde_json::to_string(&payload)?
            ],
        )?;
        tx.execute("UPDATE inputs SET status='cancelled' WHERE namespace=? AND scope=? AND status='queued'",
            params![self.namespace, key])?;
        tx.commit()?;
        Ok(true)
    }

    /// Read captured control targets even if recovery already completed the control.
    pub fn control(&self, event_id: &str) -> Result<Option<PendingControl>> {
        let payload: Option<String> = self.connection()?.query_row(
            "SELECT payload FROM controls WHERE namespace=? AND event_id=? AND payload IS NOT NULL",
            params![self.namespace, event_id], |row| row.get(0),
        ).optional()?;
        payload
            .map(|text| serde_json::from_str(&text).map_err(Into::into))
            .transpose()
    }

    pub fn pending_controls(&self) -> Result<Vec<PendingControl>> {
        let conn = self.connection()?;
        let mut query = conn.prepare(
            "SELECT payload FROM controls WHERE namespace=? AND status='pending'
            AND payload IS NOT NULL ORDER BY rowid",
        )?;
        let rows = query.query_map([&self.namespace], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn complete_control(&self, event_id: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE controls SET status='completed' WHERE namespace=? AND event_id=?",
            params![self.namespace, event_id],
        )?;
        Ok(())
    }

    pub fn cancel_queued(&self, scope: &SessionScope) -> Result<()> {
        self.connection()?.execute(
            "UPDATE inputs SET status='cancelled'
            WHERE namespace=? AND scope=? AND status='queued'",
            params![self.namespace, scope_key(scope)],
        )?;
        Ok(())
    }

    pub fn queued_inputs(&self) -> Result<Vec<(SessionScope, StoredInput)>> {
        let conn = self.connection()?;
        let mut query = conn.prepare(
            "SELECT scope,event_id,payload FROM inputs
            WHERE namespace=? AND status='queued' ORDER BY sequence",
        )?;
        let rows = query.query_map([&self.namespace], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (scope, event_id, payload) = row?;
            Ok((
                parse_scope(&scope)?,
                StoredInput {
                    event_id,
                    payload: serde_json::from_str(&payload)?,
                    status: "queued".into(),
                    request_key: None,
                },
            ))
        })
        .collect()
    }

    pub fn all_bindings(&self) -> Result<Vec<(SessionScope, Binding)>> {
        let conn = self.connection()?;
        let mut query = conn.prepare(
            "SELECT scope,session_id,primed,generation FROM bindings
            WHERE namespace=? AND session_id IS NOT NULL ORDER BY scope",
        )?;
        let rows = query.query_map([&self.namespace], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Binding {
                    session_id: row.get(1)?,
                    primed: row.get(2)?,
                    generation: row.get(3)?,
                },
            ))
        })?;
        rows.map(|row| {
            let (key, binding) = row?;
            Ok((parse_scope(&key)?, binding))
        })
        .collect()
    }

    /// Freeze the exact prompt and claim its complete event batch before sending it.
    pub fn prepare_submission(
        &self,
        event_ids: &[String],
        request_key: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        ensure!(
            !event_ids.is_empty() && !request_key.is_empty(),
            "empty submission identity"
        );
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous: Option<String> = tx
            .query_row(
                "SELECT payload FROM submissions WHERE namespace=? AND request_key=?",
                params![self.namespace, request_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            let stored: serde_json::Value = serde_json::from_str(&previous)?;
            ensure!(
                stored == *payload,
                "request key reused with a different payload"
            );
            return Ok(stored);
        }
        for id in event_ids {
            ensure!(
                tx.execute(
                    "UPDATE inputs SET status='submitting',request_key=?
                WHERE namespace=? AND event_id=? AND status='queued'",
                    params![request_key, self.namespace, id]
                )? == 1,
                "submission includes an absent or already submitted event"
            );
        }
        tx.execute(
            "INSERT INTO submissions(namespace,request_key,payload) VALUES (?,?,?)",
            params![self.namespace, request_key, serde_json::to_string(payload)?],
        )?;
        tx.commit()?;
        Ok(payload.clone())
    }

    pub fn submission(&self, request_key: &str) -> Result<Option<serde_json::Value>> {
        let raw: Option<String> = self
            .connection()?
            .query_row(
                "SELECT payload FROM submissions WHERE namespace=? AND request_key=?",
                params![self.namespace, request_key],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
            .transpose()
    }

    /// Read the current durable binding, regardless of which worker created it.
    pub fn binding(&self, scope: &SessionScope) -> Result<Option<Binding>> {
        self.connection()?
            .query_row(
                "SELECT session_id,primed,generation FROM bindings
             WHERE namespace=? AND scope=? AND session_id IS NOT NULL",
                params![self.namespace, scope_key(scope)],
                |row| {
                    Ok(Binding {
                        session_id: row.get(0)?,
                        primed: row.get(1)?,
                        generation: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Atomically choose a binding; losing creators receive the winner's stored ID.
    pub fn bind_if_absent(&self, scope: &SessionScope, session_id: &str) -> Result<Binding> {
        ensure!(!session_id.is_empty(), "empty stored session ID");
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO bindings(namespace,scope,session_id) VALUES (?,?,?)
             ON CONFLICT(namespace,scope) DO UPDATE SET session_id=excluded.session_id,primed=0
             WHERE bindings.session_id IS NULL",
            params![self.namespace, scope_key(scope), session_id],
        )?;
        let binding = tx.query_row(
            "SELECT session_id,primed,generation FROM bindings WHERE namespace=? AND scope=?",
            params![self.namespace, scope_key(scope)],
            |row| {
                Ok(Binding {
                    session_id: row.get(0)?,
                    primed: row.get(1)?,
                    generation: row.get(2)?,
                })
            },
        )?;
        tx.commit()?;
        Ok(binding)
    }

    /// Stale workers cannot prime a replacement conversation.
    pub fn mark_primed(&self, scope: &SessionScope, expected_id: &str) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE bindings SET primed=1 WHERE namespace=? AND scope=? AND session_id=?",
            params![self.namespace, scope_key(scope), expected_id],
        )? == 1)
    }

    /// Retire only a drained, explicitly selected binding; preserve its generation.
    pub fn retire(&self, scope: &SessionScope, expected_id: &str) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE bindings SET session_id=NULL,primed=0,generation=generation+1
             WHERE namespace=?1 AND scope=?2 AND session_id=?3 AND NOT EXISTS (
               SELECT 1 FROM inputs WHERE namespace=?1 AND scope=?2
                 AND status IN ('queued','submitting','accepted','uncertain'))",
            params![self.namespace, scope_key(scope), expected_id],
        )? == 1)
    }

    /// Accept an eligible signed event once; replayed relay events keep their original place.
    pub fn accept_event(
        &self,
        scope: &SessionScope,
        event_id: &str,
        payload: &serde_json::Value,
    ) -> Result<bool> {
        ensure!(!event_id.is_empty(), "empty event ID");
        Ok(self.connection()?.execute(
            "INSERT INTO inputs(namespace,event_id,scope,payload) VALUES (?,?,?,?)
             ON CONFLICT(namespace,event_id) DO NOTHING",
            params![
                self.namespace,
                event_id,
                scope_key(scope),
                serde_json::to_string(payload)?
            ],
        )? == 1)
    }

    /// Only never-submitted inputs are eligible for automatic dispatch after restart.
    pub fn pending(&self, scope: &SessionScope) -> Result<Vec<StoredInput>> {
        self.inputs(scope, Some("queued"))
    }

    /// Include uncertain work for reconciliation and operator status.
    pub fn inputs(&self, scope: &SessionScope, status: Option<&str>) -> Result<Vec<StoredInput>> {
        let conn = self.connection()?;
        let mut statement = conn.prepare(
            "SELECT event_id,payload,status,request_key FROM inputs WHERE namespace=? AND scope=?
             AND (?3 IS NULL OR status=?3) ORDER BY sequence",
        )?;
        let rows =
            statement.query_map(params![self.namespace, scope_key(scope), status], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?;
        rows.map(|row| {
            let (event_id, payload, status, request_key) = row?;
            Ok(StoredInput {
                event_id,
                payload: serde_json::from_str(&payload)?,
                status,
                request_key,
            })
        })
        .collect()
    }

    /// Commit the correlation key before crossing the network boundary.
    pub fn mark_submitting(&self, event_id: &str, request_key: &str) -> Result<bool> {
        ensure!(!request_key.is_empty(), "empty request key");
        Ok(self.connection()?.execute(
            "UPDATE inputs SET status='submitting',request_key=? WHERE namespace=? AND event_id=? AND status='queued'",
            params![request_key, self.namespace, event_id])? == 1)
    }

    /// Record positive server admission, including reconciliation after a lost response.
    pub fn mark_accepted_batch(&self, event_ids: &[String]) -> Result<()> {
        self.transition_batch(event_ids, "accepted")
    }

    pub fn finish_batch(&self, event_ids: &[String], status: &str) -> Result<()> {
        ensure!(
            [
                "completed",
                "failed",
                "cancelled",
                "interrupted",
                "uncertain"
            ]
            .contains(&status),
            "invalid input outcome"
        );
        self.transition_batch(event_ids, status)
    }

    fn transition_batch(&self, event_ids: &[String], status: &str) -> Result<()> {
        ensure!(!event_ids.is_empty(), "empty receipt batch");
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for id in event_ids {
            let current: String = tx.query_row(
                "SELECT status FROM inputs WHERE namespace=? AND event_id=?",
                params![self.namespace, id],
                |row| row.get(0),
            )?;
            // A concurrent receipt observer may be one poll behind the worker.
            if ["completed", "failed", "cancelled", "interrupted"].contains(&current.as_str()) {
                continue;
            }
            ensure!(
                ["submitting", "accepted", "uncertain"].contains(&current.as_str()),
                "cannot settle an input that has not been submitted"
            );
            tx.execute(
                "UPDATE inputs SET status=? WHERE namespace=? AND event_id=?",
                params![status, self.namespace, id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Record positive server admission, including reconciliation after a lost response.
    pub fn mark_accepted(&self, event_id: &str) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE inputs SET status='accepted' WHERE namespace=? AND event_id=?
             AND status IN ('submitting','uncertain')",
            params![self.namespace, event_id],
        )? == 1)
    }

    /// Terminal failures stay visible; uncertain submissions never silently return to queued.
    pub fn finish(&self, event_id: &str, status: &str) -> Result<bool> {
        ensure!(
            [
                "completed",
                "failed",
                "cancelled",
                "interrupted",
                "uncertain"
            ]
            .contains(&status),
            "invalid input outcome"
        );
        Ok(self.connection()?.execute(
            "UPDATE inputs SET status=? WHERE namespace=? AND event_id=?
             AND status IN ('submitting','accepted','uncertain')",
            params![status, self.namespace, event_id],
        )? == 1)
    }

    /// Import an old JSON map once. Bad input aborts the whole import and remains untouched.
    pub fn import_legacy_json(&self, path: &Path) -> Result<usize> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if tx
            .query_row(
                "SELECT 1 FROM migrations WHERE namespace=? AND name='legacy-json'",
                [&self.namespace],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Ok(0);
        }
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(err) => return Err(err).context("read legacy bridge map"),
        };
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Legacy {
            Id(String),
            Record {
                id: String,
                #[serde(default)]
                primed: bool,
            },
        }
        let records: std::collections::BTreeMap<String, Legacy> = serde_json::from_str(&text)
            .context("invalid legacy bridge map; refusing to forget conversations")?;
        let mut imported = 0;
        for (scope, record) in records {
            let (id, primed) = match record {
                Legacy::Id(id) => (id, false),
                Legacy::Record { id, primed } => (id, primed),
            };
            ensure!(!id.is_empty(), "legacy binding has empty session ID");
            imported += tx.execute("INSERT OR IGNORE INTO bindings(namespace,scope,session_id,primed) VALUES (?,?,?,?)",
                                   params![self.namespace, scope, id, primed])?;
        }
        tx.execute(
            "INSERT INTO migrations(namespace,name) VALUES (?,'legacy-json')",
            [&self.namespace],
        )?;
        tx.commit()?;
        Ok(imported)
    }
}

fn parse_scope(key: &str) -> Result<SessionScope> {
    let mut parts = key.splitn(3, ':');
    let kind = parts.next().unwrap_or_default();
    let channel_id = parts.next().context("missing binding channel")?.parse()?;
    match kind {
        "conversation" => Ok(SessionScope::Conversation { channel_id }),
        "thread" => Ok(SessionScope::Thread {
            channel_id,
            root_event_id: parts.next().context("missing binding thread")?.to_owned(),
        }),
        _ => bail!("invalid binding scope"),
    }
}

#[cfg(test)]
mod tests;
