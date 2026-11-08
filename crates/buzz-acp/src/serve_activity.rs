//! Serve-owned activity, independent of Buzz request dispatch and completion.
//!
//! An old Serve's inventory is process-wide, so only runtime IDs learned through this profile's
//! recovery attachments may light up a durable Buzz conversation. A Serve that advertises
//! residency filters the inventory to this profile and names each row's `conversation_root`,
//! which maps to a durable binding with no attachment at all. `session_key` is never used: it
//! is the compression tip, not proof of ownership.
use crate::{
    bridge_state::{Binding, BridgeState},
    hermes_serve::{ServeClient, ServeConfig},
    observer::{ObserverContext, ObserverHandle},
    scope::SessionScope,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const LIVENESS_INTERVAL: Duration = Duration::from_secs(10);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_AGE: Duration = Duration::from_secs(15);

#[derive(Default)]
struct ActivityState {
    attachments: HashMap<String, String>,
    working: HashMap<SessionScope, ObservedTurn>,
    refreshed: Option<Instant>,
    diagnostic: Option<([usize; 6], Instant)>,
}
struct ObservedTurn {
    last_emitted: Instant,
    runtime_id: String,
    context: ObserverContext,
}

/// Shared routing and freshness state; it never owns or interrupts a model turn.
#[derive(Clone)]
pub(crate) struct ActivityHandle {
    inner: Arc<Mutex<ActivityState>>,
    observer: Option<ObserverHandle>,
}
impl ActivityHandle {
    /// Replace the connection-scoped IDs obtained through profile-scoped resume.
    pub(crate) fn replace_attachments(&self, attachments: HashMap<String, String>) {
        match self.inner.lock() {
            Ok(mut state) => state.attachments = attachments,
            Err(error) => tracing::error!(%error, "Serve activity routing lock poisoned"),
        }
    }

    fn emit(&self, kind: &str, scope: &SessionScope, turn: &ObservedTurn, status: &str) {
        if let Some(observer) = &self.observer {
            if kind == "turn_error" {
                // Close only this observation's live marker; no success/failure claim.
                observer.emit(
                    "turn_completed",
                    None,
                    &turn.context,
                    json!({"outcome":"unknown"}),
                );
                let context = ObserverContext {
                    turn_id: None,
                    started_at: None,
                    ..turn.context.clone()
                };
                observer.emit(
                    "acp_read",
                    None,
                    &context,
                    json!({
                    "status":"observation_unavailable", "title":"Activity unavailable",
                        "text":"Activity observation was lost; the agent's outcome is unknown.",
                        "runtimeSessionId":turn.runtime_id,"threadRoot":scope.root_event_id()}),
                );
                return;
            }
            observer.emit(
                kind,
                None,
                &turn.context,
                json!({
                    "source": "hermes_serve", "status": status, "outcome": "unknown",
                    "runtimeSessionId": turn.runtime_id, "threadRoot": scope.root_event_id(),
                }),
            );
        }
    }

    /// Project a whole-process inventory through this connection's attachments.
    fn refresh(
        &self,
        snapshot: &Value,
        bindings: Vec<(SessionScope, Binding)>,
        now: Instant,
    ) -> anyhow::Result<()> {
        let rows = inventory_rows(snapshot)?;
        let routes = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
            .attachments
            .clone();
        self.project(&rows, &routes, bindings, false, now)
    }

    /// Project this profile's working rows through their conversation roots.
    fn refresh_working(
        &self,
        snapshot: &Value,
        bindings: Vec<(SessionScope, Binding)>,
        now: Instant,
    ) -> anyhow::Result<()> {
        let rows = inventory_rows(snapshot)?;
        let mut routes: HashMap<String, String> = HashMap::new();
        for row in snapshot["sessions"].as_array().into_iter().flatten() {
            let (Some(root), Some(runtime)) =
                (row["conversation_root"].as_str(), row["id"].as_str())
            else {
                continue;
            };
            // One conversation can have several live rows; a working one decides its typing.
            if row["status"] == "working" || !routes.contains_key(root) {
                routes.insert(root.to_owned(), runtime.to_owned());
            }
        }
        self.project(&rows, &routes, bindings, true, now)
    }

    /// `routes` maps stored IDs to runtimes. With `working_only`, a missing row means the
    /// session stopped working; in a full inventory it means observation was lost.
    fn project(
        &self,
        rows: &HashMap<&str, &str>,
        routes: &HashMap<String, String>,
        bindings: Vec<(SessionScope, Binding)>,
        working_only: bool,
        now: Instant,
    ) -> anyhow::Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?;
        let owned: HashMap<_, _> = bindings
            .iter()
            .filter_map(|(scope, binding)| {
                routes
                    .get(&binding.session_id)
                    .map(|runtime| (scope.clone(), (binding.session_id.clone(), runtime.clone())))
            })
            .collect();
        let bound: HashMap<SessionScope, String> = bindings
            .iter()
            .map(|(scope, binding)| (scope.clone(), binding.session_id.clone()))
            .collect();
        let attachment_count = state.attachments.len();
        let binding_count = bindings.len();
        let mapped_live_count = owned
            .values()
            .filter(|(_, runtime)| rows.contains_key(runtime.as_str()))
            .count();
        let mut previous = std::mem::take(&mut state.working);
        for (scope, binding) in bindings {
            let Some(runtime) = routes.get(&binding.session_id) else {
                continue;
            };
            if rows.get(runtime.as_str()) != Some(&"working") {
                continue;
            }
            let retained = previous.remove(&scope).and_then(|turn| {
                if turn.runtime_id == *runtime
                    && turn.context.session_id.as_deref() == Some(binding.session_id.as_str())
                {
                    Some(turn)
                } else {
                    self.emit("turn_error", &scope, &turn, "unknown");
                    None
                }
            });
            let existing = retained.is_some();
            let mut turn = retained.unwrap_or_else(|| ObservedTurn {
                last_emitted: now,
                runtime_id: runtime.clone(),
                context: ObserverContext {
                    channel_id: Some(scope.channel_id().to_string()),
                    session_id: Some(binding.session_id),
                    turn_id: Some(uuid::Uuid::new_v4().to_string()),
                    started_at: Some(chrono::Utc::now().to_rfc3339()),
                },
            });
            if !existing || now.saturating_duration_since(turn.last_emitted) >= LIVENESS_INTERVAL {
                self.emit(
                    if existing {
                        "turn_liveness"
                    } else {
                        "turn_started"
                    },
                    &scope,
                    &turn,
                    "working",
                );
                turn.last_emitted = now;
            }
            state.working.insert(scope, turn);
        }
        for (scope, turn) in previous {
            let still_owned = if working_only {
                bound.get(&scope).map(String::as_str) == turn.context.session_id.as_deref()
            } else {
                owned.get(&scope).is_some_and(|(stored, runtime)| {
                    *runtime == turn.runtime_id
                        && turn.context.session_id.as_deref() == Some(stored.as_str())
                })
            };
            let status = if still_owned {
                rows.get(turn.runtime_id.as_str())
                    .copied()
                    .unwrap_or(if working_only { "idle" } else { "unknown" })
            } else {
                "unknown"
            };
            self.emit(
                if status == "unknown" {
                    "turn_error"
                } else {
                    "turn_completed"
                },
                &scope,
                &turn,
                status,
            );
        }
        let counts = [
            attachment_count,
            binding_count,
            rows.len(),
            rows.values().filter(|status| **status == "working").count(),
            mapped_live_count,
            state.working.len(),
        ];
        if state.diagnostic.is_none_or(|(previous, at)| {
            previous != counts || now.saturating_duration_since(at) >= Duration::from_secs(60)
        }) {
            tracing::info!(
                attachments = counts[0],
                bindings = counts[1],
                inventory = counts[2],
                inventory_working = counts[3],
                mapped_live = counts[4],
                working_scopes = counts[5],
                "Serve activity projection"
            );
            state.diagnostic = Some((counts, now));
        }
        state.refreshed = Some(now);
        Ok(())
    }

    fn unavailable(&self) {
        if let Ok(mut state) = self.inner.lock() {
            for (scope, turn) in state.working.drain() {
                self.emit("turn_error", &scope, &turn, "unknown");
            }
            state.refreshed = None;
        }
    }

    /// Fresh observations only; queued requests and unknown connections never type.
    pub(crate) fn working_scopes(&self) -> Vec<SessionScope> {
        let Ok(mut state) = self.inner.lock() else {
            return Vec::new();
        };
        if state.refreshed.is_none_or(|at| at.elapsed() > MAX_AGE) {
            for (scope, turn) in state.working.drain() {
                self.emit("turn_error", &scope, &turn, "unknown");
            }
            return Vec::new();
        }
        state.working.keys().cloned().collect()
    }
}

fn inventory_rows(snapshot: &Value) -> anyhow::Result<HashMap<&str, &str>> {
    let rows = snapshot["sessions"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Serve activity inventory has no sessions array"))?;
    Ok(rows
        .iter()
        .filter_map(|row| Some((row["id"].as_str()?, row["status"].as_str()?)))
        .collect())
}

/// A bounded snapshot observer; no resume, prompt, or control RPC is issued here.
pub(crate) struct ActivityMonitor {
    handle: ActivityHandle,
    task: tokio::task::JoinHandle<()>,
}
impl ActivityMonitor {
    /// Start observation without waiting for recovery or performing any attachment.
    pub(crate) fn start(
        config: ServeConfig,
        state: BridgeState,
        observer: Option<ObserverHandle>,
    ) -> Self {
        let handle = ActivityHandle {
            inner: Arc::new(Mutex::new(ActivityState::default())),
            observer,
        };
        let observed = handle.clone();
        let task = tokio::spawn(async move {
            let mut previous_counts = HashMap::new();
            loop {
                let connected = ServeClient::connect(config.clone()).await;
                match connected {
                    Ok(client) => loop {
                        // Read once: the request and its projection must agree.
                        let working_only = client.residency().is_supported();
                        let result = tokio::time::timeout(SNAPSHOT_TIMEOUT, async {
                            if working_only {
                                client.working_sessions().await
                            } else {
                                client.active_sessions().await
                            }
                        })
                        .await;
                        let refreshed = match result {
                            Ok(Ok(snapshot)) => state.all_bindings().and_then(|bindings| {
                                if working_only {
                                    observed.refresh_working(&snapshot, bindings, Instant::now())
                                } else {
                                    observed.refresh(&snapshot, bindings, Instant::now())
                                }
                            }),
                            Ok(Err(error)) => Err(error.into()),
                            Err(_) => {
                                Err(anyhow::anyhow!("Serve activity snapshot deadline exceeded"))
                            }
                        };
                        if let Err(error) = refreshed {
                            tracing::warn!(%error, "Serve activity unknown; stopping indicator refresh");
                            observed.unavailable();
                            break;
                        }
                        match state.unfinished_counts() {
                            Ok(counts) if counts != previous_counts => {
                                tracing::info!(states=?counts, "Serve durable input backlog (not model activity)");
                                previous_counts = counts;
                            }
                            Err(error) => {
                                tracing::warn!(%error, "Serve durable input backlog unavailable")
                            }
                            _ => {}
                        }
                        tokio::time::sleep(POLL_INTERVAL).await;
                    },
                    Err(error) => {
                        tracing::warn!(%error, "Serve activity connection unavailable");
                        observed.unavailable();
                    }
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
        Self { handle, task }
    }
    /// Share profile-proven attachment routing with the recovery connection.
    pub(crate) fn handle(&self) -> ActivityHandle {
        self.handle.clone()
    }
    /// Read only recent working observations for the existing typing publisher.
    pub(crate) fn working_scopes(&self) -> Vec<SessionScope> {
        self.handle.working_scopes()
    }
}
impl Drop for ActivityMonitor {
    fn drop(&mut self) {
        self.task.abort();
        self.handle.unavailable();
    }
}

#[cfg(test)]
mod tests;
