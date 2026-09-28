//! Reattach unattended sessions and reconcile exact durable requests against server receipts.
use crate::{
    bridge_state::BridgeState,
    hermes_serve::{ServeClient, ServeConfig},
    scope::SessionScope,
};
use std::{collections::HashMap, time::Duration};

/// Cancels only the bridge's observation task when the pool is retired.
pub(crate) struct RecoveryTask(tokio::task::JoinHandle<()>);
impl Drop for RecoveryTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One observer per bridge, independent of its busy execution handles.
pub(crate) fn start(
    config: ServeConfig,
    state: BridgeState,
    activity: crate::serve_activity::ActivityHandle,
) -> RecoveryTask {
    RecoveryTask(tokio::spawn(async move {
        loop {
            activity.replace_attachments(HashMap::new());
            match ServeClient::connect(config.clone()).await {
                Ok(mut client) => {
                    // Reattach all conversations even if no Buzz message arrives after
                    // a Serve restart: reattachment starts the session completion pump.
                    loop {
                        if let Err(error) =
                            reconcile_with_activity(&mut client, &state, Some(&activity)).await
                        {
                            activity.replace_attachments(HashMap::new());
                            tracing::error!(error=%error,"Serve recovery pending; preserving all durable requests");
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
                Err(error) => tracing::error!(error=%error,"Serve recovery connection unavailable"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }))
}

/// Restore bindings and retry only an exact request whose durable absence is proven.
#[cfg(test)]
pub(crate) async fn reconcile(client: &mut ServeClient, state: &BridgeState) -> anyhow::Result<()> {
    reconcile_with_activity(client, state, None).await
}

async fn reconcile_with_activity(
    client: &mut ServeClient,
    state: &BridgeState,
    activity: Option<&crate::serve_activity::ActivityHandle>,
) -> anyhow::Result<()> {
    reconcile_controls(client, state).await?;
    for (scope, binding) in state.all_bindings()? {
        let result = async {
            client.ensure_attached(&binding.session_id).await?;
            if let Some(activity) = activity {
                activity.replace_attachments(client.attachment_ids());
            }
            reconcile_scope(client, state, &scope, &binding.session_id).await
        }
        .await;
        if let Err(error) = result {
            if matches!(
                error.downcast_ref::<crate::acp::AcpError>(),
                Some(crate::acp::AcpError::ServeUnavailable(_))
            ) {
                return Err(error);
            }
            tracing::error!(scope=%scope.telemetry_label(),error=%error,"Serve conversation recovery blocked; continuing other conversations");
        }
    }
    Ok(())
}

async fn reconcile_controls(client: &mut ServeClient, state: &BridgeState) -> anyhow::Result<()> {
    for control in state.pending_controls()? {
        let mut terminal = true;
        for key in &control.request_keys {
            let Some(stored) = control.session_id.as_deref() else {
                tracing::error!(event_id=%control.event_id, "Durable cancel has requests but no session; reconciliation required");
                terminal = false;
                break;
            };
            match client.cancel_request(stored, key).await {
                Ok(receipt) => {
                    // An absent key is only safe after Serve reserves a cancellation
                    // tombstone. A delayed submit must not escape this control.
                    terminal &= receipt["accepted"].as_bool() == Some(true)
                        && matches!(
                            receipt["status"].as_str(),
                            Some("completed" | "cancelled" | "failed" | "interrupted")
                        );
                }
                Err(error @ crate::acp::AcpError::ServeUnavailable(_)) => return Err(error.into()),
                Err(error) => {
                    tracing::error!(event_id=%control.event_id,request_key=%key,error=%error,
                        "Durable cancel pending; continuing other controls");
                    terminal = false;
                }
            }
        }
        if terminal {
            state.complete_control(&control.event_id)?;
        }
    }
    Ok(())
}

async fn reconcile_scope(
    client: &mut ServeClient,
    state: &BridgeState,
    scope: &SessionScope,
    stored: &str,
) -> anyhow::Result<()> {
    let mut requests: HashMap<String, Vec<String>> = HashMap::new();
    for input in state.inputs(scope, None)? {
        if matches!(
            input.status.as_str(),
            "submitting" | "accepted" | "uncertain"
        ) {
            if let Some(key) = input.request_key {
                requests.entry(key).or_default().push(input.event_id);
            }
        }
    }
    for (key, ids) in requests {
        let mut receipt = client.request_status(stored, &key).await?;
        if receipt["accepted"].as_bool() == Some(false) && receipt["status"] == "unknown" {
            anyhow::ensure!(
                receipt["client_request_id"] == key,
                "absent receipt identity mismatch"
            );
            if state.submission_admission(&key)? != Some(false) {
                tracing::warn!(request_key=%key,"Absent receipt lacks proof of no prior admission; preserving fence");
                continue;
            }
            let payload = state
                .submission(&key)?
                .ok_or_else(|| anyhow::anyhow!("missing frozen request {key}"))?;
            anyhow::ensure!(
                payload.as_object().is_some_and(|map| map.len() == 3)
                    && payload["client_request_id"] == key
                    && payload["stored_session_id"] == stored,
                "frozen request identity does not match current binding"
            );
            let text = payload["text"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("frozen request text unavailable"))?;
            anyhow::ensure!(
                state
                    .binding(scope)?
                    .is_some_and(|binding| binding.session_id == stored),
                "conversation binding changed during recovery"
            );
            // A control recorded during the status RPC must be sent before retrying work.
            if state
                .pending_controls()?
                .iter()
                .any(|control| control.request_keys.contains(&key))
            {
                continue;
            }
            match client.submit_frozen(stored, &key, text).await {
                Ok(admission) => receipt = admission,
                Err(crate::acp::AcpError::AgentError { code: 4091, .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let accepted = receipt["accepted"].as_bool() == Some(true);
        if accepted {
            state.mark_accepted_batch(&ids)?;
            if receipt["status"] == "completed" {
                state.mark_primed(scope, stored)?;
            }
        }
        let status = match receipt["status"].as_str() {
            Some(status @ ("completed" | "failed" | "cancelled" | "interrupted")) => Some(status),
            Some("unknown") => Some("uncertain"),
            Some("queued" | "accepted" | "running" | "streaming") => None,
            _ => anyhow::bail!("invalid Serve receipt status for {key}"),
        };
        if let Some(status) = status {
            state.finish_batch(&ids, status)?;
            if status == "uncertain" {
                tracing::warn!(request_key=%key,accepted,"Serve request needs reconciliation; automatic replay disabled");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{json, Value};
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn broken_binding_does_not_prevent_another_session_recovery() {
        let path = std::env::temp_dir().join(format!("buzz-recovery-{}.db", uuid::Uuid::new_v4()));
        let state = BridgeState::open(&path, "ws://isolated.invalid", &"1".repeat(64)).unwrap();
        let broken = SessionScope::Conversation {
            channel_id: uuid::Uuid::from_u128(0),
        };
        let good = SessionScope::Conversation {
            channel_id: uuid::Uuid::from_u128(1),
        };
        state.bind_if_absent(&broken, "missing").unwrap();
        state.bind_if_absent(&good, "stored").unwrap();
        state
            .accept_event(&good, "event", &json!({"synthetic":true}))
            .unwrap();
        state
            .prepare_submission(
                &["event".into()],
                "request-key",
                &json!({"text":"synthetic task"}),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let call: Value = serde_json::from_str(&text).unwrap();
                let response = match call["method"].as_str().unwrap() {
                    "session.resume" if call["params"]["session_id"] == "missing" => {
                        json!({"id":call["id"],"error":{"code":404,"message":"session unavailable"}})
                    }
                    "session.resume" => json!({"id":call["id"],"result":{"session_id":"runtime"}}),
                    "prompt.status" => {
                        assert_eq!(call["params"]["client_request_id"], "request-key");
                        json!({"id":call["id"],"result":{"accepted":true,"status":"completed"}})
                    }
                    method => panic!("recovery must never submit work: {method}"),
                };
                socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let mut client = ServeClient::connect(ServeConfig {
            url: format!("ws://{address}/api/ws"),
            profile: "isolated".into(),
            token: None,
            credentials: None,
            model: None,
            effort: None,
        })
        .await
        .unwrap();
        reconcile(&mut client, &state).await.unwrap();
        assert_eq!(state.inputs(&good, None).unwrap()[0].status, "completed");
        assert!(state.binding(&good).unwrap().unwrap().primed);
        assert_eq!(
            state.binding(&broken).unwrap().unwrap().session_id,
            "missing"
        );
        server.abort();
        drop(client);
        drop(state);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn restart_recovers_cancel_without_a_live_pool_task() {
        let path =
            std::env::temp_dir().join(format!("buzz-cancel-recovery-{}.db", uuid::Uuid::new_v4()));
        let identity = "2".repeat(64);
        let scope = SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        };
        {
            let state = BridgeState::open(&path, "ws://isolated.invalid", &identity).unwrap();
            state.bind_if_absent(&scope, "stored").unwrap();
            state
                .accept_event(&scope, "event", &json!({"synthetic":true}))
                .unwrap();
            state
                .prepare_submission(
                    &["event".into()],
                    "original-request",
                    &json!({"text":"synthetic"}),
                )
                .unwrap();
            state.record_cancel(&scope, "cancel-event").unwrap();
        }
        let state = BridgeState::open(&path, "ws://isolated.invalid", &identity).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut cancelled = false;
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let call: Value = serde_json::from_str(&text).unwrap();
                let result = match call["method"].as_str().unwrap() {
                    "session.resume" => {
                        assert_eq!(call["params"]["session_id"], "stored");
                        json!({"session_id":"runtime"})
                    }
                    "prompt.cancel" => {
                        assert_eq!(call["params"]["client_request_id"], "original-request");
                        cancelled = true;
                        json!({"accepted":true,"status":"cancelled","cancel_requested":false})
                    }
                    "prompt.status" => {
                        assert!(cancelled);
                        assert_eq!(call["params"]["client_request_id"], "original-request");
                        json!({"accepted":true,"status":"cancelled"})
                    }
                    method => {
                        panic!("recovery must not submit or interrupt unrelated work: {method}")
                    }
                };
                socket
                    .send(Message::Text(
                        json!({"id":call["id"],"result":result}).to_string().into(),
                    ))
                    .await
                    .unwrap();
            }
        });
        let mut client = ServeClient::connect(ServeConfig {
            url: format!("ws://{address}/api/ws"),
            profile: "isolated".into(),
            token: None,
            credentials: None,
            model: None,
            effort: None,
        })
        .await
        .unwrap();
        assert!(state.scope_blocked(&scope).unwrap());
        reconcile(&mut client, &state).await.unwrap();
        assert!(state.pending_controls().unwrap().is_empty());
        assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "cancelled");
        assert!(!state.scope_blocked(&scope).unwrap());
        server.abort();
        drop(client);
        drop(state);
        std::fs::remove_file(path).unwrap();
    }
}
