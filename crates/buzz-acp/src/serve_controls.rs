//! Report the limits of Buzz controls without claiming an unrelated turn stopped.
use crate::{
    bridge_state::BridgeState,
    observer::{ObserverContext, ObserverHandle},
    scope::SessionScope,
};

/// Capture a durable cancel and disclose when it can only cancel queued inputs.
pub(crate) fn record_cancel(
    state: &BridgeState,
    scope: &SessionScope,
    event_id: &str,
    observer: Option<&ObserverHandle>,
) -> anyhow::Result<bool> {
    if !state.record_cancel(scope, event_id)? {
        return Ok(false);
    }
    let control = state
        .control(event_id)?
        .ok_or_else(|| anyhow::anyhow!("recorded cancel control disappeared"))?;
    if control.request_keys.is_empty() {
        let message = "Queued Buzz inputs cancelled. No tracked Buzz request can be stopped; autonomous work may still be running. Use Hermes Desktop Stop to stop that work.";
        tracing::warn!(scope=%scope.telemetry_label(),event_id, "{message}");
        if let Some(observer) = observer {
            observer.emit(
                "control_result",
                None,
                &ObserverContext {
                    channel_id: Some(scope.channel_id().to_string()),
                    session_id: control.session_id,
                    ..ObserverContext::default()
                },
                serde_json::json!({
                    "type":"!cancel", "requestId":event_id,
                    "channelId":scope.channel_id().to_string(),
                    "status":"queued_cancelled_no_tracked_request",
                    "message":message,
                }),
            );
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn untracked_cancel_discloses_limitation_and_still_cancels_queued_inputs() {
        let path = std::env::temp_dir().join(format!("buzz-control-{}.db", uuid::Uuid::new_v4()));
        let state = BridgeState::open(&path, "ws://isolated.invalid", &"3".repeat(64)).unwrap();
        let scope = SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        };
        state.bind_if_absent(&scope, "stored").unwrap();
        state
            .accept_event(&scope, "queued-event", &json!({"synthetic":true}))
            .unwrap();
        let observer = ObserverHandle::in_process();
        assert!(record_cancel(&state, &scope, "cancel-event", Some(&observer)).unwrap());
        assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "cancelled");
        let events = observer.snapshot();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "control_result");
        assert_eq!(events[0].session_id.as_deref(), Some("stored"));
        assert_eq!(
            events[0].payload["status"],
            "queued_cancelled_no_tracked_request"
        );
        assert!(events[0].payload["message"]
            .as_str()
            .unwrap()
            .contains("Desktop Stop"));
        // Re-delivery of this control must not duplicate its effect or its result.
        assert!(!record_cancel(&state, &scope, "cancel-event", Some(&observer)).unwrap());
        assert_eq!(observer.snapshot().len(), 1);
        state.complete_control("cancel-event").unwrap();
        assert!(state
            .control("cancel-event")
            .unwrap()
            .unwrap()
            .request_keys
            .is_empty());
        state
            .accept_event(&scope, "active-event", &json!({"synthetic":true}))
            .unwrap();
        state
            .prepare_submission(
                &["active-event".into()],
                "tracked-key",
                &json!({"text":"task"}),
            )
            .unwrap();
        assert!(record_cancel(&state, &scope, "tracked-cancel", Some(&observer)).unwrap());
        assert_eq!(
            observer.snapshot().len(),
            1,
            "a tracked request has a conditional cancellation target"
        );
        drop(state);
        std::fs::remove_file(path).unwrap();
    }
}
