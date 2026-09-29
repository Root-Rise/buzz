use super::*;
use serde_json::json;
use uuid::Uuid;

fn fixture() -> (std::path::PathBuf, String, SessionScope) {
    let path = std::env::temp_dir()
        .join(format!("buzz-state-{}", Uuid::new_v4()))
        .join("state.db");
    let scope = SessionScope::Thread {
        channel_id: Uuid::new_v4(),
        root_event_id: "b".repeat(64),
    };
    (path, "a".repeat(64), scope)
}

#[test]
fn independent_workers_keep_bindings_and_restart_does_not_replay_uncertain_work() -> Result<()> {
    let (path, key, scope) = fixture();
    let first = BridgeState::open(&path, "wss://test.example", &key)?;
    let second = BridgeState::open(&path, "https://test.example/", &key)?;
    let sibling = SessionScope::Conversation {
        channel_id: Uuid::new_v4(),
    };
    first.bind_if_absent(&scope, "session-a")?;
    second.bind_if_absent(&sibling, "session-b")?;
    first.mark_primed(&scope, "session-a")?;
    assert_eq!(
        second.bind_if_absent(&scope, "losing-create")?.session_id,
        "session-a"
    );
    assert!(second.binding(&scope)?.is_some_and(|b| b.primed));
    assert!(first.accept_event(&scope, "event-a", &json!({"text":"one"}))?);
    assert!(!second.accept_event(&scope, "event-a", &json!({"text":"duplicate"}))?);
    assert!(second.accept_event(&scope, "event-b", &json!({"text":"two"}))?);
    assert!(first.mark_submitting("event-a", "request-a")?);
    assert!(!second.mark_submitting("event-a", "request-overwrite")?);
    drop((first, second));
    let reopened = BridgeState::open(&path, "wss://test.example", &key)?;
    assert_eq!(
        reopened.binding(&sibling)?.map(|b| b.session_id),
        Some("session-b".into())
    );
    assert_eq!(
        reopened
            .pending(&scope)?
            .iter()
            .map(|i| i.event_id.as_str())
            .collect::<Vec<_>>(),
        ["event-b"]
    );
    let inputs = reopened.inputs(&scope, None)?;
    assert_eq!(inputs[0].status, "submitting");
    assert_eq!(inputs[0].request_key.as_deref(), Some("request-a"));
    assert_eq!(reopened.all_bindings()?.len(), 2);
    assert!(reopened.claim_control("cancel-once")?);
    assert!(!reopened.claim_control("cancel-once")?);
    assert_eq!(reopened.replay_floor(100)?, 100);
    assert_eq!(reopened.replay_floor(200)?, 100);
    assert!(
        !reopened.retire(&scope, "session-a")?,
        "undrained work prevents reset"
    );
    assert!(reopened.mark_accepted("event-a")?);
    assert!(reopened.finish("event-a", "completed")?);
    assert!(reopened.mark_submitting("event-b", "request-b")?);
    assert!(reopened.finish("event-b", "cancelled")?);
    assert!(reopened.retire(&scope, "session-a")?);
    let replacement = reopened.bind_if_absent(&scope, "session-new")?;
    assert_eq!(replacement.generation, 1);
    assert!(
        !reopened.retire(&scope, "session-a")?,
        "stale cleanup cannot remove successor"
    );
    assert!(!reopened.mark_primed(&scope, "session-a")?);
    assert!(BridgeState::open(&path, "wss://other.example", &key)?
        .binding(&scope)?
        .is_none());
    assert!(
        BridgeState::open(&path, "wss://test.example", &"c".repeat(64))?
            .binding(&scope)?
            .is_none()
    );
    Ok(())
}

#[test]
fn legacy_import_is_atomic_preserves_priming_and_never_overwrites_live_bindings() -> Result<()> {
    let (path, key, scope) = fixture();
    let store = BridgeState::open(&path, "wss://test.example", &key)?;
    let legacy = path.with_extension("json");
    std::fs::write(&legacy, "{ corrupt")?;
    assert!(store.import_legacy_json(&legacy).is_err());
    assert!(store.binding(&scope)?.is_none());
    std::fs::write(
        &legacy,
        serde_json::to_vec(&json!({scope_key(&scope): {"id":"old-session", "primed":true}}))?,
    )?;
    assert_eq!(store.import_legacy_json(&legacy)?, 1);
    assert!(store
        .binding(&scope)?
        .is_some_and(|b| b.primed && b.session_id == "old-session"));
    assert!(store.retire(&scope, "old-session")?);
    store.bind_if_absent(&scope, "new-session")?;
    assert_eq!(store.import_legacy_json(&legacy)?, 0);
    assert_eq!(
        store.binding(&scope)?.map(|b| b.session_id),
        Some("new-session".into())
    );
    Ok(())
}

#[test]
fn submission_payload_and_batch_transitions_are_atomic() -> Result<()> {
    let (path, key, scope) = fixture();
    let store = BridgeState::open(&path, "wss://test.example", &key)?;
    for id in ["one", "two"] {
        store.accept_event(&scope, id, &json!({"text":id}))?;
    }
    let payload = json!({"stored_session_id":"conversation", "text":"exact rendered prompt"});
    assert!(store
        .prepare_submission(&["one".into(), "absent".into()], "request", &payload)
        .is_err());
    assert_eq!(store.pending(&scope)?.len(), 2);
    assert!(store.submission("request")?.is_none());
    let ids = vec!["one".into(), "two".into()];
    store.prepare_submission(&ids, "request", &payload)?;
    assert_eq!(store.submission("request")?, Some(payload.clone()));
    assert!(store
        .prepare_submission(&ids, "request", &json!({"text":"changed"}))
        .is_err());
    assert!(store
        .mark_accepted_batch(&["one".into(), "absent".into()])
        .is_err());
    assert!(store
        .inputs(&scope, None)?
        .iter()
        .all(|row| row.status == "submitting"));
    store.mark_accepted_batch(&ids)?;
    store.finish_batch(&ids, "completed")?;
    store.mark_accepted_batch(&ids)?; // Stale observer cannot regress a terminal result.
    store.finish_batch(&ids, "uncertain")?;
    assert!(store
        .inputs(&scope, None)?
        .iter()
        .all(|row| row.status == "completed"));
    store.accept_event(&scope, "pending", &json!({"text":"cancel queued"}))?;
    store.cancel_queued(&scope)?;
    assert!(store.pending(&scope)?.is_empty());
    Ok(())
}

#[test]
fn legacy_admission_is_unknown_and_confirmed_admission_survives_uncertain_restart() -> Result<()> {
    let (path, key, scope) = fixture();
    std::fs::create_dir_all(path.parent().unwrap())?;
    let namespace = serde_json::to_string(&("https://test.example", &key))?;
    let legacy = Connection::open(&path)?;
    legacy.execute_batch(
        "CREATE TABLE submissions(namespace TEXT NOT NULL, request_key TEXT NOT NULL,
        payload TEXT NOT NULL, PRIMARY KEY(namespace,request_key));",
    )?;
    legacy.execute(
        "INSERT INTO submissions VALUES (?,'legacy','{}')",
        [&namespace],
    )?;
    drop(legacy);
    let store = BridgeState::open(&path, "https://test.example", &key)?;
    assert_eq!(store.submission_admission("legacy")?, None);
    store.accept_event(&scope, "new", &json!({"text":"new"}))?;
    let payload =
        json!({"text":"exact", "client_request_id":"request", "stored_session_id":"stored"});
    store.prepare_submission(&["new".into()], "request", &payload)?;
    assert_eq!(store.submission_admission("request")?, Some(false));
    store.mark_accepted_batch(&["new".into()])?;
    store.finish_batch(&["new".into()], "uncertain")?;
    // Retrying prepare must not erase proof that the server accepted this identity.
    store.prepare_submission(&["new".into()], "request", &payload)?;
    drop(store);
    let reopened = BridgeState::open(&path, "https://test.example", &key)?;
    assert_eq!(reopened.submission_admission("request")?, Some(true));
    assert_eq!(reopened.submission_admission("legacy")?, None);
    Ok(())
}
