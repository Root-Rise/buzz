use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

fn config(address: std::net::SocketAddr) -> ServeConfig {
    ServeConfig {
        url: format!("ws://{address}/api/ws"),
        profile: "isolated".into(),
        token: None,
        credentials: None,
        model: None,
        effort: None,
    }
}

#[tokio::test]
async fn cached_session_is_reattached_after_another_session_reconnects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for connection in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut attached = std::collections::HashSet::new();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let call: Value = serde_json::from_str(&text).unwrap();
                let result = match call["method"].as_str().unwrap() {
                    "session.resume" => {
                        let stored = call["params"]["session_id"].as_str().unwrap();
                        attached.insert(stored.to_owned());
                        json!({"session_id": format!("{stored}-{connection}")})
                    }
                    "prompt.submit" => {
                        assert!(
                            attached.contains("A"),
                            "cached pool session must attach to this connection"
                        );
                        assert_eq!(call["params"]["session_id"], "A-1");
                        json!({"accepted":true,"status":"completed"})
                    }
                    method => panic!("unexpected call {method}"),
                };
                socket
                    .send(Message::Text(
                        json!({"id":call["id"],"result":result}).to_string().into(),
                    ))
                    .await
                    .unwrap();
                if connection == 0 {
                    socket.close(None).await.unwrap();
                    break;
                }
            }
        }
    });
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    client.session_load("A", vec![]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !client.pump.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.session_load("B", vec![]).await.unwrap();
    // The pool still knows A's durable binding, and does not call session_load again.
    client.request_key = Some("A-request".into());
    assert_eq!(
        client
            .prompt("A", &["frozen A"], Duration::from_secs(5))
            .await
            .unwrap(),
        StopReason::EndTurn
    );
    assert!(!client.has_in_flight_prompt());
    server.abort();
}

#[tokio::test]
async fn recovery_retries_only_proven_absent_frozen_request_without_waiting_for_turn() {
    let dir = std::env::temp_dir().join(format!("buzz-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let state = crate::bridge_state::BridgeState::open(
        &dir.join("state.db"),
        "ws://isolated.invalid",
        &"1".repeat(64),
    )
    .unwrap();
    let scope = crate::scope::SessionScope::Conversation {
        channel_id: uuid::Uuid::new_v4(),
    };
    state.bind_if_absent(&scope, "stored").unwrap();
    for key in [
        "absent",
        "accepted-unknown",
        "cancelled",
        "previously-accepted",
        "legacy",
    ] {
        state
            .accept_event(&scope, key, &json!({"synthetic":true}))
            .unwrap();
        state.prepare_submission(&[key.into()], key, &json!({"stored_session_id":"stored","client_request_id":key,"text":format!("frozen {key}")})).unwrap();
        state.finish_batch(&[key.into()], "uncertain").unwrap();
    }
    rusqlite::Connection::open(dir.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE submissions SET server_accepted=NULL WHERE request_key='legacy'",
            [],
        )
        .unwrap();
    state
        .mark_accepted_batch(&["previously-accepted".into()])
        .unwrap();
    state
        .finish_batch(&["previously-accepted".into()], "uncertain")
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    let server_state = state.clone();
    let server_scope = scope.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            let key = call["params"]["client_request_id"].as_str().unwrap_or("");
            if call["method"] == "prompt.status" && key == "control-race" {
                server_state
                    .record_cancel(&server_scope, "cancel-during-status")
                    .unwrap();
            }
            let result = match call["method"].as_str().unwrap() {
                "session.resume" => json!({"session_id":"runtime"}),
                "prompt.status" => {
                    json!({"client_request_id":key,"accepted": !["previously-accepted", "legacy", "control-race"].contains(&key) && (key != "absent" || observed.load(std::sync::atomic::Ordering::SeqCst)>0),
                    "status":match key { "cancelled"=>"cancelled", "absent" if observed.load(std::sync::atomic::Ordering::SeqCst)>0=>"running", _=>"unknown"}})
                }
                "prompt.submit" => {
                    assert_eq!(
                        key, "absent",
                        "accepted unknown and cancel tombstones must never replay"
                    );
                    assert_eq!(call["params"]["text"], "frozen absent");
                    assert_eq!(call["params"]["queued"], true);
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    json!({"client_request_id":key,"accepted":true,"status":"running"})
                }
                method => panic!("unexpected call {method}"),
            };
            socket
                .send(Message::Text(
                    json!({"id":call["id"],"result":result}).to_string().into(),
                ))
                .await
                .unwrap();
        }
    });
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    for _ in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::serve_recovery::reconcile(&mut client, &state),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    let inputs = state.inputs(&scope, None).unwrap();
    assert_eq!(inputs[0].status, "accepted");
    assert_eq!(inputs[1].status, "uncertain");
    assert_eq!(inputs[2].status, "cancelled");
    assert_eq!(inputs[3].status, "uncertain");
    assert_eq!(
        state.submission_admission("previously-accepted").unwrap(),
        Some(true)
    );
    assert_eq!(inputs[4].status, "uncertain");
    assert_eq!(state.submission_admission("absent").unwrap(), Some(true));
    state
        .accept_event(&scope, "control-race", &json!({"synthetic":true}))
        .unwrap();
    state.prepare_submission(&["control-race".into()], "control-race", &json!({
        "stored_session_id":"stored", "client_request_id":"control-race", "text":"must not run"
    })).unwrap();
    crate::serve_recovery::reconcile(&mut client, &state)
        .await
        .unwrap();
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(state.pending_controls().unwrap().len(), 1);
    server.abort();
}
