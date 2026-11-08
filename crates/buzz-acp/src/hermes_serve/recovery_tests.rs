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

#[tokio::test]
async fn cold_pass_attaches_live_and_newly_addressed_conversations_first() {
    let dir = std::env::temp_dir().join(format!("buzz-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let state = crate::bridge_state::BridgeState::open(
        &dir.join("state.db"),
        "ws://isolated.invalid",
        &"1".repeat(64),
    )
    .unwrap();
    let channel_id = uuid::Uuid::new_v4();
    let thread = |c: char| crate::scope::SessionScope::Thread {
        channel_id,
        root_event_id: c.to_string().repeat(64),
    };
    // Scope order alone would attach finished, idle, live, recent.
    let (finished, idle, live, recent) = (thread('a'), thread('b'), thread('c'), thread('d'));
    for (scope, session) in [
        (&finished, "finished"),
        (&idle, "idle"),
        (&live, "live"),
        (&recent, "recent"),
    ] {
        state.bind_if_absent(scope, session).unwrap();
    }
    state.accept_event(&finished, "old", &json!({})).unwrap();
    state.accept_event(&live, "waiting", &json!({})).unwrap();
    state.accept_event(&recent, "new", &json!({})).unwrap();
    rusqlite::Connection::open(dir.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE inputs SET status='completed' WHERE event_id IN ('old','new')",
            [],
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = order.clone();
    let (server_state, server_idle) = (state.clone(), idle.clone());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(call["method"], "session.resume");
            let stored = call["params"]["session_id"].as_str().unwrap().to_owned();
            let first = {
                let mut order = observed.lock().unwrap();
                order.push(stored.clone());
                order.len() == 1
            };
            if first {
                // A message reaches the idle conversation while the pass is under way.
                server_state
                    .accept_event(&server_idle, "arrived", &json!({}))
                    .unwrap();
            }
            socket
                .send(Message::Text(
                    json!({"id":call["id"],"result":{"session_id":format!("{stored}-runtime")}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
    });
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::serve_recovery::reconcile(&mut client, &state),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        ["live", "idle", "recent", "finished"],
        "unfinished work first, then the conversation addressed mid-pass, then by recency"
    );
    server.abort();
}

#[tokio::test]
async fn thread_bound_after_its_first_input_is_attached_in_the_same_pass() {
    let dir = std::env::temp_dir().join(format!("buzz-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let state = crate::bridge_state::BridgeState::open(
        &dir.join("state.db"),
        "ws://isolated.invalid",
        &"1".repeat(64),
    )
    .unwrap();
    let channel_id = uuid::Uuid::new_v4();
    let thread = |c: char| crate::scope::SessionScope::Thread {
        channel_id,
        root_event_id: c.to_string().repeat(64),
    };
    let (older, old, live, new_thread) = (thread('a'), thread('b'), thread('c'), thread('e'));
    state.bind_if_absent(&older, "older").unwrap();
    state.bind_if_absent(&old, "old").unwrap();
    state.bind_if_absent(&live, "live").unwrap();
    state
        .accept_event(&older, "older-input", &json!({}))
        .unwrap();
    state.accept_event(&old, "old-input", &json!({})).unwrap();
    state.accept_event(&live, "waiting", &json!({})).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = order.clone();
    let (server_state, server_live, server_new) = (state.clone(), live.clone(), new_thread.clone());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(call["method"], "session.resume");
            let stored = call["params"]["session_id"].as_str().unwrap().to_owned();
            let attached = {
                let mut order = observed.lock().unwrap();
                order.push(stored.clone());
                order.len()
            };
            // The pool journals a new thread's first message and a later message reaches a
            // conversation already attached this pass; the recovery loop looks for addressed
            // conversations once more before the pool's session.create returns and binds it.
            if attached == 1 {
                server_state
                    .accept_event(&server_new, "first", &json!({}))
                    .unwrap();
                server_state
                    .accept_event(&server_live, "follow-up", &json!({}))
                    .unwrap();
            } else if attached == 2 {
                server_state.bind_if_absent(&server_new, "new").unwrap();
            }
            socket
                .send(Message::Text(
                    json!({"id":call["id"],"result":{"session_id":format!("{stored}-runtime")}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
    });
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::serve_recovery::reconcile(&mut client, &state),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*order.lock().unwrap(), ["live", "old", "new", "older"]);
    server.abort();
}

/// One-connection Serve scripted by call; every call is recorded in arrival order.
async fn scripted_serve(
    respond: impl Fn(&str, &Value) -> Result<Value, (i64, &'static str)> + Send + 'static,
) -> (
    std::net::SocketAddr,
    Arc<Mutex<Vec<(String, Value)>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            let method = call["method"].as_str().unwrap().to_owned();
            recorded
                .lock()
                .unwrap()
                .push((method.clone(), call["params"].clone()));
            let response = match respond(&method, &call["params"]) {
                Ok(result) => json!({"id":call["id"],"result":result}),
                Err((code, message)) => {
                    json!({"id":call["id"],"error":{"code":code,"message":message}})
                }
            };
            socket
                .send(Message::Text(response.to_string().into()))
                .await
                .unwrap();
        }
    });
    (address, calls, server)
}

struct Journal {
    state: crate::bridge_state::BridgeState,
    scope: crate::scope::SessionScope,
    dir: std::path::PathBuf,
}
impl Drop for Journal {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Journal {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("buzz-heal-{}", uuid::Uuid::new_v4()));
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
        state.accept_event(&scope, "event", &json!({})).unwrap();
        Self { state, scope, dir }
    }
    fn arm(&self, client: &mut ServeClient) {
        client.request_key = Some("frozen-key".into());
        client.prepared = Some(PreparedRequest {
            state: self.state.clone(),
            scope: self.scope.clone(),
            event_ids: vec!["event".into()],
        });
    }
    fn status(&self) -> String {
        self.state.inputs(&self.scope, None).unwrap()[0]
            .status
            .clone()
    }
}

fn methods(calls: &Mutex<Vec<(String, Value)>>) -> Vec<String> {
    calls
        .lock()
        .unwrap()
        .iter()
        .map(|(name, _)| name.clone())
        .collect()
}

#[tokio::test]
async fn stale_runtime_on_submit_heals_and_retries_the_identical_request_without_a_fence() {
    let journal = Journal::new();
    let resumes = std::sync::atomic::AtomicUsize::new(0);
    let (address, calls, server) = scripted_serve(move |method, params| match method {
        "session.resume" => {
            let number = resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(json!({"session_id":format!("runtime-{number}")}))
        }
        "prompt.submit" if params["session_id"] == "runtime-0" => Err((4001, "session not found")),
        "prompt.submit" => Ok(json!({"accepted":true,"status":"completed"})),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert_eq!(result.unwrap(), StopReason::EndTurn);
    assert_eq!(journal.status(), "completed");
    assert!(!journal.state.scope_blocked(&journal.scope).unwrap());
    assert_eq!(client.runtime("stored").unwrap(), "runtime-1");
    let submits: Vec<Value> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, _)| name == "prompt.submit")
        .map(|(_, params)| params.clone())
        .collect();
    assert_eq!(submits.len(), 2);
    assert_eq!(submits[0]["client_request_id"], "frozen-key");
    assert_eq!(submits[1]["client_request_id"], "frozen-key");
    assert_eq!(submits[0]["text"], submits[1]["text"]);
    server.abort();
}

#[tokio::test]
async fn stale_runtime_on_cancel_heals_and_retries_the_same_request() {
    let resumes = std::sync::atomic::AtomicUsize::new(0);
    let (address, calls, server) = scripted_serve(move |method, params| match method {
        "session.resume" => {
            let number = resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(json!({"session_id":format!("runtime-{number}")}))
        }
        "prompt.cancel" if params["session_id"] == "runtime-0" => Err((4001, "session not found")),
        "prompt.cancel" => Ok(json!({"accepted":true,"status":"cancelled"})),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    let receipt = client.cancel_request("stored", "frozen-key").await.unwrap();
    assert_eq!(receipt["status"], "cancelled");
    assert_eq!(client.runtime("stored").unwrap(), "runtime-1");
    let cancels: Vec<Value> = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, _)| name == "prompt.cancel")
        .map(|(_, params)| params.clone())
        .collect();
    assert_eq!(cancels.len(), 2);
    assert_eq!(cancels[1]["client_request_id"], "frozen-key");
    server.abort();
}

#[tokio::test]
async fn stale_runtime_seen_while_reconciling_a_submit_error_also_heals() {
    let journal = Journal::new();
    let resumes = std::sync::atomic::AtomicUsize::new(0);
    let (address, _, server) = scripted_serve(move |method, params| match method {
        "session.resume" => {
            let number = resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(json!({"session_id":format!("runtime-{number}")}))
        }
        "prompt.submit" if params["session_id"] == "runtime-0" => {
            Err((5071, "receipt unavailable"))
        }
        "prompt.status" if params["session_id"] == "runtime-0" => Err((4001, "session not found")),
        "prompt.submit" => Ok(json!({"accepted":true,"status":"completed"})),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert_eq!(result.unwrap(), StopReason::EndTurn);
    assert_eq!(journal.status(), "completed");
    server.abort();
}

#[tokio::test]
async fn stale_runtime_during_observation_heals_through_the_receipt() {
    let journal = Journal::new();
    let resumes = std::sync::atomic::AtomicUsize::new(0);
    let (address, calls, server) = scripted_serve(move |method, params| match method {
        "session.resume" => {
            let number = resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(json!({"session_id":format!("runtime-{number}")}))
        }
        "prompt.submit" if params["session_id"] == "runtime-0" => {
            Ok(json!({"accepted":true,"status":"running"}))
        }
        "prompt.status" if params["session_id"] == "runtime-0" => Err((4001, "session not found")),
        // The same key finds its durable receipt instead of starting a second turn.
        "prompt.submit" => Ok(json!({"accepted":true,"status":"completed"})),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert_eq!(result.unwrap(), StopReason::EndTurn);
    assert_eq!(journal.status(), "completed");
    assert_eq!(
        methods(&calls),
        [
            "session.resume",
            "prompt.submit",
            "prompt.status",
            "session.resume",
            "prompt.submit"
        ]
    );
    server.abort();
}

#[tokio::test]
async fn stale_runtime_heals_only_once_then_stays_uncertain() {
    let journal = Journal::new();
    let (address, calls, server) = scripted_serve(|method, _| match method {
        "session.resume" => Ok(json!({"session_id":"always-stale"})),
        "prompt.submit" => Err((4001, "session not found")),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(AcpError::SubmissionUncertain(_))));
    assert_eq!(journal.status(), "uncertain");
    assert_eq!(
        methods(&calls),
        [
            "session.resume",
            "prompt.submit",
            "session.resume",
            "prompt.submit"
        ]
    );
    server.abort();
}

#[tokio::test]
async fn missing_row_on_first_attach_is_a_definite_failure() {
    let journal = Journal::new();
    let (address, calls, server) = scripted_serve(|method, _| match method {
        "session.resume" => Err((4007, "session not found")),
        other => panic!("nothing may be submitted without a row: {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(AcpError::RemoteTurnFailed(_))));
    assert_eq!(journal.status(), "failed");
    assert!(!journal.state.scope_blocked(&journal.scope).unwrap());
    assert_eq!(methods(&calls), ["session.resume"]);
    server.abort();
}

#[tokio::test]
async fn missing_row_while_healing_is_a_definite_failure_with_no_retry_loop() {
    let journal = Journal::new();
    let resumes = std::sync::atomic::AtomicUsize::new(0);
    let (address, calls, server) = scripted_serve(move |method, _| match method {
        "session.resume" if resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 => {
            Ok(json!({"session_id":"runtime-0"}))
        }
        "session.resume" => Err((4007, "session not found")),
        "prompt.submit" => Err((4001, "session not found")),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(AcpError::RemoteTurnFailed(_))));
    assert_eq!(journal.status(), "failed");
    assert!(!journal.state.scope_blocked(&journal.scope).unwrap());
    assert_eq!(
        methods(&calls),
        ["session.resume", "prompt.submit", "session.resume"]
    );
    server.abort();
}

#[tokio::test]
async fn transient_resume_refusal_is_not_treated_as_a_missing_row() {
    let journal = Journal::new();
    let (address, _, server) = scripted_serve(|method, _| match method {
        "session.resume" => Err((4007, "session no longer live; retry resume")),
        other => panic!("unexpected call {other}"),
    })
    .await;
    let mut client = ServeClient::connect(config(address)).await.unwrap();
    journal.arm(&mut client);
    let result = client
        .prompt("stored", &["frozen text"], Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(AcpError::SubmissionUncertain(_))));
    assert_eq!(journal.status(), "uncertain");
    server.abort();
}
