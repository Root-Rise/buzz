use super::*;
use crate::{bridge_state::BridgeState, hermes_serve::ServeConfig};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

fn event(sid: &str, seq: u64, kind: &str, payload: Value) -> Value {
    json!({"session_id":sid,"seq":seq,"type":kind,"payload":payload})
}
fn scope(root: &str) -> SessionScope {
    SessionScope::Thread {
        channel_id: uuid::Uuid::nil(),
        root_event_id: root.repeat(64),
    }
}
fn replay(events: Vec<Value>, latest: u64) -> Value {
    json!({"epoch":"epoch","events":events,"count":latest,"latest_seq":latest,"truncated":false})
}

#[test]
fn replay_overlap_reset_and_same_channel_threads_keep_content_and_identity() {
    let output = ObserverHandle::in_process();
    let mut projector = ServeObserver::new(output.clone());
    projector
        .begin("runtime-a", "stored-a", &scope("a"))
        .unwrap();
    let start = event("runtime-a", 1, "message.start", json!({}));
    let first = event("runtime-a", 2, "message.delta", json!({"text":"hello "}));
    let second = event("runtime-a", 3, "message.delta", json!({"text":"world"}));
    projector.frame(&json!({"method":"event","params":second}));
    projector.finish(
        "runtime-a",
        replay(vec![start, first, second.clone()], 3),
        false,
    );
    projector.frame(&json!({"method":"event","params":second}));
    projector.frame(&json!({"method":"event","params":event("runtime-a",4,"message.complete",json!({"text":"hello world","status":"complete"}))}));
    projector
        .begin("runtime-b", "stored-b", &scope("b"))
        .unwrap();
    projector.finish("runtime-b", replay(vec![], 0), false);
    for sid in ["runtime-a", "runtime-b"] {
        if sid == "runtime-b" {
            projector.finish(
                sid,
                replay(vec![event(sid, 4, "message.start", json!({}))], 4),
                false,
            );
        }
        projector.frame(&json!({"method":"event","params":event(sid,5,"tool.start",json!({"tool_id":"same-id","name":"terminal","args":{"command":"true"}}))}));
        projector.frame(&json!({"method":"event","params":event(sid,6,"tool.complete",json!({"tool_id":"same-id","name":"terminal","result":{"output":"x".repeat(50_000),"exit_code":if sid == "runtime-a" {1} else {0}}}))}));
    }
    let events = output.snapshot();
    assert!(events
        .iter()
        .any(|e| e.payload["params"]["update"]["status"] == "failed"
            && e.session_id.as_deref() == Some("stored-a")));
    assert!(events
        .iter()
        .any(|e| e.payload["params"]["update"]["status"] == "completed"
            && e.session_id.as_deref() == Some("stored-b")));
    let text: String = events
        .iter()
        .filter(|e| e.payload["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|e| e.payload["params"]["update"]["content"]["text"].as_str())
        .collect();
    assert_eq!(text, "hello world");
    let ids: std::collections::HashSet<_> = events
        .iter()
        .filter(|e| e.payload["params"]["update"]["sessionUpdate"] == "tool_call")
        .map(|e| {
            e.payload["params"]["update"]["toolCallId"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        ids.len(),
        2,
        "tool IDs must be unique across threads in the same channel"
    );
    assert!(events.iter().all(|e| e.payload.to_string().len() < 20_000));
    let before = events.len();
    projector.frame(&json!({"method":"event","params":event("foreign",1,"message.delta",json!({"text":"must not leak"}))}));
    assert_eq!(output.snapshot().len(), before);
    // A delayed duplicate must not masquerade as a counter reset.
    projector.frame(&json!({"method":"event","params":event("runtime-a",2,"message.delta",json!({"text":"hello "}))}));
    assert_eq!(output.snapshot().len(), before);
    // Counter eviction resets seq within the same server epoch, proven by replay.
    projector.frame(&json!({"method":"event","params":event("runtime-a",1,"message.delta",json!({"text":"after reset"}))}));
    assert!(projector.needs_replay("runtime-a"));
    let reset = replay(
        vec![event(
            "runtime-a",
            1,
            "message.delta",
            json!({"text":"after reset"}),
        )],
        1,
    );
    assert!(projector.needs_reset("runtime-a", &reset));
    projector.finish("runtime-a", reset, true);
    assert!(output
        .snapshot()
        .iter()
        .any(|e| e.payload["params"]["update"]["content"]["text"] == "after reset"));
    assert!(output
        .snapshot()
        .iter()
        .any(|e| e.payload["status"] == "observation_notice"));
    projector
        .begin("runtime-a", "stored-a", &scope("a"))
        .unwrap();
    assert!(projector.needs_reset("runtime-a", &json!({"epoch":"new","latest_seq":0})));
}

#[tokio::test]
async fn recovery_wire_delivers_autonomous_activity_to_existing_buzz_transcript_and_batcher() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let frames = vec![
        event("runtime", 1, "message.start", json!({})),
        event("runtime", 2, "message.delta", json!({"text":"Checking "})),
        event("runtime", 3, "message.delta", json!({"text":"the tests."})),
        event(
            "runtime",
            4,
            "message.interim",
            json!({"text":"Checking the tests.","already_streamed":true}),
        ),
        event(
            "runtime",
            5,
            "tool.start",
            json!({"tool_id":"call-1","name":"terminal","args":{"command":"printf synthetic"}}),
        ),
        event(
            "runtime",
            6,
            "tool.complete",
            json!({"tool_id":"call-1","name":"terminal","result":{"output":"synthetic","exit_code":0}}),
        ),
        event(
            "runtime",
            7,
            "message.complete",
            json!({"text":"Checks passed.","status":"complete"}),
        ),
    ];
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let frames = frames.clone();
            tokio::spawn(async move {
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                while let Some(Ok(Message::Text(text))) = socket.next().await {
                    let call: Value = serde_json::from_str(&text).unwrap();
                    let result = match call["method"].as_str().unwrap() {
                        "session.active_list" => json!({"sessions":[]}),
                        "session.resume" => json!({"session_id":"runtime"}),
                        "session.events.since" => {
                            // A live completion races the replay response and must not skip earlier detail.
                            socket
                                .send(Message::Text(
                                    json!({"method":"event","params":frames[6]})
                                        .to_string()
                                        .into(),
                                ))
                                .await
                                .unwrap();
                            replay(frames.clone(), 7)
                        }
                        other => panic!("Observation must not submit work: {other}"),
                    };
                    socket
                        .send(Message::Text(
                            json!({"id":call["id"],"result":result}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                }
            });
        }
    });
    let config = ServeConfig {
        url: format!("ws://{address}"),
        profile: "isolated".into(),
        token: None,
        credentials: None,
        model: None,
        effort: None,
    };
    let output = ObserverHandle::in_process();
    let temp = std::env::temp_dir().join(format!("buzz-native-observer-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp).unwrap();
    let state = BridgeState::open(
        &temp.join("isolated.db"),
        "ws://isolated.invalid",
        &"a".repeat(64),
    )
    .unwrap();
    state.bind_if_absent(&scope("a"), "stored").unwrap();
    let monitor =
        crate::serve_activity::ActivityMonitor::start(config.clone(), state.clone(), None);
    let recovery = crate::serve_recovery::start(
        config,
        state.clone(),
        monitor.handle(),
        Some(output.clone()),
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if output
                .snapshot()
                .iter()
                .any(|e| e.payload["params"]["update"]["content"]["text"] == "Checks passed.")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let events = output.snapshot();
    assert!(events
        .iter()
        .all(|e| e.session_id.as_deref() == Some("stored")));
    assert_eq!(events.iter().filter(|e| e.kind == "acp_read").count(), 5);
    let mut queue = crate::ObserverPublishQueue::default();
    for event in events {
        queue.ingest(event);
    }
    // Test output is also consumed by the actual TS transcript reducer in the cross-language check.
    let mut batched = vec![];
    while let Some(frame) = queue.next_frame() {
        batched.push(frame);
    }
    assert_eq!(queue.dropped_events, 0);
    if let Ok(path) = std::env::var("BUZZ_OBSERVER_TEST_CAPTURE") {
        std::fs::write(path, serde_json::to_vec(&batched).unwrap()).unwrap();
    }
    let serialized = serde_json::to_value(&batched).unwrap();
    let body = serialized.to_string();
    assert!(
        body.contains("Checking the tests."),
        "adjacent deltas must coalesce"
    );
    assert!(
        !body.contains("turn_started"),
        "historical replay must not create live activity"
    );
    drop(recovery);
    drop(monitor);
    server.abort();
    drop(state);
    std::fs::remove_dir_all(temp).unwrap();
}
