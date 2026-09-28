use super::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

struct Fixture {
    state: BridgeState,
    config: ServeConfig,
    rows: Arc<Mutex<Value>>,
    disconnect: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
    path: std::path::PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rows = Arc::new(Mutex::new(json!({"sessions":[]})));
        let disconnect = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(AtomicUsize::new(0));
        let (inventory, cut, connected) = (rows.clone(), disconnect.clone(), connections.clone());
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(socket).await.unwrap();
                connected.fetch_add(1, Ordering::SeqCst);
                while let Some(Ok(Message::Text(raw))) = socket.next().await {
                    let call: Value = serde_json::from_str(&raw).unwrap();
                    // Observation must not resume/build a session or submit a model turn.
                    assert_eq!(call["method"], "session.active_list");
                    assert_eq!(call["params"]["profile"], "activity-test");
                    if cut.swap(false, Ordering::SeqCst) {
                        break;
                    }
                    let result = inventory.lock().unwrap().clone();
                    socket
                        .send(Message::Text(
                            json!({"jsonrpc":"2.0","id":call["id"],"result":result})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
            }
        });
        let path =
            std::env::temp_dir().join(format!("buzz-activity-{}.sqlite", uuid::Uuid::new_v4()));
        let state = BridgeState::open(&path, "https://activity.invalid", &"a".repeat(64)).unwrap();
        Self {
            state,
            rows,
            disconnect,
            connections,
            server,
            path,
            config: ServeConfig {
                url: format!("ws://{address}"),
                profile: "activity-test".into(),
                token: None,
                credentials: None,
                model: None,
                effort: None,
            },
        }
    }
    fn bind(&self, root: &str, stored: &str) -> SessionScope {
        let scope = SessionScope::Thread {
            channel_id: uuid::Uuid::nil(),
            root_event_id: root.repeat(64),
        };
        self.state.bind_if_absent(&scope, stored).unwrap();
        scope
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("activity observation should converge without any inbound request");
}

#[tokio::test]
async fn autonomous_threads_follow_server_inventory_with_exact_profile_and_binding_ownership() {
    let fixture = Fixture::new().await;
    let first = fixture.bind("1", "stored-a");
    let second = fixture.bind("2", "stored-b");
    let foreign = fixture.bind("3", "same-stored-id-in-other-profile");
    let observer = ObserverHandle::in_process();
    let monitor = ActivityMonitor::start(
        fixture.config.clone(),
        fixture.state.clone(),
        Some(observer.clone()),
    );
    monitor.handle().replace_attachments(HashMap::from([
        ("stored-a".into(), "runtime-a".into()),
        ("stored-b".into(), "runtime-b".into()),
        (
            "same-stored-id-in-other-profile".into(),
            "our-idle-runtime".into(),
        ),
    ]));
    *fixture.rows.lock().unwrap() = json!({"sessions":[
        {"id":"runtime-a","session_key":"stored-a","status":"working"},
        {"id":"runtime-b","session_key":"compressed-child-b","status":"working"},
        {"id":"foreign-runtime","session_key":"same-stored-id-in-other-profile","status":"working"},
        {"id":"our-idle-runtime","session_key":"same-stored-id-in-other-profile","status":"idle"}
    ]});
    until(|| monitor.working_scopes().len() == 2).await;
    assert!(!monitor.working_scopes().contains(&foreign));
    assert!(fixture.state.queued_inputs().unwrap().is_empty());
    // No bridge prompt exists; both turns belong wholly to the server.
    assert_eq!(
        observer
            .snapshot()
            .iter()
            .filter(|e| e.kind == "turn_started")
            .count(),
        2
    );
    fixture.rows.lock().unwrap()["sessions"][0]["status"] = json!("idle");
    until(|| monitor.working_scopes() == vec![second.clone()]).await;
    let events = observer.snapshot();
    assert!(events
        .iter()
        .any(|e| e.kind == "turn_completed" && e.session_id.as_deref() == Some("stored-a")));
    assert!(!events
        .iter()
        .any(|e| e.kind == "turn_completed" && e.session_id.as_deref() == Some("stored-b")));
    // Rebinding a route cannot retain the previous runtime's working claim.
    assert!(fixture.state.retire(&second, "stored-b").unwrap());
    fixture
        .state
        .bind_if_absent(&second, "replacement-stored-b")
        .unwrap();
    until(|| monitor.working_scopes().is_empty()).await;
    assert!(observer.snapshot().iter().any(|e| e.kind == "acp_read"
        && e.payload["status"] == "observation_unavailable"
        && e.session_id.as_deref() == Some("stored-b")));
    assert!(!monitor.working_scopes().contains(&first));
}

#[tokio::test]
async fn disconnect_expires_activity_and_reconnect_recovers_without_replaying_requests() {
    let fixture = Fixture::new().await;
    let scope = fixture.bind("4", "stored");
    fixture
        .state
        .accept_event(&scope, "queued", &json!({"text":"not yet admitted"}))
        .unwrap();
    assert_eq!(fixture.state.unfinished_counts().unwrap()["queued"], 1);
    let observer = ObserverHandle::in_process();
    let monitor = ActivityMonitor::start(
        fixture.config.clone(),
        fixture.state.clone(),
        Some(observer.clone()),
    );
    monitor
        .handle()
        .replace_attachments(HashMap::from([("stored".into(), "runtime".into())]));
    *fixture.rows.lock().unwrap() = json!({"sessions":[{"id":"runtime","status":"idle"}]});
    until(|| monitor.handle.inner.lock().unwrap().refreshed.is_some()).await;
    assert!(
        monitor.working_scopes().is_empty(),
        "queued input is not model activity"
    );
    fixture.rows.lock().unwrap()["sessions"][0]["status"] = json!("working");
    until(|| monitor.working_scopes() == vec![scope.clone()]).await;
    fixture.disconnect.store(true, Ordering::SeqCst);
    until(|| monitor.working_scopes().is_empty()).await;
    assert!(observer
        .snapshot()
        .iter()
        .any(|e| e.kind == "acp_read" && e.payload["status"] == "observation_unavailable"));
    assert!(!observer
        .snapshot()
        .iter()
        .any(|e| e.kind == "turn_completed" && e.payload["outcome"] != "unknown"));
    until(|| {
        fixture.connections.load(Ordering::SeqCst) >= 2
            && monitor.working_scopes() == vec![scope.clone()]
    })
    .await;
    // Even a last-known-good snapshot expires if the observer task stops progressing.
    monitor
        .handle()
        .refresh(
            &fixture.rows.lock().unwrap(),
            fixture.state.all_bindings().unwrap(),
            Instant::now() - MAX_AGE - Duration::from_secs(1),
        )
        .unwrap();
    assert!(monitor.working_scopes().is_empty());
    assert_eq!(fixture.state.unfinished_counts().unwrap()["queued"], 1);
}
