//! Recovery against a Serve that does, does not, or only later advertises residency.
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

type Handler = dyn Fn(&str, &Value) -> Result<Value, (i64, &'static str)> + Send + Sync;

fn ready(capabilities: Value) -> Value {
    json!({"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{
        "skin":{},"change_events":true,"capabilities":capabilities
    }}})
}

fn capable() -> Option<Value> {
    Some(ready(json!({"residency":1})))
}

/// `gateway.ready` exactly as a deployed Serve writes it: no capabilities key at all.
fn old_serve() -> Option<Value> {
    Some(
        json!({"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{
            "skin":{},"change_events":true,"heartbeat":true,"replay_epoch":"epoch"
        }}}),
    )
}

struct Serve {
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Serve {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Serve {
    /// A resume succeeds unless `respond` overrides it. Every call is recorded in order.
    async fn start(greeting: Option<Value>, respond: Arc<Handler>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                if let Some(frame) = &greeting {
                    socket
                        .send(Message::Text(frame.to_string().into()))
                        .await
                        .unwrap();
                }
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
            }
        });
        Self {
            calls,
            address,
            task,
        }
    }

    fn config(&self) -> ServeConfig {
        ServeConfig {
            url: format!("ws://{}/api/ws", self.address),
            profile: "isolated".into(),
            token: None,
            credentials: None,
            model: None,
            effort: None,
        }
    }

    fn count(&self, method: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == method)
            .count()
    }

    fn resumed(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == "session.resume")
            .map(|(_, params)| params["session_id"].as_str().unwrap().to_owned())
            .collect()
    }
}

/// Serve with nothing to report; a test overrides only what it is about.
fn quiet(method: &str, params: &Value) -> Result<Value, (i64, &'static str)> {
    Ok(match method {
        "session.client_hello" => json!({"pins_sessions":false}),
        "session.resume" => json!({"session_id":format!(
            "{}-runtime", params["session_id"].as_str().unwrap()
        )}),
        "session.active_list" | "session.durable_work" => json!({"sessions":[]}),
        "prompt.status" => json!({
            "accepted":true,"status":"completed",
            "client_request_id":params["client_request_id"]
        }),
        other => panic!("recovery must not call {other}"),
    })
}

fn bridge() -> (BridgeState, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("buzz-narrow-{}.db", uuid::Uuid::new_v4()));
    let state = BridgeState::open(&path, "ws://isolated.invalid", &"1".repeat(64)).unwrap();
    (state, path)
}

fn thread(tag: char) -> SessionScope {
    SessionScope::Thread {
        channel_id: uuid::Uuid::nil(),
        root_event_id: tag.to_string().repeat(64),
    }
}

/// Six bindings, of which only `queued` and `fenced` have local work.
fn six_bindings(state: &BridgeState) -> (SessionScope, SessionScope) {
    for (tag, stored) in [
        ('a', "idle-1"),
        ('b', "idle-2"),
        ('c', "working"),
        ('d', "durable"),
    ] {
        state.bind_if_absent(&thread(tag), stored).unwrap();
    }
    let (queued, fenced) = (thread('e'), thread('f'));
    state.bind_if_absent(&queued, "queued").unwrap();
    state.bind_if_absent(&fenced, "fenced").unwrap();
    state.accept_event(&queued, "waiting", &json!({})).unwrap();
    state.accept_event(&fenced, "unsure", &json!({})).unwrap();
    state
        .prepare_submission(
            &["unsure".into()],
            "unsure-key",
            &json!({"stored_session_id":"fenced","client_request_id":"unsure-key","text":"t"}),
        )
        .unwrap();
    state.finish_batch(&["unsure".into()], "uncertain").unwrap();
    assert!(state.scope_blocked(&fenced).unwrap());
    (queued, fenced)
}

async fn run(serve: &Serve, state: &BridgeState) -> anyhow::Result<()> {
    let mut client = ServeClient::connect(serve.config()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), reconcile(&mut client, state))
        .await
        .unwrap()
}

#[tokio::test]
async fn narrowed_pass_skips_idle_bindings_and_still_clears_an_uncertain_fence() {
    let (state, path) = bridge();
    let (_queued, fenced) = six_bindings(&state);
    let serve = Serve::start(capable(), Arc::new(quiet)).await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.resumed(), ["fenced", "queued"]);
    assert_eq!(state.inputs(&fenced, None).unwrap()[0].status, "completed");
    assert!(!state.scope_blocked(&fenced).unwrap());
    assert_eq!(serve.count("session.client_hello"), 1);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn durable_work_and_working_rows_are_attached_by_exact_stored_id() {
    let (state, path) = bridge();
    six_bindings(&state);
    let serve = Serve::start(
        capable(),
        Arc::new(|method: &str, params: &Value| match method {
            "session.active_list" => {
                assert_eq!(params["working_only"], true);
                assert_eq!(params["profile"], "isolated");
                Ok(json!({"sessions":[
                    {"id":"r1","status":"working","session_key":"x","conversation_root":"working"},
                    {"id":"r2","status":"waiting","session_key":"idle-1","conversation_root":"unbound"},
                    {"id":"r3","status":"working","session_key":"idle-2"}
                ]}))
            }
            "session.durable_work" => {
                assert_eq!(params["profile"], "isolated");
                Ok(json!({"sessions":[
                    {"conversation_root":"durable","reasons":["delegation"]},
                    {"conversation_root":"never-bound","reasons":["loop"]}
                ]}))
            }
            _ => quiet(method, params),
        }),
    )
    .await;
    run(&serve, &state).await.unwrap();
    assert_eq!(
        serve.resumed(),
        ["fenced", "queued", "working", "durable"],
        "local work first; a session_key match alone must not attach an idle binding"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn input_arriving_mid_pass_jumps_the_queue_on_a_narrowed_pass() {
    let (state, path) = bridge();
    let (live, addressed) = (thread('a'), thread('b'));
    state.bind_if_absent(&live, "live").unwrap();
    state.bind_if_absent(&addressed, "addressed").unwrap();
    state.bind_if_absent(&thread('c'), "idle").unwrap();
    state.accept_event(&live, "waiting", &json!({})).unwrap();
    let (server_state, server_scope) = (state.clone(), addressed.clone());
    let serve = Serve::start(
        capable(),
        Arc::new(move |method: &str, params: &Value| {
            if method == "session.resume" {
                server_state
                    .accept_event(&server_scope, "arrived", &json!({}))
                    .unwrap();
            }
            quiet(method, params)
        }),
    )
    .await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.resumed(), ["live", "addressed"]);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn old_serve_keeps_attach_all_and_receives_no_new_calls() {
    let (state, path) = bridge();
    six_bindings(&state);
    let serve = Serve::start(old_serve(), Arc::new(quiet)).await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.resumed().len(), 6);
    assert_eq!(serve.count("session.client_hello"), 0);
    assert_eq!(serve.count("session.durable_work"), 0);
    assert_eq!(serve.count("session.active_list"), 0);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn serve_that_never_sends_ready_keeps_attach_all() {
    let (state, path) = bridge();
    six_bindings(&state);
    let serve = Serve::start(None, Arc::new(quiet)).await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.resumed().len(), 6);
    assert_eq!(serve.count("session.client_hello"), 0);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn late_ready_frame_stops_an_attach_all_pass_after_one_attach() {
    let (state, path) = bridge();
    six_bindings(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let resumes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = resumes.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            let method = call["method"].as_str().unwrap();
            if method == "session.resume" {
                // Serve writes the ready frame before any RPC response, however late.
                if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    socket
                        .send(Message::Text(capable().unwrap().to_string().into()))
                        .await
                        .unwrap();
                }
            }
            let result = quiet(method, &call["params"]).unwrap();
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
    assert_eq!(
        client.residency(),
        crate::hermes_serve::ServeResidency::Unknown
    );
    reconcile(&mut client, &state).await.unwrap();
    assert_eq!(resumes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(client.residency().is_supported());
    server.abort();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn failed_durable_work_never_attaches_all_and_still_serves_local_work() {
    let (state, path) = bridge();
    let (_queued, fenced) = six_bindings(&state);
    let serve = Serve::start(
        capable(),
        Arc::new(|method: &str, params: &Value| match method {
            "session.durable_work" => Err((5000, "durable work unavailable")),
            _ => quiet(method, params),
        }),
    )
    .await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.resumed(), ["fenced", "queued"]);
    assert!(!state.scope_blocked(&fenced).unwrap());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn failed_or_malformed_inventory_never_attaches_all() {
    for bad_inventory in [Err((5000, "inventory unavailable")), Ok(json!({"nope":1}))] {
        let (state, path) = bridge();
        six_bindings(&state);
        let serve = Serve::start(
            capable(),
            Arc::new(move |method: &str, params: &Value| match method {
                "session.active_list" => bad_inventory.clone(),
                _ => quiet(method, params),
            }),
        )
        .await;
        run(&serve, &state).await.unwrap();
        assert_eq!(serve.resumed(), ["fenced", "queued"]);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn lost_connection_during_narrowing_ends_the_pass_for_reconnect() {
    let (state, path) = bridge();
    six_bindings(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(capable().unwrap().to_string().into()))
            .await
            .unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let call: Value = serde_json::from_str(&text).unwrap();
            if call["method"] == "session.active_list" {
                socket.close(None).await.unwrap();
                break;
            }
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
    let outcome = reconcile(&mut client, &state).await;
    assert!(matches!(
        outcome.unwrap_err().downcast_ref::<crate::acp::AcpError>(),
        Some(crate::acp::AcpError::ServeUnavailable(_))
    ));
    server.abort();
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn refused_hello_does_not_fail_the_connection_or_narrowing() {
    let (state, path) = bridge();
    let (_queued, fenced) = six_bindings(&state);
    let serve = Serve::start(
        capable(),
        Arc::new(|method: &str, params: &Value| match method {
            "session.client_hello" => Err((-32601, "unknown method")),
            _ => quiet(method, params),
        }),
    )
    .await;
    let mut client = ServeClient::connect(serve.config()).await.unwrap();
    assert!(client.residency().is_supported());
    reconcile(&mut client, &state).await.unwrap();
    assert_eq!(serve.resumed(), ["fenced", "queued"]);
    assert!(!state.scope_blocked(&fenced).unwrap());
    assert_eq!(serve.count("session.client_hello"), 1);
    let first = serve.calls.lock().unwrap()[0].clone();
    assert_eq!(first.0, "session.client_hello", "hello precedes every RPC");
    assert_eq!(first.1["pins_sessions"], false);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn every_new_connection_says_hello_once() {
    let serve = Serve::start(capable(), Arc::new(quiet)).await;
    for expected in 1..=2 {
        let client = ServeClient::connect(serve.config()).await.unwrap();
        client.active_sessions().await.unwrap();
        client.active_sessions().await.unwrap();
        assert_eq!(serve.count("session.client_hello"), expected);
    }
}

#[tokio::test]
async fn gone_row_settles_its_requests_and_stops_being_retried() {
    let (state, path) = bridge();
    let scope = thread('a');
    state.bind_if_absent(&scope, "deleted").unwrap();
    state.accept_event(&scope, "lost", &json!({})).unwrap();
    state
        .prepare_submission(&["lost".into()], "lost-key", &json!({"text":"t"}))
        .unwrap();
    state.finish_batch(&["lost".into()], "uncertain").unwrap();
    state.record_cancel(&scope, "stop").unwrap();
    assert!(state.scope_blocked(&scope).unwrap());
    let serve = Serve::start(
        old_serve(),
        Arc::new(|method: &str, params: &Value| match method {
            "session.resume" => Err((4007, "session not found")),
            _ => quiet(method, params),
        }),
    )
    .await;
    let mut client = ServeClient::connect(serve.config()).await.unwrap();
    reconcile(&mut client, &state).await.unwrap();
    assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "failed");
    assert!(state.pending_controls().unwrap().is_empty());
    assert!(!state.scope_blocked(&scope).unwrap());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn transient_resume_refusal_does_not_settle_requests() {
    let (state, path) = bridge();
    let scope = thread('a');
    state.bind_if_absent(&scope, "racing").unwrap();
    state.accept_event(&scope, "unsure", &json!({})).unwrap();
    state
        .prepare_submission(&["unsure".into()], "unsure-key", &json!({"text":"t"}))
        .unwrap();
    state.finish_batch(&["unsure".into()], "uncertain").unwrap();
    let serve = Serve::start(
        old_serve(),
        Arc::new(|method: &str, params: &Value| match method {
            "session.resume" => Err((4007, "session no longer live; retry resume")),
            _ => quiet(method, params),
        }),
    )
    .await;
    run(&serve, &state).await.unwrap();
    assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "uncertain");
    assert!(state.scope_blocked(&scope).unwrap());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn recovery_heals_a_stale_cached_runtime_and_clears_the_fence() {
    let (state, path) = bridge();
    let scope = thread('a');
    state.bind_if_absent(&scope, "stored").unwrap();
    state.accept_event(&scope, "unsure", &json!({})).unwrap();
    state
        .prepare_submission(&["unsure".into()], "unsure-key", &json!({"text":"t"}))
        .unwrap();
    state.finish_batch(&["unsure".into()], "uncertain").unwrap();
    let generation = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serve = Serve::start(
        old_serve(),
        Arc::new(move |method: &str, params: &Value| {
            use std::sync::atomic::Ordering::SeqCst;
            match method {
                "session.resume" => {
                    let number = generation.fetch_add(1, SeqCst);
                    Ok(json!({"session_id":format!("runtime-{number}")}))
                }
                "prompt.status" if params["session_id"] == "runtime-0" => {
                    Err((4001, "session not found"))
                }
                "prompt.status" => {
                    assert_eq!(params["session_id"], "runtime-1");
                    quiet(method, params)
                }
                _ => quiet(method, params),
            }
        }),
    )
    .await;
    run(&serve, &state).await.unwrap();
    assert_eq!(serve.count("session.resume"), 2);
    assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "completed");
    assert!(!state.scope_blocked(&scope).unwrap());
    let _ = std::fs::remove_file(path);
}
