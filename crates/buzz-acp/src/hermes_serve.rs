//! Hermes Serve connection. A socket owns no agent process and never terminates server work.

use crate::{
    acp::{AcpError, McpServer, SessionNewResponse, StopReason},
    observer::{ObserverContext, ObserverHandle},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;

/// Configuration for a profile on the existing shared Serve endpoint.
#[derive(Clone)]
pub struct ServeConfig {
    pub url: String,
    pub profile: String,
    pub token: Option<String>,
    pub credentials: Option<Arc<crate::serve_auth::ServeCredentials>>,
    pub model: Option<String>,
    pub effort: Option<String>,
}

impl std::fmt::Debug for ServeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeConfig")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct PreparedRequest {
    pub state: crate::bridge_state::BridgeState,
    pub scope: crate::scope::SessionScope,
    pub event_ids: Vec<String>,
}

impl PreparedRequest {
    fn record(&self, stored: &str, receipt: &Value) -> Result<(), AcpError> {
        let accepted = receipt["accepted"].as_bool() == Some(true);
        if accepted {
            self.state
                .mark_accepted_batch(&self.event_ids)
                .map_err(journal_error)?;
            if receipt["status"] == "completed" {
                self.state
                    .mark_primed(&self.scope, stored)
                    .map_err(journal_error)?;
            }
        }
        let terminal = match receipt["status"].as_str() {
            Some(status @ ("completed" | "failed" | "cancelled" | "interrupted")) => Some(status),
            Some("unknown") => Some("uncertain"),
            _ => None,
        };
        if let Some(status) = terminal {
            self.finish(status)?;
        }
        Ok(())
    }
    fn finish(&self, status: &str) -> Result<(), AcpError> {
        self.state
            .finish_batch(&self.event_ids, status)
            .map_err(journal_error)?;
        Ok(())
    }
}
fn journal_error(error: anyhow::Error) -> AcpError {
    AcpError::SubmissionUncertain(format!("durable bridge receipt failed: {error}"))
}

/// How long a new connection waits for `gateway.ready` before its first RPC. Serve writes that
/// frame before it reads any RPC, so an RPC response can never overtake it: a frame still absent
/// when this expires means a stalled Serve or one that never sends it, not a lost race.
const READY_WAIT: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(5)
};

/// What this connection's `gateway.ready` frame said about memory-residency support.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServeResidency {
    /// No frame yet. Acted on as `Unsupported`; a later frame still upgrades the connection.
    Unknown,
    Unsupported,
    Supported,
}

impl ServeResidency {
    pub(crate) fn is_supported(self) -> bool {
        self == Self::Supported
    }
}

fn ready_residency(frame: &Value) -> Option<ServeResidency> {
    if frame["method"] != "event" || frame["params"]["type"] != "gateway.ready" {
        return None;
    }
    let advertised = frame["params"]["payload"]["capabilities"]["residency"]
        .as_u64()
        .is_some_and(|version| version >= 1);
    Some(if advertised {
        ServeResidency::Supported
    } else {
        ServeResidency::Unsupported
    })
}

/// 4001: this runtime ID is not in Serve's memory, so nothing was admitted on it.
fn runtime_is_stale(error: &AcpError) -> bool {
    matches!(error, AcpError::AgentError { code: 4001, .. })
}

/// 4007 "session not found": the stored row no longer exists. The same code also carries
/// "session no longer live; retry resume", a transient reattach race that proves nothing.
pub(crate) fn stored_row_is_gone(error: &AcpError) -> bool {
    matches!(error, AcpError::AgentError { code: 4007, message } if message.contains("not found"))
}

/// A request could not be placed on a runtime: failed only if the row is provably gone.
fn attach_failure(error: AcpError) -> AcpError {
    if stored_row_is_gone(&error) {
        AcpError::RemoteTurnFailed(error.to_string())
    } else {
        AcpError::SubmissionUncertain(error.to_string())
    }
}

type Reply = oneshot::Sender<Result<Value, AcpError>>;
struct Call {
    method: String,
    params: Value,
    reply: Reply,
}
#[derive(Default)]
struct Observation {
    handle: Option<ObserverHandle>,
    index: Option<usize>,
    context: ObserverContext,
    details: Option<Arc<Mutex<crate::serve_observer::ServeObserver>>>,
}

/// Connection handle with a continuous read pump, including while its pool slot is idle.
pub struct ServeClient {
    config: ServeConfig,
    calls: mpsc::Sender<Call>,
    pump: tokio::task::JoinHandle<()>,
    observation: Arc<Mutex<Observation>>,
    residency: watch::Receiver<ServeResidency>,
    sessions: HashMap<String, String>,
    observed_sessions: HashSet<String>,
    pub(crate) request_key: Option<String>,
    in_flight: Option<(String, String)>,
    pub(crate) prepared: Option<PreparedRequest>,
}

impl Drop for ServeClient {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

impl ServeClient {
    /// Connect without starting or taking custody of a local child process.
    pub async fn connect(config: ServeConfig) -> Result<Self, AcpError> {
        let mut url = url::Url::parse(&config.url)
            .map_err(|_| AcpError::ServeUnavailable("invalid Serve URL".into()))?;
        if let Some(credentials) = &config.credentials {
            let ticket = credentials.ticket(&url).await?;
            url.query_pairs_mut().append_pair("ticket", &ticket);
        } else if let Some(token) = &config.token {
            url.query_pairs_mut().append_pair("token", token);
        }
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(30),
            tokio_tungstenite::connect_async_with_config(
                url.as_str(),
                Some(
                    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                        .max_message_size(Some(10_000_000))
                        .max_frame_size(Some(10_000_000)),
                ),
                false,
            ),
        )
        .await
        .map_err(|_| AcpError::ServeUnavailable("Serve connect deadline exceeded".into()))?
        .map_err(|_| AcpError::ServeUnavailable("Serve WebSocket connection failed".into()))?;
        let (mut sink, mut stream) = socket.split();
        let (calls, mut rx) = mpsc::channel::<Call>(32);
        let observation = Arc::new(Mutex::new(Observation::default()));
        let observed = observation.clone();
        let (advertise, residency) = watch::channel(ServeResidency::Unknown);
        let profile = config.profile.clone();
        let pump = tokio::spawn(async move {
            let mut pending: HashMap<u64, Reply> = HashMap::new();
            let mut next_id = 1u64;
            let mut hello_id = None;
            loop {
                tokio::select! {
                    call = rx.recv() => {
                        let Some(call) = call else { break };
                        pending.retain(|_, reply| !reply.is_closed());
                        let id = next_id; next_id += 1;
                        let message = json!({"jsonrpc":"2.0", "id":id, "method":call.method, "params":call.params});
                        pending.insert(id, call.reply);
                        if !matches!(tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Text(message.to_string().into()))).await, Ok(Ok(()))) { break; }
                    }
                    message = stream.next() => {
                        let Some(Ok(message)) = message else { break };
                        match message {
                            Message::Text(text) => {
                                // Serve coalesces JSON-RPC frames with newline separators.
                                for line in text.lines().filter(|line| !line.trim().is_empty()) {
                                    let Ok(frame) = serde_json::from_str::<Value>(line) else { continue };
                                    if let Some(capability) = ready_residency(&frame) {
                                        // Written here so it precedes every RPC issued after connect,
                                        // on every socket. No caller waits on it: a refused hello only
                                        // leaves this socket pinning sessions, as an old bridge's would.
                                        if capability.is_supported() && hello_id.is_none() {
                                            let id = next_id; next_id += 1;
                                            hello_id = Some(id);
                                            let hello = json!({"jsonrpc":"2.0", "id":id, "method":"session.client_hello", "params":{"pins_sessions":false, "profile":profile}});
                                            if !matches!(tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Text(hello.to_string().into()))).await, Ok(Ok(()))) {
                                                tracing::warn!("Serve client hello could not be written");
                                            }
                                        }
                                        let _ = advertise.send(capability);
                                        continue;
                                    }
                                    if let Some(id) = frame.get("id").and_then(Value::as_u64) {
                                        if hello_id == Some(id) {
                                            if let Some(error) = frame.get("error") {
                                                tracing::warn!(%error, "Serve refused client hello; this socket keeps pinning sessions");
                                            }
                                            continue;
                                        }
                                        if let Some(reply) = pending.remove(&id) {
                                            let result = if let Some(error) = frame.get("error") {
                                                Err(AcpError::AgentError {code:error["code"].as_i64().unwrap_or(-32000), message:error["message"].as_str().unwrap_or("Serve rejected request").to_owned()})
                                            } else { Ok(frame["result"].clone()) };
                                            let _ = reply.send(result);
                                        }
                                    } else if let Ok(obs) = observed.lock() {
                                        // Only the profile recovery observer publishes details.
                                        // Pool sockets can be attached to many unrelated threads.
                                        if let Some(details) = &obs.details {
                                            if let Ok(mut details) = details.lock() { details.frame(&frame); }
                                        }
                                    }
                                }
                            }
                            Message::Ping(data) => {
                                match tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Pong(data))).await {
                                    Ok(Ok(())) => {},
                                    _ => break,
                                }
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                }
            }
            for (_, reply) in pending {
                let _ = reply.send(Err(AcpError::ServeUnavailable(
                    "Serve socket disconnected; server work may still be running".into(),
                )));
            }
        });
        // Known before the first RPC on every path that matters. A timeout is tolerated: recovery
        // re-reads the capability before each attach, so a late frame still stops attach-all.
        let mut ready = residency.clone();
        let _ = tokio::time::timeout(
            READY_WAIT,
            ready.wait_for(|known| *known != ServeResidency::Unknown),
        )
        .await;
        Ok(Self {
            config,
            calls,
            pump,
            observation,
            residency,
            sessions: HashMap::new(),
            observed_sessions: HashSet::new(),
            request_key: None,
            in_flight: None,
            prepared: None,
        })
    }

    async fn reconnect_if_closed(&mut self) -> Result<bool, AcpError> {
        if !self.pump.is_finished() {
            return Ok(false);
        }
        let mut replacement = Self::connect(self.config.clone()).await?;
        replacement.set_observer(
            self.observer_handle(),
            self.observer_agent_index().unwrap_or_default(),
        );
        if let Ok(obs) = self.observation.lock() {
            replacement.set_observer_context(obs.context.clone());
            replacement.set_activity_observer(obs.details.clone());
        }
        replacement.request_key = self.request_key.take();
        replacement.in_flight = self.in_flight.take();
        replacement.prepared = self.prepared.take();
        *self = replacement;
        Ok(true)
    }

    /// Residency support advertised on the current connection.
    pub(crate) fn residency(&self) -> ServeResidency {
        *self.residency.borrow()
    }

    /// Reconcile one durable request without submitting or interrupting work.
    pub(crate) async fn request_status(
        &mut self,
        stored: &str,
        key: &str,
    ) -> Result<Value, AcpError> {
        self.attached_rpc(stored, "prompt.status", json!({"client_request_id":key}))
            .await
    }

    /// Snapshot for profile-safe activity correlation; transport runtimes are process-unique.
    pub(crate) fn attachment_ids(&self) -> HashMap<String, String> {
        self.sessions.clone()
    }

    /// Observe live state without attaching or starting a turn.
    pub(crate) async fn active_sessions(&self) -> Result<Value, AcpError> {
        self.rpc("session.active_list", json!({})).await
    }

    /// This profile's working or waiting rows, each carrying its `conversation_root`.
    pub(crate) async fn working_sessions(&self) -> Result<Value, AcpError> {
        self.rpc("session.active_list", json!({"working_only":true}))
            .await
    }

    /// Conversations that must be live for work Serve still owes them.
    pub(crate) async fn durable_work(&self) -> Result<Value, AcpError> {
        self.rpc("session.durable_work", json!({})).await
    }

    /// A pool's durable binding can outlive every attachment on this connection.
    pub(crate) async fn ensure_attached(&mut self, stored: &str) -> Result<(), AcpError> {
        self.reconnect_if_closed().await?;
        if !self.sessions.contains_key(stored) {
            self.session_load(stored, vec![]).await?;
        }
        Ok(())
    }

    /// Drop a runtime Serve no longer holds, so the stored row gets a fresh attachment.
    async fn reattach(&mut self, stored: &str) -> Result<(), AcpError> {
        tracing::warn!(session = stored, "Serve runtime is stale; reattaching");
        self.sessions.remove(stored);
        self.observed_sessions.remove(stored);
        self.session_load(stored, vec![]).await
    }

    /// Issue one receipt RPC, healing a stale cached runtime once. Receipts make the repeat safe.
    async fn attached_rpc(
        &mut self,
        stored: &str,
        method: &str,
        mut params: Value,
    ) -> Result<Value, AcpError> {
        self.ensure_attached(stored).await?;
        params["session_id"] = json!(self.runtime(stored)?);
        match self.rpc(method, params.clone()).await {
            Err(error) if runtime_is_stale(&error) => {
                self.reattach(stored).await?;
                params["session_id"] = json!(self.runtime(stored)?);
                self.rpc(method, params).await
            }
            result => result,
        }
    }

    /// Retry one already-journaled request; return admission, never wait for its model turn.
    pub(crate) async fn submit_frozen(
        &mut self,
        stored: &str,
        key: &str,
        text: &str,
    ) -> Result<Value, AcpError> {
        self.attached_rpc(
            stored,
            "prompt.submit",
            json!({"text":text,"queued":true,"client_request_id":key}),
        )
        .await
    }

    async fn rpc(&self, method: &str, mut params: Value) -> Result<Value, AcpError> {
        params["profile"] = json!(self.config.profile);
        let (reply, result) = oneshot::channel();
        self.calls
            .send(Call {
                method: method.into(),
                params,
                reply,
            })
            .await
            .map_err(|_| AcpError::ServeUnavailable("Serve connection closed".into()))?;
        tokio::time::timeout(Duration::from_secs(30), result)
            .await
            .map_err(|_| {
                self.pump.abort();
                AcpError::ServeUnavailable(
                    "Serve RPC deadline exceeded; request outcome unknown".into(),
                )
            })?
            .map_err(|_| AcpError::ServeUnavailable("Serve connection closed".into()))?
    }

    pub(crate) fn set_observer(&mut self, handle: Option<ObserverHandle>, index: usize) {
        if let Ok(mut obs) = self.observation.lock() {
            obs.handle = handle;
            obs.index = Some(index);
        }
    }

    pub(crate) fn set_activity_observer(
        &mut self,
        details: Option<Arc<Mutex<crate::serve_observer::ServeObserver>>>,
    ) {
        if let Ok(mut obs) = self.observation.lock() {
            obs.details = details;
        }
    }

    /// Subscribe to a bound session's activity, replaying only telemetry (never prompts).
    pub(crate) async fn observe_session(
        &mut self,
        stored: &str,
        scope: &crate::scope::SessionScope,
    ) -> anyhow::Result<()> {
        let details = self
            .observation
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve observation lock poisoned"))?
            .details
            .clone();
        let Some(details) = details else {
            return Ok(());
        };
        let runtime = self.runtime(stored)?.to_owned();
        if self.observed_sessions.contains(stored)
            && !details
                .lock()
                .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
                .needs_replay(&runtime)
        {
            return Ok(());
        }
        let resync = details
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
            .needs_replay(&runtime);
        let last = details
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
            .begin(&runtime, stored, scope)?;
        let mut replay = self
            .rpc(
                "session.events.since",
                json!({"session_id":runtime,"last_seen":if resync {0} else {last}}),
            )
            .await?;
        let reset = details
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
            .needs_reset(&runtime, &replay);
        if reset {
            replay = self
                .rpc(
                    "session.events.since",
                    json!({"session_id":runtime,"last_seen":0}),
                )
                .await?;
        }
        anyhow::ensure!(
            replay["events"].is_array() && replay["epoch"].is_string(),
            "Invalid Serve event replay"
        );
        let count = replay["count"].as_u64().unwrap_or_default();
        let truncated = replay["truncated"] == true;
        details
            .lock()
            .map_err(|_| anyhow::anyhow!("Serve activity lock poisoned"))?
            .finish(&runtime, replay, reset);
        self.observed_sessions.insert(stored.to_owned());
        tracing::info!(
            session = stored,
            count,
            truncated,
            reset,
            "Serve activity stream attached"
        );
        Ok(())
    }
    pub(crate) fn set_observer_context(&mut self, context: ObserverContext) {
        if let Ok(mut obs) = self.observation.lock() {
            obs.context = context;
        }
    }
    pub(crate) fn observer_handle(&self) -> Option<ObserverHandle> {
        self.observation
            .lock()
            .ok()
            .and_then(|obs| obs.handle.clone())
    }
    pub(crate) fn observer_agent_index(&self) -> Option<usize> {
        self.observation.lock().ok().and_then(|obs| obs.index)
    }
    pub(crate) fn observe(&self, kind: impl Into<String>, payload: Value) {
        if let Ok(obs) = self.observation.lock() {
            if let Some(handle) = &obs.handle {
                handle.emit(kind, obs.index, &obs.context, payload);
            }
        }
    }
    pub(crate) async fn shutdown(&mut self) {
        self.pump.abort();
    }
    pub(crate) fn has_in_flight_prompt(&self) -> bool {
        self.in_flight.is_some()
    }

    fn runtime(&self, stored: &str) -> Result<&str, AcpError> {
        self.sessions
            .get(stored)
            .map(String::as_str)
            .ok_or_else(|| AcpError::ServeUnavailable("stored session is not attached".into()))
    }

    pub(crate) async fn session_new(
        &mut self,
        cwd: &str,
        mcp: Vec<McpServer>,
        title: Option<&str>,
    ) -> Result<SessionNewResponse, AcpError> {
        if !mcp.is_empty() {
            return Err(AcpError::ServeUnavailable(
                "Configure MCP servers in the Hermes profile, not the Buzz bridge".into(),
            ));
        }
        self.reconnect_if_closed().await?;
        let mut params = json!({"cwd":cwd,"source":"buzz","lifecycle_owner":"server","close_on_disconnect":false,"title":title});
        if let Some(model) = &self.config.model {
            params["model"] = json!(model);
        }
        if let Some(effort) = &self.config.effort {
            params["reasoning_effort"] = json!(effort);
        }
        let raw = self.rpc("session.create", params).await?;
        let stored = raw["stored_session_id"]
            .as_str()
            .ok_or_else(|| AcpError::ServeUnavailable("session.create missing stored ID".into()))?
            .to_owned();
        let runtime = raw["session_id"]
            .as_str()
            .ok_or_else(|| AcpError::ServeUnavailable("session.create missing runtime ID".into()))?
            .to_owned();
        self.sessions.insert(stored.clone(), runtime);
        Ok(SessionNewResponse {
            session_id: stored,
            raw,
        })
    }

    pub(crate) async fn session_load(
        &mut self,
        stored: &str,
        mcp: Vec<McpServer>,
    ) -> Result<(), AcpError> {
        if !mcp.is_empty() {
            return Err(AcpError::ServeUnavailable(
                "Configure MCP servers in Hermes".into(),
            ));
        }
        self.reconnect_if_closed().await?;
        let raw = self.rpc("session.resume",json!({"session_id":stored,"source":"buzz","close_on_disconnect":false,"omit_messages":true})).await?;
        let runtime = raw["session_id"].as_str().ok_or_else(|| {
            AcpError::ServeUnavailable("session.resume missing runtime ID".into())
        })?;
        self.sessions.insert(stored.into(), runtime.into());
        Ok(())
    }

    pub(crate) async fn prompt(
        &mut self,
        stored: &str,
        blocks: &[&str],
        max_duration: Duration,
    ) -> Result<StopReason, AcpError> {
        let key = self.request_key.take().ok_or_else(|| {
            AcpError::ServeUnavailable("durable client request key required".into())
        })?;
        let payload =
            json!({"stored_session_id":stored,"text":blocks.join("\n\n"),"client_request_id":key});
        let payload = if let Some(prepared) = &self.prepared {
            prepared
                .state
                .prepare_submission(&prepared.event_ids, &key, &payload)
                .map_err(journal_error)?
        } else {
            payload
        };
        self.in_flight = Some((stored.into(), key.clone()));
        // Correlation and durable intent precede the first await, so a control
        // arriving during reconnect cannot mistake an unsubmitted turn for success.
        let result = self
            .place_prompt(stored, &key, &payload["text"], max_duration)
            .await;
        self.in_flight = None;
        if let Some(prepared) = &self.prepared {
            match &result {
                Err(AcpError::SubmissionUncertain(_)) => prepared.finish("uncertain")?,
                Err(AcpError::RemoteTurnFailed(_)) => prepared.finish("failed")?,
                _ => {}
            }
        }
        result
    }

    /// Attach and run one frozen request. A runtime that vanished proves non-admission there,
    /// so it heals once with the identical request; the receipt makes the repeat idempotent.
    async fn place_prompt(
        &mut self,
        stored: &str,
        key: &str,
        text: &Value,
        max_duration: Duration,
    ) -> Result<StopReason, AcpError> {
        self.ensure_attached(stored).await.map_err(attach_failure)?;
        let deadline = tokio::time::Instant::now() + max_duration;
        let mut healed = false;
        loop {
            let runtime = self.runtime(stored)?.to_owned();
            match self.submit_and_observe(&runtime, key, text, deadline).await {
                Err(error) if runtime_is_stale(&error) && !healed => {
                    healed = true;
                    self.reattach(stored).await.map_err(attach_failure)?;
                }
                Err(error) if runtime_is_stale(&error) => {
                    return Err(AcpError::SubmissionUncertain(error.to_string()));
                }
                result => return result,
            }
        }
    }

    /// Submit once and follow its receipt. A stale-runtime 4001 is returned raw for the caller.
    async fn submit_and_observe(
        &self,
        runtime: &str,
        key: &str,
        text: &Value,
        deadline: tokio::time::Instant,
    ) -> Result<StopReason, AcpError> {
        let remaining = || deadline.saturating_duration_since(tokio::time::Instant::now());
        let submitted = loop {
            let response = self
                .rpc(
                    "prompt.submit",
                    json!({"session_id":runtime,"text":text,"queued":true,"client_request_id":key}),
                )
                .await;
            match response {
                Err(AcpError::AgentError { code: 4091, .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    // This is an explicit refusal before acceptance. Retry the identical
                    // frozen request rather than re-rendering thread context.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                response => break response,
            }
        };
        match submitted {
            Ok(receipt) => self.wait_receipt(runtime, key, receipt, remaining()).await,
            Err(AcpError::ServeUnavailable(message)) => Err(AcpError::SubmissionUncertain(message)),
            Err(error) if runtime_is_stale(&error) => Err(error),
            Err(error) => {
                // A server error can occur after durable acceptance (for example
                // a failed receipt write). Resolve admission before declaring failure.
                match self
                    .rpc(
                        "prompt.status",
                        json!({"session_id":runtime,"client_request_id":key}),
                    )
                    .await
                {
                    Ok(receipt) if receipt["accepted"] == false => {
                        Err(AcpError::RemoteTurnFailed(error.to_string()))
                    }
                    Ok(receipt) => self.wait_receipt(runtime, key, receipt, remaining()).await,
                    Err(status_error) if runtime_is_stale(&status_error) => Err(status_error),
                    Err(_) => Err(AcpError::SubmissionUncertain(error.to_string())),
                }
            }
        }
    }

    async fn wait_receipt(
        &self,
        runtime: &str,
        key: &str,
        mut receipt: Value,
        deadline: Duration,
    ) -> Result<StopReason, AcpError> {
        let until = tokio::time::Instant::now() + deadline;
        loop {
            if let (Some(prepared), Some((stored, _))) = (&self.prepared, &self.in_flight) {
                prepared.record(stored, &receipt)?;
            }
            match receipt["status"].as_str() {
                Some("completed") => return Ok(StopReason::EndTurn),
                Some("cancelled") => return Ok(StopReason::Cancelled),
                Some("failed" | "interrupted") => {
                    return Err(AcpError::RemoteTurnFailed(receipt.to_string()))
                }
                Some("unknown") => {
                    return Err(AcpError::SubmissionUncertain(
                        "server cannot determine prior request outcome".into(),
                    ))
                }
                Some("queued" | "running" | "streaming" | "accepted") => {}
                _ => {
                    return Err(AcpError::SubmissionUncertain(
                        "unrecognized request receipt".into(),
                    ))
                }
            }
            if tokio::time::Instant::now() >= until {
                return Err(AcpError::SubmissionUncertain(
                    "observation deadline exceeded; server work may still run".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            receipt = self
                .rpc(
                    "prompt.status",
                    json!({"session_id":runtime,"client_request_id":key}),
                )
                .await
                .map_err(|e| {
                    if runtime_is_stale(&e) {
                        e
                    } else {
                        AcpError::SubmissionUncertain(e.to_string())
                    }
                })?;
        }
    }

    /// Cancel a durable request after the bridge lost its original connection.
    pub(crate) async fn cancel_request(
        &mut self,
        stored: &str,
        key: &str,
    ) -> Result<Value, AcpError> {
        self.attached_rpc(stored, "prompt.cancel", json!({"client_request_id":key}))
            .await
    }

    pub(crate) async fn cancel(
        &mut self,
        stored: &str,
        grace: Duration,
    ) -> Result<StopReason, AcpError> {
        if self.reconnect_if_closed().await? {
            self.session_load(stored, vec![]).await?;
        }
        let runtime = self.runtime(stored)?.to_owned();
        let key = self
            .in_flight
            .as_ref()
            .map(|(_, key)| key.clone())
            .ok_or_else(|| AcpError::ServeUnavailable("no tracked request to cancel".into()))?;
        let receipt = self
            .rpc(
                "prompt.cancel",
                json!({"session_id":runtime,"client_request_id":key}),
            )
            .await
            .map_err(|e| AcpError::SubmissionUncertain(e.to_string()))?;
        let result = self
            .wait_receipt(&runtime, &key, receipt, grace)
            .await
            .map_err(|e| match e {
                stale if runtime_is_stale(&stale) => {
                    AcpError::SubmissionUncertain(stale.to_string())
                }
                other => other,
            });
        self.in_flight = None;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn config(address: std::net::SocketAddr) -> ServeConfig {
        ServeConfig {
            url: format!("ws://{address}/api/ws"),
            profile: "isolated-test".into(),
            token: None,
            credentials: None,
            model: None,
            effort: Some("high".into()),
        }
    }

    #[tokio::test]
    async fn admission_and_unrelated_terminal_event_do_not_complete_the_request() {
        assert_request_correlation(false).await;
    }

    #[tokio::test]
    async fn submit_error_after_acceptance_is_reconciled_before_classifying_failure() {
        assert_request_correlation(true).await;
    }

    async fn assert_request_correlation(submit_error: bool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let finish = Arc::new(AtomicBool::new(false));
        let server_finish = finish.clone();
        let (polled, observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut polled = Some(polled);
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let call: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(call["params"]["profile"], "isolated-test");
                let result = match call["method"].as_str().unwrap() {
                    "session.create" => {
                        assert_eq!(call["params"]["reasoning_effort"], "high");
                        assert_eq!(call["params"]["lifecycle_owner"], "server");
                        json!({"session_id":"runtime","stored_session_id":"stored"})
                    }
                    "prompt.submit" => {
                        assert_eq!(call["params"]["session_id"], "runtime");
                        assert_eq!(call["params"]["client_request_id"], "event-key");
                        assert_eq!(call["params"]["queued"], true);
                        socket.send(Message::Text(json!({"method":"turn.complete","params":{"session_id":"runtime","turn_id":"another-client-turn"}}).to_string().into())).await.unwrap();
                        json!({"status":"streaming","accepted":true})
                    }
                    "prompt.status" => {
                        if let Some(polled) = polled.take() {
                            let _ = polled.send(());
                        }
                        json!({"status":if server_finish.load(Ordering::SeqCst) {"completed"} else {"running"},"accepted":true})
                    }
                    method => panic!("unexpected method {method}"),
                };
                let response = if submit_error && call["method"] == "prompt.submit" {
                    json!({"jsonrpc":"2.0","id":call["id"],"error":{"code":5071,"message":"receipt write unavailable"}})
                } else {
                    json!({"jsonrpc":"2.0","id":call["id"],"result":result})
                };
                socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let mut client = ServeClient::connect(config(address)).await.unwrap();
        let session = client.session_new("/tmp", vec![], None).await.unwrap();
        assert_eq!(session.session_id, "stored");
        let path =
            std::env::temp_dir().join(format!("buzz-serve-test-{}.db", uuid::Uuid::new_v4()));
        let state =
            crate::bridge_state::BridgeState::open(&path, "ws://isolated.invalid", &"1".repeat(64))
                .unwrap();
        let scope = crate::scope::SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        };
        state.bind_if_absent(&scope, "stored").unwrap();
        state
            .accept_event(&scope, "event", &json!({"synthetic":true}))
            .unwrap();
        client.request_key = Some("event-key".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event".into()],
        });
        let task = tokio::spawn(async move {
            client
                .prompt("stored", &["synthetic task"], Duration::from_secs(10))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), observed)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !task.is_finished(),
            "admission or another turn's completion must not finish this request"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.inputs(&scope, None).unwrap()[0].status != "accepted" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !state.binding(&scope).unwrap().unwrap().primed,
            "admission alone does not prove standing context persisted"
        );
        assert_eq!(
            state.submission("event-key").unwrap().unwrap()["text"],
            "synthetic task"
        );
        finish.store(true, Ordering::SeqCst);
        assert_eq!(task.await.unwrap().unwrap(), StopReason::EndTurn);
        assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "completed");
        assert!(state.binding(&scope).unwrap().unwrap().primed);
        server.abort();
        drop(state);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn lost_submit_response_is_uncertain_and_never_replayed_or_cancelled() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                panic!("expected resume")
            };
            let call: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(call["method"], "session.resume");
            assert_eq!(call["params"]["session_id"], "stored");
            socket
                .send(Message::Text(
                    json!({"id":call["id"],"result":{"session_id":"new-runtime"}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                panic!("expected prompt")
            };
            let call: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(call["method"], "prompt.submit");
            assert_eq!(call["params"]["session_id"], "new-runtime");
            socket.close(None).await.unwrap();
        });
        let mut client = ServeClient::connect(config(address)).await.unwrap();
        client.session_load("stored", vec![]).await.unwrap();
        let path =
            std::env::temp_dir().join(format!("buzz-serve-test-{}.db", uuid::Uuid::new_v4()));
        let state =
            crate::bridge_state::BridgeState::open(&path, "ws://isolated.invalid", &"1".repeat(64))
                .unwrap();
        let scope = crate::scope::SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        };
        state
            .accept_event(&scope, "event", &json!({"synthetic":true}))
            .unwrap();
        client.request_key = Some("stable-key".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event".into()],
        });
        let result = client
            .prompt("stored", &["synthetic task"], Duration::from_secs(10))
            .await;
        assert!(matches!(result, Err(AcpError::SubmissionUncertain(_))));
        assert!(client.sessions.contains_key("stored"));
        assert_eq!(state.inputs(&scope, None).unwrap()[0].status, "uncertain");
        assert!(state.scope_blocked(&scope).unwrap());
        assert!(state.pending(&scope).unwrap().is_empty());
        server.await.unwrap();
        drop(client);
        drop(state);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn cancel_targets_the_receipt_and_does_not_interrupt_a_later_turn() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let call: Value = serde_json::from_str(&text).unwrap();
                let result = match call["method"].as_str().unwrap() {
                    "session.resume" => json!({"session_id":"runtime"}),
                    "prompt.cancel" => {
                        assert_eq!(call["params"]["client_request_id"], "original-request");
                        json!({"accepted":true,"status":"completed","cancel_requested":false})
                    }
                    method => panic!("must not issue unconditional interrupt: {method}"),
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
        client.session_load("stored", vec![]).await.unwrap();
        client.in_flight = Some(("stored".into(), "original-request".into()));
        assert_eq!(
            client
                .cancel("stored", Duration::from_secs(5))
                .await
                .unwrap(),
            StopReason::EndTurn
        );
        assert!(!client.has_in_flight_prompt());
        server.abort();
    }

    /// Invoked by the Hermes pytest fixture, which owns an isolated real WS server.
    #[tokio::test]
    #[ignore = "requires the isolated Hermes interoperability fixture"]
    async fn real_hermes_contract() {
        let home = std::path::PathBuf::from(
            std::env::var("BUZZ_TEST_ISOLATED_HOME").expect("isolated fixture home"),
        );
        let canonical = home.canonicalize().unwrap();
        assert!(canonical.starts_with(std::env::temp_dir().canonicalize().unwrap()));
        assert_eq!(
            std::fs::read_to_string(home.join("buzz-interop-test-marker")).unwrap(),
            "isolated synthetic runtime"
        );
        let url = std::env::var("BUZZ_TEST_SERVE_URL").expect("isolated fixture URL");
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str(), Some("127.0.0.1"));
        assert_ne!(parsed.port(), Some(9119));
        let config = ServeConfig {
            url,
            profile: std::env::var("BUZZ_TEST_SERVE_PROFILE").unwrap_or_else(|_| "default".into()),
            token: std::env::var("BUZZ_TEST_SERVE_TOKEN").ok(),
            credentials: std::env::var("BUZZ_TEST_SERVE_CREDENTIALS")
                .ok()
                .map(|path| Arc::new(crate::serve_auth::ServeCredentials::new(path.into()))),
            model: None,
            effort: None,
        };
        let (mut client, sibling) = tokio::try_join!(
            ServeClient::connect(config.clone()),
            ServeClient::connect(config.clone())
        )
        .unwrap();
        drop(sibling);
        let created = client
            .session_new(home.to_str().unwrap(), vec![], Some("isolated interop"))
            .await
            .unwrap();
        let stored = created.session_id;
        let state = crate::bridge_state::BridgeState::open(
            &home.join("bridge.db"),
            "ws://isolated.invalid",
            &"1".repeat(64),
        )
        .unwrap();
        let scope = crate::scope::SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        };
        state.bind_if_absent(&scope, &stored).unwrap();
        state
            .accept_event(&scope, "event-1", &json!({"synthetic":true}))
            .unwrap();
        client.request_key = Some("interop-1".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event-1".into()],
        });
        assert_eq!(
            client
                .prompt(&stored, &["interop complete"], Duration::from_secs(15))
                .await
                .unwrap(),
            StopReason::EndTurn
        );
        drop(client);
        let mut client = ServeClient::connect(config.clone()).await.unwrap();
        client.session_load(&stored, vec![]).await.unwrap();
        assert_eq!(
            client.request_status(&stored, "interop-1").await.unwrap()["status"],
            "completed"
        );
        // Retry the exact frozen request after reconnect: server must return its receipt.
        client.request_key = Some("interop-1".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event-1".into()],
        });
        assert_eq!(
            client
                .prompt(&stored, &["interop complete"], Duration::from_secs(15))
                .await
                .unwrap(),
            StopReason::EndTurn
        );
        state
            .accept_event(&scope, "event-2", &json!({"synthetic":true}))
            .unwrap();
        client.request_key = Some("interop-2".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event-2".into()],
        });
        tokio::select! {
            result=client.prompt(&stored,&["interop wait"],Duration::from_secs(15))=>panic!("fixture long turn finished before cancel: {result:?}"),
            _=tokio::time::sleep(Duration::from_millis(500))=>{}
        }
        let mut viewer = ServeClient::connect(config.clone()).await.unwrap();
        viewer.session_load(&stored, vec![]).await.unwrap();
        assert_eq!(
            viewer.runtime(&stored).unwrap(),
            client.runtime(&stored).unwrap()
        );
        assert_eq!(
            viewer.request_status(&stored, "interop-2").await.unwrap()["status"],
            "running"
        );
        // Residency contract, against a real Serve while this turn is running.
        assert!(viewer.residency().is_supported());
        let working = viewer.working_sessions().await.unwrap();
        assert!(
            working["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |row| row["conversation_root"] == stored.as_str() && row["status"] == "working"
                ),
            "working row must map to the durable binding: {working}"
        );
        let durable = viewer.durable_work().await.unwrap();
        assert!(durable["sessions"].is_array(), "{durable}");
        drop(viewer);
        drop(client);
        // A restarted bridge has no pool task to signal. Recovery must find the
        // frozen request in the journal and cancel only that request.
        state.record_cancel(&scope, "cancel-event").unwrap();
        let mut client = ServeClient::connect(config).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                crate::serve_recovery::reconcile(&mut client, &state)
                    .await
                    .unwrap();
                if state.pending_controls().unwrap().is_empty()
                    && state.inputs(&scope, None).unwrap()[1].status == "cancelled"
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert!(!state.scope_blocked(&scope).unwrap());
        // Simulate a crash after journaling but before the socket sends the request.
        // Recovery must use the original bytes/key. A late original submission then
        // races/reuses that same receipt rather than starting another model turn.
        state
            .accept_event(&scope, "event-3", &json!({"synthetic":true}))
            .unwrap();
        state.prepare_submission(&["event-3".into()], "interop-3", &json!({
            "stored_session_id":stored,"client_request_id":"interop-3","text":"interop recovered"
        })).unwrap();
        state
            .finish_batch(&["event-3".into()], "uncertain")
            .unwrap();
        crate::serve_recovery::reconcile(&mut client, &state)
            .await
            .unwrap();
        assert_eq!(
            state.submission_admission("interop-3").unwrap(),
            Some(true),
            "recovery itself must obtain admission before the late original submits"
        );
        let mut delayed = ServeClient::connect(client.config.clone()).await.unwrap();
        delayed.request_key = Some("interop-3".into());
        delayed.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event-3".into()],
        });
        // Deliberately do not call session_load: a fresh transport must attach here.
        assert_eq!(
            delayed
                .prompt(&stored, &["interop recovered"], Duration::from_secs(15))
                .await
                .unwrap(),
            StopReason::EndTurn
        );
        assert_eq!(state.inputs(&scope, None).unwrap()[2].status, "completed");
        drop(delayed);
        // A runtime Serve no longer holds must heal in place, not fence the thread.
        state
            .accept_event(&scope, "event-4", &json!({"synthetic":true}))
            .unwrap();
        client
            .sessions
            .insert(stored.clone(), "runtime-gone-from-serve".into());
        client.request_key = Some("interop-heal".into());
        client.prepared = Some(PreparedRequest {
            state: state.clone(),
            scope: scope.clone(),
            event_ids: vec!["event-4".into()],
        });
        assert_eq!(
            client
                .prompt(&stored, &["interop healed"], Duration::from_secs(15))
                .await
                .unwrap(),
            StopReason::EndTurn
        );
        assert_eq!(state.inputs(&scope, None).unwrap()[3].status, "completed");
        assert!(!state.scope_blocked(&scope).unwrap());
        client.prepared = None;
        // Cancellation may win the race against a delayed submission on another
        // socket. Its durable tombstone must reject later execution of that key.
        let receipt = client
            .cancel_request(&stored, "delayed-submit")
            .await
            .unwrap();
        assert_eq!(receipt["status"], "cancelled");
        client.request_key = Some("delayed-submit".into());
        assert_eq!(
            client
                .prompt(&stored, &["must never execute"], Duration::from_secs(15))
                .await
                .unwrap(),
            StopReason::Cancelled
        );
    }
}

#[cfg(test)]
#[path = "hermes_serve/recovery_tests.rs"]
mod recovery_tests;
