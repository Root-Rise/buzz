//! Transport boundary for the existing pool. Serve connections never own a child process.
use crate::{
    acp::{AcpClient, AcpError, McpServer, SessionNewResponse, StopReason, SystemPromptTransport},
    hermes_serve::{ServeClient, ServeConfig},
    observer::{ObserverContext, ObserverHandle},
    usage::TurnUsage,
};
use serde_json::Value;
use std::time::Duration;

/// Agent transport selected once at harness startup.
pub enum BackendClient {
    Acp(Box<AcpClient>),
    Serve(Box<ServeClient>),
}
impl From<AcpClient> for BackendClient {
    fn from(client: AcpClient) -> Self {
        Self::Acp(Box::new(client))
    }
}
impl BackendClient {
    /// Whether this handle attaches to a server-owned agent.
    pub fn is_serve(&self) -> bool {
        matches!(self, Self::Serve(_))
    }
    /// Create either a child-process handle or a remote connection handle.
    pub async fn connect(
        serve: Option<ServeConfig>,
        command: &str,
        args: &[String],
        env: &[(String, String)],
        codex: bool,
    ) -> Result<Self, AcpError> {
        match serve {
            Some(config) => Ok(Self::Serve(Box::new(ServeClient::connect(config).await?))),
            None => Ok(Self::Acp(Box::new(
                AcpClient::spawn(command, args, env, codex).await?,
            ))),
        }
    }
    /// Negotiate ACP only for a local adapter. Serve has no ACP protocol version.
    pub async fn initialize(&mut self) -> Result<Value, AcpError> {
        match self {
            Self::Acp(client) => client.initialize().await,
            Self::Serve(_) => Ok(serde_json::json!({"agentInfo":{"name":"hermes-serve"}})),
        }
    }
    /// Stable request identity supplied by the durable bridge event journal.
    pub(crate) fn prepare_request(
        &mut self,
        key: String,
        state: crate::bridge_state::BridgeState,
        scope: crate::scope::SessionScope,
        event_ids: Vec<String>,
    ) {
        if let Self::Serve(client) = self {
            client.request_key = Some(key);
            client.prepared = Some(crate::hermes_serve::PreparedRequest {
                state,
                scope,
                event_ids,
            });
        }
    }
    pub(crate) async fn shutdown(&mut self) {
        match self {
            Self::Acp(client) => client.shutdown().await,
            Self::Serve(client) => client.shutdown().await,
        }
    }
    pub(crate) fn set_observer(&mut self, handle: Option<ObserverHandle>, index: usize) {
        match self {
            Self::Acp(client) => client.set_observer(handle, index),
            Self::Serve(client) => client.set_observer(handle, index),
        }
    }
    pub(crate) fn set_observer_context(&mut self, context: ObserverContext) {
        match self {
            Self::Acp(client) => client.set_observer_context(context),
            Self::Serve(client) => client.set_observer_context(context),
        }
    }
    pub(crate) fn observer_handle(&self) -> Option<ObserverHandle> {
        match self {
            Self::Acp(client) => client.observer_handle(),
            Self::Serve(client) => client.observer_handle(),
        }
    }
    pub(crate) fn observer_agent_index(&self) -> Option<usize> {
        match self {
            Self::Acp(client) => client.observer_agent_index(),
            Self::Serve(client) => client.observer_agent_index(),
        }
    }
    pub(crate) fn observe(&self, kind: impl Into<String>, payload: Value) {
        match self {
            Self::Acp(client) => client.observe(kind, payload),
            Self::Serve(client) => client.observe(kind, payload),
        }
    }
    pub(crate) fn has_in_flight_prompt(&self) -> bool {
        match self {
            Self::Acp(client) => client.has_in_flight_prompt(),
            Self::Serve(client) => client.has_in_flight_prompt(),
        }
    }
    pub(crate) fn steering_supported(&self) -> bool {
        match self {
            Self::Acp(client) => client.steering_supported(),
            Self::Serve(_) => false,
        }
    }
    pub(crate) fn take_turn_usage(&mut self) -> Option<TurnUsage> {
        match self {
            Self::Acp(client) => client.take_turn_usage(),
            Self::Serve(_) => None,
        }
    }
    pub(crate) fn notify_session_spawned(&mut self, id: &str) {
        match self {
            Self::Acp(client) => client.notify_session_spawned(id),
            Self::Serve(_) => {}
        }
    }
    pub(crate) fn install_steer_rx(
        &mut self,
        rx: tokio::sync::mpsc::Receiver<crate::pool::SteerRequest>,
    ) {
        match self {
            Self::Acp(client) => client.install_steer_rx(rx),
            Self::Serve(_) => drop(rx),
        }
    }
    pub(crate) fn clear_steer_rx(&mut self) {
        match self {
            Self::Acp(client) => client.clear_steer_rx(),
            Self::Serve(_) => {}
        }
    }
    pub(crate) async fn session_new_full(
        &mut self,
        cwd: &str,
        mcp: Vec<McpServer>,
        system: Option<SystemPromptTransport<'_>>,
        title: Option<&str>,
    ) -> Result<SessionNewResponse, AcpError> {
        match self {
            Self::Acp(client) => client.session_new_full(cwd, mcp, system, title).await,
            Self::Serve(client) => client.session_new(cwd, mcp, title).await,
        }
    }
    pub(crate) async fn session_load(
        &mut self,
        id: &str,
        cwd: &str,
        mcp: Vec<McpServer>,
    ) -> Result<(), AcpError> {
        match self {
            Self::Acp(client) => client.session_load(id, cwd, mcp).await,
            Self::Serve(client) => client.session_load(id, mcp).await,
        }
    }
    pub(crate) async fn session_prompt_blocks_with_idle_timeout(
        &mut self,
        id: &str,
        blocks: &[&str],
        idle: Duration,
        max: Duration,
    ) -> Result<StopReason, AcpError> {
        match self {
            Self::Acp(client) => {
                client
                    .session_prompt_blocks_with_idle_timeout(id, blocks, idle, max)
                    .await
            }
            Self::Serve(client) => client.prompt(id, blocks, max).await,
        }
    }
    pub(crate) async fn session_prompt_with_idle_timeout(
        &mut self,
        id: &str,
        text: &str,
        idle: Duration,
        max: Duration,
    ) -> Result<StopReason, AcpError> {
        match self {
            Self::Acp(client) => {
                client
                    .session_prompt_with_idle_timeout(id, text, idle, max)
                    .await
            }
            Self::Serve(client) => client.prompt(id, &[text], max).await,
        }
    }
    pub(crate) async fn cancel_with_cleanup(
        &mut self,
        id: &str,
        grace: Duration,
    ) -> Result<StopReason, AcpError> {
        match self {
            Self::Acp(client) => client.cancel_with_cleanup(id, grace).await,
            Self::Serve(client) => client.cancel(id, grace).await,
        }
    }
    pub(crate) async fn cancel_with_cleanup_grace(
        &mut self,
        id: &str,
        grace: Duration,
    ) -> Result<StopReason, AcpError> {
        match self {
            Self::Acp(client) => client.cancel_with_cleanup_grace(id, grace).await,
            Self::Serve(client) => client.cancel(id, grace).await,
        }
    }
    pub(crate) async fn session_set_goose_system_prompt(
        &mut self,
        id: &str,
        text: &str,
    ) -> Result<Value, AcpError> {
        match self {
            Self::Acp(client) => client.session_set_goose_system_prompt(id, text).await,
            Self::Serve(_) => Err(AcpError::ServeUnavailable(
                "Goose-specific settings are unsupported by Serve".into(),
            )),
        }
    }
    pub(crate) async fn session_set_config_option(
        &mut self,
        id: &str,
        key: &str,
        value: &str,
    ) -> Result<Value, AcpError> {
        match self {
            Self::Acp(client) => client.session_set_config_option(id, key, value).await,
            Self::Serve(_) => Err(AcpError::ServeUnavailable(
                "Configure settings on the Hermes profile".into(),
            )),
        }
    }
    pub(crate) async fn session_set_model(
        &mut self,
        id: &str,
        value: &str,
    ) -> Result<Value, AcpError> {
        match self {
            Self::Acp(client) => client.session_set_model(id, value).await,
            Self::Serve(_) => Err(AcpError::ServeUnavailable(
                "Use Hermes model controls".into(),
            )),
        }
    }
    #[cfg(test)]
    pub(crate) fn steer_rx_is_none(&self) -> bool {
        match self {
            Self::Acp(client) => client.steer_rx_is_none(),
            Self::Serve(_) => true,
        }
    }
}
