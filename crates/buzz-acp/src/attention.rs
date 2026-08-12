//! apiary: thread attention.
//!
//! Mentions are the relay's delivery mechanism: an event without the agent's
//! `#p` tag is never sent to it. That makes conversation stilted — every
//! follow-up inside a thread the agent is already working in must re-@ it.
//!
//! With thread attention enabled the harness subscribes to the channel
//! without the `#p` filter and gates client-side instead:
//!
//!   accept if  (event mentions us)                          — unchanged
//!          or  (event is in a thread we are engaged in
//!               AND its author is not another agent
//!               AND the engagement has not expired)
//!
//! Everything else is dropped exactly as before, so this is strictly
//! additive: mentions behave identically, unengaged threads stay silent.
//!
//! Agent authors are excluded deliberately. Two agents in one thread would
//! otherwise hear each other with no mention required and ping-pong with no
//! terminator; mentions remain the only way agents wake each other.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use nostr::Event;
use uuid::Uuid;

/// How long a thread stays "engaged" after the last accepted event in it.
pub const ENGAGEMENT_TTL: Duration = Duration::from_secs(45 * 60);
/// Bound on tracked threads per process (LRU-ish by expiry sweep).
const MAX_TRACKED: usize = 256;
/// How often the known-agent roster is refreshed from the relay.
pub const AGENT_ROSTER_REFRESH: Duration = Duration::from_secs(15 * 60);

/// Threads this agent is currently engaged in, keyed by (channel, thread root).
pub struct Attention {
    engaged: HashMap<(Uuid, String), Instant>,
    /// Pubkeys (hex) known to be agents — excluded from unmentioned wakeups.
    agents: std::collections::HashSet<String>,
    agents_refreshed: Option<Instant>,
}

impl Attention {
    pub fn new() -> Self {
        Self {
            engaged: HashMap::new(),
            agents: std::collections::HashSet::new(),
            agents_refreshed: None,
        }
    }

    /// The thread identity of an event: its thread root, or its own id when
    /// the event is itself a thread root.
    pub fn thread_key(event: &Event) -> String {
        crate::queue::parse_thread_tags(event)
            .root_event_id
            .unwrap_or_else(|| event.id.to_hex())
    }

    /// Record engagement — called for every event we accept, so a thread
    /// stays warm as long as the conversation continues.
    pub fn engage(&mut self, channel_id: Uuid, event: &Event) {
        let key = (channel_id, Self::thread_key(event));
        self.engaged.insert(key, Instant::now());
        if self.engaged.len() > MAX_TRACKED {
            self.sweep();
        }
    }

    fn sweep(&mut self) {
        let now = Instant::now();
        self.engaged
            .retain(|_, seen| now.duration_since(*seen) < ENGAGEMENT_TTL);
    }

    /// Should an event that does NOT mention us still be delivered?
    pub fn should_wake(&mut self, channel_id: Uuid, event: &Event) -> bool {
        let author = event.pubkey.to_hex();
        if self.agents.contains(&author) {
            return false; // agents wake each other by mention only
        }
        let key = (channel_id, Self::thread_key(event));
        match self.engaged.get(&key) {
            Some(seen) if Instant::now().duration_since(*seen) < ENGAGEMENT_TTL => true,
            Some(_) => {
                self.engaged.remove(&key);
                false
            }
            None => false,
        }
    }

    pub fn agent_roster_stale(&self) -> bool {
        match self.agents_refreshed {
            None => true,
            Some(t) => Instant::now().duration_since(t) >= AGENT_ROSTER_REFRESH,
        }
    }

    pub fn set_agents(&mut self, agents: std::collections::HashSet<String>) {
        self.agents = agents;
        self.agents_refreshed = Some(Instant::now());
    }
}

/// Fetch the pubkeys of every registered agent (kind:10100 profiles), so
/// unmentioned wakeups can exclude them. Best-effort: on failure the caller
/// keeps the previous roster (fail-safe — an unknown agent simply keeps the
/// mention-only behavior it has today).
pub async fn fetch_agent_pubkeys(
    rest: &crate::relay::RestClient,
) -> Option<std::collections::HashSet<String>> {
    let filter = nostr::Filter::new().kind(nostr::Kind::Custom(10100)).limit(500);
    match rest.query(&[filter]).await {
        Ok(value) => {
            let mut set = std::collections::HashSet::new();
            if let Some(arr) = value.as_array() {
                for ev in arr {
                    if let Some(pk) = ev.get("pubkey").and_then(|v| v.as_str()) {
                        set.insert(pk.to_string());
                    }
                }
            }
            Some(set)
        }
        Err(e) => {
            tracing::debug!("thread attention: agent roster fetch failed: {e}");
            None
        }
    }
}
