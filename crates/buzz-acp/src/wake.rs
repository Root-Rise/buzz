//! Wake policy: which channel messages wake this agent.
//!
//! Under the default [`WakePolicy::Mentions`] the relay subscription carries a
//! `#p` filter, so only events that tag this agent ever arrive. Clients do not
//! always tag the agent a human is talking to: Buzz desktop offers no
//! @-autocomplete when a room has a single agent, and a reply in an agent's
//! thread tags nobody unless the agent is picked again. Those messages were
//! never delivered to any bridge and left no trace.
//!
//! [`WakePolicy::Addressed`] subscribes without `#p` and decides here, once per
//! event, whether the message is addressed to this agent:
//!
//! 1. The event tags this agent → wake. Same as `Mentions`.
//! 2. Otherwise only a human-authored kind:9 can wake an agent. An
//!    agent-authored message needs an explicit tag, so agents cannot wake each
//!    other in a loop.
//! 3. If the event tags any other agent, it is addressed to that agent.
//! 4. A direct message → wake.
//! 5. A reply in a thread whose root this agent wrote, or whose root tags this
//!    agent → wake.
//! 6. This agent is the only agent member of the channel → wake.
//!
//! An agent is a pubkey whose kind:0 profile carries a valid NIP-OA `auth`
//! attestation, or that holds the `bot` role in the channel's kind:39002 member
//! list. A channel role alone is not enough: the agent that created a channel
//! holds `owner` there, not `bot`.
//!
//! Every lookup fails closed: an author, tag, root or member list that cannot be
//! resolved means no implicit wake, reported as [`SkipReason::Unresolved`] so
//! the caller logs it at WARN instead of dropping the message silently.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use nostr::{Alphabet, Event, Kind, PublicKey, SingleLetterTag};
use serde_json::Value;
use uuid::Uuid;

use buzz_core::kind::KIND_STREAM_MESSAGE;

use crate::queue::parse_thread_tags;
use crate::relay::RestClient;

/// Which channel messages wake this agent.
///
/// Selected via `--wake-policy` / `BUZZ_ACP_WAKE_POLICY`. Defaults to
/// [`Mentions`](WakePolicy::Mentions) so a bridge keeps today's behavior until
/// it opts in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum WakePolicy {
    /// Wake only on events that tag this agent; the relay filters on `#p`.
    #[default]
    Mentions,
    /// Wake on events addressed to this agent, tagged or not (see module docs).
    Addressed,
}

impl std::fmt::Display for WakePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mentions => f.write_str("mentions"),
            Self::Addressed => f.write_str("addressed"),
        }
    }
}

/// Whether a pubkey belongs to an agent or a human.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Principal {
    Agent,
    Human,
}

/// The facts about a thread root that decide who the thread belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RootFacts {
    pub author: String,
    pub tagged: Vec<String>,
}

/// Why an event woke this agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeReason {
    Mentioned,
    DirectMessage,
    ThreadParty,
    SoleAgent,
}

impl WakeReason {
    /// The event type shown to the agent in its prompt.
    pub(crate) fn prompt_tag(self) -> &'static str {
        match self {
            Self::Mentioned => "@mention",
            Self::DirectMessage => "addressed:dm",
            Self::ThreadParty => "addressed:thread",
            Self::SoleAgent => "addressed:sole-agent",
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mentioned => "mentioned",
            Self::DirectMessage => "direct_message",
            Self::ThreadParty => "thread_party",
            Self::SoleAgent => "sole_agent",
        }
    }
}

/// Why an event did not wake this agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Not a stream message and not tagged.
    NotMessage,
    /// Written by an agent and not tagged.
    AgentAuthor,
    /// Tags another agent and not this one.
    AddressedToOtherAgent,
    /// None of the rules matched.
    NotAddressed,
    /// A fact the decision needed could not be resolved; fail closed.
    Unresolved(&'static str),
}

impl SkipReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::NotMessage => "not_message",
            Self::AgentAuthor => "agent_author",
            Self::AddressedToOtherAgent => "addressed_to_other_agent",
            Self::NotAddressed => "not_addressed",
            Self::Unresolved(what) => what,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WakeDecision {
    Wake(WakeReason),
    Skip(SkipReason),
}

/// The relay facts [`decide`] needs. Implementations cache; [`decide`] asks
/// for each fact only when an earlier rule did not already settle the event.
pub(crate) trait WakeLookup {
    /// Classify each pubkey. A pubkey absent from the result could not be
    /// classified.
    async fn classify(&self, channel_id: Uuid, pubkeys: &[String]) -> HashMap<String, Principal>;
    /// The thread root event, or `None` if it could not be fetched.
    async fn thread_root(&self, root_event_id: &str) -> Option<RootFacts>;
    /// Every member pubkey of the channel, or `None` if unavailable.
    async fn channel_members(&self, channel_id: Uuid) -> Option<Vec<String>>;
}

fn p_tags(event: &Event) -> Vec<String> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let s = tag.as_slice();
            (s.first().map(String::as_str) == Some("p"))
                .then(|| s.get(1).map(|v| v.to_ascii_lowercase()))
                .flatten()
        })
        .collect()
}

/// Decide whether `event` in `channel_id` wakes the agent `agent_pubkey_hex`.
pub(crate) async fn decide(
    lookup: &impl WakeLookup,
    event: &Event,
    channel_id: Uuid,
    is_dm: bool,
    agent_pubkey_hex: &str,
) -> WakeDecision {
    let agent = agent_pubkey_hex.to_ascii_lowercase();
    let author = event.pubkey.to_hex();
    let tagged = p_tags(event);

    // 1. Explicit tag.
    if tagged.contains(&agent) {
        return WakeDecision::Wake(WakeReason::Mentioned);
    }

    // 2. Implicit wakes: human-authored stream messages only.
    // Only a stream message can be addressed implicitly.
    if event.kind != Kind::Custom(KIND_STREAM_MESSAGE as u16) {
        return WakeDecision::Skip(SkipReason::NotMessage);
    }
    let others: Vec<String> = tagged
        .into_iter()
        .filter(|p| *p != author)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let mut to_classify = others.clone();
    to_classify.push(author.clone());
    let principals = lookup.classify(channel_id, &to_classify).await;
    match principals.get(&author) {
        Some(Principal::Human) => {}
        Some(Principal::Agent) => return WakeDecision::Skip(SkipReason::AgentAuthor),
        None => return WakeDecision::Skip(SkipReason::Unresolved("author_unclassified")),
    }

    // 3. Explicitly addressed to someone else.
    let mut unclassified_tag = false;
    for p in &others {
        match principals.get(p) {
            Some(Principal::Agent) => {
                return WakeDecision::Skip(SkipReason::AddressedToOtherAgent);
            }
            Some(Principal::Human) => {}
            None => unclassified_tag = true,
        }
    }
    if unclassified_tag {
        return WakeDecision::Skip(SkipReason::Unresolved("tag_unclassified"));
    }

    // 4. Direct message.
    if is_dm {
        return WakeDecision::Wake(WakeReason::DirectMessage);
    }

    // 5. Reply in a thread this agent is party to.
    if let Some(root_id) = parse_thread_tags(event).root_event_id {
        let Some(root) = lookup.thread_root(&root_id.to_ascii_lowercase()).await else {
            return WakeDecision::Skip(SkipReason::Unresolved("thread_root_unavailable"));
        };
        if root.author.eq_ignore_ascii_case(&agent)
            || root.tagged.iter().any(|p| p.eq_ignore_ascii_case(&agent))
        {
            return WakeDecision::Wake(WakeReason::ThreadParty);
        }
    }

    // 6. Sole agent in the channel.
    let Some(members) = lookup.channel_members(channel_id).await else {
        return WakeDecision::Skip(SkipReason::Unresolved("channel_members_unavailable"));
    };
    let member_principals = lookup.classify(channel_id, &members).await;
    let mut agents = Vec::new();
    for member in &members {
        match member_principals.get(member) {
            Some(Principal::Agent) => agents.push(member.as_str()),
            Some(Principal::Human) => {}
            None => return WakeDecision::Skip(SkipReason::Unresolved("member_unclassified")),
        }
    }
    if agents == [agent.as_str()] {
        return WakeDecision::Wake(WakeReason::SoleAgent);
    }

    WakeDecision::Skip(SkipReason::NotAddressed)
}

/// How long a pubkey classification is trusted. Profiles and roles change
/// rarely; a newly registered agent is picked up within this window.
const PRINCIPAL_TTL: Duration = Duration::from_secs(15 * 60);
/// How long a channel member list is trusted.
const MEMBERS_TTL: Duration = Duration::from_secs(60);
/// Thread roots are immutable; the cache is cleared when it reaches this size.
const ROOT_CACHE_MAX: usize = 4096;
/// Bound on each relay query so a slow relay cannot stall the listener.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

struct Member {
    pubkey: String,
    role: Option<String>,
}

type MemberList = std::sync::Arc<Vec<Member>>;

/// [`WakeLookup`] backed by the relay REST API, with caches.
pub(crate) struct RelayWakeLookup {
    rest: RestClient,
    principals: Mutex<HashMap<String, (Principal, Instant)>>,
    members: Mutex<HashMap<Uuid, (MemberList, Instant)>>,
    roots: Mutex<HashMap<String, RootFacts>>,
}

impl RelayWakeLookup {
    pub(crate) fn new(rest: RestClient) -> Self {
        Self {
            rest,
            principals: Mutex::new(HashMap::new()),
            members: Mutex::new(HashMap::new()),
            roots: Mutex::new(HashMap::new()),
        }
    }

    async fn query(&self, filter: nostr::Filter) -> Option<Vec<Value>> {
        match tokio::time::timeout(LOOKUP_TIMEOUT, self.rest.query(&[filter])).await {
            Ok(Ok(Value::Array(events))) => Some(events),
            Ok(Ok(_)) => {
                tracing::warn!("wake lookup: relay query returned a non-array");
                None
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "wake lookup: relay query failed");
                None
            }
            Err(_) => {
                tracing::warn!("wake lookup: relay query timed out");
                None
            }
        }
    }

    async fn member_list(&self, channel_id: Uuid) -> Option<MemberList> {
        if let Some((members, at)) = self.members.lock().unwrap().get(&channel_id) {
            if at.elapsed() < MEMBERS_TTL {
                return Some(members.clone());
            }
        }
        let filter = nostr::Filter::new()
            .kind(Kind::Custom(
                buzz_core::kind::KIND_NIP29_GROUP_MEMBERS as u16,
            ))
            .custom_tags(
                SingleLetterTag::lowercase(Alphabet::D),
                [channel_id.to_string()],
            );
        let events = self.query(filter).await?;
        let latest = events
            .iter()
            .max_by_key(|e| e.get("created_at").and_then(Value::as_u64).unwrap_or(0))?;
        let members: Vec<Member> = tag_rows(latest)
            .filter(|row| row.first().map(String::as_str) == Some("p"))
            .filter_map(|row| {
                let pubkey = PublicKey::from_hex(row.get(1)?).ok()?.to_hex();
                Some(Member {
                    pubkey,
                    role: row.get(3).cloned(),
                })
            })
            .collect();
        let members = std::sync::Arc::new(members);
        self.members
            .lock()
            .unwrap()
            .insert(channel_id, (members.clone(), Instant::now()));
        Some(members)
    }
}

fn tag_rows(event: &Value) -> impl Iterator<Item = Vec<String>> + '_ {
    event
        .get("tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tag| {
            tag.as_array().map(|parts| {
                parts
                    .iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect()
            })
        })
}

/// True if a kind:0 profile carries a NIP-OA attestation that verifies for
/// `pubkey`, whatever the owner.
fn profile_has_valid_attestation(profile: &Value, pubkey: &PublicKey) -> bool {
    profile
        .get("tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|tag| {
            tag.as_array()
                .is_some_and(|p| p.len() >= 4 && p[0].as_str() == Some("auth"))
        })
        .any(|tag| {
            let json = serde_json::to_string(tag).unwrap_or_default();
            buzz_sdk::nip_oa::verify_auth_tag(&json, pubkey).is_ok()
        })
}

impl WakeLookup for RelayWakeLookup {
    async fn classify(&self, channel_id: Uuid, pubkeys: &[String]) -> HashMap<String, Principal> {
        let mut out = HashMap::new();
        let mut pending = Vec::new();
        {
            let cache = self.principals.lock().unwrap();
            for pk in pubkeys {
                match cache.get(pk) {
                    Some((principal, at)) if at.elapsed() < PRINCIPAL_TTL => {
                        out.insert(pk.clone(), *principal);
                    }
                    _ => pending.push(pk.clone()),
                }
            }
        }
        if pending.is_empty() {
            return out;
        }

        // A `bot` role in this channel is enough to call a pubkey an agent.
        if let Some(members) = self.member_list(channel_id).await {
            pending.retain(|pk| {
                let is_bot = members
                    .iter()
                    .any(|m| m.pubkey == *pk && m.role.as_deref() == Some("bot"));
                if is_bot {
                    out.insert(pk.clone(), Principal::Agent);
                }
                !is_bot
            });
        }

        let keys: Vec<PublicKey> = pending
            .iter()
            .filter_map(|pk| PublicKey::from_hex(pk).ok())
            .collect();
        if !keys.is_empty() {
            let filter = nostr::Filter::new()
                .kind(Kind::Metadata)
                .authors(keys.clone());
            if let Some(profiles) = self.query(filter).await {
                let mut latest: HashMap<String, &Value> = HashMap::new();
                for profile in &profiles {
                    let Some(author) = profile.get("pubkey").and_then(Value::as_str) else {
                        continue;
                    };
                    let created = profile
                        .get("created_at")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let newer = latest.get(author).is_none_or(|prev| {
                        prev.get("created_at").and_then(Value::as_u64).unwrap_or(0) < created
                    });
                    if newer {
                        latest.insert(author.to_ascii_lowercase(), profile);
                    }
                }
                for key in &keys {
                    let hex = key.to_hex();
                    // No profile at all proves nothing either way: leave it
                    // unclassified rather than assume human.
                    if let Some(profile) = latest.get(&hex) {
                        let principal = if profile_has_valid_attestation(profile, key) {
                            Principal::Agent
                        } else {
                            Principal::Human
                        };
                        out.insert(hex, principal);
                    }
                }
            }
        }

        let mut cache = self.principals.lock().unwrap();
        let now = Instant::now();
        for (pk, principal) in &out {
            cache.insert(pk.clone(), (*principal, now));
        }
        out
    }

    async fn thread_root(&self, root_event_id: &str) -> Option<RootFacts> {
        if let Some(root) = self.roots.lock().unwrap().get(root_event_id) {
            return Some(root.clone());
        }
        let id = nostr::EventId::from_hex(root_event_id).ok()?;
        let events = self.query(nostr::Filter::new().id(id)).await?;
        let event = events.first()?;
        let author = event
            .get("pubkey")
            .and_then(Value::as_str)?
            .to_ascii_lowercase();
        let tagged = tag_rows(event)
            .filter(|row| row.first().map(String::as_str) == Some("p"))
            .filter_map(|row| row.get(1).map(|v| v.to_ascii_lowercase()))
            .collect();
        let root = RootFacts { author, tagged };
        let mut roots = self.roots.lock().unwrap();
        if roots.len() >= ROOT_CACHE_MAX {
            roots.clear();
        }
        roots.insert(root_event_id.to_string(), root.clone());
        Some(root)
    }

    async fn channel_members(&self, channel_id: Uuid) -> Option<Vec<String>> {
        let members = self.member_list(channel_id).await?;
        Some(members.iter().map(|m| m.pubkey.clone()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Tag};

    #[derive(Default)]
    struct FakeLookup {
        principals: HashMap<String, Principal>,
        roots: HashMap<String, RootFacts>,
        members: HashMap<Uuid, Vec<String>>,
    }

    impl FakeLookup {
        fn agent(mut self, keys: &Keys) -> Self {
            self.principals
                .insert(keys.public_key().to_hex(), Principal::Agent);
            self
        }
        fn human(mut self, keys: &Keys) -> Self {
            self.principals
                .insert(keys.public_key().to_hex(), Principal::Human);
            self
        }
        fn root(mut self, root: &Event) -> Self {
            self.roots.insert(
                root.id.to_hex(),
                RootFacts {
                    author: root.pubkey.to_hex(),
                    tagged: p_tags(root),
                },
            );
            self
        }
        fn members(mut self, channel: Uuid, keys: &[&Keys]) -> Self {
            self.members.insert(
                channel,
                keys.iter().map(|k| k.public_key().to_hex()).collect(),
            );
            self
        }
    }

    impl WakeLookup for FakeLookup {
        async fn classify(&self, _: Uuid, pubkeys: &[String]) -> HashMap<String, Principal> {
            pubkeys
                .iter()
                .filter_map(|pk| self.principals.get(pk).map(|p| (pk.clone(), *p)))
                .collect()
        }
        async fn thread_root(&self, id: &str) -> Option<RootFacts> {
            self.roots.get(id).cloned()
        }
        async fn channel_members(&self, channel: Uuid) -> Option<Vec<String>> {
            self.members.get(&channel).cloned()
        }
    }

    fn message(author: &Keys, tagged: &[&Keys], root: Option<&Event>) -> Event {
        let mut tags: Vec<Tag> = tagged
            .iter()
            .map(|k| Tag::parse(["p", &k.public_key().to_hex()]).unwrap())
            .collect();
        if let Some(root) = root {
            tags.push(Tag::parse(["e", &root.id.to_hex(), "", "reply"]).unwrap());
        }
        EventBuilder::new(Kind::Custom(KIND_STREAM_MESSAGE as u16), "hi")
            .tags(tags)
            .sign_with_keys(author)
            .unwrap()
    }

    struct World {
        channel: Uuid,
        me: Keys,
        other: Keys,
        human: Keys,
        second_human: Keys,
    }

    fn world() -> World {
        World {
            channel: Uuid::new_v4(),
            me: Keys::generate(),
            other: Keys::generate(),
            human: Keys::generate(),
            second_human: Keys::generate(),
        }
    }

    fn base(w: &World) -> FakeLookup {
        FakeLookup::default()
            .agent(&w.me)
            .agent(&w.other)
            .human(&w.human)
            .human(&w.second_human)
            .members(w.channel, &[&w.me, &w.other, &w.human])
    }

    async fn run(lookup: &FakeLookup, w: &World, event: &Event, is_dm: bool) -> WakeDecision {
        decide(lookup, event, w.channel, is_dm, &w.me.public_key().to_hex()).await
    }

    #[tokio::test]
    async fn explicit_tag_wakes_even_from_an_agent() {
        let w = world();
        let event = message(&w.other, &[&w.me], None);
        assert_eq!(
            run(&FakeLookup::default(), &w, &event, false).await,
            WakeDecision::Wake(WakeReason::Mentioned)
        );
    }

    #[tokio::test]
    async fn untagged_agent_message_never_wakes() {
        let w = world();
        let root = message(&w.me, &[&w.human], None);
        let lookup = base(&w).root(&root).members(w.channel, &[&w.me, &w.other]);
        let event = message(&w.other, &[], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, true).await,
            WakeDecision::Skip(SkipReason::AgentAuthor)
        );
    }

    #[tokio::test]
    async fn human_reply_in_my_thread_wakes_me() {
        let w = world();
        let root = message(&w.me, &[&w.human], None);
        let lookup = base(&w).root(&root);
        let event = message(&w.human, &[], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Wake(WakeReason::ThreadParty)
        );
    }

    #[tokio::test]
    async fn human_reply_in_thread_whose_root_tagged_me_wakes_me() {
        let w = world();
        let root = message(&w.human, &[&w.me], None);
        let lookup = base(&w).root(&root);
        let event = message(&w.human, &[], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Wake(WakeReason::ThreadParty)
        );
    }

    #[tokio::test]
    async fn human_reply_in_another_agents_thread_does_not_wake_me() {
        let w = world();
        let root = message(&w.other, &[&w.human], None);
        let lookup = base(&w).root(&root);
        let event = message(&w.human, &[], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::NotAddressed)
        );
    }

    #[tokio::test]
    async fn tagging_another_agent_in_my_thread_goes_to_them_only() {
        let w = world();
        let root = message(&w.me, &[&w.human], None);
        let lookup = base(&w).root(&root);
        let event = message(&w.human, &[&w.other], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::AddressedToOtherAgent)
        );
    }

    #[tokio::test]
    async fn tagging_a_human_does_not_block_my_thread() {
        let w = world();
        let root = message(&w.me, &[&w.human], None);
        let lookup = base(&w).root(&root);
        let event = message(&w.human, &[&w.second_human], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Wake(WakeReason::ThreadParty)
        );
    }

    #[tokio::test]
    async fn human_direct_message_wakes() {
        let w = world();
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&base(&w), &w, &event, true).await,
            WakeDecision::Wake(WakeReason::DirectMessage)
        );
    }

    #[tokio::test]
    async fn sole_agent_in_channel_wakes_on_top_level_message() {
        let w = world();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human, &w.second_human]);
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Wake(WakeReason::SoleAgent)
        );
    }

    #[tokio::test]
    async fn top_level_untagged_message_in_multi_agent_channel_wakes_nobody() {
        let w = world();
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&base(&w), &w, &event, false).await,
            WakeDecision::Skip(SkipReason::NotAddressed)
        );
    }

    #[tokio::test]
    async fn unclassified_author_fails_closed() {
        let w = world();
        let stranger = Keys::generate();
        let event = message(&stranger, &[], None);
        assert_eq!(
            run(&base(&w), &w, &event, true).await,
            WakeDecision::Skip(SkipReason::Unresolved("author_unclassified"))
        );
    }

    #[tokio::test]
    async fn unclassified_tag_fails_closed() {
        let w = world();
        let stranger = Keys::generate();
        let event = message(&w.human, &[&stranger], None);
        assert_eq!(
            run(&base(&w), &w, &event, true).await,
            WakeDecision::Skip(SkipReason::Unresolved("tag_unclassified"))
        );
    }

    #[tokio::test]
    async fn missing_thread_root_fails_closed() {
        let w = world();
        let root = message(&w.me, &[], None);
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human]);
        let event = message(&w.human, &[], Some(&root));
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::Unresolved("thread_root_unavailable"))
        );
    }

    #[tokio::test]
    async fn unclassified_member_fails_closed_for_sole_agent() {
        let w = world();
        let stranger = Keys::generate();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human, &stranger]);
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::Unresolved("member_unclassified"))
        );
    }

    #[tokio::test]
    async fn missing_member_list_fails_closed() {
        let w = world();
        let lookup = FakeLookup::default().agent(&w.me).human(&w.human);
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::Unresolved("channel_members_unavailable"))
        );
    }

    #[tokio::test]
    async fn untagged_non_message_kind_does_not_wake() {
        let w = world();
        let event = EventBuilder::new(Kind::Custom(7), "+")
            .sign_with_keys(&w.human)
            .unwrap();
        assert_eq!(
            run(&base(&w), &w, &event, true).await,
            WakeDecision::Skip(SkipReason::NotMessage)
        );
    }

    #[test]
    fn attestation_check_accepts_an_owner_signed_auth_tag() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "").unwrap();
        let tag: Value = serde_json::from_str(&tag).unwrap();
        let profile = serde_json::json!({ "tags": [tag] });
        assert!(profile_has_valid_attestation(&profile, &agent.public_key()));
        // The same tag does not make a different key an agent.
        assert!(!profile_has_valid_attestation(
            &profile,
            &owner.public_key()
        ));
    }

    #[test]
    fn attestation_check_rejects_a_forged_auth_tag() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let forged = serde_json::json!({
            "tags": [["auth", owner.public_key().to_hex(), "", "00".repeat(64)]]
        });
        assert!(!profile_has_valid_attestation(&forged, &agent.public_key()));
        let no_tag = serde_json::json!({ "tags": [] });
        assert!(!profile_has_valid_attestation(&no_tag, &agent.public_key()));
    }
}
