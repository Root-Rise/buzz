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
//!    other in a loop. Relay-signed workflow messages and owner control
//!    commands (`!stop`, `!cancel`, …) also need an explicit tag: a control
//!    command must never reach the model as an ordinary prompt.
//! 3. If the event tags any other agent, it is addressed to that agent.
//! 4. A reply in a thread whose root this agent wrote, or whose root tags this
//!    agent → wake. Every agent the root names wakes, and the root decides for
//!    the whole thread: tag an agent to pull it into someone else's thread.
//! 5. This agent is the only agent member of the channel or DM → wake. A group
//!    DM with several agents wakes none of them implicitly.
//!
//! An agent is a pubkey that holds the `bot` role in a relay-signed kind:39002
//! member list this bridge can read, or whose kind:0 profile carries a valid
//! NIP-OA `auth` attestation. The role is checked across all readable channels,
//! not just this one: the agent that created a channel holds `owner` there, not
//! `bot`. Deployments must therefore keep every agent `bot` in a channel all
//! bridges share. A human is a pubkey with a profile and neither signal; a
//! pubkey that cannot be looked up is neither. This bridge's own pubkey is
//! always an agent.
//!
//! Every lookup fails closed: a channel type, author, tag, root or member list
//! that cannot be resolved means no implicit wake, reported as
//! [`SkipReason::Unresolved`] so the caller logs it at WARN instead of dropping
//! the message silently. The whole decision is bounded by
//! [`DECISION_DEADLINE`] because it runs on the listener loop.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use nostr::{Alphabet, Event, Kind, PublicKey, SingleLetterTag};
use serde_json::Value;
use uuid::Uuid;

use buzz_core::kind::{KIND_NIP29_GROUP_MEMBERS, KIND_STREAM_MESSAGE};

use crate::queue::parse_thread_tags;
use crate::relay::RestClient;

/// Owner control commands; untagged, they are never delivered implicitly.
const CONTROL_COMMANDS: [&str; 4] = ["!stop", "!cancel", "!rotate", "!shutdown"];

/// Upper bound on one wake decision, all relay lookups included. The decision
/// runs on the listener loop, so a degraded relay must not stall it longer.
pub(crate) const DECISION_DEADLINE: Duration = Duration::from_secs(4);

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
    ThreadParty,
    DirectMessage,
    SoleAgent,
}

impl WakeReason {
    /// The event type shown to the agent in its prompt.
    pub(crate) fn prompt_tag(self) -> &'static str {
        match self {
            Self::Mentioned => "@mention",
            Self::ThreadParty => "addressed:thread",
            Self::DirectMessage => "addressed:dm",
            Self::SoleAgent => "addressed:sole-agent",
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mentioned => "mentioned",
            Self::ThreadParty => "thread_party",
            Self::DirectMessage => "direct_message",
            Self::SoleAgent => "sole_agent",
        }
    }
}

/// Why an event did not wake this agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Not a stream message and not tagged.
    NotMessage,
    /// A relay-signed workflow message that does not tag this agent.
    Workflow,
    /// An owner control command that does not tag this agent.
    UntaggedControl,
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
            Self::Workflow => "workflow",
            Self::UntaggedControl => "untagged_control",
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
    async fn classify(&self, pubkeys: &[String]) -> HashMap<String, Principal>;
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

fn has_tag(event: &Event, name: &str) -> bool {
    event
        .tags
        .iter()
        .any(|tag| tag.as_slice().first().map(String::as_str) == Some(name))
}

/// [`decide`] bounded by [`DECISION_DEADLINE`]; a timeout fails closed.
pub(crate) async fn decide_within_deadline(
    lookup: &impl WakeLookup,
    event: &Event,
    channel_id: Uuid,
    is_dm: Option<bool>,
    agent_pubkey_hex: &str,
) -> WakeDecision {
    tokio::time::timeout(
        DECISION_DEADLINE,
        decide(lookup, event, channel_id, is_dm, agent_pubkey_hex),
    )
    .await
    .unwrap_or(WakeDecision::Skip(SkipReason::Unresolved(
        "decision_timeout",
    )))
}

/// Decide whether `event` in `channel_id` wakes the agent `agent_pubkey_hex`.
///
/// `is_dm` is `None` when the channel type could not be resolved.
pub(crate) async fn decide(
    lookup: &impl WakeLookup,
    event: &Event,
    channel_id: Uuid,
    is_dm: Option<bool>,
    agent_pubkey_hex: &str,
) -> WakeDecision {
    let agent = agent_pubkey_hex.to_ascii_lowercase();
    let author = event.pubkey.to_hex();
    let tagged = p_tags(event);

    // 1. Explicit tag.
    if tagged.contains(&agent) {
        return WakeDecision::Wake(WakeReason::Mentioned);
    }

    // 2. Implicit wakes: human-authored, ordinary stream messages only.
    if event.kind != Kind::Custom(KIND_STREAM_MESSAGE as u16) {
        return WakeDecision::Skip(SkipReason::NotMessage);
    }
    if has_tag(event, "buzz:workflow") {
        return WakeDecision::Skip(SkipReason::Workflow);
    }
    if CONTROL_COMMANDS.contains(&event.content.trim()) {
        return WakeDecision::Skip(SkipReason::UntaggedControl);
    }
    let others: Vec<String> = tagged
        .into_iter()
        .filter(|p| *p != author)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let mut to_classify = others.clone();
    to_classify.push(author.clone());
    let principals = lookup.classify(&to_classify).await;
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

    let Some(is_dm) = is_dm else {
        return WakeDecision::Skip(SkipReason::Unresolved("channel_type_unresolved"));
    };

    // 4. Reply in a thread this agent is party to.
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

    // 5. Sole agent in the channel or DM. This agent never needs a lookup.
    let Some(members) = lookup.channel_members(channel_id).await else {
        return WakeDecision::Skip(SkipReason::Unresolved("channel_members_unavailable"));
    };
    let others: Vec<String> = members.into_iter().filter(|m| *m != agent).collect();
    let other_principals = lookup.classify(&others).await;
    for member in &others {
        match other_principals.get(member) {
            Some(Principal::Agent) => return WakeDecision::Skip(SkipReason::NotAddressed),
            Some(Principal::Human) => {}
            None => return WakeDecision::Skip(SkipReason::Unresolved("member_unclassified")),
        }
    }
    WakeDecision::Wake(if is_dm {
        WakeReason::DirectMessage
    } else {
        WakeReason::SoleAgent
    })
}

/// How long a pubkey classification is trusted. Profiles and roles change
/// rarely; a newly registered agent is picked up within this window.
const PRINCIPAL_TTL: Duration = Duration::from_secs(15 * 60);
/// How long a pubkey that could not be classified is left unclassified before
/// it is looked up again. Bounds relay load from members without a profile.
const UNRESOLVED_TTL: Duration = Duration::from_secs(60);
/// How long a channel member list is trusted.
const MEMBERS_TTL: Duration = Duration::from_secs(60);
/// Thread roots are immutable; the cache is cleared when it reaches this size.
const ROOT_CACHE_MAX: usize = 4096;
/// Bound on each relay query.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
/// The relay answers a multi-value `#p` query from its newest events up to this
/// cap and filters afterwards, so a full page may have dropped matches.
const RELAY_QUERY_CAP: usize = 1000;

/// [`WakeLookup`] backed by the relay REST API, with caches.
pub(crate) struct RelayWakeLookup {
    rest: RestClient,
    self_pubkey: String,
    principals: Mutex<HashMap<String, (Principal, Instant)>>,
    unresolved: Mutex<HashMap<String, Instant>>,
    members: Mutex<HashMap<Uuid, (Vec<String>, Instant)>>,
    roots: Mutex<HashMap<String, RootFacts>>,
}

impl RelayWakeLookup {
    pub(crate) fn new(rest: RestClient, self_pubkey_hex: &str) -> Self {
        Self {
            rest,
            self_pubkey: self_pubkey_hex.to_ascii_lowercase(),
            principals: Mutex::new(HashMap::new()),
            unresolved: Mutex::new(HashMap::new()),
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

    /// Split `pubkeys` into cached answers and the ones still to look up.
    fn split_cached(&self, pubkeys: &[String]) -> (HashMap<String, Principal>, Vec<String>) {
        let mut known = HashMap::new();
        let mut pending = Vec::new();
        let principals = self.principals.lock().unwrap();
        let unresolved = self.unresolved.lock().unwrap();
        for pk in pubkeys {
            let pk = pk.to_ascii_lowercase();
            if pk == self.self_pubkey {
                known.insert(pk, Principal::Agent);
            } else if let Some((principal, at)) = principals.get(&pk) {
                if at.elapsed() < PRINCIPAL_TTL {
                    known.insert(pk, *principal);
                } else {
                    pending.push(pk);
                }
            } else if unresolved
                .get(&pk)
                .is_none_or(|at| at.elapsed() >= UNRESOLVED_TTL)
            {
                pending.push(pk);
            }
        }
        (known, pending)
    }

    fn remember(&self, looked_up: &[String], found: &HashMap<String, Principal>) {
        let now = Instant::now();
        let mut principals = self.principals.lock().unwrap();
        let mut unresolved = self.unresolved.lock().unwrap();
        for pk in looked_up {
            match found.get(pk) {
                Some(principal) => {
                    principals.insert(pk.clone(), (*principal, now));
                    unresolved.remove(pk);
                }
                None => {
                    unresolved.insert(pk.clone(), now);
                }
            }
        }
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

fn created_at(event: &Value) -> u64 {
    event.get("created_at").and_then(Value::as_u64).unwrap_or(0)
}

/// Pubkeys holding the `bot` role in any of these kind:39002 member lists
/// (`["p", <pubkey>, <relay>, <role>]`). `None` when the answer may be
/// incomplete because the relay returned a full page.
fn bot_pubkeys(member_lists: &[Value]) -> Option<HashSet<String>> {
    if member_lists.len() >= RELAY_QUERY_CAP {
        return None;
    }
    Some(
        member_lists
            .iter()
            .flat_map(tag_rows)
            .filter(|row| {
                row.first().map(String::as_str) == Some("p")
                    && row.get(3).map(String::as_str) == Some("bot")
            })
            .filter_map(|row| row.get(1).map(|pk| pk.to_ascii_lowercase()))
            .collect(),
    )
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

/// Classify `pending` from the relay's answers. `bots` is `None` when the
/// bot-role query failed: then nobody can be shown human, and only a valid
/// attestation still proves an agent. A pubkey with no profile stays
/// unclassified.
fn classify_from(
    pending: &[String],
    bots: Option<&HashSet<String>>,
    profiles: &[Value],
) -> HashMap<String, Principal> {
    let mut latest: HashMap<String, &Value> = HashMap::new();
    for profile in profiles {
        let Some(author) = profile.get("pubkey").and_then(Value::as_str) else {
            continue;
        };
        let author = author.to_ascii_lowercase();
        if latest
            .get(&author)
            .is_none_or(|prev| created_at(prev) < created_at(profile))
        {
            latest.insert(author, profile);
        }
    }
    let mut out = HashMap::new();
    for pk in pending {
        if bots.is_some_and(|bots| bots.contains(pk)) {
            out.insert(pk.clone(), Principal::Agent);
            continue;
        }
        let (Some(profile), Ok(key)) = (latest.get(pk), PublicKey::from_hex(pk)) else {
            continue;
        };
        if profile_has_valid_attestation(profile, &key) {
            out.insert(pk.clone(), Principal::Agent);
        } else if bots.is_some() {
            out.insert(pk.clone(), Principal::Human);
        }
    }
    out
}

/// Member pubkeys from the newest kind:39002 list for a channel.
fn members_from_lists(member_lists: &[Value]) -> Option<Vec<String>> {
    let latest = member_lists.iter().max_by_key(|e| created_at(e))?;
    Some(
        tag_rows(latest)
            .filter(|row| row.first().map(String::as_str) == Some("p"))
            .filter_map(|row| Some(PublicKey::from_hex(row.get(1)?).ok()?.to_hex()))
            .collect(),
    )
}

/// Author and tagged pubkeys of a fetched thread root.
fn root_from_event(event: &Value) -> Option<RootFacts> {
    let author = event
        .get("pubkey")
        .and_then(Value::as_str)?
        .to_ascii_lowercase();
    let tagged = tag_rows(event)
        .filter(|row| row.first().map(String::as_str) == Some("p"))
        .filter_map(|row| row.get(1).map(|v| v.to_ascii_lowercase()))
        .collect();
    Some(RootFacts { author, tagged })
}

impl WakeLookup for RelayWakeLookup {
    async fn classify(&self, pubkeys: &[String]) -> HashMap<String, Principal> {
        let (mut out, pending) = self.split_cached(pubkeys);
        if pending.is_empty() {
            return out;
        }

        let bot_filter = nostr::Filter::new()
            .kind(Kind::Custom(KIND_NIP29_GROUP_MEMBERS as u16))
            .custom_tags(SingleLetterTag::lowercase(Alphabet::P), pending.clone());
        let bots = self
            .query(bot_filter)
            .await
            .and_then(|lists| bot_pubkeys(&lists));
        if bots.is_none() {
            tracing::warn!("wake lookup: bot roles unavailable; no pubkey can be shown human");
        }

        let profile_keys: Vec<PublicKey> = pending
            .iter()
            .filter(|pk| bots.as_ref().is_none_or(|bots| !bots.contains(*pk)))
            .filter_map(|pk| PublicKey::from_hex(pk).ok())
            .collect();
        let profiles = if profile_keys.is_empty() {
            Vec::new()
        } else {
            let filter = nostr::Filter::new()
                .kind(Kind::Metadata)
                .authors(profile_keys);
            self.query(filter).await.unwrap_or_default()
        };

        let found = classify_from(&pending, bots.as_ref(), &profiles);
        self.remember(&pending, &found);
        out.extend(found);
        out
    }

    async fn thread_root(&self, root_event_id: &str) -> Option<RootFacts> {
        if let Some(root) = self.roots.lock().unwrap().get(root_event_id) {
            return Some(root.clone());
        }
        let id = nostr::EventId::from_hex(root_event_id).ok()?;
        let events = self.query(nostr::Filter::new().id(id)).await?;
        let root = root_from_event(events.first()?)?;
        let mut roots = self.roots.lock().unwrap();
        if roots.len() >= ROOT_CACHE_MAX {
            roots.clear();
        }
        roots.insert(root_event_id.to_string(), root.clone());
        Some(root)
    }

    async fn channel_members(&self, channel_id: Uuid) -> Option<Vec<String>> {
        if let Some((members, at)) = self.members.lock().unwrap().get(&channel_id) {
            if at.elapsed() < MEMBERS_TTL {
                return Some(members.clone());
            }
        }
        let filter = nostr::Filter::new()
            .kind(Kind::Custom(KIND_NIP29_GROUP_MEMBERS as u16))
            .custom_tags(
                SingleLetterTag::lowercase(Alphabet::D),
                [channel_id.to_string()],
            );
        let members = members_from_lists(&self.query(filter).await?)?;
        self.members
            .lock()
            .unwrap()
            .insert(channel_id, (members.clone(), Instant::now()));
        Some(members)
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
        async fn classify(&self, pubkeys: &[String]) -> HashMap<String, Principal> {
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

    fn message_with(
        author: &Keys,
        tagged: &[&Keys],
        root: Option<&Event>,
        content: &str,
        extra: &[[&str; 2]],
    ) -> Event {
        let mut tags: Vec<Tag> = tagged
            .iter()
            .map(|k| Tag::parse(["p", &k.public_key().to_hex()]).unwrap())
            .collect();
        if let Some(root) = root {
            tags.push(Tag::parse(["e", &root.id.to_hex(), "", "reply"]).unwrap());
        }
        for tag in extra {
            tags.push(Tag::parse(*tag).unwrap());
        }
        EventBuilder::new(Kind::Custom(KIND_STREAM_MESSAGE as u16), content)
            .tags(tags)
            .sign_with_keys(author)
            .unwrap()
    }

    fn message(author: &Keys, tagged: &[&Keys], root: Option<&Event>) -> Event {
        message_with(author, tagged, root, "hi", &[])
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

    /// A channel with this agent, another agent and a human. The fake never
    /// classifies `me`: `decide` must not need to look itself up.
    fn base(w: &World) -> FakeLookup {
        FakeLookup::default()
            .agent(&w.other)
            .human(&w.human)
            .human(&w.second_human)
            .members(w.channel, &[&w.me, &w.other, &w.human])
    }

    async fn run_as(
        lookup: &FakeLookup,
        w: &World,
        event: &Event,
        is_dm: Option<bool>,
    ) -> WakeDecision {
        decide(lookup, event, w.channel, is_dm, &w.me.public_key().to_hex()).await
    }

    async fn run(lookup: &FakeLookup, w: &World, event: &Event, is_dm: bool) -> WakeDecision {
        run_as(lookup, w, event, Some(is_dm)).await
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
    async fn human_direct_message_to_me_alone_wakes() {
        let w = world();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human]);
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&lookup, &w, &event, true).await,
            WakeDecision::Wake(WakeReason::DirectMessage)
        );
    }

    #[tokio::test]
    async fn group_dm_with_two_agents_wakes_neither_implicitly() {
        let w = world();
        let lookup = base(&w); // me, other agent, human
        let event = message(&w.human, &[], None);
        assert_eq!(
            run(&lookup, &w, &event, true).await,
            WakeDecision::Skip(SkipReason::NotAddressed)
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
    async fn unresolved_channel_type_fails_closed() {
        let w = world();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human]);
        let event = message(&w.human, &[], None);
        assert_eq!(
            run_as(&lookup, &w, &event, None).await,
            WakeDecision::Skip(SkipReason::Unresolved("channel_type_unresolved"))
        );
    }

    #[tokio::test]
    async fn untagged_control_command_is_never_delivered() {
        let w = world();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human]);
        for command in CONTROL_COMMANDS {
            let event = message_with(&w.human, &[], None, &format!(" {command}\n"), &[]);
            assert_eq!(
                run(&lookup, &w, &event, false).await,
                WakeDecision::Skip(SkipReason::UntaggedControl),
                "{command}"
            );
        }
    }

    #[tokio::test]
    async fn untagged_workflow_message_does_not_wake() {
        let w = world();
        let lookup = base(&w).members(w.channel, &[&w.me, &w.human]);
        let event = message_with(&w.human, &[], None, "run", &[["buzz:workflow", "true"]]);
        assert_eq!(
            run(&lookup, &w, &event, false).await,
            WakeDecision::Skip(SkipReason::Workflow)
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
        let lookup = FakeLookup::default().human(&w.human);
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

    struct SlowLookup;

    impl WakeLookup for SlowLookup {
        async fn classify(&self, _: &[String]) -> HashMap<String, Principal> {
            tokio::time::sleep(DECISION_DEADLINE * 2).await;
            HashMap::new()
        }
        async fn thread_root(&self, _: &str) -> Option<RootFacts> {
            None
        }
        async fn channel_members(&self, _: Uuid) -> Option<Vec<String>> {
            None
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_relay_hits_the_decision_deadline() {
        let w = world();
        let event = message(&w.human, &[], None);
        let decision = decide_within_deadline(
            &SlowLookup,
            &event,
            w.channel,
            Some(false),
            &w.me.public_key().to_hex(),
        )
        .await;
        assert_eq!(
            decision,
            WakeDecision::Skip(SkipReason::Unresolved("decision_timeout"))
        );
    }

    // ── RelayWakeLookup caching, against a relay that refuses connections ──

    fn dead_relay_lookup(me: &Keys) -> RelayWakeLookup {
        let rest = RestClient {
            http: reqwest::Client::new(),
            base_url: "http://127.0.0.1:9".into(),
            keys: Keys::generate(),
            auth_tag_json: None,
        };
        RelayWakeLookup::new(rest, &me.public_key().to_hex())
    }

    #[tokio::test]
    async fn a_dead_relay_classifies_nobody_except_self() {
        let me = Keys::generate();
        let lookup = dead_relay_lookup(&me);
        let human = Keys::generate().public_key().to_hex();
        let found = lookup
            .classify(&[human.clone(), me.public_key().to_hex().to_uppercase()])
            .await;
        assert_eq!(found.get(&human), None, "a failed lookup is not human");
        assert_eq!(
            found.get(&me.public_key().to_hex()),
            Some(&Principal::Agent)
        );
    }

    #[test]
    fn failures_are_remembered_as_unresolved_never_as_a_classification() {
        let me = Keys::generate();
        let lookup = dead_relay_lookup(&me);
        let (known, unknown) = ("aa".to_string(), "bb".to_string());
        lookup.remember(
            &[known.clone(), unknown.clone()],
            &HashMap::from([(known.clone(), Principal::Human)]),
        );
        let (cached, pending) = lookup.split_cached(&[known.clone(), unknown.clone()]);
        assert_eq!(cached, HashMap::from([(known, Principal::Human)]));
        // Within UNRESOLVED_TTL the failure is neither re-queried nor answered.
        assert!(pending.is_empty());
        assert!(!cached.contains_key(&unknown));
    }

    // ── Relay answer parsing ──────────────────────────────────────────────

    fn profile(keys: &Keys, auth: Option<&Keys>, created_at: u64) -> Value {
        let tags = match auth {
            Some(owner) => vec![serde_json::from_str::<Value>(
                &buzz_sdk::nip_oa::compute_auth_tag(owner, &keys.public_key(), "").unwrap(),
            )
            .unwrap()],
            None => vec![],
        };
        serde_json::json!({
            "pubkey": keys.public_key().to_hex(),
            "created_at": created_at,
            "tags": tags,
        })
    }

    #[test]
    fn bot_role_in_any_member_list_marks_an_agent() {
        let lists = vec![
            serde_json::json!({ "tags": [
                ["d", "room-a"],
                ["p", "AA", "", "owner"],
                ["p", "bb", "", "admin"],
            ]}),
            serde_json::json!({ "tags": [
                ["d", "room-b"],
                ["p", "aa", "", "bot"],
                ["p", "cc", "", "member"],
                ["p", "dd"],
            ]}),
        ];
        // `aa` owns room-a but is a bot in room-b: an agent everywhere.
        assert_eq!(bot_pubkeys(&lists), Some(HashSet::from(["aa".to_string()])));
    }

    #[test]
    fn a_full_page_of_member_lists_is_not_trusted() {
        let lists = vec![serde_json::json!({ "tags": [] }); RELAY_QUERY_CAP];
        assert_eq!(bot_pubkeys(&lists), None);
    }

    #[test]
    fn classify_from_uses_bots_profiles_and_never_guesses() {
        let (bot, attested, plain, absent, owner) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let hex = |k: &Keys| k.public_key().to_hex();
        let pending = [hex(&bot), hex(&attested), hex(&plain), hex(&absent)];
        let bots = HashSet::from([hex(&bot)]);
        let profiles = vec![
            profile(&attested, Some(&owner), 1),
            profile(&plain, None, 1),
        ];

        let found = classify_from(&pending, Some(&bots), &profiles);
        assert_eq!(found.get(&hex(&bot)), Some(&Principal::Agent));
        assert_eq!(found.get(&hex(&attested)), Some(&Principal::Agent));
        assert_eq!(found.get(&hex(&plain)), Some(&Principal::Human));
        assert_eq!(found.get(&hex(&absent)), None, "no profile is not human");

        // Without the bot-role answer nobody can be shown human.
        let found = classify_from(&pending, None, &profiles);
        assert_eq!(found.get(&hex(&attested)), Some(&Principal::Agent));
        assert_eq!(found.get(&hex(&plain)), None);
        assert_eq!(found.get(&hex(&bot)), None);
    }

    #[test]
    fn classify_from_reads_the_newest_profile() {
        let (agent, owner) = (Keys::generate(), Keys::generate());
        let pk = agent.public_key().to_hex();
        let bots = HashSet::new();
        let newest_plain = vec![profile(&agent, Some(&owner), 1), profile(&agent, None, 2)];
        assert_eq!(
            classify_from(std::slice::from_ref(&pk), Some(&bots), &newest_plain).get(&pk),
            Some(&Principal::Human)
        );
        let newest_attested = vec![profile(&agent, None, 1), profile(&agent, Some(&owner), 2)];
        assert_eq!(
            classify_from(std::slice::from_ref(&pk), Some(&bots), &newest_attested).get(&pk),
            Some(&Principal::Agent)
        );
    }

    #[test]
    fn members_come_from_the_newest_list() {
        let (a, b) = (Keys::generate(), Keys::generate());
        let lists = vec![
            serde_json::json!({ "created_at": 1, "tags": [["p", a.public_key().to_hex(), "", "bot"]] }),
            serde_json::json!({ "created_at": 2, "tags": [
                ["d", "room"],
                ["p", b.public_key().to_hex(), "", "member"],
                ["p", "not-hex", "", "member"],
            ]}),
        ];
        assert_eq!(
            members_from_lists(&lists),
            Some(vec![b.public_key().to_hex()])
        );
        assert_eq!(members_from_lists(&[]), None);
    }

    #[test]
    fn root_facts_are_lowercased() {
        let root = serde_json::json!({
            "pubkey": "ABCD",
            "tags": [["p", "EF01"], ["e", "1234"]],
        });
        assert_eq!(
            root_from_event(&root),
            Some(RootFacts {
                author: "abcd".into(),
                tagged: vec!["ef01".into()],
            })
        );
    }

    #[test]
    fn attestation_check_accepts_an_owner_signed_auth_tag() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let profile = profile(&agent, Some(&owner), 1);
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
