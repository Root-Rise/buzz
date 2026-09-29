//! Translate one profile's session-addressed Serve stream into Buzz's existing activity wire.
use crate::{
    observer::{ObserverContext, ObserverHandle},
    scope::SessionScope,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
};

const MAX_ROUTES: usize = 2048;
const MAX_PENDING: usize = 512;
const MAX_PENDING_BYTES: usize = 4 * 1024 * 1024;
const DETAIL_BYTES: usize = 8192;

/// Kept across observation socket reconnects; never shared between agent identities.
pub(crate) struct ServeObserver {
    observer: ObserverHandle,
    routes: HashMap<String, Route>,
}

struct Route {
    context: ObserverContext,
    epoch: String,
    last: u64,
    generation: u64,
    segment: u64,
    streamed: bool,
    buffering: bool,
    pending: BTreeMap<u64, Value>,
    pending_bytes: usize,
    gap: bool,
    resync: bool,
    seen: BTreeMap<u64, u64>,
}

impl ServeObserver {
    pub(crate) fn new(observer: ObserverHandle) -> Self {
        Self {
            observer,
            routes: HashMap::new(),
        }
    }

    /// Establish routing from the durable binding, never from a pool slot's latest prompt.
    pub(crate) fn begin(
        &mut self,
        runtime: &str,
        stored: &str,
        scope: &SessionScope,
    ) -> anyhow::Result<u64> {
        self.routes.retain(|key, route| {
            key == runtime || route.context.session_id.as_deref() != Some(stored)
        });
        anyhow::ensure!(
            self.routes.contains_key(runtime) || self.routes.len() < MAX_ROUTES,
            "Serve activity route capacity exceeded"
        );
        let route = self
            .routes
            .entry(runtime.to_owned())
            .or_insert_with(|| Route {
                context: ObserverContext {
                    channel_id: Some(scope.channel_id().to_string()),
                    session_id: Some(stored.to_owned()),
                    turn_id: None,
                    started_at: None,
                },
                epoch: String::new(),
                last: 0,
                generation: 0,
                segment: 0,
                streamed: false,
                buffering: false,
                pending: BTreeMap::new(),
                pending_bytes: 0,
                gap: false,
                resync: false,
                seen: BTreeMap::new(),
            });
        anyhow::ensure!(
            route.context.session_id.as_deref() == Some(stored)
                && route.context.channel_id.as_deref()
                    == Some(scope.channel_id().to_string().as_str()),
            "Serve activity session ownership changed"
        );
        route.buffering = true;
        Ok(route.last)
    }

    pub(crate) fn needs_replay(&self, runtime: &str) -> bool {
        self.routes
            .get(runtime)
            .is_none_or(|route| route.resync || route.buffering)
    }

    pub(crate) fn needs_reset(&self, runtime: &str, replay: &Value) -> bool {
        self.routes.get(runtime).is_some_and(|route| {
            (!route.epoch.is_empty() && replay["epoch"].as_str() != Some(route.epoch.as_str()))
                || replay["latest_seq"]
                    .as_u64()
                    .is_some_and(|seq| seq < route.last)
                || replay["events"].as_array().is_some_and(|events| {
                    events.iter().any(|event| {
                        event["seq"]
                            .as_u64()
                            .and_then(|seq| route.seen.get(&seq))
                            .is_some_and(|old| *old != fingerprint(event))
                    })
                })
        })
    }

    /// Merge replay before live frames, since live events may arrive while the RPC is outstanding.
    pub(crate) fn finish(&mut self, runtime: &str, replay: Value, reset: bool) {
        let Some(route) = self.routes.get_mut(runtime) else {
            return;
        };
        if reset {
            route.last = 0;
            route.generation += 1;
            route.streamed = false;
            route.context.turn_id = None;
            route.seen.clear();
            route.pending.clear();
        }
        route.epoch = replay["epoch"].as_str().unwrap_or_default().to_owned();
        if reset || route.gap || replay["truncated"] == true {
            notice(
                &self.observer,
                route,
                "Some activity history is unavailable; showing retained events.",
            );
        }
        let mut events = BTreeMap::new();
        if let Some(frames) = replay["events"].as_array() {
            for event in frames.iter().take(MAX_PENDING) {
                if event["session_id"] == runtime {
                    if let Some(seq) = event["seq"].as_u64() {
                        events.insert(seq, event.clone());
                    }
                }
            }
        }
        events.append(&mut route.pending);
        route.pending_bytes = 0;
        route.buffering = false;
        route.resync = reset;
        route.gap = false;
        for (seq, event) in events {
            if seq > route.last {
                project(&self.observer, route, runtime, &event);
                route.last = seq;
                remember(route, seq, &event);
            }
        }
    }

    pub(crate) fn frame(&mut self, frame: &Value) {
        if frame["method"] != "event" {
            return;
        }
        let event = &frame["params"];
        let (Some(runtime), Some(seq)) = (event["session_id"].as_str(), event["seq"].as_u64())
        else {
            return;
        };
        let pending_total: usize = self.routes.values().map(|route| route.pending_bytes).sum();
        let Some(route) = self.routes.get_mut(runtime) else {
            return;
        };
        if route.buffering {
            let bytes = event.to_string().len();
            if route.pending.len() < MAX_PENDING && pending_total + bytes <= MAX_PENDING_BYTES {
                if let std::collections::btree_map::Entry::Vacant(entry) = route.pending.entry(seq)
                {
                    entry.insert(event.clone());
                    route.pending_bytes += bytes;
                }
            } else {
                route.gap = true;
            }
            return;
        }
        // Emitters can race after assigning seq. A gap or lower sequence needs
        // authoritative replay, not a guessed server restart from arrival order.
        if route.seen.get(&seq) == Some(&fingerprint(event)) {
            return;
        }
        if seq <= route.last || seq > route.last + 1 {
            route.buffering = true;
            route.resync = true;
            let bytes = event.to_string().len();
            if pending_total + bytes <= MAX_PENDING_BYTES {
                route.pending_bytes = bytes;
                route.pending.insert(seq, event.clone());
            } else {
                route.gap = true;
            }
            return;
        }
        if seq == route.last {
            return;
        }
        project(&self.observer, route, runtime, event);
        route.last = seq;
        remember(route, seq, event);
    }
}

fn fingerprint(event: &Value) -> u64 {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    event.to_string().hash(&mut hash);
    hash.finish()
}
fn remember(route: &mut Route, seq: u64, event: &Value) {
    route.seen.insert(seq, fingerprint(event));
    while route.seen.len() > MAX_PENDING {
        route.seen.pop_first();
    }
}

fn bounded(text: &str) -> String {
    if text.len() <= DETAIL_BYTES {
        return text.to_owned();
    }
    let mut end = DETAIL_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[Activity preview truncated; full output remains in Hermes]",
        &text[..end]
    )
}

fn detail(value: &Value) -> Value {
    if value.to_string().len() <= DETAIL_BYTES {
        value.clone()
    } else {
        json!({"preview": bounded(&value.to_string()), "truncated": true})
    }
}

fn detail_context(route: &Route) -> ObserverContext {
    ObserverContext {
        channel_id: route.context.channel_id.clone(),
        session_id: route.context.session_id.clone(),
        turn_id: None,
        started_at: None,
    }
}
fn update(observer: &ObserverHandle, route: &Route, value: Value) {
    observer.emit("acp_read", None, &detail_context(route),
        json!({"method":"session/update", "params":{"sessionId":route.context.session_id,"update":value}}));
}

fn notice(observer: &ObserverHandle, route: &Route, text: &str) {
    // Existing clients render explicit free-form status records in the activity feed.
    observer.emit(
        "acp_read",
        None,
        &detail_context(route),
        json!({"status":"observation_notice","title":"Activity history incomplete","text":text}),
    );
}

fn text_update(observer: &ObserverHandle, route: &Route, runtime: &str, kind: &str, text: &str) {
    if text.is_empty() {
        return;
    }
    update(
        observer,
        route,
        json!({"sessionUpdate":kind,
        "messageId":format!("{}:{}:{}:{}",runtime,route.generation,route.context.turn_id.as_deref().unwrap_or("observed"),route.segment),
        "content":{"type":"text","text":bounded(text)}}),
    );
}

fn project(observer: &ObserverHandle, route: &mut Route, runtime: &str, event: &Value) {
    let kind = event["type"].as_str().unwrap_or_default();
    let payload = &event["payload"];
    let seq = event["seq"].as_u64().unwrap_or_default();
    if kind == "message.start" || route.context.turn_id.is_none() {
        route.context.turn_id = Some(format!("serve:{runtime}:{}:{seq}", route.generation));
        route.context.started_at = Some(chrono::Utc::now().to_rfc3339());
        route.segment = seq;
        route.streamed = false;
    }
    match kind {
        // The inventory monitor alone owns live lifecycle indicators. Replayed
        // historical starts must never make an idle agent appear to be working.
        "message.start" => {}
        "message.delta" => {
            text_update(
                observer,
                route,
                runtime,
                "agent_message_chunk",
                payload["text"].as_str().unwrap_or_default(),
            );
            route.streamed = true;
        }
        "reasoning.delta" | "thinking.delta" | "reasoning.available" => {
            text_update(
                observer,
                route,
                runtime,
                "agent_thought_chunk",
                payload["text"].as_str().unwrap_or_default(),
            );
        }
        "message.interim" => {
            if payload["already_streamed"] != true && !route.streamed {
                text_update(
                    observer,
                    route,
                    runtime,
                    "agent_message_chunk",
                    payload["text"].as_str().unwrap_or_default(),
                );
            }
            route.segment = seq;
            route.streamed = false;
        }
        "tool.start" | "tool.complete" => {
            let Some(id) = payload["tool_id"].as_str() else {
                return;
            };
            let name = payload["name"].as_str().unwrap_or("tool");
            let done = kind == "tool.complete";
            let result = payload
                .get("result")
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or_else(|| payload["result_text"].clone());
            let failed = result["exit_code"].as_i64().is_some_and(|code| code != 0)
                || result["is_error"] == true
                || result["error"].as_str().is_some_and(|s| !s.is_empty());
            update(
                observer,
                route,
                json!({"sessionUpdate":if done {"tool_call_update"} else {"tool_call"},
                "toolCallId":format!("{runtime}:{}:{id}",route.generation),"title":name,"toolName":name,
                "status":if !done {"in_progress"} else if failed {"failed"} else {"completed"},
                "rawInput":detail(&payload["args"]),"rawOutput":if done {detail(&result)} else {Value::Null}}),
            );
        }
        "message.complete" => {
            if !route.streamed && payload["response_previewed"] != true {
                text_update(
                    observer,
                    route,
                    runtime,
                    "agent_message_chunk",
                    payload["text"].as_str().unwrap_or_default(),
                );
            }
            if payload["status"] == "error" {
                observer.emit("acp_read",None,&detail_context(route),
                    json!({"status":"error", "title":"Turn error", "text":bounded(payload.get("failure_reason").and_then(Value::as_str).or_else(||payload.get("error").and_then(Value::as_str)).unwrap_or("Agent turn failed"))}));
            }
        }
        "error" => {
            observer.emit("acp_read",None,&detail_context(route),
            json!({"status":"error","title":"Turn error","text":bounded(payload["message"].as_str().unwrap_or("Agent reported an error"))}));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
