//! The bounded memory queue is a cache of accepted durable inputs in Serve mode.

use super::*;
use anyhow::{Context, Result};
use serde_json::json;

impl EventQueue {
    pub(crate) fn cancel_durable_pending(&mut self, scope: &SessionScope) -> Result<()> {
        if let Some(state) = &self.durable {
            state.cancel_queued(scope)?;
            self.queues.remove(scope);
            self.cancelled_batches.remove(scope);
            self.cancel_reasons.remove(scope);
            self.retry_after.remove(scope);
        }
        Ok(())
    }

    pub(crate) fn with_durable(mut self, state: Option<crate::bridge_state::BridgeState>) -> Self {
        self.durable = state;
        self
    }

    pub(super) fn push_journaled(&mut self, event: QueuedEvent) -> bool {
        let state = self.durable.as_ref().expect("durable queue");
        match state.accept_event(
            &event.scope,
            &event.event.id.to_hex(),
            &json!({"event": event.event, "prompt_tag": event.prompt_tag}),
        ) {
            Ok(false) => false,
            Ok(true) => {
                // Full memory queues retain the input on disk for the refill tick.
                self.cache_durable(event);
                true
            }
            Err(error) => {
                tracing::error!(%error, "durable input admission failed; event not accepted");
                false
            }
        }
    }

    fn cache_durable(&mut self, event: QueuedEvent) {
        let scope = &event.scope;
        if self.in_flight_scopes.contains(scope)
            || self.channel_event_total(event.channel_id) >= MAX_PENDING_PER_CHANNEL
            || self.queues.get(scope).is_some_and(|q| {
                q.len() >= MAX_PENDING_PER_SCOPE
                    || q.iter().any(|old| old.event.id == event.event.id)
            })
        {
            return;
        }
        self.queues
            .entry(event.scope.clone())
            .or_default()
            .push_back(event);
    }

    pub(crate) fn refill_durable(&mut self, subscribed: &HashSet<Uuid>) -> Result<()> {
        let Some(state) = &self.durable else {
            return Ok(());
        };
        for (scope, input) in state.queued_inputs()? {
            if !subscribed.contains(&scope.channel_id()) {
                continue;
            }
            let event = serde_json::from_value(input.payload["event"].clone())
                .context("invalid durable Buzz event")?;
            let prompt_tag = input.payload["prompt_tag"]
                .as_str()
                .context("missing durable admission rule")?
                .to_owned();
            self.cache_durable(QueuedEvent {
                channel_id: scope.channel_id(),
                scope,
                event,
                prompt_tag,
                received_at: Instant::now(),
            });
        }
        Ok(())
    }

    pub(super) fn durable_scope_ready(&self, scope: &SessionScope) -> bool {
        let Some(state) = &self.durable else {
            return true;
        };
        match state.scope_blocked(scope) {
            Ok(blocked) => !blocked,
            Err(error) => {
                tracing::error!(%error, "cannot verify durable scope; dispatch blocked");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
