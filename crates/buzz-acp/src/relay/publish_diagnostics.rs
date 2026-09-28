//! Bounded, content-free evidence between ephemeral enqueue and relay admission.
use nostr::EventId;
use std::time::Duration;
use tokio::time::Instant;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

pub(super) struct PublishDiagnostics {
    category: &'static str,
    last_sample: Option<Instant>,
    pending: Option<(EventId, Instant)>,
}
impl PublishDiagnostics {
    pub(super) fn new(category: &'static str) -> Self {
        Self {
            category,
            last_sample: None,
            pending: None,
        }
    }
    pub(super) fn sample(&mut self) -> bool {
        self.expire();
        if self.pending.is_some() {
            return false;
        }
        let now = Instant::now();
        if self
            .last_sample
            .is_some_and(|last| now.duration_since(last) < SAMPLE_INTERVAL)
        {
            return false;
        }
        self.last_sample = Some(now);
        true
    }

    pub(super) fn sent(&mut self, id: EventId) {
        self.pending = Some((id, Instant::now()));
        tracing::info!(category=self.category, event_id=%id, "Relay publish sample written to relay socket; admission not yet proven");
    }

    pub(super) fn dropped(&self, id: EventId, reason: &'static str) {
        tracing::warn!(category=self.category, event_id=%id, reason, "Relay publish sample not written to relay socket");
    }

    pub(super) fn write_uncertain(&self, id: EventId) {
        tracing::warn!(category=self.category, event_id=%id, "Relay publish sample socket write not confirmed; outcome unknown");
    }

    pub(super) fn acknowledge(&mut self, id: &str, accepted: bool, reason: &str) {
        let Ok(id) = EventId::from_hex(id) else {
            return;
        };
        if self.pending.is_none_or(|(expected, _)| expected != id) {
            return;
        }
        self.pending = None;
        // Correlation limits this to a sampled typing/observer EVENT, never
        // AUTH. Bound even a misbehaving relay's explanation; log no content.
        let reason: String = reason.chars().take(160).collect();
        tracing::info!(category=self.category, event_id=%id, accepted, reason, "Relay publish sample relay acknowledgement; recipient delivery not yet proven");
    }

    pub(super) fn expire(&mut self) {
        if let Some((id, sent)) = self.pending {
            if sent.elapsed() >= SAMPLE_INTERVAL {
                self.pending = None;
                tracing::warn!(category=self.category, event_id=%id, "Relay publish sample relay acknowledgement timed out; outcome unknown");
            }
        }
    }
}

#[cfg(test)]
mod tests;
