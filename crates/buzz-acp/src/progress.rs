//! apiary: in-channel progress reporting for long turns.
//!
//! Renders the agent's working narrative — interleaved intent (reasoning
//! summaries), speech, and actions (tool calls) — into one threaded message
//! that is edited in place, the same UX Hermes's own Slack gateway renders.
//! Turns that produce no activity post nothing, so quick answers stay clean.
//!
//! Streamed text (`agent_thought_chunk`, `agent_message_chunk`) arrives
//! token-by-token, so text is ACCUMULATED and flushed as one line when the
//! agent switches mode (starts a tool call, or switches think<->say) or when
//! the turn ends. Without that, a single sentence would become dozens of
//! one-token lines.
//!
//! Errors are logged and swallowed throughout: progress must never take
//! down a turn.

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// One activity sighting forwarded from the ACP session-update stream.
/// `kind` is a tool kind (`read`/`edit`/`execute`/…) for actions, or the
/// pseudo-kinds `think` (reasoning summary) / `say` (agent prose).
pub struct ProgressEvent {
    pub title: String,
    pub kind: String,
}

const THROTTLE: Duration = Duration::from_secs(10);
const SHOWN: usize = 8;
const MAX_LINE: usize = 160;
const POST_TIMEOUT: Duration = Duration::from_secs(5);

fn glyph(kind: &str) -> &'static str {
    match kind {
        "read" => "📖",
        "edit" | "delete" | "move" => "✏️",
        "execute" => "💻",
        "search" | "fetch" => "🔍",
        "think" => "💭",
        "say" => "💬",
        _ => "·",
    }
}

fn is_text(kind: &str) -> bool {
    kind == "think" || kind == "say"
}

/// Rolling activity log with text accumulation.
struct Log {
    lines: VecDeque<String>,
    steps: usize,
    /// Streamed text not yet flushed into a line: (kind, accumulated).
    pending: Option<(String, String)>,
}

impl Log {
    fn new() -> Self {
        Self { lines: VecDeque::new(), steps: 0, pending: None }
    }

    fn emit(&mut self, kind: &str, text: &str) {
        let t = text.trim();
        if t.is_empty() {
            return;
        }
        let mut line = format!("{} {}", glyph(kind), t);
        if line.chars().count() > MAX_LINE {
            line = line.chars().take(MAX_LINE).collect::<String>() + "…";
        }
        // Collapse consecutive duplicates (chatty tools repeat titles).
        if self.lines.back().map(|b| b != &line).unwrap_or(true) {
            self.lines.push_back(line);
            if self.lines.len() > SHOWN {
                self.lines.pop_front();
            }
        }
        self.steps += 1;
    }

    /// Flush accumulated streamed text into a line.
    fn flush(&mut self) {
        if let Some((kind, text)) = self.pending.take() {
            self.emit(&kind, &text);
        }
    }

    fn ingest(&mut self, e: &ProgressEvent) {
        if is_text(&e.kind) {
            match &mut self.pending {
                // Same mode: keep accumulating this sentence.
                Some((k, buf)) if *k == e.kind => {
                    if buf.chars().count() < MAX_LINE * 2 {
                        buf.push_str(&e.title);
                    }
                }
                // Mode switch: the previous thought/utterance is complete.
                _ => {
                    self.flush();
                    self.pending = Some((e.kind.clone(), e.title.clone()));
                }
            }
        } else {
            // An action means any narration that preceded it is complete.
            self.flush();
            self.emit(&e.kind, &e.title);
        }
    }

    fn render(&self, elapsed: Duration, done: bool) -> String {
        let secs = elapsed.as_secs();
        let mut s = if done {
            format!("✅ **done** — {} steps · {secs}s", self.steps)
        } else {
            format!("⚙️ **working…** — {} steps · {secs}s", self.steps)
        };
        for l in &self.lines {
            s.push_str("\n› ");
            s.push_str(l);
        }
        s
    }
}

async fn submit(
    rest: &crate::relay::RestClient,
    builder: nostr::EventBuilder,
    what: &str,
) -> Option<nostr::EventId> {
    let event = match builder.sign_with_keys(&rest.keys) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!("progress {what}: sign failed: {e}");
            return None;
        }
    };
    let id = event.id;
    match tokio::time::timeout(POST_TIMEOUT, rest.submit_event(&event)).await {
        Ok(Ok(_)) => Some(id),
        Ok(Err(e)) => {
            tracing::debug!("progress {what} failed: {e}");
            None
        }
        Err(_) => {
            tracing::debug!("progress {what} timed out");
            None
        }
    }
}

/// Spawn the per-turn reporter. Ends when the sender side (the ACP client's
/// sink) is cleared on turn completion. `thread_root_hex` is `None` in DMs,
/// which read as flat conversations.
pub fn spawn_reporter(
    rest: crate::relay::RestClient,
    channel_id: Uuid,
    thread_root_hex: Option<String>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<ProgressEvent>,
) {
    tokio::spawn(async move {
        // Nothing until the first activity — silent turns stay silent.
        let first = match rx.recv().await {
            Some(e) => e,
            None => return,
        };
        // parent == root (flat threads); None in DMs (flat conversation).
        let tref = thread_root_hex
            .as_deref()
            .and_then(|h| nostr::EventId::from_hex(h).ok())
            .map(|root_id| buzz_sdk::ThreadRef {
                root_event_id: root_id,
                parent_event_id: root_id,
            });

        let started = Instant::now();
        let mut log = Log::new();
        log.ingest(&first);

        // Hold the first post until there is something to say: a lone
        // in-flight text fragment renders as an empty log.
        let mut msg_id: Option<nostr::EventId> = None;
        let mut last_edit = Instant::now();

        while let Some(e) = rx.recv().await {
            log.ingest(&e);

            if msg_id.is_none() {
                if log.lines.is_empty() {
                    continue;
                }
                let content = log.render(started.elapsed(), false);
                let builder =
                    match buzz_sdk::build_message(channel_id, &content, tref.as_ref(), &[], false, &[])
                    {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::debug!("progress post: build failed: {e}");
                            return;
                        }
                    };
                msg_id = submit(&rest, builder, "post").await;
                if msg_id.is_none() {
                    return;
                }
                last_edit = Instant::now();
                continue;
            }

            if last_edit.elapsed() >= THROTTLE {
                let content = log.render(started.elapsed(), false);
                if let (Some(id), Ok(b)) =
                    (msg_id, buzz_sdk::build_edit(channel_id, msg_id.unwrap(), &content))
                {
                    let _ = id;
                    let _ = submit(&rest, b, "edit").await;
                }
                last_edit = Instant::now();
            }
        }

        // Sender cleared — turn is over. Flush any trailing narration.
        log.flush();
        let Some(id) = msg_id else {
            return;
        };
        let content = log.render(started.elapsed(), true);
        if let Ok(b) = buzz_sdk::build_edit(channel_id, id, &content) {
            let _ = submit(&rest, b, "final").await;
        }
    });
}
