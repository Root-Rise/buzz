//! apiary: in-channel progress reporting for long turns.
//!
//! When a channel turn starts using tools, post one threaded "working…"
//! message and edit it in place (throttled) with a rolling tail of tool
//! activity — the same UX Hermes's own Slack gateway renders. Turns that
//! never touch a tool post nothing, so quick answers stay clean.
//!
//! Errors are logged and swallowed throughout: progress must never take
//! down a turn.

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// One tool-call sighting forwarded from the ACP session-update stream.
pub struct ProgressEvent {
    pub title: String,
    pub kind: String,
}

const THROTTLE: Duration = Duration::from_secs(10);
const SHOWN: usize = 6;
const POST_TIMEOUT: Duration = Duration::from_secs(5);

fn glyph(kind: &str) -> &'static str {
    match kind {
        "read" => "📖",
        "edit" | "delete" | "move" => "✏️",
        "execute" => "💻",
        "search" | "fetch" => "🔍",
        "think" => "💭",
        _ => "·",
    }
}

fn render(lines: &VecDeque<String>, total: usize, elapsed: Duration, done: bool) -> String {
    let secs = elapsed.as_secs();
    let mut s = if done {
        format!("✅ **done** — {total} steps · {secs}s")
    } else {
        format!("⚙️ **working…** — {total} steps · {secs}s")
    };
    for l in lines {
        s.push_str("\n› ");
        s.push_str(l);
    }
    s
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
/// sink) is cleared on turn completion.
pub fn spawn_reporter(
    rest: crate::relay::RestClient,
    channel_id: Uuid,
    thread_root_hex: Option<String>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<ProgressEvent>,
) {
    tokio::spawn(async move {
        // Nothing until the first tool call — tool-free turns stay silent.
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
        let mut lines: VecDeque<String> = VecDeque::new();
        let mut total = 0usize;
        let mut push = |lines: &mut VecDeque<String>, total: &mut usize, e: &ProgressEvent| {
            *total += 1;
            let l = format!("{} {}", glyph(&e.kind), e.title);
            if lines.back().map(|b| b != &l).unwrap_or(true) {
                lines.push_back(l);
                if lines.len() > SHOWN {
                    lines.pop_front();
                }
            }
        };
        push(&mut lines, &mut total, &first);

        let content = render(&lines, total, started.elapsed(), false);
        let builder = match buzz_sdk::build_message(channel_id, &content, tref.as_ref(), &[], false, &[])
        {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!("progress post: build failed: {e}");
                return;
            }
        };
        let msg_id = match submit(&rest, builder, "post").await {
            Some(id) => id,
            None => return,
        };

        let mut last_edit = Instant::now();
        while let Some(e) = rx.recv().await {
            push(&mut lines, &mut total, &e);
            if last_edit.elapsed() >= THROTTLE {
                let content = render(&lines, total, started.elapsed(), false);
                if let Ok(b) = buzz_sdk::build_edit(channel_id, msg_id, &content) {
                    let _ = submit(&rest, b, "edit").await;
                }
                last_edit = Instant::now();
            }
        }
        // Sender cleared — turn is over. Final state.
        let content = render(&lines, total, started.elapsed(), true);
        if let Ok(b) = buzz_sdk::build_edit(channel_id, msg_id, &content) {
            let _ = submit(&rest, b, "final").await;
        }
    });
}
