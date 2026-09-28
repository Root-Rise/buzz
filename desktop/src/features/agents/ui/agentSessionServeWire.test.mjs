import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  syncAgentTurnsFromEvents,
  getActiveTurnsForAgent,
  resetActiveAgentTurnsStore,
} from "../activeAgentTurnsStore.ts";
import { buildTranscript } from "./agentSessionTranscript.ts";
import { isMeaningfulItem } from "./agentSessionTranscriptPresentation.ts";

// Run via scripts/test-serve-activity.sh: input comes from the real Rust recovery
// socket and publisher queue, so a wire-shape regression cannot pass a UI mock.
test("Serve recovery wire renders text and one completed tool in the existing UI", {
  skip: !process.env.BUZZ_OBSERVER_TEST_CAPTURE,
}, () => {
  const frames = JSON.parse(
    readFileSync(process.env.BUZZ_OBSERVER_TEST_CAPTURE, "utf8"),
  );
  const events = frames.flatMap((frame) =>
    frame.kind === "batch" ? frame.payload.events : [frame],
  );
  resetActiveAgentTurnsStore();
  syncAgentTurnsFromEvents("a".repeat(64), events);
  assert.equal(
    getActiveTurnsForAgent("a".repeat(64)).length,
    0,
    "historical activity must not revive a live badge",
  );
  const items = buildTranscript(events).filter(isMeaningfulItem);
  assert.deepEqual(
    items.filter((item) => item.type === "message").map((item) => item.text),
    ["Checking the tests.", "Checks passed."],
  );
  const tools = items.filter((item) => item.type === "tool");
  assert.equal(tools.length, 1);
  assert.equal(tools[0].status, "completed");
  assert.match(JSON.stringify(tools[0]), /synthetic/);
  assert.equal(
    items.some((item) => item.title === "Turn error"),
    false,
  );
  const notice = buildTranscript([
    {
      ...events[0],
      seq: 1000,
      kind: "acp_read",
      payload: {
        status: "observation_unavailable",
        title: "Activity unavailable",
        text: "Observation lost; agent outcome unknown.",
      },
    },
  ]).filter(isMeaningfulItem);
  assert.equal(
    notice.some((item) => item.title === "Activity unavailable"),
    true,
  );
});
