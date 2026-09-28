#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. ./bin/activate-hermit
capture="$(mktemp)"
trap 'rm -f "$capture"' EXIT
export BUZZ_OBSERVER_TEST_CAPTURE="$capture"
cargo test --locked -p buzz-acp --lib serve_observer
cd desktop
node --import ./test-loader.mjs --experimental-strip-types --test src/features/agents/ui/agentSessionServeWire.test.mjs
