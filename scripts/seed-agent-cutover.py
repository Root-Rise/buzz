#!/usr/bin/env python3
"""Seed an audited preactivation replay boundary, never dispatch old requests.

Manifest version 1: community, agent_pubkey, replay_floor (Unix seconds),
executed_inputs [{event_id, scope, source_session_id, source_message_id}],
observed_controls [event_id]. Inputs must be actual triggering Buzz events,
NOT hydrated conversation history. The old bridge's 'delivered Buzz events'
log includes both and cannot be used without intersecting actual trigger IDs.
Seeds use status 'historical': evidence of prior submission, not model success.

Stop the old bridge, drain/check external jobs, refresh history/routing audit,
then prepare and review this manifest. Choose the OLD effective startup replay
watermark to retain its unpersisted backlog; a recent arbitrary overlap can
lose older pending work. Review controls separately: consumed owner controls
do not appear in agent transcripts. Keep both backends stopped until migration
and this receipt succeed. This tool does not prove the manifest's evidence.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sqlite3


SPEC = importlib.util.spec_from_file_location("binding_transfer", Path(__file__).with_name("transfer-agent-binding.py"))
binding = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(binding)


def seed(path, manifest, *, apply=False, drained=False, controls_reviewed=False):
    if apply and not (drained and controls_reviewed):
        raise ValueError("Apply requires stopped/drained agent and independently reviewed control-event overlap")
    if manifest.get("version") != 1:
        raise ValueError("Unsupported manifest version")
    floor = manifest.get("replay_floor")
    if not isinstance(floor, int) or isinstance(floor, bool) or floor < 0:
        raise ValueError("Explicit nonnegative Unix replay_floor required")
    ns = binding.namespace(manifest["community"], manifest["agent_pubkey"])
    inputs, controls = manifest.get("executed_inputs"), manifest.get("observed_controls")
    if not isinstance(inputs, list) or not isinstance(controls, list):
        raise ValueError("Explicit executed_inputs and observed_controls lists required")
    ids = set()
    for entry in inputs:
        binding.validate_scope(entry["scope"])
        if (not isinstance(entry.get("source_session_id"), str) or not entry["source_session_id"]
                or not isinstance(entry.get("source_message_id"), int) or entry["source_message_id"] <= 0):
            raise ValueError("Each executed input requires audited source session/message provenance")
        event_id = entry["event_id"]
        if not isinstance(event_id, str) or not re.fullmatch(r"[0-9a-f]{64}", event_id) or event_id in ids:
            raise ValueError("Invalid or duplicate executed event ID")
        ids.add(event_id)
    for event_id in controls:
        if not isinstance(event_id, str) or not re.fullmatch(r"[0-9a-f]{64}", event_id) or event_id in ids:
            raise ValueError("Invalid, duplicate or input-overlapping control ID")
        ids.add(event_id)
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    marker = "audited-cutover:" + digest
    path = Path(path).resolve()
    # Routing transfer creates the DB from the shared schema before this step.
    conn = sqlite3.connect(path.as_uri() + ("?mode=rw" if apply else "?mode=ro"), uri=True, timeout=5)
    try:
        conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        existing = conn.execute("SELECT name FROM migrations WHERE namespace=? AND name LIKE 'audited-cutover:%'", (ns,)).fetchall()
        if existing:
            if existing != [(marker,)]:
                raise ValueError("Different cutover manifest already applied")
            return {"applied": apply, "already_done": True, "manifest_sha256": digest}
        if conn.execute("SELECT 1 FROM inputs WHERE namespace=? LIMIT 1", (ns,)).fetchone():
            raise ValueError("Store already has live inputs; only an unused preactivation store can be seeded")
        if conn.execute("SELECT 1 FROM controls WHERE namespace=? LIMIT 1", (ns,)).fetchone():
            raise ValueError("Store already has controls; review instead of overwriting")
        previous = conn.execute("SELECT replay_floor FROM relay_progress WHERE namespace=?", (ns,)).fetchone()
        if previous is not None and previous[0] != floor:
            raise ValueError("Existing replay floor conflicts with audited boundary")
        receipt = {"applied": apply, "already_done": False, "manifest_sha256": digest,
                   "replay_floor": floor, "seeded_inputs": len(inputs), "seeded_controls": len(controls)}
        if not apply:
            return receipt
        for entry in inputs:
            conn.execute("INSERT INTO inputs(namespace,event_id,scope,payload,status) VALUES (?,?,?,?,'historical')",
                         (ns, entry["event_id"], entry["scope"], json.dumps({"migration_evidence": entry})))
        for event_id in controls:
            conn.execute("INSERT INTO controls(namespace,event_id,status) VALUES (?,?,'observed')", (ns, event_id))
        conn.execute("INSERT OR IGNORE INTO relay_progress(namespace,replay_floor) VALUES (?,?)", (ns, floor))
        conn.execute("INSERT INTO migrations(namespace,name) VALUES (?,?)", (ns, marker))
        conn.commit()
        return receipt
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--drained", action="store_true")
    parser.add_argument("--controls-reviewed", action="store_true")
    args = parser.parse_args()
    try:
        result = seed(args.state_db, json.loads(Path(args.manifest).read_text()),
                      apply=args.apply, drained=args.drained, controls_reviewed=args.controls_reviewed)
    except (ValueError, KeyError, sqlite3.Error, OSError) as error:
        parser.exit(2, f"Cutover seed refused; keep both bridges stopped: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
