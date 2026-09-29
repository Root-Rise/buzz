#!/usr/bin/env python3
"""Read-only metadata inventory for a drained ACP-to-Serve migration.

Never prints or exports prompt bodies, tool output, model configuration or
credentials. Writes only session/routing/event identifiers and timestamps to
the explicit output directory. Run once for planning and again after drain;
an audit made while the bridge runs is provisional. Journal delivery lists
include hydrated history, so they are never treated as executed event lists.
"""

import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time


UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
EVENT = r"[0-9a-f]{64}"
SKIP_TAGS = {"thread-context", "conversation-context", "what-you-were-working-on",
             "previous-request-interrupted-before-completion"}
EVENT_TAGS = {"buzz-event", "buzz-events", "new-message-arrived-while-you-were-working",
              "new-request-supersedes-previous"}


def _single_envelope(text, legacy_thread_channels=(), legacy_dm_channels=()):
    if set(legacy_thread_channels) & set(legacy_dm_channels):
        raise ValueError("A verified legacy channel cannot be both DM and thread-scoped")
    if not isinstance(text, str):
        return None
    match = re.search(r"(?:\A|\n)<context>\n(.*?)\n</context>", text, re.S)
    if not match:
        # Old Scope described reply routing, not ownership. The operator must
        # verify channel type before selecting thread or DM conversation policy.
        legacy = re.search(r"(?:\A|\n)\[Context\]\n", text)
        if not legacy:
            return None
        suffix = text[legacy.end():]
        boundary = suffix.find("Event ID:")
        header = suffix[:boundary] if boundary >= 0 else ""
        channel = re.search(r"^Channel: .*?(" + UUID + r")[^\n]*$", header, re.M)
        root = re.search(r"^Thread root: (" + EVENT + r")$", header, re.M)
        ids = re.findall(r"(?:\A|\n)Event ID: (" + EVENT + r")\nChannel: [^\n]+\nKind: \d+\nFrom: [^\n]+\nTime: [^\n]+\nContent:", suffix)
        if not channel or channel.group(1) not in {*legacy_thread_channels, *legacy_dm_channels} or len(ids) != 1:
            return None
        scope = ("conversation:" + channel.group(1) if channel.group(1) in legacy_dm_channels
                 else "thread:" + channel.group(1) + ":" + (root.group(1) if root else ids[0]))
        return {"scope": scope,
                "event_ids": ids, "error": None, "primed": "[Base]" in text[:legacy.start()]}
    header = match.group(1)
    channel = re.search(r"^Channel: .*?(" + UUID + r")[^\n]*$", header, re.M)
    kind = re.search(r"^Session scope: ([^\n]+)", header, re.M) or re.search(r"^Scope: ([^\n]+)", header, re.M)
    root = re.search(r"^Thread root: (" + EVENT + r")$", header, re.M)
    if not channel or not kind or kind.group(1) not in {"thread", "channel", "dm", "dm conversation"}:
        return None
    if kind.group(1) == "thread" and not root:
        return None
    scope = "thread:" + channel.group(1) + ":" + root.group(1) if kind.group(1) == "thread" else "conversation:" + channel.group(1)
    suffix = text[match.end():].strip()
    while (opening := re.match(r"<([a-z-]+)(?: [^>\n]*)?>\n", suffix)) and opening.group(1) in SKIP_TAGS:
        closing = "</" + opening.group(1) + ">"
        end = suffix.rfind(closing)
        if end < 0:
            return {"scope": scope, "event_ids": [], "error": "unclosed preceding context", "primed": False}
        suffix = suffix[end + len(closing):].strip()
    opening = re.match(r"<([a-z-]+)(?: [^>\n]*)?>\n", suffix)
    ids, error = [], None
    if not opening or opening.group(1) not in EVENT_TAGS:
        error = "unsupported direct event framing"
    else:
        tag = opening.group(1)
        closing = "</" + tag + ">"
        end = suffix.rfind(closing)
        body = suffix[opening.end():end] if end >= 0 else ""
        ids = re.findall(r"(?:\A|\n)Event ID: (" + EVENT + r")\nChannel: [^\n]+\nKind: \d+\nFrom: [^\n]+\nTime: [^\n]+\nContent:", body)
        count = re.search(r' count="(\d+)"', opening.group(0))
        expected = int(count.group(1)) if count else 1
        if end < 0 or len(ids) != expected or len(set(ids)) != len(ids):
            ids, error = [], "ambiguous direct event count; manual audit required"
    return {"scope": scope, "event_ids": ids, "error": error,
            "primed": "<base>" in text[:match.start()]}


def envelope(text, legacy_thread_channels=(), legacy_dm_channels=()):
    """Adjacent user-message canonicalization can join complete direct envelopes.

    Only accept a fully consumed sequence of individually valid, same-scope
    envelopes. Nested/quoted contexts and hydrated history remain non-triggers.
    """
    parsed = _single_envelope(text, legacy_thread_channels, legacy_dm_channels)
    if not parsed or parsed["error"] != "ambiguous direct event count; manual audit required":
        return parsed
    parts = re.split(r"\n\n(?=<context>\n)", text)
    if len(parts) < 2:
        return parsed
    combined, seen = [], set()
    for part in parts:
        item = _single_envelope(part, legacy_thread_channels, legacy_dm_channels)
        if not item or item["error"] or item["scope"] != parsed["scope"]:
            return parsed
        context = re.search(r"(?:\A|\n)<context>\n.*?\n</context>", part, re.S)
        suffix = part[context.end():].strip() if context else ""
        opening = re.match(r"<([a-z-]+)(?: [^>\n]*)?>\n", suffix)
        # Concatenation support is intentionally narrower than ordinary framing:
        # no history wrappers, embedded context/event tags, or leftover suffix.
        # Such rows remain manual audits instead of promoting quoted headers.
        if not opening or opening.group(1) not in EVENT_TAGS:
            return parsed
        tag = opening.group(1)
        closing = "</" + tag + ">"
        if not suffix.endswith(closing) or suffix.count(closing) != 1:
            return parsed
        body = suffix[opening.end():-len(closing)]
        if re.search(r"(?:\A|\n)</?(?:context|" + "|".join(sorted(EVENT_TAGS | SKIP_TAGS)) + r")(?:[ >])", body):
            return parsed
        if any(event in seen for event in item["event_ids"]):
            return parsed
        seen.update(item["event_ids"])
        combined.append(item)
    return {"scope": parsed["scope"], "event_ids": [event for item in combined for event in item["event_ids"]],
            "error": None, "primed": any(item["primed"] for item in combined)}


def audit(db_path, legacy_path, legacy_thread_channels=(), legacy_dm_channels=(), *, allow_missing_legacy=False):
    if set(legacy_thread_channels) & set(legacy_dm_channels):
        raise ValueError("A verified legacy channel cannot be both DM and thread-scoped")
    try:
        legacy_bytes = Path(legacy_path).read_bytes()
    except FileNotFoundError:
        if not allow_missing_legacy:
            raise
        legacy_bytes = None
    legacy = json.loads(legacy_bytes) if legacy_bytes is not None else {}
    if legacy and all(isinstance(value, str) for value in legacy.values()):
        legacy = {key: {"id": value, "primed": False} for key, value in legacy.items()}
    db = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    routes, evidence, errors = collections.defaultdict(dict), collections.defaultdict(list), []
    try:
        db.execute("BEGIN")
        sessions = {row["id"]: dict(row) for row in db.execute(
            "SELECT id,source,parent_session_id,started_at,ended_at,end_reason,message_count,tool_call_count FROM sessions WHERE source='acp'")}
        metadata = {key: {"user_messages": 0, "leading_tags": set(), "header_rows": []} for key in sessions}
        for row in db.execute("SELECT id,session_id,content,timestamp,active FROM messages WHERE role='user' ORDER BY id"):
            sid = row["session_id"]
            if sid not in sessions:
                continue
            meta = metadata[sid]
            meta["user_messages"] += 1
            content = row["content"]
            if isinstance(content, str) and (tag := re.match(r"\s*<([a-z-]+)", content)):
                meta["leading_tags"].add(tag.group(1))
            parsed = envelope(content, legacy_thread_channels, legacy_dm_channels)
            if not parsed:
                continue
            meta["header_rows"].append(row["id"])
            candidate = routes[parsed["scope"]].setdefault(sid, {**sessions[sid], "header_message_ids": [],
                "active_header_message_ids": [], "last_header_at": row["timestamp"], "standing_context_header_ids": []})
            candidate["header_message_ids"].append(row["id"])
            candidate["last_header_at"] = row["timestamp"]
            if row["active"]:
                candidate["active_header_message_ids"].append(row["id"])
            if parsed["primed"]:
                candidate["standing_context_header_ids"].append(row["id"])
            if parsed["error"]:
                errors.append({"session_id": sid, "message_id": row["id"], "error": parsed["error"]})
            for event in parsed["event_ids"]:
                evidence[event].append({"event_id": event, "scope": parsed["scope"], "source_session_id": sid,
                                        "source_message_id": row["id"], "source_message_active": bool(row["active"])})
        result_routes, forward = [], {}
        assigned = collections.defaultdict(set)
        for scope, candidates in sorted(routes.items()):
            fragments = list(candidates.values())
            for candidate in fragments:
                sid = candidate["id"]
                candidate["last_message"] = dict(db.execute("SELECT id,role,timestamp FROM messages WHERE session_id=? AND active=1 ORDER BY id DESC LIMIT 1", (sid,)).fetchone() or {})
                assigned[sid].add(scope)
            selected = max(fragments, key=lambda item: max(item["active_header_message_ids"] or item["header_message_ids"]))
            saved = legacy.get(scope)
            primed = saved.get("primed", False) if saved and saved["id"] == selected["id"] else bool(selected["standing_context_header_ids"])
            selection = {"source_id": selected["id"], "primed": primed, "fragment_ids": [item["id"] for item in fragments],
                         "legacy_expected_id": saved["id"] if saved else "absent", "requires_post_drain_refresh": True}
            result_routes.append({"scope": scope, "legacy_binding": saved, "fragments": fragments, "selection": selection})
            forward[scope] = {"id": selected["id"], "primed": primed}
        unmatched = [{**row, **metadata[sid], "leading_tags": sorted(metadata[sid]["leading_tags"])}
                     for sid, row in sessions.items() if sid not in assigned]
        return {"observed_at": time.time(), "database": str(db_path), "legacy_path": str(legacy_path),
                "verified_legacy_thread_channels": list(legacy_thread_channels),
                "verified_legacy_dm_channels": list(legacy_dm_channels),
                "legacy_map_missing": legacy_bytes is None,
                "legacy_sha256": hashlib.sha256(legacy_bytes).hexdigest() if legacy_bytes is not None else None, "legacy_bindings": legacy,
                "routes": result_routes, "forward_mapping": forward, "unmatched_sessions": unmatched,
                "multi_scope_sessions": {key: sorted(value) for key, value in assigned.items() if len(value) > 1},
                "multi_scope_trigger_ids": {key: sorted({item["scope"] for item in rows}) for key, rows in evidence.items()
                                            if len({item["scope"] for item in rows}) > 1},
                "trigger_event_evidence": dict(evidence), "trigger_parse_errors": errors}
    finally:
        db.close()


def journal_metadata(pid, since):
    result = subprocess.run(["journalctl", "_PID=" + str(pid), "--since", since, "-o", "json", "--no-pager"], capture_output=True, check=True)
    records = []
    for line in result.stdout.splitlines():
        row = json.loads(line)
        message = row.get("MESSAGE", "")
        message = bytes(message).decode("utf-8", "replace") if isinstance(message, list) else message
        message = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", message)
        kind = next((label for label in ("turn delivered Buzz events", "cancel", "rotat", "shutdown", "queue_depth") if label.lower() in message.lower()), None)
        if kind:
            records.append({"journal_time": row.get("__REALTIME_TIMESTAMP"), "kind": kind,
                            "mixed_event_ids_not_execution_proof": re.findall(r"(?<![0-9a-f])" + EVENT + r"(?![0-9a-f])", message)})
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ("database", "legacy-json", "output-dir", "profile"):
        parser.add_argument("--" + argument, required=True)
    parser.add_argument("--bridge-pid", type=int)
    parser.add_argument("--journal-since")
    parser.add_argument("--legacy-thread-channel", action="append", default=[], help="Explicitly verified non-DM channel using current thread policy")
    parser.add_argument("--legacy-dm-channel", action="append", default=[], help="Explicitly verified DM channel using conversation policy")
    parser.add_argument("--allow-missing-legacy-map", action="store_true", help="Record an audited absent map without creating it")
    args = parser.parse_args()
    manifest = audit(args.database, args.legacy_json, args.legacy_thread_channel,
                     args.legacy_dm_channel, allow_missing_legacy=args.allow_missing_legacy_map)
    if args.bridge_pid:
        if not args.journal_since:
            parser.error("--journal-since required with --bridge-pid")
        manifest["journal_metadata"] = journal_metadata(args.bridge_pid, args.journal_since)
    out = Path(args.output_dir)
    out.mkdir(mode=0o700, parents=True, exist_ok=True)
    for suffix, value in (("routing-audit", manifest), ("audited-forward-mapping", manifest["forward_mapping"])):
        target = out / (args.profile + "-" + suffix + ".json")
        target.write_text(json.dumps(value, indent=2) + "\n")
        target.chmod(0o600)
    print(json.dumps({"artifact": str(out / (args.profile + "-routing-audit.json")), "routes": len(manifest["routes"]),
                      "fragmented_routes": sum(len(row["fragments"]) > 1 for row in manifest["routes"]),
                      "trigger_ids": len(manifest["trigger_event_evidence"]), "trigger_parse_errors": manifest["trigger_parse_errors"],
                      "unmatched_sessions": manifest["unmatched_sessions"], "multi_scope_sessions": manifest["multi_scope_sessions"],
                      "multi_scope_trigger_ids": manifest["multi_scope_trigger_ids"]}, indent=2))


if __name__ == "__main__":
    main()
