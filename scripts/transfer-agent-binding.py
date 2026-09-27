#!/usr/bin/env python3
"""Switch a drained agent's history pointer; never start either backend.

Stop BOTH bridge backends and Serve recovery, pause/drain all agent sessions and
external jobs, then stage history with Hermes transfer_agent_session.py. Back up
both stores. Run as the bridge service user so a new SQLite store has its owner.
Apply this helper to every audited scope BEFORE restarting exactly
one backend. No old requests are imported or dispatched.

Forward migration checks the pinned legacy entry, retaining its primed flag.
Rollback writes the legacy JSON atomically, then retires the Serve binding. The
two files cannot commit atomically: after a crash, keep both services stopped and
rerun the IDENTICAL command to complete retirement. Never restart on a partial
result. Originals/history are untouched; after new work, stage its current tip
back before changing the binding again. --expected-legacy-id is the actual old
JSON pointer, which may differ from the current Serve session.
Use --expected-legacy-id absent when rolling a Serve-created thread back for the
first time; its key must actually be absent from the existing legacy JSON file.
"""

import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from urllib.parse import urlsplit
import uuid


SCHEMA = Path(__file__).resolve().parents[1] / "crates/buzz-acp/src/bridge_state/schema.sql"


def namespace(community, pubkey):
    if not re.fullmatch(r"[0-9a-fA-F]{64}", pubkey):
        raise ValueError("Full 64-character public key required")
    url = urlsplit(community)
    scheme = {"ws": "http", "http": "http", "wss": "https", "https": "https"}.get(url.scheme)
    if not scheme or not url.hostname:
        raise ValueError("Community must have an HTTP(S)/WS(S) origin")
    host = url.hostname.encode("idna").decode().lower()
    if ":" in host:
        host = f"[{host}]"
    port = url.port
    suffix = f":{port}" if port and port != (443 if scheme == "https" else 80) else ""
    return json.dumps([f"{scheme}://{host}{suffix}", pubkey.lower()], separators=(",", ":"))


def validate_scope(scope):
    parts = scope.split(":")
    if len(parts) not in (2, 3) or parts[0] not in ("conversation", "thread"):
        raise ValueError("Expected conversation:<uuid> or thread:<uuid>:<root-event-id>")
    if str(uuid.UUID(parts[1])) != parts[1] or len(parts) != (3 if parts[0] == "thread" else 2):
        raise ValueError("Scope must use canonical channel UUID")
    if parts[0] == "thread" and not re.fullmatch(r"[0-9a-f]{64}", parts[2]):
        raise ValueError("Thread root must be a full lowercase event ID")


def read_legacy(path):
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("Legacy store must be an object")
    # Rust accepts homogeneous legacy-string or object maps, not mixed maps.
    if data and all(isinstance(value, str) for value in data.values()):
        data = {key: {"id": value, "primed": False} for key, value in data.items()}
    if any(not isinstance(value, dict) or not isinstance(value.get("id"), str)
           or not isinstance(value.get("primed", False), bool) for value in data.values()):
        raise ValueError("Malformed legacy map; refusing an implicit reset")
    return data


def write_legacy(path, data):
    metadata = path.stat()
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".transfer-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), metadata.st_mode & 0o777)
            if hasattr(os, "fchown"):
                os.fchown(stream.fileno(), metadata.st_uid, metadata.st_gid)
            stream.write(json.dumps(data, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def transfer(state_db, community, pubkey, scope, expected_old, new_id, target,
             legacy_json, expected_legacy_id, *, apply=False, drained=False):
    if apply and not drained:
        raise ValueError("Apply requires --drained: both bridges/recovery stopped and whole agent drained")
    if target not in {"serve", "acp"} or not expected_old or not new_id or expected_old == new_id:
        raise ValueError("Specify target and distinct nonempty old/new session IDs")
    validate_scope(scope)
    ns = namespace(community, pubkey)
    state_db, legacy_json = Path(state_db).resolve(), Path(legacy_json).resolve()
    existing = state_db.exists()
    legacy = read_legacy(legacy_json)
    entry = legacy.get(scope)
    allowed_missing = target == "acp" and expected_legacy_id == "absent" and entry is None
    if entry is None and not allowed_missing:
        raise ValueError("No audited legacy binding for scope; missing-history inventory must be resolved separately")
    # Identical rerun after JSON replacement may finish the interrupted retirement.
    recovery = target == "acp" and entry is not None and entry["id"] == new_id
    if not allowed_missing and entry["id"] != expected_legacy_id and not recovery:
        raise ValueError("Legacy pointer changed; expected-legacy-id does not match")
    if not existing and target == "acp":
        raise ValueError("ACP rollback requires an existing Serve state database")
    if not existing and apply:
        # Avoid both permissive process umasks and replacing a concurrently created DB.
        descriptor = os.open(state_db, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
    conn = sqlite3.connect(state_db.as_uri() + ("?mode=rw" if apply else "?mode=ro"), uri=True, timeout=5) if existing else sqlite3.connect(state_db if apply else ":memory:")
    conn.row_factory = sqlite3.Row
    try:
        if not existing:
            conn.executescript(SCHEMA.read_text())
        conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        if conn.execute("SELECT 1 FROM inputs WHERE namespace=? AND status IN ('queued','submitting','accepted','uncertain') LIMIT 1", (ns,)).fetchone():
            raise ValueError("Agent has unresolved or queued inputs; reconcile before switching")
        if conn.execute("SELECT 1 FROM controls WHERE namespace=? AND status='pending' LIMIT 1", (ns,)).fetchone():
            raise ValueError("Agent has pending controls")
        row = conn.execute("SELECT * FROM bindings WHERE namespace=? AND scope=?", (ns, scope)).fetchone()
        current = row["session_id"] if row else None
        already_done = (target == "acp" and recovery and row is not None and current is None)
        if current != expected_old and not already_done:
            if not (target == "serve" and current is None and entry["id"] == expected_old):
                raise ValueError("Serve binding changed; expected-old does not match")
        primed = bool(row["primed"] if row else entry.get("primed", False))
        receipt = {"applied": apply, "namespace": ns, "scope": scope, "old_id": expected_old,
                   "new_id": new_id, "target": target, "primed": primed, "already_done": already_done}
        if not apply or already_done:
            conn.rollback()
            return receipt
        if target == "serve":
            conn.execute("INSERT INTO bindings(namespace,scope,session_id,primed) VALUES (?,?,?,?) "
                         "ON CONFLICT(namespace,scope) DO UPDATE SET session_id=excluded.session_id,primed=excluded.primed,generation=bindings.generation+1",
                         (ns, scope, new_id, primed))
        else:
            if not recovery:
                # Check again immediately before replace; old workers must be stopped.
                if read_legacy(legacy_json) != legacy:
                    raise ValueError("Legacy map changed during transfer")
                legacy[scope] = {"id": new_id, "primed": primed}
                write_legacy(legacy_json, legacy)
            conn.execute("UPDATE bindings SET session_id=NULL,primed=0,generation=generation+1 WHERE namespace=? AND scope=? AND session_id=?",
                         (ns, scope, expected_old))
        conn.commit()
        return receipt
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("state-db", "community", "pubkey", "scope", "expected-old", "new-id", "legacy-json", "expected-legacy-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--target", choices=("serve", "acp"), required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--drained", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(transfer(**vars(args)), indent=2))
    except (ValueError, sqlite3.Error, OSError) as error:
        parser.exit(2, f"Binding transfer incomplete/refused; keep both bridges stopped: {error}\n")


if __name__ == "__main__":
    main()
