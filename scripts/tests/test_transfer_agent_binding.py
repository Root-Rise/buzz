"""Operator transfer tests use only disposable stores and the runtime SQL asset."""

import importlib.util
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "transfer-agent-binding.py"
SPEC = importlib.util.spec_from_file_location("binding_transfer", SCRIPT)
transfer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transfer)
SCOPE = "thread:00000000-0000-0000-0000-000000000001:" + "a" * 64


class BindingTransferTests(unittest.TestCase):
    def fixture(self, root):
        legacy = root / "sessions.json"
        legacy.write_text(json.dumps({SCOPE: {"id": "old-acp", "primed": True},
                                      "other": {"id": "untouched", "primed": False}}))
        return dict(state_db=root / "bridge.db", community="wss://EXAMPLE.test:443/path", pubkey="A" * 64,
                    scope=SCOPE, expected_old="old-acp", new_id="new-serve", target="serve",
                    legacy_json=legacy, expected_legacy_id="old-acp")

    def test_forward_dry_run_and_interrupted_rollback_preserve_other_history(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory))
            result = transfer.transfer(**args)
            self.assertFalse(args["state_db"].exists())
            self.assertEqual(result["namespace"], json.dumps(["https://example.test", "a" * 64], separators=(",", ":")))
            transfer.transfer(**args, apply=True, drained=True)
            args.update(expected_old="new-serve", new_id="new-acp", target="acp")
            write = transfer.write_legacy
            def fail_after_json(*values):
                write(*values)
                raise OSError("simulated death before SQLite commit")
            with patch.object(transfer, "write_legacy", fail_after_json), self.assertRaises(OSError):
                transfer.transfer(**args, apply=True, drained=True)
            with closing(sqlite3.connect(args["state_db"])) as db, db:
                self.assertEqual(db.execute("SELECT session_id FROM bindings").fetchone()[0], "new-serve")
            transfer.transfer(**args, apply=True, drained=True)
            self.assertTrue(transfer.transfer(**args, apply=True, drained=True)["already_done"])
            entries = json.loads(args["legacy_json"].read_text())
            self.assertEqual(entries[SCOPE], {"id": "new-acp", "primed": True})
            self.assertEqual(entries["other"], {"id": "untouched", "primed": False})
            with closing(sqlite3.connect(args["state_db"])) as db, db:
                self.assertIsNone(db.execute("SELECT session_id FROM bindings").fetchone()[0])
                self.assertEqual(db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0], 0)
                ns = transfer.namespace(args["community"], args["pubkey"])
                new_scope = SCOPE.replace("a" * 64, "b" * 64)
                db.execute("INSERT INTO bindings(namespace,scope,session_id,primed) VALUES (?,?,?,1)", (ns, new_scope, "serve-created"))
            args.update(scope=new_scope, expected_old="serve-created", new_id="imported-acp", expected_legacy_id="absent")
            transfer.transfer(**args, apply=True, drained=True)
            self.assertEqual(json.loads(args["legacy_json"].read_text())[new_scope], {"id": "imported-acp", "primed": True})

    def test_unsettled_agent_and_stale_pointers_refuse_both_store_mutations(self):
        for blocker in ("queued", "submitting", "accepted", "uncertain", "control", "legacy", "binding", "not-drained"):
            with self.subTest(blocker=blocker), tempfile.TemporaryDirectory() as directory:
                args = self.fixture(Path(directory))
                transfer.transfer(**args, apply=True, drained=True)
                args.update(expected_old="new-serve", new_id="new-acp", target="acp")
                ns = transfer.namespace(args["community"], args["pubkey"])
                with closing(sqlite3.connect(args["state_db"])) as db, db:
                    if blocker in {"queued", "submitting", "accepted", "uncertain"}:
                        db.execute("INSERT INTO inputs(namespace,event_id,scope,payload,status) VALUES (?,?,?,?,?)",
                                   (ns, "event", "a-different-scope", "{}", blocker))
                    elif blocker == "control":
                        db.execute("INSERT INTO controls(namespace,event_id,status) VALUES (?,?,'pending')", (ns, "stop"))
                    elif blocker == "legacy":
                        args["expected_legacy_id"] = "stale"
                    elif blocker == "binding":
                        args["expected_old"] = "stale"
                before = args["legacy_json"].read_text()
                with self.assertRaises(ValueError):
                    transfer.transfer(**args, apply=True, drained=blocker != "not-drained")
                self.assertEqual(args["legacy_json"].read_text(), before)
                with closing(sqlite3.connect(args["state_db"])) as db, db:
                    self.assertEqual(db.execute("SELECT session_id FROM bindings").fetchone()[0], "new-serve")


if __name__ == "__main__":
    unittest.main()
