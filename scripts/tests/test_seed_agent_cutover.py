import importlib.util
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("cutover_seed", Path(__file__).resolve().parents[1] / "seed-agent-cutover.py")
seed = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(seed)


class CutoverSeedTests(unittest.TestCase):
    def manifest(self):
        return {"version": 1, "community": "wss://isolated.invalid", "agent_pubkey": "1" * 64,
                "replay_floor": 100, "executed_inputs": [{"event_id": "2" * 64,
                "scope": "conversation:00000000-0000-0000-0000-000000000001",
                "source_session_id": "source", "source_message_id": 42}], "observed_controls": ["3" * 64]}

    def test_seed_is_atomic_dedup_only_and_identical_rerun_does_not_reset_live_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bridge.db"
            with closing(sqlite3.connect(path)) as db, db:
                db.executescript(seed.binding.SCHEMA.read_text())
            manifest = self.manifest()
            seed.seed(path, manifest)
            with closing(sqlite3.connect(path)) as db, db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0], 0)
            seed.seed(path, manifest, apply=True, drained=True, controls_reviewed=True)
            with closing(sqlite3.connect(path)) as db, db:
                ns = seed.binding.namespace(manifest["community"], manifest["agent_pubkey"])
                self.assertEqual(db.execute("SELECT status FROM inputs").fetchone()[0], "historical")
                self.assertEqual(db.execute("SELECT status FROM controls").fetchone()[0], "observed")
                self.assertEqual(db.execute("SELECT replay_floor FROM relay_progress").fetchone()[0], 100)
                db.execute("INSERT INTO inputs(namespace,event_id,scope,payload) VALUES (?,?,?,'{}')", (ns, "4" * 64, "new"))
            self.assertTrue(seed.seed(path, manifest, apply=True, drained=True, controls_reviewed=True)["already_done"])
            with closing(sqlite3.connect(path)) as db, db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM inputs WHERE status='queued'").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM submissions").fetchone()[0], 0)

    def test_missing_evidence_or_live_state_refuses_without_partial_seeds(self):
        for blocker in ("provenance", "live", "floor", "control-review", "drain"):
            with self.subTest(blocker=blocker), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "bridge.db"
                manifest = self.manifest()
                ns = seed.binding.namespace(manifest["community"], manifest["agent_pubkey"])
                with closing(sqlite3.connect(path)) as db, db:
                    db.executescript(seed.binding.SCHEMA.read_text())
                    if blocker == "live":
                        db.execute("INSERT INTO controls(namespace,event_id) VALUES (?,?)", (ns, "pending"))
                    elif blocker == "floor":
                        db.execute("INSERT INTO relay_progress VALUES (?,?)", (ns, 99))
                if blocker == "provenance":
                    manifest["executed_inputs"][0].pop("source_session_id")
                with self.assertRaises(ValueError):
                    seed.seed(path, manifest, apply=True, drained=blocker != "drain", controls_reviewed=blocker != "control-review")
                with closing(sqlite3.connect(path)) as db, db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
