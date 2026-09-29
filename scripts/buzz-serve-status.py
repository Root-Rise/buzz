#!/usr/bin/env python3
"""Read bridge/Serve delivery metadata without model calls or chat publication.

Use one agent's bridge database and matching profile home. This reports recorded
state; session.active_list supplies current runtime activity. It does not prove
Buzz CLI replies were published or that arbitrary shell processes have stopped.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
import time


def inspect(bridge_state, hermes_home):
    def connect(path):
        db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        return db
    bridge = connect(bridge_state)
    hermes = connect(Path(hermes_home) / 'state.db')
    try:
        inputs = list(bridge.execute('SELECT namespace,scope,status,payload FROM inputs'))
        bindings = list(bridge.execute('SELECT namespace,scope,session_id FROM bindings WHERE session_id IS NOT NULL'))
        pending = [row for row in inputs if row['status'] in ('queued', 'submitting', 'accepted', 'uncertain')]
        timestamps = []
        for row in pending:
            event = json.loads(row['payload']).get('event', {})
            if isinstance(event.get('created_at'), (int, float)):
                timestamps.append(event['created_at'])
        tables = {row[0] for row in hermes.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        receipts = Counter()
        missing = []
        paused = 0
        for binding in bindings:
            row = hermes.execute('SELECT * FROM sessions WHERE id=?', (binding['session_id'],)).fetchone()
            if row is None:
                missing.append({'scope': binding['scope'], 'session_id': binding['session_id']})
            elif 'automation_paused' in row.keys():
                paused += bool(row['automation_paused'])
        if 'prompt_receipts' in tables:
            receipts.update({row['status']: row['n'] for row in hermes.execute('SELECT status,count(*) n FROM prompt_receipts GROUP BY status')})
        return {
            'observed_at': int(time.time()),
            'bindings': len(bindings), 'namespaces': len({row['namespace'] for row in bindings}),
            'inputs_by_status': dict(Counter(row['status'] for row in inputs)),
            'pending_scopes': len({(row['namespace'], row['scope']) for row in pending}),
            'oldest_pending_event_age_seconds': max(0, int(time.time() - min(timestamps))) if timestamps else None,
            'pending_controls': bridge.execute("SELECT count(*) FROM controls WHERE status='pending'").fetchone()[0],
            'profile_receipts_by_status': dict(receipts), 'paused_bindings': paused,
            'missing_session_rows': missing,
            'limits': 'Event age includes relay delay; live activity and Buzz publication require separate observation.',
        }
    finally:
        bridge.close()
        hermes.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bridge-state', required=True, type=Path)
    parser.add_argument('--hermes-home', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(inspect(args.bridge_state, args.hermes_home), indent=2))


if __name__ == '__main__':
    main()
