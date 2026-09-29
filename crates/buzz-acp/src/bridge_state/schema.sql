PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS bindings (
               namespace TEXT NOT NULL, scope TEXT NOT NULL, session_id TEXT,
               primed INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(namespace,scope));
             CREATE TABLE IF NOT EXISTS inputs (
               sequence INTEGER PRIMARY KEY AUTOINCREMENT,
               namespace TEXT NOT NULL, event_id TEXT NOT NULL, scope TEXT NOT NULL,
               payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', request_key TEXT,
               UNIQUE(namespace,event_id));
             CREATE INDEX IF NOT EXISTS inputs_pending ON inputs(namespace,scope,status,sequence);
             CREATE TABLE IF NOT EXISTS submissions (
               namespace TEXT NOT NULL, request_key TEXT NOT NULL, payload TEXT NOT NULL, server_accepted INTEGER,
               PRIMARY KEY(namespace,request_key));
             CREATE TABLE IF NOT EXISTS relay_progress (
               namespace TEXT PRIMARY KEY, replay_floor INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS controls (
               namespace TEXT NOT NULL, event_id TEXT NOT NULL, scope TEXT, payload TEXT,
               status TEXT NOT NULL DEFAULT 'pending', PRIMARY KEY(namespace,event_id));
             CREATE TABLE IF NOT EXISTS migrations (namespace TEXT NOT NULL, name TEXT NOT NULL,
               PRIMARY KEY(namespace,name));
