-- migrations/001_create_runs_and_events.sql
-- Creates the append-only event store and runs table.

CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    workflow TEXT,
    parent_run_id UUID,
    fork_seq INT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    run_id UUID NOT NULL,
    seq INT NOT NULL,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    prev_hash BYTEA,
    hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_prev ON events(prev_hash);

-- Revoke UPDATE/DELETE for the app role; only INSERT/SELECT are allowed.
-- This guard is enforced at the DB level; a trigger is a second guard.
GRANT INSERT, SELECT ON events TO replay_app;
GRANT INSERT, SELECT ON runs TO replay_app;

REVOKE UPDATE, DELETE ON events FROM replay_app;
REVOKE UPDATE, DELETE ON runs FROM replay_app;