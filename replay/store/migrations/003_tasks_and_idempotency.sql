-- Migration 003: durable task queue and idempotency store for Phase 3.
--
-- tasks: lease-based work queue. FOR UPDATE SKIP LOCKED picks the next row.
-- idempotency_results: stores tool-call results so retried calls return the
--   same value without re-executing. Key is attempt-independent (see ADR-05).

CREATE TABLE IF NOT EXISTS tasks (
    run_id      UUID    NOT NULL,
    step_id     TEXT    NOT NULL,
    workflow    TEXT,
    -- When the task becomes claimable (future = scheduled retry, past = now)
    available_at DOUBLE PRECISION NOT NULL DEFAULT extract(epoch FROM now()),
    -- NULL means unclaimed; non-NULL and > now() means held by a worker.
    -- Expired leases (lease_until <= now()) are re-claimable.
    lease_until  DOUBLE PRECISION,
    worker_id    TEXT,
    attempts     INT     NOT NULL DEFAULT 0,
    status       TEXT    NOT NULL DEFAULT 'pending',  -- pending | running | done | failed
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_claimable
    ON tasks (available_at, lease_until)
    WHERE status = 'pending';

-- Idempotency table: maps attempt-independent key -> serialised result.
-- The key is SHA-256(run_id || ':' || step_id || ':' || canonical_input_hash).
-- Attempt number is intentionally excluded so retries hit the same row.
CREATE TABLE IF NOT EXISTS idempotency_results (
    idem_key     CHAR(64)    PRIMARY KEY,   -- hex SHA-256
    result_json  JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
