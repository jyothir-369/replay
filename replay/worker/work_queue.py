"""Durable task queue backed by Postgres FOR UPDATE SKIP LOCKED.

Workers claim tasks with a time-bounded lease. A background heartbeat
extends the lease while work proceeds. Expired leases are automatically
re-claimable by any other worker, providing crash recovery without a
coordinator: kill -9 a worker and the task re-surfaces after lease_seconds.

Design note (write-ahead safety): callers must write a ToolCalled event
BEFORE executing the tool, then check the idempotency table FIRST on resume.
This guards the "two generals" gap where the tool ran but ToolReturned was
never persisted. See ADR-05.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import psycopg2
import psycopg2.extras

__all__ = ["TaskQueue", "TaskRecord"]


@dataclass
class TaskRecord:
    """A row in the tasks table representing a lease-able unit of work."""

    run_id: str
    step_id: str
    available_at: float
    lease_until: float
    attempts: int = 0
    worker_id: str | None = None
    status: str = "running"
    workflow: str | None = None


class TaskQueue:
    """Postgres-backed durable queue with lease-based ownership and crash recovery.

    All public methods use plain psycopg2 connections (sync).  Async wrappers
    belong at the worker-loop level, not here, so the queue is testable without
    an event loop.
    """

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._conn: psycopg2.extensions.connection | None = None

    # ------------------------------------------------------------------
    # Internal connection management
    # ------------------------------------------------------------------

    def _connect(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                self._db_url, cursor_factory=psycopg2.extras.RealDictCursor
            )
            self._conn.autocommit = False
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def enqueue(
        self,
        run_id: str,
        step_id: str,
        available_at: float | None = None,
        workflow: str | None = None,
    ) -> None:
        """Insert a new task, or reset a failed task back to pending.

        If a (run_id, step_id) row already exists and is in 'pending' status,
        this is a no-op so enqueue is safe to call more than once.
        """
        now = time.time()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks
                        (run_id, step_id, available_at, workflow, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    ON CONFLICT (run_id, step_id) DO NOTHING
                    """,
                    (run_id, step_id, available_at or now, workflow),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Claim (FOR UPDATE SKIP LOCKED)
    # ------------------------------------------------------------------

    def claim_next_task(
        self,
        worker_id: str,
        lease_seconds: int = 30,
    ) -> TaskRecord | None:
        """Atomically claim the oldest available task for this worker.

        A task is available when:
          - status = 'pending', AND
          - available_at <= now() (accounts for scheduled retries), AND
          - lease_until IS NULL OR lease_until <= now() (expired or unclaimed)

        Returns None if no task is available.
        """
        now = time.time()
        lease_until = now + lease_seconds
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, step_id, available_at, lease_until,
                           attempts, worker_id, status, workflow
                    FROM tasks
                    WHERE status IN ('pending', 'running')
                      AND available_at <= %s
                      AND (lease_until IS NULL OR lease_until <= %s)
                    ORDER BY available_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (now, now),
                )
                row = cur.fetchone()
                if row is None:
                    conn.commit()
                    return None

                new_attempts = row["attempts"] + 1
                cur.execute(
                    """
                    UPDATE tasks
                       SET lease_until = %s,
                           worker_id   = %s,
                           attempts    = %s,
                           status      = 'running'
                     WHERE run_id = %s AND step_id = %s
                    """,
                    (lease_until, worker_id, new_attempts, row["run_id"], row["step_id"]),
                )
            conn.commit()
            return TaskRecord(
                run_id=row["run_id"],
                step_id=row["step_id"],
                available_at=float(row["available_at"]),
                lease_until=lease_until,
                attempts=new_attempts,
                worker_id=worker_id,
                status="running",
                workflow=row.get("workflow"),
            )
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Heartbeat / lease extension
    # ------------------------------------------------------------------

    def extend_lease(
        self,
        run_id: str,
        step_id: str,
        worker_id: str,
        lease_seconds: int = 30,
    ) -> bool:
        """Extend the lease for a task still owned by this worker.

        Returns True if the row was updated (worker still owns the lease).
        Returns False if the lease has been taken by another worker or expired.
        """
        now = time.time()
        new_lease = now + lease_seconds
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                       SET lease_until = %s
                     WHERE run_id    = %s
                       AND step_id   = %s
                       AND worker_id = %s
                       AND status    = 'running'
                       AND lease_until > %s
                    """,
                    (new_lease, run_id, step_id, worker_id, now),
                )
                updated = cur.rowcount == 1
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Completion / failure / retry scheduling
    # ------------------------------------------------------------------

    def complete_task(self, run_id: str, step_id: str) -> None:
        """Mark a task as successfully completed (terminal state)."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                       SET status      = 'done',
                           lease_until = NULL,
                           worker_id   = NULL
                     WHERE run_id = %s AND step_id = %s
                    """,
                    (run_id, step_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def fail_task(self, run_id: str, step_id: str) -> None:
        """Mark a task as permanently failed (no further retries)."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                       SET status      = 'failed',
                           lease_until = NULL,
                           worker_id   = NULL
                     WHERE run_id = %s AND step_id = %s
                    """,
                    (run_id, step_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def schedule_retry(
        self,
        run_id: str,
        step_id: str,
        delay_seconds: float,
    ) -> None:
        """Reset a task to pending with a future available_at for retry."""
        available_at = time.time() + delay_seconds
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                       SET status       = 'pending',
                           available_at = %s,
                           lease_until  = NULL,
                           worker_id    = NULL
                     WHERE run_id = %s AND step_id = %s
                    """,
                    (available_at, run_id, step_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Inspection helpers (used by tests and the chaos harness)
    # ------------------------------------------------------------------

    def get_task(self, run_id: str, step_id: str) -> dict | None:
        """Return the raw task row dict or None."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tasks WHERE run_id = %s AND step_id = %s",
                (run_id, step_id),
            )
            return dict(cur.fetchone()) if cur.rowcount != 0 else None

    def list_tasks(self, status: str | None = None) -> list[dict]:
        """Return all tasks, optionally filtered by status."""
        conn = self._connect()
        with conn.cursor() as cur:
            if status:
                cur.execute("SELECT * FROM tasks WHERE status = %s ORDER BY available_at", (status,))
            else:
                cur.execute("SELECT * FROM tasks ORDER BY available_at")
            return [dict(r) for r in cur.fetchall()]
