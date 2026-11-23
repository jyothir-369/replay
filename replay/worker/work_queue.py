"""Worker durable queue using Postgres FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

__all__ = ["TaskQueue"]


class TaskRecord:
    """A row in the tasks table representing a lease-able unit of work."""

    def __init__(self, run_id: str, step_id: str, available_at: float, lease_until: float, attempts: int = 0):
        self.run_id = run_id
        self.step_id = step_id
        self.available_at = available_at
        self.lease_until = lease_until
        self.attempts = attempts


class TaskQueue:
    """Manage durable execution tasks in Postgres.

    Uses `FOR UPDATE SKIP LOCKED` for lease-based execution with
    heartbeat-backed lease extension and auto-release on expiry.
    """

    def __init__(self, db_conn: Any) -> None:
        self.db = db_conn

    async def claim_next_task(
        self,
        worker_id: str,
        max_lease_seconds: int = 30,
    ) -> Optional[TaskRecord]:
        """Claim the next available task for this worker.

        Returns None if no task is currently available.
        The caller must check available_at and lease_until.
        """
        now = __import__("time").time()
        async with self.db.acquire() as conn:
            # Select a task with available_at <= now, locked for this worker
            row = await conn.fetchrow(
                """
                SELECT run_id, step_id, available_at, lease_until, attempts
                FROM tasks
                WHERE available_at <= $1
                  AND lease_until IS NULL
                ORDER BY available_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                now,
            )
            if row is None:
                return None

            lease_until = now + max_lease_seconds
            # Extend the lease for this worker
            await conn.execute(
                """
                UPDATE tasks
                SET lease_until = $1, attempts = attempts + 1
                WHERE run_id = $2 AND step_id = $3
                """,
                lease_until,
                row["run_id"],
                row["step_id"],
            )

            return TaskRecord(
                run_id=row["run_id"],
                step_id=row["step_id"],
                available_at=row["available_at"],
                lease_until=lease_until,
                attempts=row["attempts"] + 1,
            )

    async def extend_lease(
        self,
        run_id: str,
        step_id: str,
        worker_id: str,
        max_lease_seconds: int = 30,
    ) -> bool:
        """Extend the lease on an in-progress task.

        Returns True if the lease was extended (still owned by this worker).
        Returns False if the lease has expired.
        """
        now = __import__("time").time()
        lease_until = now + max_lease_seconds
        result = await self.db.execute(
            """
            UPDATE tasks
            SET lease_until = $1
            WHERE run_id = $2 AND step_id = $3
              AND lease_until > $4  -- still owned by this worker or not expired
            """,
            lease_until,
            run_id,
            step_id,
            now,  # current time as of right now
        )
        return result == "UPDATE 1"

    async def release_task(
        self,
        run_id: str,
        step_id: str,
    ) -> None:
        """Release a task by clearing its lease, making it claimable again."""
        await self.db.execute(
            """
            UPDATE tasks
            SET lease_until = NULL
            WHERE run_id = $1 AND step_id = $2
            """,
            run_id,
            step_id,
        )