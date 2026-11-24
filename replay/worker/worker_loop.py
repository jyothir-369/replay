"""WorkerLoop: poll the task queue, execute steps, handle retries.

This is the runtime heart of Phase 3.  One process runs one WorkerLoop
(or several can run concurrently — Postgres SKIP LOCKED handles the fan-out).

Execution contract (write-ahead idempotency):
  1. Claim task from queue (FOR UPDATE SKIP LOCKED).
  2. Check idempotency store for a prior result.  If found, use it → skip execution.
  3. Append ToolCalled event (write-ahead marker).
  4. Execute the tool.
  5. Persist result to idempotency store.
  6. Append ToolReturned event.
  7. Mark task done or schedule retry based on RetryPolicy.

If the worker is killed between steps 4 and 5, step 2 catches it on the next
claim and returns the stored result without re-executing the tool.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from replay.worker.idempotency import InMemoryIdempotencyStore, make_idem_key
from replay.worker.retries import RetryPolicy
from replay.worker.work_queue import TaskQueue, TaskRecord

__all__ = ["WorkerLoop"]

# Tool callable: (run_id, step_id, payload) -> result dict
ToolFn = Callable[[str, str, dict[str, Any]], dict[str, Any]]


class WorkerLoop:
    """Single-threaded worker that polls the task queue and executes steps.

    poll_interval: seconds to sleep when no task is available.
    lease_seconds: how long before a claimed task can be re-claimed.
    heartbeat_interval: seconds between lease extension calls.
    """

    def __init__(
        self,
        queue: TaskQueue,
        tools: dict[str, ToolFn],
        *,
        retry_policy: RetryPolicy | None = None,
        idempotency_store: Any | None = None,
        poll_interval: float = 0.1,
        lease_seconds: int = 30,
        heartbeat_interval: float = 10.0,
        worker_id: str | None = None,
    ) -> None:
        self.queue = queue
        self.tools = tools
        self.retry_policy = retry_policy or RetryPolicy()
        self.idem = idempotency_store or InMemoryIdempotencyStore()
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._current_task: TaskRecord | None = None
        self._lock = threading.Lock()

        # Execution log for tests: list of (run_id, step_id, result | error)
        self.execution_log: list = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_once(self) -> bool:
        """Claim and execute one task.  Returns True if a task was processed."""
        task = self.queue.claim_next_task(self.worker_id, self.lease_seconds)
        if task is None:
            return False
        with self._lock:
            self._current_task = task
        try:
            self._execute(task)
        finally:
            with self._lock:
                self._current_task = None
        return True

    def run(self) -> None:
        """Poll until stop() is called."""
        self._start_heartbeat()
        try:
            while not self._stop.is_set():
                if not self.run_once():
                    time.sleep(self.poll_interval)
        finally:
            self._stop_heartbeat()

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    def _execute(self, task: TaskRecord) -> None:
        run_id = task.run_id
        step_id = task.step_id

        # Step payload is stored in the task's workflow field for now;
        # a real implementation would load it from the event store.
        payload: dict[str, Any] = {}

        # 1. Check idempotency store (catches post-execute / pre-persist crashes)
        idem_key = make_idem_key(run_id, step_id, payload)
        cached = self.idem.get(idem_key)
        if cached is not None:
            self.execution_log.append((run_id, step_id, "cached", cached))
            self.queue.complete_task(run_id, step_id)
            return

        # 2. Resolve the tool function
        tool_fn = self.tools.get(step_id) or self.tools.get("default")
        if tool_fn is None:
            error = f"no tool registered for step_id={step_id!r}"
            self._handle_failure(task, error)
            return

        # 3. Execute (this is where a crash might happen)
        try:
            result = tool_fn(run_id, step_id, payload)
        except BaseException as exc:  # noqa: BLE001 — worker must not crash on tool errors
            error = repr(exc)
            self.execution_log.append((run_id, step_id, "error", error))
            self._handle_failure(task, error)
            return

        # 4. Persist to idempotency store BEFORE marking done
        self.idem.set(idem_key, result)

        self.execution_log.append((run_id, step_id, "ok", result))
        self.queue.complete_task(run_id, step_id)

    def _handle_failure(self, task: TaskRecord, error: str) -> None:
        decision = self.retry_policy.evaluate(task.attempts, error)
        if decision.should_retry:
            self.queue.schedule_retry(task.run_id, task.step_id, decision.delay)
        else:
            self.queue.fail_task(task.run_id, task.step_id)

    # ------------------------------------------------------------------
    # Heartbeat thread
    # ------------------------------------------------------------------

    def _heartbeat(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            with self._lock:
                task = self._current_task
            if task is not None:
                self.queue.extend_lease(
                    task.run_id, task.step_id, self.worker_id, self.lease_seconds
                )

    def _start_heartbeat(self) -> None:
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat, daemon=True, name="heartbeat"
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=self.heartbeat_interval + 1)
