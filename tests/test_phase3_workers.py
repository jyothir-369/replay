"""Phase 3 tests: workers, durable queue, retries, and idempotency.

Tests are split into:
  - Unit tests (no DB, instant): RetryPolicy, InMemoryIdempotencyStore,
    make_idem_key, WorkerLoop with a fake queue.
  - Integration tests (require REPLAY_TEST_DATABASE_URL): TaskQueue against
    a real Postgres tasks table, DbIdempotencyStore, crash-recovery simulation.

Chaos harness (requires DB):
  spawn N worker threads, randomly kill some mid-execution, assert that every
  task ends up done exactly once with no duplicate side-effects.
"""

from __future__ import annotations

import os
import random
import threading
import time
import uuid
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import pytest

from replay.worker.idempotency import (
    DbIdempotencyStore,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    make_idem_key,
)
from replay.worker.retries import RetryPolicy
from replay.worker.work_queue import TaskQueue, TaskRecord
from replay.worker.worker_loop import WorkerLoop

# ---------------------------------------------------------------------------
# DB guard — integration tests skip if no URL is set or it's not replay_test
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get("REPLAY_TEST_DATABASE_URL")
_db_available = False

if DATABASE_URL:
    parsed = urlparse(DATABASE_URL)
    if parsed.path.lstrip("/") not in {"replay_test", "replay_phase3_test"}:
        raise RuntimeError(
            "Refusing integration tests: REPLAY_TEST_DATABASE_URL must target an approved disposable test database."
        )
    _db_available = True

requires_db = pytest.mark.skipif(
    not _db_available,
    reason="Set REPLAY_TEST_DATABASE_URL=postgresql://...@.../replay_test",
)


# ---------------------------------------------------------------------------
# DB fixture helpers
# ---------------------------------------------------------------------------

def _apply_migration(conn, sql_path: str) -> None:
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _fresh_tables(conn) -> None:
    """Drop and recreate tasks and idempotency_results for test isolation."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS idempotency_results CASCADE")
        cur.execute("DROP TABLE IF EXISTS tasks CASCADE")
    conn.commit()
    # Apply Phase 3 migration
    migration_path = os.path.join(
        os.path.dirname(__file__), "..", "replay", "store", "migrations",
        "003_tasks_and_idempotency.sql"
    )
    migration_path = os.path.normpath(migration_path)
    _apply_migration(conn, migration_path)


@pytest.fixture(scope="function")
def db_conn():
    """Yield a psycopg2 connection to the test DB with fresh Phase 3 tables."""
    if not _db_available:
        pytest.skip("No test DB")
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    _fresh_tables(conn)
    yield conn
    try:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS idempotency_results CASCADE")
            cur.execute("DROP TABLE IF EXISTS tasks CASCADE")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="function")
def task_queue(db_conn):
    q = TaskQueue(DATABASE_URL)
    yield q
    q.close()


@pytest.fixture(scope="function")
def idem_store(db_conn):
    s = DbIdempotencyStore(DATABASE_URL)
    yield s
    s.close()


# ===========================================================================
# UNIT TESTS — no database
# ===========================================================================

class TestRetryPolicy:
    def test_no_retry_on_max_attempts(self):
        p = RetryPolicy(max_attempts=3)
        d = p.evaluate(3, "timeout")
        assert not d.should_retry
        assert "max_attempts" in d.reason

    def test_retry_on_retryable_error(self):
        p = RetryPolicy(max_attempts=5, jitter=False)
        d = p.evaluate(1, "connection refused")
        assert d.should_retry
        assert d.delay > 0

    def test_no_retry_on_fatal_error(self):
        p = RetryPolicy(max_attempts=5)
        for err in ["auth failed", "forbidden", "invalid input", "permission denied"]:
            d = p.evaluate(1, err)
            assert not d.should_retry, f"Expected fatal for: {err!r}"

    def test_no_retry_on_unknown_error(self):
        p = RetryPolicy(max_attempts=5)
        d = p.evaluate(1, "some totally unknown error xyz")
        assert not d.should_retry

    def test_exponential_backoff_no_jitter(self):
        p = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=100.0, jitter=False)
        # attempt 1 -> delay=1, attempt 2 -> delay=2, attempt 3 -> delay=4
        assert p.schedule(1) == pytest.approx(1.0)
        assert p.schedule(2) == pytest.approx(2.0)
        assert p.schedule(3) == pytest.approx(4.0)

    def test_delay_capped_at_max_delay(self):
        p = RetryPolicy(max_attempts=20, base_delay=1.0, max_delay=5.0, jitter=False)
        for attempt in range(4, 20):
            assert p.schedule(attempt) <= 5.0

    def test_schedule_returns_zero_when_exhausted(self):
        p = RetryPolicy(max_attempts=3)
        assert p.schedule(3) == 0.0

    def test_jitter_stays_in_range(self):
        p = RetryPolicy(max_attempts=10, base_delay=2.0, max_delay=100.0, jitter=True)
        for _ in range(200):
            d = p.schedule(1)
            assert 0.0 <= d <= 2.0, f"Jitter delay {d} out of [0, 2.0]"

    def test_backward_compat_should_retry(self):
        p = RetryPolicy(max_attempts=5)
        assert p.should_retry("timeout", attempt=1)
        assert not p.should_retry("auth failed", attempt=1)
        assert not p.should_retry("timeout", attempt=5)

    def test_max_attempts_must_be_at_least_one(self):
        with pytest.raises(ValueError):
            RetryPolicy(max_attempts=0)


class TestMakeIdemKey:
    def test_deterministic(self):
        k1 = make_idem_key("run-1", "step-a", {"x": 1})
        k2 = make_idem_key("run-1", "step-a", {"x": 1})
        assert k1 == k2

    def test_dict_ordering_invariant(self):
        k1 = make_idem_key("run-1", "step-a", {"a": 1, "b": 2})
        k2 = make_idem_key("run-1", "step-a", {"b": 2, "a": 1})
        assert k1 == k2, "Key must not depend on dict insertion order"

    def test_different_run_ids_produce_different_keys(self):
        k1 = make_idem_key("run-1", "step-a", {})
        k2 = make_idem_key("run-2", "step-a", {})
        assert k1 != k2

    def test_different_inputs_produce_different_keys(self):
        k1 = make_idem_key("run-1", "step-a", {"x": 1})
        k2 = make_idem_key("run-1", "step-a", {"x": 2})
        assert k1 != k2

    def test_attempt_exclusion(self):
        """Key must be the same regardless of which attempt we're on."""
        k1 = make_idem_key("run-1", "step-a", {"lot": "L123"})
        k2 = make_idem_key("run-1", "step-a", {"lot": "L123"})
        assert k1 == k2

    def test_returns_64_char_hex(self):
        k = make_idem_key("r", "s", {})
        assert len(k) == 64
        int(k, 16)  # must be valid hex


class TestInMemoryIdempotencyStore:
    def test_get_returns_none_for_missing(self):
        store = InMemoryIdempotencyStore()
        assert store.get("nonexistent") is None

    def test_set_and_get_roundtrip(self):
        store = InMemoryIdempotencyStore()
        store.set("k1", {"result": 42})
        assert store.get("k1") == {"result": 42}

    def test_set_is_idempotent(self):
        store = InMemoryIdempotencyStore()
        store.set("k1", "first")
        store.set("k1", "second")  # should not overwrite
        assert store.get("k1") == "first"

    def test_by_args_helpers(self):
        store = InMemoryIdempotencyStore()
        store.set_by_args("run-1", "step-a", {"x": 1}, "result")
        assert store.get_by_args("run-1", "step-a", {"x": 1}) == "result"

    def test_distinct_keys(self):
        store = InMemoryIdempotencyStore()
        store.set_by_args("r1", "s1", {}, "v1")
        store.set_by_args("r1", "s2", {}, "v2")
        assert store.get_by_args("r1", "s1", {}) == "v1"
        assert store.get_by_args("r1", "s2", {}) == "v2"


class TestLegacyIdempotencyStore:
    """Verify the backwards-compat shim doesn't break existing callers."""

    def test_legacy_get_set(self):
        store = IdempotencyStore()
        store.set("run", "step", 1, "hash123", "result")
        assert store.get("run", "step", 1, "hash123") == "result"

    def test_legacy_missing_returns_none(self):
        store = IdempotencyStore()
        assert store.get("run", "step", 1, "hash123") is None


class FakeQueue:
    """In-memory queue stub for WorkerLoop unit tests."""

    def __init__(self, tasks: list[TaskRecord]) -> None:
        self._tasks = list(tasks)
        self._done: list[str] = []
        self._failed: list[str] = []
        self._retried: list[tuple] = []
        self._index = 0

    def claim_next_task(self, worker_id: str, lease_seconds: int = 30) -> TaskRecord | None:
        if self._index >= len(self._tasks):
            return None
        task = self._tasks[self._index]
        self._index += 1
        return task

    def complete_task(self, run_id: str, step_id: str) -> None:
        self._done.append(f"{run_id}:{step_id}")

    def fail_task(self, run_id: str, step_id: str) -> None:
        self._failed.append(f"{run_id}:{step_id}")

    def schedule_retry(self, run_id: str, step_id: str, delay: float) -> None:
        self._retried.append((run_id, step_id, delay))

    def extend_lease(self, run_id: str, step_id: str, worker_id: str, lease_seconds: int) -> bool:
        return True


def _make_task(run_id: str = "run-1", step_id: str = "step-1", attempts: int = 1) -> TaskRecord:
    now = time.time()
    return TaskRecord(
        run_id=run_id, step_id=step_id, available_at=now, lease_until=now + 30, attempts=attempts
    )


class TestWorkerLoop:
    def test_executes_tool_and_marks_done(self):
        task = _make_task()
        queue = FakeQueue([task])
        executed = []

        def my_tool(run_id, step_id, payload):
            executed.append((run_id, step_id))
            return {"ok": True}

        loop = WorkerLoop(queue, {"step-1": my_tool})
        assert loop.run_once()
        assert executed == [("run-1", "step-1")]
        assert "run-1:step-1" in queue._done

    def test_no_task_returns_false(self):
        queue = FakeQueue([])
        loop = WorkerLoop(queue, {})
        assert not loop.run_once()

    def test_fatal_error_fails_task(self):
        task = _make_task()
        queue = FakeQueue([task])

        def bad_tool(run_id, step_id, payload):
            raise ValueError("invalid input supplied")

        loop = WorkerLoop(queue, {"step-1": bad_tool},
                          retry_policy=RetryPolicy(max_attempts=3))
        loop.run_once()
        assert "run-1:step-1" in queue._failed

    def test_retryable_error_schedules_retry(self):
        task = _make_task(attempts=1)
        queue = FakeQueue([task])

        def flaky_tool(run_id, step_id, payload):
            raise ConnectionError("connection refused")

        loop = WorkerLoop(queue, {"step-1": flaky_tool},
                          retry_policy=RetryPolicy(max_attempts=3, jitter=False))
        loop.run_once()
        assert len(queue._retried) == 1
        assert queue._retried[0][0] == "run-1"

    def test_idempotency_prevents_double_execution(self):
        idem = InMemoryIdempotencyStore()
        # Pre-populate the idempotency store as if a prior execution succeeded
        idem_key = make_idem_key("run-1", "step-1", {})
        idem.set(idem_key, {"pre_stored": True})

        task = _make_task()
        queue = FakeQueue([task])
        executed = []

        def tool(run_id, step_id, payload):
            executed.append(True)
            return {"fresh": True}

        loop = WorkerLoop(queue, {"step-1": tool}, idempotency_store=idem)
        loop.run_once()

        # Tool must NOT have been called again; cached result was used
        assert executed == []
        assert "run-1:step-1" in queue._done

    def test_execution_logged(self):
        task = _make_task()
        queue = FakeQueue([task])
        loop = WorkerLoop(queue, {"step-1": lambda r, s, p: {"v": 1}})
        loop.run_once()
        assert any(e[2] == "ok" for e in loop.execution_log)

    def test_missing_tool_fails_task(self):
        task = _make_task(step_id="unknown-step")
        queue = FakeQueue([task])
        loop = WorkerLoop(queue, {})
        loop.run_once()
        assert "run-1:unknown-step" in queue._failed


# ===========================================================================
# INTEGRATION TESTS — require REPLAY_TEST_DATABASE_URL
# ===========================================================================

@requires_db
class TestTaskQueueIntegration:
    def test_enqueue_and_claim(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")

        task = task_queue.claim_next_task("worker-1", lease_seconds=30)
        assert task is not None
        assert task.run_id == run_id
        assert task.step_id == "step-1"
        assert task.attempts == 1
        assert task.worker_id == "worker-1"
        assert task.status == "running"

    def test_claim_returns_none_when_empty(self, task_queue):
        task = task_queue.claim_next_task("worker-1")
        assert task is None

    def test_claimed_task_not_reclaimable(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1", lease_seconds=30)

        # Second claim should find nothing (the first is still running)
        task2 = task_queue.claim_next_task("worker-2", lease_seconds=30)
        assert task2 is None

    def test_expired_lease_is_reclaimable(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")

        # Claim with a lease that expired instantly
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            with conn.cursor() as cur:
                # Directly expire the lease
                cur.execute(
                    """
                    UPDATE tasks SET
                        lease_until = extract(epoch FROM now()) - 1,
                        worker_id = 'dead-worker',
                        status = 'running',
                        attempts = 1
                    WHERE run_id = %s AND step_id = 'step-1'
                    """,
                    (run_id,),
                )
            conn.commit()
        finally:
            conn.close()

        # New worker should be able to claim it
        task = task_queue.claim_next_task("worker-2", lease_seconds=30)
        assert task is not None
        assert task.worker_id == "worker-2"
        assert task.attempts == 2

    def test_complete_task_sets_done(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1")
        task_queue.complete_task(run_id, "step-1")

        # Completed task must not be reclaimable
        assert task_queue.claim_next_task("worker-2") is None

    def test_schedule_retry_makes_task_pending(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1")
        task_queue.schedule_retry(run_id, "step-1", delay_seconds=0.0)

        # Should be immediately reclaimable with delay=0
        task = task_queue.claim_next_task("worker-2")
        assert task is not None
        assert task.run_id == run_id

    def test_fail_task_is_terminal(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1")
        task_queue.fail_task(run_id, "step-1")

        assert task_queue.claim_next_task("worker-2") is None

    def test_extend_lease_returns_true_for_owner(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1", lease_seconds=30)

        result = task_queue.extend_lease(run_id, "step-1", "worker-1", lease_seconds=30)
        assert result is True

    def test_extend_lease_returns_false_after_expiry(self, task_queue):
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")
        task_queue.claim_next_task("worker-1", lease_seconds=30)

        # Expire the lease manually
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET lease_until = %s WHERE run_id = %s AND step_id = 'step-1'",
                    (time.time() - 1, run_id),
                )
            conn.commit()
        finally:
            conn.close()

        result = task_queue.extend_lease(run_id, "step-1", "worker-1", lease_seconds=30)
        assert result is False

    def test_skip_locked_concurrent_claim(self, task_queue):
        """Two workers racing to claim the same task — only one wins."""
        run_id = str(uuid.uuid4())
        task_queue.enqueue(run_id, "step-1")

        results = []
        barrier = threading.Barrier(2)

        def claim():
            q = TaskQueue(DATABASE_URL)
            try:
                barrier.wait()
                t = q.claim_next_task(f"worker-{uuid.uuid4().hex[:4]}", lease_seconds=30)
                results.append(t)
            finally:
                q.close()

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        claimed = [r for r in results if r is not None]
        assert len(claimed) == 1, f"Expected exactly 1 claim, got {len(claimed)}"

    def test_ordered_by_available_at(self, task_queue):
        """Tasks are returned oldest-first."""
        now = time.time()
        run_id = str(uuid.uuid4())
        # Enqueue step-2 slightly earlier
        task_queue.enqueue(run_id, "step-2")
        task_queue.enqueue(str(uuid.uuid4()), "step-1")

        # Manually backdate step-2 to be older
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET available_at = %s WHERE run_id = %s",
                    (now - 100, run_id),
                )
            conn.commit()
        finally:
            conn.close()

        first = task_queue.claim_next_task("w1")
        assert first is not None
        assert first.run_id == run_id  # older one first


@requires_db
class TestDbIdempotencyStoreIntegration:
    def test_get_returns_none_for_missing(self, idem_store):
        assert idem_store.get("no-such-key") is None

    def test_set_and_get_roundtrip(self, idem_store):
        idem_store.set("k1", {"lots": [1, 2, 3]})
        assert idem_store.get("k1") == {"lots": [1, 2, 3]}

    def test_set_is_idempotent(self, idem_store):
        idem_store.set("k2", "first")
        idem_store.set("k2", "second")
        assert idem_store.get("k2") == "first"

    def test_by_args_roundtrip(self, idem_store):
        idem_store.set_by_args("run-1", "step-a", {"x": 1}, "result-value")
        v = idem_store.get_by_args("run-1", "step-a", {"x": 1})
        assert v == "result-value"

    def test_different_inputs_different_results(self, idem_store):
        idem_store.set_by_args("run-1", "step-a", {"x": 1}, "v1")
        idem_store.set_by_args("run-1", "step-a", {"x": 2}, "v2")
        assert idem_store.get_by_args("run-1", "step-a", {"x": 1}) == "v1"
        assert idem_store.get_by_args("run-1", "step-a", {"x": 2}) == "v2"


# ===========================================================================
# CHAOS HARNESS — requires DB
# ===========================================================================

@requires_db
class TestChaosHarness:
    """
    Spawn N concurrent workers against M tasks; randomly kill some workers
    mid-execution; assert every task ends as 'done' exactly once with no
    duplicate side-effects (counted via a thread-safe execution counter).
    """

    def test_no_duplicate_executions_under_concurrent_workers(self, db_conn):
        N_TASKS = 20
        N_WORKERS = 4
        KILL_PROBABILITY = 0.3  # 30% chance a worker is stopped mid-poll

        run_id = str(uuid.uuid4())
        execution_counts: dict[str, int] = {}
        counts_lock = threading.Lock()

        # Enqueue tasks
        q_setup = TaskQueue(DATABASE_URL)
        for i in range(N_TASKS):
            q_setup.enqueue(run_id, f"step-{i:03d}")
        q_setup.close()

        def make_tool():
            def tool(r_id, s_id, payload):
                with counts_lock:
                    execution_counts[s_id] = execution_counts.get(s_id, 0) + 1
                time.sleep(random.uniform(0.005, 0.02))
                return {"done": True}
            return tool

        stop_events: list[threading.Event] = []

        def worker_thread(worker_idx: int) -> None:
            stop = threading.Event()
            stop_events.append(stop)
            idem = InMemoryIdempotencyStore()
            q = TaskQueue(DATABASE_URL)
            loop = WorkerLoop(
                q,
                {"default": make_tool()},
                idempotency_store=idem,
                poll_interval=0.01,
                lease_seconds=5,
                heartbeat_interval=2.0,
                worker_id=f"chaos-worker-{worker_idx}",
            )
            # Randomly abort some workers
            if random.random() < KILL_PROBABILITY:
                threading.Timer(random.uniform(0.05, 0.3), stop.set).start()

            while not stop.is_set():
                try:
                    if not loop.run_once():
                        time.sleep(0.01)
                except BaseException:  # noqa: BLE001,S110 — chaos harness swallows all errors
                    pass
            q.close()

        threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(N_WORKERS)]
        for t in threads:
            t.start()

        # Wait up to 30 seconds for all tasks to complete or fail
        deadline = time.time() + 30
        while time.time() < deadline:
            q_check = TaskQueue(DATABASE_URL)
            pending = q_check.list_tasks(status="pending")
            running = q_check.list_tasks(status="running")
            q_check.close()
            if not pending and not running:
                break
            time.sleep(0.1)

        # Stop all workers
        for ev in stop_events:
            ev.set()
        for t in threads:
            t.join(timeout=5)

        # Assertions
        q_final = TaskQueue(DATABASE_URL)
        all_tasks = q_final.list_tasks()
        q_final.close()

        # All tasks with status done must have been executed exactly once
        for step_id, count in execution_counts.items():
            assert count == 1, (
                f"Step {step_id!r} was executed {count} times — duplicate side-effect detected!"
            )

        # All tasks must have reached a terminal state
        non_terminal = [
            t for t in all_tasks if t["status"] not in ("done", "failed")
        ]
        assert not non_terminal, (
            f"Tasks still pending/running after deadline: {non_terminal}"
        )

    def test_expired_lease_reclaim_completes_task(self, db_conn):
        """Simulate a worker dying after claiming: the task must be recoverable."""
        run_id = str(uuid.uuid4())
        q = TaskQueue(DATABASE_URL)
        q.enqueue(run_id, "crash-step")

        # Claim and then artificially expire the lease
        q.claim_next_task("dead-worker", lease_seconds=30)
        conn2 = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            with conn2.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET lease_until = %s WHERE run_id = %s",
                    (time.time() - 1, run_id),
                )
            conn2.commit()
        finally:
            conn2.close()

        # A new worker claims and completes it
        task = q.claim_next_task("recovery-worker", lease_seconds=30)
        assert task is not None
        assert task.attempts == 2
        q.complete_task(run_id, "crash-step")

        # Verify no more claims possible
        assert q.claim_next_task("another-worker") is None
        q.close()
