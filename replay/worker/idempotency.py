"""Idempotency store: prevents duplicate tool-call side-effects on retry.

Key design (ADR-05):
  idem_key = SHA-256(run_id + ':' + step_id + ':' + sha256(canonical_input))

The attempt number is intentionally EXCLUDED from the key so that every
retry of the same logical tool call maps to the same row.  The worker writes
a ToolCalled event BEFORE executing the tool (write-ahead); on resume after a
crash it checks this store first.  If a result is found, it is returned as-is
and the tool is not called again.

Two backends:
  - DbIdempotencyStore: psycopg2, backed by the idempotency_results table.
  - InMemoryIdempotencyStore: dict, for unit tests with no DB.
Both implement the same interface.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import psycopg2
import psycopg2.extras

__all__ = ["DbIdempotencyStore", "InMemoryIdempotencyStore", "make_idem_key"]


def make_idem_key(run_id: str, step_id: str, canonical_input: Any) -> str:
    """Compute an attempt-independent idempotency key.

    canonical_input is serialised with sorted keys so dict ordering does not
    affect the key.  Returns a 64-character hex SHA-256 digest.
    """
    input_bytes = json.dumps(canonical_input, sort_keys=True, ensure_ascii=False).encode()
    input_hash = hashlib.sha256(input_bytes).hexdigest()
    composite = f"{run_id}:{step_id}:{input_hash}"
    return hashlib.sha256(composite.encode()).hexdigest()


class DbIdempotencyStore:
    """Postgres-backed idempotency store using the idempotency_results table."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._conn: psycopg2.extensions.connection | None = None

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

    def get(self, idem_key: str) -> Any | None:
        """Return the stored result or None if not found."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT result_json FROM idempotency_results WHERE idem_key = %s",
                (idem_key,),
            )
            row = cur.fetchone()
        return row["result_json"] if row else None

    def set(self, idem_key: str, result: Any) -> None:
        """Persist a result.  No-ops if the key already exists (INSERT … DO NOTHING)."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idempotency_results (idem_key, result_json)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (idem_key) DO NOTHING
                    """,
                    (idem_key, json.dumps(result, ensure_ascii=False)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # Convenience wrapper that accepts the same args as make_idem_key
    def get_by_args(self, run_id: str, step_id: str, canonical_input: Any) -> Any | None:
        return self.get(make_idem_key(run_id, step_id, canonical_input))

    def set_by_args(self, run_id: str, step_id: str, canonical_input: Any, result: Any) -> None:
        self.set(make_idem_key(run_id, step_id, canonical_input), result)


class InMemoryIdempotencyStore:
    """Dict-backed idempotency store for unit tests (no DB required)."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    def get(self, idem_key: str) -> Any | None:
        return self._store.get(idem_key)

    def set(self, idem_key: str, result: Any) -> None:
        self._store.setdefault(idem_key, result)

    def get_by_args(self, run_id: str, step_id: str, canonical_input: Any) -> Any | None:
        return self.get(make_idem_key(run_id, step_id, canonical_input))

    def set_by_args(self, run_id: str, step_id: str, canonical_input: Any, result: Any) -> None:
        self.set(make_idem_key(run_id, step_id, canonical_input), result)


# ---------------------------------------------------------------------------
# Backwards-compatible shim so existing tests that import IdempotencyStore
# or call old-style .get(run_id, step_id, attempt, payload_hash) still work.
# ---------------------------------------------------------------------------

class IdempotencyStore(InMemoryIdempotencyStore):
    """Legacy in-memory store kept for test backwards-compatibility.

    Old signature: get/set(run_id, step_id, attempt, payload_hash, [result])
    New callers should use InMemoryIdempotencyStore or DbIdempotencyStore.
    """

    def _legacy_key(self, run_id: str, step_id: str, attempt: int, payload_hash: str) -> str:
        composite = f"{run_id}:{step_id}:{attempt}:{payload_hash}"
        return hashlib.sha256(composite.encode()).hexdigest()[:32]

    def get(self, run_id_or_key: str, step_id: str | None = None,  # type: ignore[override]
            attempt: int | None = None, payload_hash: str | None = None) -> Any | None:
        if step_id is None:
            # New-style call: first arg is the idem_key
            return self._store.get(run_id_or_key)
        k = self._legacy_key(run_id_or_key, step_id, attempt or 0, payload_hash or "")
        return self._store.get(k)

    def set(self, run_id_or_key: str, step_id: str | None = None,  # type: ignore[override]
            attempt: int | None = None, payload_hash: str | None = None,
            result: Any | None = None) -> None:
        if step_id is None:
            # New-style call: (idem_key, result)
            self._store.setdefault(run_id_or_key, attempt)  # attempt holds result here
            return
        k = self._legacy_key(run_id_or_key, step_id, attempt or 0, payload_hash or "")
        self._store.setdefault(k, result)
