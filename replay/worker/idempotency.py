"""Idempotency key storage for side-effect deduplication.

Each tool call carries an idempotency_key = hash(run_id, step_id, attempt_independent_input).
The tool executor stores (key -> result); a retried call returns the stored result.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


class IdempotencyStore:
    """In-memory (or DB-backed) store mapping idempotency keys to results.

    The default is a dict, but the constructor can accept a DB connection.
    """

    def __init__(self, store: Optional[Dict[str, Any]] = None) -> None:
        self._store: Dict[str, Any] = store or {}

    def _key(self, run_id: str, step_id: str, attempt: int, payload_hash: str) -> str:
        """Compute a composite idempotency key."""
        return hashlib.sha256(
            f"{run_id}:{step_id}:{attempt}:{payload_hash}".encode()
        ).hexdigest()[:32]

    def get(self, run_id: str, step_id: str, attempt: int, payload_hash: str) -> Optional[Any]:
        """Look up a previously stored result."""
        k = self._key(run_id, step_id, attempt, payload_hash)
        return self._store.get(k)

    def set(self, run_id: str, step_id: str, attempt: int, payload_hash: str, result: Any) -> None:
        """Store a result against its idempotency key."""
        k = self._key(run_id, step_id, attempt, payload_hash)
        self._store[k] = result