"""Canonical JSON serialization and SHA-256 event hashing.

Hash = SHA256(canonical_json(type, payload, seq, prev_hash))
Canonical JSON: sorted keys, no whitespace, utf-8.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from replay.core.events import Event

__all__ = ["canonical_json", "hash_event", "compute_event_hash", "verify_hash_chain"]


def canonical_json(
    event_type: str,
    payload: Dict[str, Any],
    seq: int,
    prev_hash: Optional[bytes] = None,
) -> bytes:
    """Produce canonical JSON bytes for hashing.

    The structure is: {"type": ..., "payload": {...}, "seq": ..., "prev_hash": "..."}
    Keys are sorted; prev_hash is hex-encoded or omitted if None.
    """
    obj: Dict[str, Any] = {
        "type": event_type,
        "payload": payload,
        "seq": seq,
    }
    if prev_hash is not None:
        obj["prev_hash"] = prev_hash.hex()
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")


def hash_event(
    event_type: str,
    payload: Dict[str, Any],
    seq: int,
    prev_hash: Optional[bytes] = None,
) -> bytes:
    """Compute the SHA-256 hash of an event (the chain link)."""
    data = canonical_json(event_type, payload, seq, prev_hash)
    return hashlib.sha256(data).digest()


def compute_event_hash(
    event_type: str,
    payload: Dict[str, Any],
    seq: int,
    prev_hash: Optional[bytes] = None,
) -> bytes:
    """Alias for hash_event, for clarity in store implementations."""
    return hash_event(event_type, payload, seq, prev_hash)


def verify_hash_chain(events: List[Event]) -> bool:
    """Verify event hash values and the complete hash chain.

    Recomputes each event hash, compares it with the stored hash, and
    verifies that each event links to the preceding computed hash.
    Returns False if any stored hash or link is invalid.
    """
    prev: Optional[bytes] = None

    for ev in events:
        if getattr(ev, "prev_hash", None) != prev:
            return False

        stored_hash = getattr(ev, "hash", None)
        if stored_hash is None:
            return False

        try:
            stored_hash = bytes(stored_hash)
        except (TypeError, ValueError):
            return False

        computed_hash = hash_event(ev.type, ev.payload, ev.seq, prev)

        if stored_hash != computed_hash:
            return False

        prev = computed_hash

    return True
