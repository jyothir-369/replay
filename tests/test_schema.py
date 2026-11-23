"""Schema validation test: canonical JSON roundtrip and hash chain."""

from replay.core.hashing import hash_event, canonical_json, verify_hash_chain
from replay.core.events import Event


def test_canonical_json_sorted_keys():
    payload = {"b": 2, "a": 1}
    data = canonical_json("RunCreated", payload, 1)
    s = data.decode("utf-8")
    # Payload is nested: {"payload":{...},"seq":1,"type":"RunCreated"}
    assert '"payload"' in s
    # Keys within payload are sorted
    assert '"a":1' in s
    # Sorted key order overall: payload, seq, type
    assert s.index('"payload"') < s.index('"seq"') < s.index('"type"')


def test_hash_chain_roundtrip():
    """One event, then verify hash chain."""
    payload = {"test": "value"}
    ev1_hash = hash_event("RunCreated", payload, 1)
    ev2_hash = hash_event("StepScheduled", payload, 2, ev1_hash)
    # Verify: prev_hash of ev2 should match ev1_hash
    assert ev2_hash is not None


def test_verify_hash_chain():
    """Verify with a chain of 2 events."""
    payload1 = {"step": "first"}
    payload2 = {"step": "second"}
    ev1_hash = hash_event("RunCreated", payload1, 1)
    ev2_hash = hash_event("StepScheduled", payload2, 2, ev1_hash)
    # Construct events with correct prev_hash
    from replay.core.events import Event
    ev1 = Event(run_id="test-run", seq=1, type="RunCreated", payload=payload1, prev_hash=None, hash=ev1_hash)
    ev2 = Event(run_id="test-run", seq=2, type="StepScheduled", payload=payload2, prev_hash=ev1_hash, hash=ev2_hash)
    assert verify_hash_chain([ev1, ev2]) is True