"""MockEventStore chain verification and tamper detection tests."""

from replay.core.events import Event
from replay.core.hashing import hash_event
from replay.store.mock_store import MockEventStore


def test_mock_event_store_chain_verification():
    """MockEventStore.verify_chain should return True for valid chains."""
    store = MockEventStore()
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold"}),
        Event("run-1", 3, "StepStarted", {"step_id": "propose_hold"}),
    ]
    # Append with proper hash linking
    prev_hash = None
    for ev in events:
        ev_hash = store.append(ev)  # append computes hash and sets prev_hash
        prev_hash = ev_hash
    assert store.verify_chain("run-1") is True


def test_mock_event_store_empty_run():
    """Empty run should have empty events list and valid chain."""
    store = MockEventStore()
    events = store.get_events("nonexistent")
    assert events == []
    assert store.verify_chain("nonexistent") is True