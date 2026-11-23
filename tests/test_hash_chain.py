"""Hash chain integrity tests using MockEventStore."""

from replay.core.events import Event
from replay.core.hashing import hash_event, canonical_json, verify_hash_chain
from replay.store.mock_store import MockEventStore


def test_hash_chain_one_event():
    """A single event has a valid hash."""
    payload = {"step": "test"}
    h = hash_event("RunCreated", payload, 1)
    assert len(h) == 32  # SHA-256


def test_hash_chain_two_events():
    """Two events linked by prev_hash."""
    payload1 = {"step": "first"}
    payload2 = {"step": "second"}
    h1 = hash_event("RunCreated", payload1, 1)
    h2 = hash_event("StepScheduled", payload2, 2, h1)
    assert len(h2) == 32
    events = [
        Event("run-1", 1, "RunCreated", payload1, prev_hash=None, hash=h1),
        Event("run-1", 2, "StepScheduled", payload2, prev_hash=h1, hash=h2),
    ]
    assert verify_hash_chain(events) is True


def test_verify_hash_chain():
    """Verify a chain of 3 events."""
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold"}),
        Event("run-1", 3, "StepStarted", {"step_id": "propose_hold", "attempt": 1}),
    ]
    prev_hash = None
    linked_events = []
    for ev in events:
        ev_hash = hash_event(ev.type, ev.payload, ev.seq, prev_hash)
        linked_events.append(Event(
            run_id=ev.run_id, seq=ev.seq, type=ev.type, payload=ev.payload,
            prev_hash=prev_hash, hash=ev_hash,
        ))
        prev_hash = ev_hash
    assert verify_hash_chain(linked_events) is True


def test_hash_chain_detects_tampering():
    """Tampering with a payload changes the hash chain."""
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold"}),
    ]
    original_payload = events[1].payload.copy()
    tampered_event = Event(
        "run-1", 2, "StepScheduled",
        {**original_payload, "step_id": "TAMPERED"},
        events[0].hash,
    )
    h_orig = hash_event("StepScheduled", original_payload, 2, None)
    h_tampered = hash_event("StepScheduled", tampered_event.payload, 2, None)
    assert h_orig != h_tampered


def test_mock_event_store_append_and_get():
    """MockEventStore appends events and retrieves them in order."""
    store = MockEventStore()
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold"}),
        Event("run-1", 3, "StepStarted", {"step_id": "propose_hold"}),
    ]
    for ev in events:
        store.append(ev)
    retrieved = store.get_events("run-1")
    assert len(retrieved) == 3
    assert retrieved[0].type == "RunCreated"
    assert retrieved[1].type == "StepScheduled"
    assert retrieved[2].type == "StepStarted"


def test_mock_event_store_sequence_validation():
    """MockEventStore rejects out-of-order sequences."""
    store = MockEventStore()
    events = [
        Event("run-1", 1, "RunCreated", {}),
        Event("run-1", 3, "StepScheduled", {}),  # skip 2
    ]
    store.append(events[0])
    try:
        store.append(events[1])
        assert False, "Should have raised ValueError for invalid sequence"
    except ValueError as e:
        assert "expected 2" in str(e)
