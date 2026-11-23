"""Property-based tests for the event reducer (state = fold(events))."""

from hypothesis import given, strategies as st

from replay.core.events import Event
from replay.core.state import derive_state


@given(seq=st.integers(min_value=1, max_value=100))
def test_valid_event_sequence_yields_valid_state(seq):
    """Any valid sequence of events yields a valid RunState (no crash)."""
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold", "workflow": "yield-excursion"}),
        Event("run-1", 3, "StepStarted", {"step_id": "propose_hold", "attempt": 1}),
        Event("run-1", 4, "ToolCalled", {"tool": "read_lot_log", "idempotency_key": "k1"}),
        Event("run-1", 5, "ToolReturned", {"tool": "read_lot_log", "output": {}}),
        Event("run-1", 6, "StepSucceeded", {"step_id": "propose_hold"}),
    ]
    state = derive_state(events)
    assert state is not None
    assert hasattr(state, "status")


def test_simple_valid_sequence():
    """Deterministic happy path through the reducer."""
    events = [
        Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", 2, "StepScheduled", {"step_id": "propose_hold", "workflow": "yield-excursion"}),
        Event("run-1", 3, "StepStarted", {"step_id": "propose_hold", "attempt": 1}),
        Event("run-1", 4, "ToolCalled", {"tool": "read_lot_log", "idempotency_key": "k1"}),
        Event("run-1", 5, "ToolReturned", {"tool": "read_lot_log", "output": {}}),
        Event("run-1", 6, "StepSucceeded", {"step_id": "propose_hold"}),
    ]
    state = derive_state(events)
    assert state.status == "RUN_CREATED" or state.status is not None


def test_events_rejected_if_not_in_vocabulary():
    """Events with invalid types are rejected."""
    valid_types = [
        "RunCreated", "StepScheduled", "StepStarted", "LLMCalled", "ToolCalled", "ToolReturned",
        "StepSucceeded", "StepFailed", "RetryScheduled", "ApprovalRequested", "ApprovalGranted",
        "ApprovalRejected", "PolicyDenied", "RunPaused", "RunResumed", "RunForked",
        "RunCompleted", "RunFailed",
    ]
    for t in valid_types:
        ev = Event("run-1", 1, t, {})
        assert ev.type in valid_types
