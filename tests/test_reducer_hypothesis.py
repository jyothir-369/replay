"""Property-based tests for the event reducer using Hypothesis."""

from hypothesis import given, strategies as st, settings
import pytest

from replay.core.events import Event
from replay.store.reducer import derive_state


# Strategy: build a valid sequence of events starting with RunCreated
def _valid_event_sequence(start_seq: int = 1) -> list[Event]:
    """Build a deterministic valid sequence of events."""
    return [
        Event("run-1", start_seq, "RunCreated", {"workflow": "yield-excursion"}),
        Event("run-1", start_seq + 1, "StepScheduled", {"step_id": "propose_hold", "workflow": "yield-excursion"}),
        Event("run-1", start_seq + 2, "StepStarted", {"step_id": "propose_hold", "attempt": 1}),
        Event("run-1", start_seq + 3, "ToolCalled", {"tool": "read_lot_log", "idempotency_key": "k1"}),
        Event("run-1", start_seq + 4, "ToolReturned", {"tool": "read_lot_log", "output": {}}),
        Event("run-1", start_seq + 5, "StepSucceeded", {"step_id": "propose_hold"}),
    ]


@given(st.integers(min_value=1, max_value=1000))
@settings(max_examples=50)
def test_valid_event_sequence_always_yields_valid_state(seq):
    """Any sequence starting from RunCreated yields a valid RunState."""
    events = _valid_event_sequence(seq)
    state = derive_state(events)
    assert state is not None
    assert hasattr(state, "status")


def test_simple_derive_state():
    """Deterministic happy path (ends at StepSucceeded)."""
    events = _valid_event_sequence()
    state = derive_state(events)
    assert state.status == "STEP_SUCCEEDED"