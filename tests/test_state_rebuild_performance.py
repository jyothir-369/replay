"""Performance test: rebuilding state of a 10,000-event run must be under 500ms."""

import time
from replay.core.events import Event
from replay.store.reducer import derive_state


def test_rebuild_state_of_10000_event_run():
    """Rebuild state by folding 10,000 events; must complete under 500ms."""
    events = [Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"})]

    # Create 9,999 more events: tool call/return pairs, then a RunCompleted.
    seq = 2
    for i in range(4999):
        events.append(Event("run-1", seq, "ToolCalled", {"tool": f"tool_{i}"}))
        events.append(Event("run-1", seq + 1, "ToolReturned", {"tool": f"tool_{i}"}))
        seq += 2
    events.append(Event("run-1", seq, "RunCompleted", {"result": "ok"}))

    assert len(events) == 10000

    start = time.perf_counter()
    state = derive_state(events)
    elapsed_ms = (time.perf_counter() - start) * 1000

    # Must complete in under 500ms
    assert elapsed_ms < 500, f"State rebuild took {elapsed_ms}ms, which exceeds the 500ms budget"
    assert state.status == "RUN_COMPLETED"


def test_rebuild_state_of_many_events():
    """Rebuild state of a run with 2,000 events, also under 500ms."""
    events = [Event("run-1", 1, "RunCreated", {"workflow": "yield-excursion"})]
    seq = 2
    for i in range(999):
        events.append(Event("run-1", seq, "ToolCalled", {"tool": f"tool_{i}"}))
        events.append(Event("run-1", seq + 1, "ToolReturned", {"tool": f"tool_{i}"}))
        seq += 2

    start = time.perf_counter()
    derive_state(events)
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert elapsed_ms < 500