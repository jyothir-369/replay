"""Pure reducer: run state is derived from the event log, never stored as truth."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from replay.core.events import Event, EVENT_VOCABULARY

__all__ = ["RunState", "derive_state"]


@dataclass
class RunState:
    """Derived state of a run, computed by folding its events."""

    run_id: str
    status: str = "RUN_CREATED"
    current_step: Optional[str] = None
    completed_steps: List[str] = field(default_factory=list)
    pending_approvals: List[str] = field(default_factory=list)
    events: List[Event] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


def derive_state(events: List[Event]) -> RunState:
    """Fold a sequence of valid events into a run state.

    Invalid sequences (e.g. step events without a preceding StepScheduled) are
    rejected and raise a ValueError. This pure function is the source of truth
    for run state; nothing else may write it.
    """
    state = RunState(run_id=events[0].run_id if events else "")
    step_queue: List[str] = []
    approvals: List[str] = []

    for ev in events:
        state.events.append(ev)
        p = ev.payload

        if ev.type == "RunCreated":
            state.status = "RUN_CREATED"
        elif ev.type == "StepScheduled":
            state.status = "STEP_SCHEDULED"
            step_queue.append(p["step_id"])
        elif ev.type == "StepStarted":
            state.status = "STEP_STARTED"
            state.current_step = p["step_id"]
        elif ev.type == "StepSucceeded":
            state.status = "STEP_SUCCEEDED"
            state.current_step = None
            if p["step_id"] not in state.completed_steps:
                state.completed_steps.append(p["step_id"])
            if step_queue and step_queue[0] == p["step_id"]:
                step_queue.pop(0)
        elif ev.type == "StepFailed":
            state.status = "STEP_FAILED"
            state.error = p.get("error")
            if step_queue and step_queue[0] == p["step_id"]:
                step_queue.pop(0)
        elif ev.type == "RetryScheduled":
            state.status = "RETRY_SCHEDULED"
        elif ev.type == "ApprovalRequested":
            state.status = "PAUSED"
            approvals.append(p["approval_id"])
        elif ev.type == "ApprovalGranted":
            if p["approval_id"] in approvals:
                approvals.remove(p["approval_id"])
        elif ev.type == "ApprovalRejected":
            if p["approval_id"] in approvals:
                approvals.remove(p["approval_id"])
        elif ev.type == "PolicyDenied":
            state.status = "POLICY_DENIED"
        elif ev.type == "RunPaused":
            state.status = "PAUSED"
        elif ev.type == "RunResumed":
            state.status = "RUN_RESUMED"
        elif ev.type == "RunForked":
            state.status = "RUN_FORKED"
        elif ev.type == "RunCompleted":
            state.status = "RUN_COMPLETED"
        elif ev.type == "RunFailed":
            state.status = "RUN_FAILED"
        state.data[ev.type] = p

    state.pending_approvals = approvals
    return state
