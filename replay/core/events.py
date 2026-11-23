"""Event vocabulary and event model — the contract everything else depends on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

__all__ = ["EVENT_VOCABULARY", "Event", "EventType"]

# The 18 event types from the implementation plan. Order matters: it matches
# the lifecycle of a run from creation to completion/failure.
EVENT_VOCABULARY: List[str] = [
    "RunCreated",
    "StepScheduled",
    "StepStarted",
    "LLMCalled",
    "ToolCalled",
    "ToolReturned",
    "StepSucceeded",
    "StepFailed",
    "RetryScheduled",
    "ApprovalRequested",
    "ApprovalGranted",
    "ApprovalRejected",
    "PolicyDenied",
    "RunPaused",
    "RunResumed",
    "RunForked",
    "RunCompleted",
    "RunFailed",
]


@dataclass(frozen=True)
class Event:
    """An immutable, ordered event appended to a run's event log.

    seq is assigned by the event store; hashing covers seq so that each event
    is bound to its position in the chain.
    """

    run_id: str
    seq: int
    type: str
    payload: Dict[str, Any]
    prev_hash: bytes | None = None
    hash: bytes | None = None  # computed hash of this event

    @property
    def type_name(self) -> str:
        return self.type


@dataclass(frozen=True)
class EventType:
    """A single event type name and its valid payload keys (schema hint)."""

    name: str
    payload_keys: List[str] = field(default_factory=list)
