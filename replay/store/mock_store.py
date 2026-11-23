"""Mock EventStore for testing without PostgreSQL.

This mock implements the same interface as the real EventStore
but uses in-memory structures for validation purposes.
"""

from typing import Any, Dict, List, Optional

from replay.core.events import Event, EVENT_VOCABULARY
from replay.core.hashing import hash_event, verify_hash_chain


class MockEventStore:
    """In-memory mock of the EventStore for testing and validation."""

    def __init__(self) -> None:
        self._events: Dict[str, List[Event]] = {}  # run_id -> events (in order)
        self._last_hash: Dict[str, Optional[bytes]] = {}  # run_id -> hash of last event

    def append(self, event: Event) -> bytes:
        """Append event and return its computed hash.

        The event's `prev_hash` is set automatically from the chain link.
        Raises ValueError if seq is not the expected next sequence number.
        """
        # Determine the expected previous hash
        prev_hash = self._last_hash.get(event.run_id)

        # Validate sequence continuity
        expected_seq = self._expected_seq(event.run_id)
        if event.seq != expected_seq:
            raise ValueError(
                f"Invalid sequence: expected {expected_seq}, got {event.seq}"
            )

        # Compute hash: this event's hash becomes next event's prev_hash
        event_hash = hash_event(event.type, event.payload, event.seq, prev_hash)

        # Create a new event with prev_hash linked and hash stored
        linked_event = Event(
            run_id=event.run_id,
            seq=event.seq,
            type=event.type,
            payload=event.payload,
            prev_hash=prev_hash,
            hash=event_hash,
        )

        # Store
        self._events.setdefault(event.run_id, []).append(linked_event)
        self._last_hash[event.run_id] = event_hash
        return event_hash

    def _expected_seq(self, run_id: str) -> int:
        """Expected next sequence number for a run."""
        if run_id not in self._last_hash or not self._events.get(run_id):
            return 1
        return len(self._events[run_id]) + 1

    def _get_prev_hash(self, event: Event) -> Optional[bytes]:
        """Get prev_hash from last event in the same run."""
        return self._last_hash.get(event.run_id)

    def get_events(self, run_id: str) -> List[Event]:
        """Get all events for a run in order."""
        return self._events.get(run_id, [])

    def verify_chain(self, run_id: str) -> bool:
        """Verify hash chain integrity."""
        events = self.get_events(run_id)
        return verify_hash_chain(events)

    def __len__(self):
        return sum(len(events) for events in self._events.values())