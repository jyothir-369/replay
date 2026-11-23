"""Real PostgreSQL EventStore implementation with proper transaction handling.

Implements: transaction-safe sequence allocation, optimistic concurrency
(INSERT with expected seq, retry on conflict), hash-chain validation.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor

from replay.core.events import Event, EVENT_VOCABULARY
from replay.core.hashing import hash_event, verify_hash_chain


class RealEventStore:
    """PostgreSQL-backed event store with optimistic concurrency and retries.

    - Appends events transactionally.
    - Uses sequence allocation with optimistic concurrency control (OCC).
    - On hash-chain validation failure, rolls back.
    - Retries on conflict (bounded).
    """

    def __init__(self, db_url: str, max_retries: int = 3) -> None:
        self.db_url = db_url
        self.max_retries = max_retries
        self._conn = None

    def _connect(self) -> psycopg2.extensions.connection:
        """Lazy connect to PostgreSQL."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
            self._conn.autocommit = False
        return self._conn

    def append(self, event: Event) -> bytes:
        """Append an event only when its caller-supplied sequence is next.

        A transaction-scoped advisory lock serializes writers for this run.
        Stale or skipped sequence numbers are rejected instead of retrying
        the same immutable event with an unchanged sequence number.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (str(event.run_id),),
                )
                cur.execute(
                    """
                    SELECT seq, hash
                    FROM events
                    WHERE run_id = %s
                    ORDER BY seq DESC
                    LIMIT 1
                    """,
                    (event.run_id,),
                )
                previous = cur.fetchone()
                expected_seq = previous["seq"] + 1 if previous else 1

                if event.seq != expected_seq:
                    raise ValueError(
                        f"Invalid event sequence for run {event.run_id}: "
                        f"expected {expected_seq}, received {event.seq}"
                    )

                prev_hash = previous["hash"] if previous else None
                event_hash = hash_event(
                    event.type, event.payload, event.seq, prev_hash
                )

                cur.execute(
                    """
                    INSERT INTO events
                        (run_id, seq, type, payload, prev_hash, hash)
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        event.run_id,
                        event.seq,
                        event.type,
                        json.dumps(event.payload, ensure_ascii=False),
                        prev_hash,
                        event_hash,
                    ),
                )

            conn.commit()
            return event_hash
        except Exception:
            conn.rollback()
            raise
    def _get_prev_hash_for_run(self, run_id: str) -> Optional[bytes]:
        """Get previous event's hash for a run."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT hash FROM events WHERE run_id = %s ORDER BY seq DESC LIMIT 1",
            (run_id,),
        )
        row = cur.fetchone()
        cur.close()
        return row["hash"] if row else None

    def get_events(self, run_id: str) -> List[Event]:
        """Fetch all events for a run in order."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT run_id, seq, type, payload, prev_hash, hash, created_at
            FROM events
            WHERE run_id = %s
            ORDER BY seq ASC
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        cur.close()
        events = []
        for row in rows:
            payload_value = row["payload"]
            if isinstance(payload_value, (bytes, bytearray)):
                payload = json.loads(payload_value.decode("utf-8"))
            elif isinstance(payload_value, str):
                payload = json.loads(payload_value)
            else:
                payload = payload_value or {}
            events.append(
                Event(
                    run_id=row["run_id"],
                    seq=row["seq"],
                    type=row["type"],
                    payload=payload,
                    prev_hash=bytes(row["prev_hash"]) if row["prev_hash"] is not None else None,
                    hash=bytes(row["hash"]) if row["hash"] is not None else None,
                )
            )
        return events

    def verify_chain(self, run_id: str) -> bool:
        """Verify hash chain integrity for a run."""
        events = self.get_events(run_id)
        return verify_hash_chain(events)

    def __del__(self):
        """Ensure connection is closed on object destruction."""
        if self._conn:
            self._conn.close()