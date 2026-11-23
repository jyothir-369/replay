import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import psycopg2
import pytest

from replay.core.events import Event
from replay.store import EventStore
from replay.store.real_event_store import RealEventStore


DATABASE_URL = os.environ.get("REPLAY_TEST_DATABASE_URL")

if not DATABASE_URL:
    pytest.skip(
        "Set REPLAY_TEST_DATABASE_URL to the isolated replay_test database.",
        allow_module_level=True,
    )

parsed = urlparse(DATABASE_URL)
if parsed.path.lstrip("/") != "replay_test":
    raise RuntimeError(
        "Refusing integration tests: REPLAY_TEST_DATABASE_URL must target replay_test."
    )

STORE_TYPES = [EventStore, RealEventStore]


def new_event(run_id, seq, payload=None):
    return Event(
        run_id=str(run_id),
        seq=seq,
        type="RunCreated" if seq == 1 else "StepSucceeded",
        payload=payload or {"sequence": seq},
    )


@pytest.mark.parametrize("store_type", STORE_TYPES)
def test_postgres_append_round_trip_and_hash_chain(store_type):
    run_id = str(uuid.uuid4())
    store = store_type(DATABASE_URL)

    try:
        first_hash = store.append(new_event(run_id, 1, {"value": "first"}))
        second_hash = store.append(new_event(run_id, 2, {"value": "second"}))

        assert isinstance(first_hash, bytes)
        assert isinstance(second_hash, bytes)

        events = store.get_events(run_id)
        assert len(events) == 2
        assert [event.seq for event in events] == [1, 2]
        assert events[0].payload == {"value": "first"}
        assert events[1].payload == {"value": "second"}
        assert events[0].hash == first_hash
        assert events[1].hash == second_hash
        assert events[1].prev_hash == first_hash
        assert store.verify_chain(run_id) is True
    finally:
        if store._conn is not None:
            store._conn.close()


@pytest.mark.parametrize("store_type", STORE_TYPES)
def test_postgres_rejects_duplicate_and_skipped_sequences(store_type):
    run_id = str(uuid.uuid4())
    store = store_type(DATABASE_URL)

    try:
        store.append(new_event(run_id, 1))

        with pytest.raises(ValueError):
            store.append(new_event(run_id, 1))

        with pytest.raises(ValueError):
            store.append(new_event(run_id, 3))

        events = store.get_events(run_id)
        assert [event.seq for event in events] == [1]
    finally:
        if store._conn is not None:
            store._conn.close()


@pytest.mark.parametrize("store_type", STORE_TYPES)
def test_postgres_serializes_concurrent_same_sequence_appends(store_type):
    run_id = str(uuid.uuid4())

    def append_once():
        store = store_type(DATABASE_URL)
        try:
            return store.append(new_event(run_id, 1))
        finally:
            if store._conn is not None:
                store._conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(append_once) for _ in range(2)]

    outcomes = []
    for future in futures:
        try:
            future.result()
            outcomes.append("success")
        except ValueError:
            outcomes.append("rejected")

    assert outcomes.count("success") == 1
    assert outcomes.count("rejected") == 1

    verifier = store_type(DATABASE_URL)
    try:
        assert len(verifier.get_events(run_id)) == 1
        assert verifier.verify_chain(run_id) is True
    finally:
        if verifier._conn is not None:
            verifier._conn.close()


@pytest.mark.parametrize("store_type", STORE_TYPES)
def test_postgres_append_only_triggers_reject_mutations(store_type):
    run_id = str(uuid.uuid4())
    store = store_type(DATABASE_URL)

    try:
        store.append(new_event(run_id, 1))
    finally:
        if store._conn is not None:
            store._conn.close()

    operations = [
        "UPDATE events SET type = 'Tampered' WHERE run_id = %s",
        "DELETE FROM events WHERE run_id = %s",
        "TRUNCATE TABLE events",
    ]

    for statement in operations:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cursor:
                if "%s" in statement:
                    cursor.execute(statement, (run_id,))
                else:
                    cursor.execute(statement)
            conn.commit()
            pytest.fail(f"Append-only protection failed to reject: {statement}")
        except psycopg2.Error:
            conn.rollback()
        finally:
            conn.close()

    verifier = store_type(DATABASE_URL)
    try:
        events = verifier.get_events(run_id)
        assert len(events) == 1
        assert events[0].type == "RunCreated"
        assert verifier.verify_chain(run_id) is True
    finally:
        if verifier._conn is not None:
            verifier._conn.close()
