-- Migration 002: preserve insert audit behavior and enforce append-only events.

CREATE OR REPLACE FUNCTION events_append_only_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.created_at := COALESCE(NEW.created_at, now());
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS events_append_only ON events;

CREATE TRIGGER events_append_only
BEFORE INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION events_append_only_trigger();


CREATE OR REPLACE FUNCTION replay_reject_event_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only: % is not permitted', TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS trg_events_reject_update_delete ON events;

CREATE TRIGGER trg_events_reject_update_delete
BEFORE UPDATE OR DELETE ON events
FOR EACH ROW
EXECUTE FUNCTION replay_reject_event_mutation();

DROP TRIGGER IF EXISTS trg_events_reject_truncate ON events;

CREATE TRIGGER trg_events_reject_truncate
BEFORE TRUNCATE ON events
FOR EACH STATEMENT
EXECUTE FUNCTION replay_reject_event_mutation();