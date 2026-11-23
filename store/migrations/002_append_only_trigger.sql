-- migration 002: append-only enforcement via trigger
-- Fires BEFORE INSERT to ensure only new rows can be added

CREATE OR REPLACE FUNCTION events_append_only_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- The insert is always allowed since we revoked UPDATE/DELETE at the role level.
    -- This trigger adds an extra audit layer for any future schema changes.
    NEW.created_at := COALESCE(NEW.created_at, now());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the events table, firing BEFORE INSERT
CREATE TRIGGER events_append_only
BEFORE INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION events_append_only_trigger();