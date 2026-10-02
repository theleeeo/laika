-- The Change Sequence: drawn by every RegisterChanges item (an accepted one
-- keeps it as its change_seq), every BeginBuild and every Rebuild walk start
-- (NextChangeSeq). The drift check compares a resource's change_seq with
-- the start of the build that fetched it, so nextval order must be real-time
-- order across sessions (CACHE 1: no per-session blocks) and the sequence
-- must never wrap below existing stamps (NO CYCLE: exhausting it fails
-- registrations loudly).
CREATE SEQUENCE IF NOT EXISTS change_sequence AS bigint INCREMENT BY 1 CACHE 1 NO CYCLE;

CREATE TABLE IF NOT EXISTS resources (
    type VARCHAR NOT NULL,
    id VARCHAR NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    build_idx BIGINT NOT NULL DEFAULT 0,
    stale_seq BIGINT NOT NULL DEFAULT 0,
    change_seq BIGINT NOT NULL DEFAULT 0,   -- Change Sequence value of the last accepted change; 0 = never changed
    stale_since TIMESTAMPTZ,                -- NULL = clean
    deleted BOOLEAN NOT NULL DEFAULT false,
    metadata JSONB,                         -- notification metadata of the last stale mark
    owner_seq BIGINT,                       -- Build owner token: the stale_seq its claim saw; NULL = no owner
    owner_since TIMESTAMPTZ,                -- when the owner last claimed or renewed; its lease runs from here
    UNIQUE (type, id)
);

CREATE INDEX IF NOT EXISTS idx_resources_stale ON resources (stale_since)
    WHERE stale_since IS NOT NULL;

CREATE TABLE IF NOT EXISTS relations (
	resource VARCHAR NOT NULL,
	resource_id VARCHAR NOT NULL,
	related_resource VARCHAR NOT NULL,
	related_resource_id VARCHAR NOT NULL,
	UNIQUE (resource, resource_id, related_resource, related_resource_id)
);
CREATE INDEX IF NOT EXISTS idx_resource ON relations (resource, resource_id);
CREATE INDEX IF NOT EXISTS idx_related_resource ON relations (related_resource, related_resource_id);

-- Keep the Change Sequence ahead of every stamp. A sequence behind the stamps
-- — recreated, RESTARTed, or left behind by a restore of the table alone —
-- would hand out starts below existing change_seq values, and every parent
-- of such a child would drift on every build until it caught up. Raise it to
-- max(change_seq) only when its next value would not exceed that; an empty
-- table, rows that never changed, or a sequence already ahead are left alone,
-- so applying this file never moves the sequence backwards. The next value is
-- last_value + 1 once called, but last_value itself on a fresh or RESTARTed
-- sequence (is_called false).
DO $$
DECLARE
    stamped bigint;
    next_value bigint;
BEGIN
    SELECT max(change_seq) INTO stamped FROM resources;
    SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END
        INTO next_value FROM change_sequence;
    IF stamped >= next_value THEN
        PERFORM setval('change_sequence', stamped, true);
    END IF;
END
$$;
