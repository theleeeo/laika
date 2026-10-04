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

-- The edges of the Relation graph, Parent (resource) to Child (related), one
-- set per Schema Version: each version's plan finds its own Children, and a
-- version's set is replaced only by a build at or above its edge_sets stamp.
-- Fanout reads the union across versions.
CREATE TABLE IF NOT EXISTS relations (
	resource VARCHAR NOT NULL,
	resource_id VARCHAR NOT NULL,
	schema_version INTEGER NOT NULL,        -- the Schema Version whose plan found the edge
	related_resource VARCHAR NOT NULL,
	related_resource_id VARCHAR NOT NULL,
	UNIQUE (resource, resource_id, schema_version, related_resource, related_resource_id)
);
CREATE INDEX IF NOT EXISTS idx_resource ON relations (resource, resource_id);
CREATE INDEX IF NOT EXISTS idx_related_resource ON relations (related_resource, related_resource_id);

-- One row per stored edge set: a resource's edges of one Schema Version, as
-- the build stamped in build_seq wrote them. A version's relations rows
-- exist only with its row here; an empty set is a row with no relations.
-- Writers of a resource's edges lock its rows here in ascending
-- schema_version order (ReplaceEdges, RemoveResource).
CREATE TABLE IF NOT EXISTS edge_sets (
    type VARCHAR NOT NULL,
    id VARCHAR NOT NULL,
    schema_version INTEGER NOT NULL,
    build_seq BIGINT NOT NULL,              -- Build Sequence of the build that wrote the set
    PRIMARY KEY (type, id, schema_version)
);

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
