-- The Change Sequence: drawn by every RegisterChanges item (an accepted one
-- keeps it as its change_seq and its stale_seq), every other stale mark (a
-- marked Parent, a MarkStale row: its stale_seq), every BeginBuild (its
-- build_idx, which is also the build's start), every BeginDelete (its
-- build_idx) and every Rebuild walk start (NextChangeSeq). The drift check
-- compares a resource's change_seq with the start of the build that fetched
-- it, so nextval order must be real-time order across sessions (CACHE 1: no
-- per-session blocks) and the sequence must never wrap below existing stamps
-- (NO CYCLE: exhausting it fails registrations loudly). Because build_idx and
-- stale_seq come from it too, their values never repeat, across a hard delete
-- and a recreate of a row as well, so no token of the old row matches the new
-- one. build_idx never goes backwards, so a recreated resource is built above
-- every Elasticsearch version its old document had. stale_seq can: a mark of
-- a row created by a transaction uncommitted when the mark started can draw a
-- value below the creator's (seams S14). The guards compare stale_seq and
-- owner tokens only for equality, so that costs them nothing. ReleaseFailed
-- does order change_seq against the owner token, to tell a registration
-- since the claim; a stale_seq lowered below change_seq would make every
-- later claim's token look like one, so a row whose change_seq is above its
-- stale_seq is backed off as if none came (see ReleaseFailed).
CREATE SEQUENCE IF NOT EXISTS change_sequence AS bigint INCREMENT BY 1 CACHE 1 NO CYCLE;

CREATE TABLE IF NOT EXISTS resources (
    type VARCHAR NOT NULL,
    id VARCHAR NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    build_idx BIGINT NOT NULL DEFAULT 0,    -- Build Sequence: Change Sequence value of the last BeginBuild or BeginDelete; 0 = never built
    stale_seq BIGINT NOT NULL DEFAULT 0,    -- Change Sequence value of the last stale mark; 0 = never marked
    change_seq BIGINT NOT NULL DEFAULT 0,   -- Change Sequence value of the last accepted change; 0 = never changed
    stale_since TIMESTAMPTZ,                -- NULL = clean
    deleted BOOLEAN NOT NULL DEFAULT false,
    metadata JSONB,                         -- the resource's own: its last registration's, or its plans' report while it has none (NULL or {}); a mark never writes it
    owner_seq BIGINT,                       -- Build owner token: the stale_seq its claim saw; NULL = no owner
    owner_since TIMESTAMPTZ,                -- when the owner last claimed or renewed; its lease runs from here
    sweep_attempts INTEGER,                 -- failed owned builds or deletes in a row (ReleaseFailed); NULL = none since the last success or own registration
    sweep_after TIMESTAMPTZ,                -- the sweep's next turn after the last such failure (ListStale skips the row until then); NULL = never failed
    UNIQUE (type, id)
);

-- ListStale's order: a row's turn is its backoff's sweep_after once a build
-- of it failed, else its stale mark.
CREATE INDEX IF NOT EXISTS idx_resources_stale ON resources ((COALESCE(sweep_after, stale_since)))
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

-- Keep the Change Sequence ahead of every number drawn from it. A sequence
-- behind them — recreated, RESTARTed, or left behind by a restore of the
-- table alone — would hand out starts below existing change_seq values, so
-- every parent of such a child would drift on every build until it caught
-- up, and would repeat build_idx and stale_seq values rows already carry: a
-- build below its document's Elasticsearch version loses its write, and a
-- repeated stale_seq can match a token or a captured mark it doesn't belong
-- to. An edge set's build_seq is a Build Sequence too, and can outlive its
-- row (a late build writes it after a hard delete): behind it, a recreate's
-- ReplaceEdges would be rejected for that version. Raise the sequence to the
-- largest change_seq, build_idx, stale_seq or edge-set build_seq only when
-- its next value would not exceed that; empty tables, rows never changed,
-- built or marked, or a sequence already ahead are left alone, so applying
-- this file never moves the sequence backwards. The next value is last_value
-- + 1 once called, but last_value itself on a fresh or RESTARTed sequence
-- (is_called false).
DO $$
DECLARE
    stamped bigint;
    next_value bigint;
BEGIN
    SELECT greatest(
               (SELECT greatest(max(change_seq), max(build_idx), max(stale_seq)) FROM resources),
               (SELECT max(build_seq) FROM edge_sets))
        INTO stamped;
    SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END
        INTO next_value FROM change_sequence;
    IF stamped >= next_value THEN
        PERFORM setval('change_sequence', stamped, true);
    END IF;
END
$$;
