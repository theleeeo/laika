CREATE TABLE IF NOT EXISTS resources (
    type VARCHAR NOT NULL,
    id VARCHAR NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    build_idx BIGINT NOT NULL DEFAULT 0,
    stale_seq BIGINT NOT NULL DEFAULT 0,
    stale_since TIMESTAMPTZ,                -- NULL = clean
    deleted BOOLEAN NOT NULL DEFAULT false,
    metadata JSONB,                         -- notification metadata of the last stale mark
    UNIQUE (type, id)
);

-- Upgrade path for databases created before the metadata column existed.
ALTER TABLE resources ADD COLUMN IF NOT EXISTS metadata JSONB;

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