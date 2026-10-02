package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) AddRelations(ctx context.Context, relations []core.Relation) error {
	if len(relations) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, relation := range relations {
		batch.Queue(
			`INSERT INTO relations (resource, resource_id, related_resource, related_resource_id) 
			 VALUES ($1, $2, $3, $4) 
			 ON CONFLICT (resource, resource_id, related_resource, related_resource_id) DO NOTHING`,
			relation.Parent.Type, relation.Parent.Id, relation.Child.Type, relation.Child.Id,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Store) GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT resource, resource_id FROM relations WHERE related_resource=$1 AND related_resource_id=$2`,
		childResource.Type, childResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parents []model.Resource
	for rows.Next() {
		var parentResource, parentResourceId string
		if err := rows.Scan(&parentResource, &parentResourceId); err != nil {
			return nil, err
		}
		parents = append(parents, model.Resource{Type: parentResource, Id: parentResourceId})
	}
	return parents, nil
}

func (s *Store) GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT related_resource, related_resource_id FROM relations WHERE resource=$1 AND resource_id=$2`,
		parentResource.Type, parentResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []model.Resource
	for rows.Next() {
		var childResource, childResourceId string
		if err := rows.Scan(&childResource, &childResourceId); err != nil {
			return nil, err
		}
		children = append(children, model.Resource{Type: childResource, Id: childResourceId})
	}
	return children, nil
}

func (s *Store) RemoveResource(ctx context.Context, resource model.Resource) error {
	_, err := s.pool.Exec(
		ctx,
		`DELETE FROM relations WHERE resource=$1 AND resource_id=$2`,
		resource.Type, resource.Id,
	)
	return err
}

func (s *Store) AddChildResources(ctx context.Context, parent model.Resource, childs []model.Resource) error {
	var relations []core.Relation
	for _, child := range childs {
		relations = append(relations, core.Relation{
			Parent: parent,
			Child:  child,
		})
	}
	return s.AddRelations(ctx, relations)
}

// RegisterChanges records a batch of changes in one statement, so each
// accepted item's version, its stale mark and its Parents' marks commit
// together or not at all. The item upsert's WHERE is the stale-version check:
// a delete or a version-0 item is always accepted, a versioned one only when
// strictly newer. Parents are found from the accepted items only, so a stale
// item marks nothing; accepted in-batch items are excluded from the Parent
// mark because one statement may modify a row only once, and their own row
// already carries the mark. A Parent shared by several accepted children
// stores the metadata of the last one in batch order.
//
// Every accepted row — upsert, version-0 item or delete — is stamped with a
// fresh Change Sequence value; a rejected item and a marked Parent keep
// theirs. The value is drawn for every input row, so a rejected item leaves
// a gap in the sequence, which the drift check doesn't mind.
func (s *Store) RegisterChanges(ctx context.Context, items []core.Registration) (core.Registered, error) {
	if len(items) == 0 {
		return core.Registered{}, nil
	}
	types := make([]string, len(items))
	ids := make([]string, len(items))
	deleted := make([]bool, len(items))
	versions := make([]int64, len(items))
	metadata := make([]map[string]string, len(items))
	for i, it := range items {
		types[i], ids[i] = it.Resource.Type, it.Resource.Id
		deleted[i] = it.Deleted
		if !it.Deleted {
			versions[i] = it.Version
		}
		metadata[i] = it.Metadata
	}
	// One JSON array index-aligned with the items; a nil map marshals to
	// JSON null, stored as SQL NULL like MarkStale stores nil metadata.
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return core.Registered{}, err
	}

	rows, err := s.pool.Query(ctx,
		`WITH input AS (
		     SELECT x.ord, x.t, x.i, x.del, x.v, NULLIF(m.meta, 'null'::jsonb) AS meta
		     FROM unnest($1::text[], $2::text[], $3::bool[], $4::bigint[]) WITH ORDINALITY AS x(t, i, del, v, ord)
		     JOIN jsonb_array_elements($5::jsonb) WITH ORDINALITY AS m(meta, ord) USING (ord)
		 ),
		 accepted AS (
		     INSERT INTO resources AS r (type, id, version, deleted, stale_seq, stale_since, metadata, change_seq)
		     SELECT t, i, v, del, 1, now(), meta, nextval('change_sequence') FROM input
		     ON CONFLICT (type, id) DO UPDATE
		     SET version = CASE WHEN EXCLUDED.deleted THEN 0
		                        WHEN EXCLUDED.version = 0 THEN r.version
		                        ELSE EXCLUDED.version END,
		         deleted = EXCLUDED.deleted,
		         change_seq = EXCLUDED.change_seq,
		         stale_seq = r.stale_seq + 1,
		         stale_since = COALESCE(r.stale_since, now()),
		         metadata = EXCLUDED.metadata
		     WHERE EXCLUDED.deleted OR EXCLUDED.version = 0 OR r.version < EXCLUDED.version
		     RETURNING r.type, r.id, r.stale_seq
		 ),
		 parents AS (
		     SELECT DISTINCT ON (rel.resource, rel.resource_id)
		            rel.resource AS t, rel.resource_id AS i, input.meta
		     FROM accepted a
		     JOIN input ON input.t = a.type AND input.i = a.id
		     JOIN relations rel ON rel.related_resource = a.type AND rel.related_resource_id = a.id
		     WHERE NOT EXISTS (SELECT 1 FROM accepted a2 WHERE a2.type = rel.resource AND a2.id = rel.resource_id)
		     ORDER BY rel.resource, rel.resource_id, input.ord DESC
		 ),
		 marked AS (
		     INSERT INTO resources AS r (type, id, stale_seq, stale_since, metadata)
		     SELECT t, i, 1, now(), meta FROM parents
		     ON CONFLICT (type, id) DO UPDATE
		     SET stale_seq = r.stale_seq + 1,
		         stale_since = COALESCE(r.stale_since, now()),
		         metadata = EXCLUDED.metadata
		     RETURNING r.type, r.id, r.metadata
		 )
		 SELECT input.ord, a.stale_seq, NULL::text, NULL::text, NULL::jsonb
		 FROM input LEFT JOIN accepted a ON a.type = input.t AND a.id = input.i
		 UNION ALL
		 SELECT NULL, NULL, type, id, metadata FROM marked`,
		types, ids, deleted, versions, metaJSON,
	)
	if err != nil {
		return core.Registered{}, err
	}
	defer rows.Close()

	out := core.Registered{Items: make([]core.RegisteredItem, len(items))}
	for rows.Next() {
		var ord, staleSeq *int64
		var typ, id *string
		var meta map[string]string
		if err := rows.Scan(&ord, &staleSeq, &typ, &id, &meta); err != nil {
			return core.Registered{}, err
		}
		if ord == nil {
			out.Parents = append(out.Parents, core.MarkedParent{Resource: model.Resource{Type: *typ, Id: *id}, Metadata: meta})
			continue
		}
		if staleSeq != nil {
			out.Items[*ord-1] = core.RegisteredItem{Accepted: true, StaleSeq: *staleSeq}
		}
	}
	if err := rows.Err(); err != nil {
		return core.Registered{}, err
	}
	return out, nil
}

// AnyResourceVersionDrifted reports whether any of the given versioned
// resources now has a version in the resources table that is strictly greater
// than the observed version. Resources with ObservedVersion == 0 are skipped
// (provider didn't supply a version). Resources absent from the table are
// treated as not-drifted.
func (s *Store) AnyResourceVersionDrifted(ctx context.Context, observed []model.VersionedResource) (bool, error) {
	// Filter to those with a known observed version.
	var types []string
	var ids []string
	var versions []int64
	for _, r := range observed {
		if r.Version <= 0 {
			continue
		}
		types = append(types, r.Type)
		ids = append(ids, r.Id)
		versions = append(versions, r.Version)
	}
	if len(types) == 0 {
		return false, nil
	}

	var found int
	err := s.pool.QueryRow(ctx,
		`SELECT 1
		 FROM resources r, unnest($1::text[], $2::text[], $3::bigint[]) AS x(type, id, observed_version)
		 WHERE r.type = x.type AND r.id = x.id AND r.version > x.observed_version
		 LIMIT 1`,
		types, ids, versions,
	).Scan(&found)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// MarkStale durably records build intent for the given resources: bump
// stale_seq and set stale_since — keeping the OLDEST timestamp, so
// "stale for too long" measures the oldest unserved change. The notification
// metadata is stored alongside the mark (last mark wins) so a sweep-recovered
// build runs with the same context an inline build would have.
func (s *Store) MarkStale(ctx context.Context, resources []model.Resource, metadata map[string]string) error {
	if len(resources) == 0 {
		return nil
	}
	types := make([]string, len(resources))
	ids := make([]string, len(resources))
	for i, r := range resources {
		types[i] = r.Type
		ids[i] = r.Id
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO resources (type, id, stale_seq, stale_since, metadata)
		 SELECT DISTINCT t, i, 1, now(), $3::jsonb FROM unnest($1::text[], $2::text[]) AS x(t, i)
		 ON CONFLICT (type, id) DO UPDATE
		 SET stale_seq = resources.stale_seq + 1,
		     stale_since = COALESCE(resources.stale_since, now()),
		     metadata = EXCLUDED.metadata`,
		types, ids, metadata,
	)
	return err
}

// BeginBuild atomically bumps the Build Sequence (ES external_gte OCC version),
// captures the current stale_seq for the race-safe ClearStale at the end of
// the build, and takes the build's start from the Change Sequence in the same
// statement. It leaves the row's change_seq alone: a build is not a change.
func (s *Store) BeginBuild(ctx context.Context, resource model.Resource) (core.BuildBegun, error) {
	var b core.BuildBegun
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, build_idx)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (type, id) DO UPDATE
		 SET build_idx = resources.build_idx + 1
		 RETURNING build_idx, stale_seq, nextval('change_sequence')`,
		resource.Type, resource.Id,
	).Scan(&b.BuildIdx, &b.StaleSeq, &b.Start)
	if err != nil {
		return core.BuildBegun{}, err
	}
	return b, nil
}

// NextChangeSeq takes a value of the Change Sequence.
func (s *Store) NextChangeSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT nextval('change_sequence')`).Scan(&seq)
	return seq, err
}

// AnyChangedSince reports whether any checked resource's change_seq exceeds
// its check's Start, all checks in one query. A resource without a row has
// never changed. It takes no row locks: a registration numbered above a
// start but not yet committed is not seen (seam S6).
func (s *Store) AnyChangedSince(ctx context.Context, checks []core.ChangeCheck) (bool, error) {
	if len(checks) == 0 {
		return false, nil
	}
	types := make([]string, len(checks))
	ids := make([]string, len(checks))
	starts := make([]int64, len(checks))
	for i, c := range checks {
		types[i], ids[i], starts[i] = c.Resource.Type, c.Resource.Id, c.Start
	}
	var changed bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		     SELECT 1
		     FROM unnest($1::text[], $2::text[], $3::bigint[]) AS x(t, i, start)
		     JOIN resources r ON r.type = x.t AND r.id = x.i
		     WHERE r.change_seq > x.start
		 )`,
		types, ids, starts,
	).Scan(&changed)
	return changed, err
}

// ClearStale clears the stale mark only if no newer change arrived since the
// build captured staleSeq. A moved seq makes this a no-op, leaving the row
// stale for the newer change's own build or the sweep.
func (s *Store) ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE resources SET stale_since = NULL
		 WHERE type=$1 AND id=$2 AND stale_seq=$3`,
		resource.Type, resource.Id, staleSeq,
	)
	return err
}

// DeleteResourceIfSeq hard-deletes a tombstoned row, guarded by stale_seq so a
// concurrent re-create (which bumps the seq) wins over the in-flight delete.
func (s *Store) DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM resources WHERE type=$1 AND id=$2 AND stale_seq=$3 AND deleted`,
		resource.Type, resource.Id, staleSeq,
	)
	return err
}

// ListStale returns up to limit resources whose stale mark is older than
// before, oldest first, including delete tombstones. Each entry carries the
// metadata stored by its most recent MarkStale.
func (s *Store) ListStale(ctx context.Context, before time.Time, limit int) ([]core.StaleResource, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT type, id, stale_seq, deleted, metadata FROM resources
		 WHERE stale_since IS NOT NULL AND stale_since < $1
		 ORDER BY stale_since
		 LIMIT $2`,
		before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []core.StaleResource
	for rows.Next() {
		var e core.StaleResource
		if err := rows.Scan(&e.Type, &e.Id, &e.StaleSeq, &e.Deleted, &e.Metadata); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountStale returns how many resources of the type (tombstones included)
// have a stale mark older than before, and the oldest such mark — zero when
// the count is zero. This is core.StaleCounter, the cutover readiness gate's
// view of the backlog.
func (s *Store) CountStale(ctx context.Context, resourceType string, before time.Time) (int, time.Time, error) {
	var count int
	var oldest *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT count(*), min(stale_since) FROM resources
		 WHERE type=$1 AND stale_since IS NOT NULL AND stale_since < $2`,
		resourceType, before,
	).Scan(&count, &oldest)
	if err != nil {
		return 0, time.Time{}, err
	}
	if oldest == nil {
		return count, time.Time{}, nil
	}
	return count, *oldest, nil
}
