package postgres

import (
	"context"
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

// NextRebuildCounter atomically increments and returns the rebuild counter for
// a root resource. The returned value is used as the ES external version for
// OCC so older writes are rejected.
func (s *Store) NextRebuildCounter(ctx context.Context, resource model.Resource) (int64, error) {
	var counter int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, build_idx)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (type, id) DO UPDATE
		 SET build_idx = resources.build_idx + 1
		 RETURNING build_idx`,
		resource.Type, resource.Id,
	).Scan(&counter)
	if err != nil {
		return 0, err
	}

	return counter, nil
}

// UpsertResource inserts or updates the resource in the resources table.
// When version is 0, the resource is inserted without version control (existing
// rows are left unchanged). When version > 0, the resource is only inserted or
// updated if the new version is strictly greater than the stored one; otherwise
// ErrStaleVersion is returned.
func (s *Store) UpsertResource(ctx context.Context, resource model.Resource, version int64) error {
	// TODO: Always require version, set it at a higher level if omitted in the api.
	if version == 0 {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO resources (type, id) VALUES ($1, $2)
			 ON CONFLICT (type, id) DO UPDATE SET deleted = false`,
			resource.Type, resource.Id,
		)
		return err
	}

	tag, err := s.pool.Exec(ctx,
		`INSERT INTO resources (type, id, version) VALUES ($1, $2, $3)
		 ON CONFLICT (type, id) DO UPDATE SET version = EXCLUDED.version, deleted = false
		 WHERE resources.version < EXCLUDED.version`,
		resource.Type, resource.Id, version,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return core.ErrStaleVersion
	}
	return nil
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

// DeleteResource removes a resource from the resources table.
func (s *Store) DeleteResource(ctx context.Context, resource model.Resource) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM resources WHERE type=$1 AND id=$2`,
		resource.Type, resource.Id,
	)

	return err
}

// MarkStale durably records build intent for the given resources: bump
// stale_seq and set stale_since — keeping the OLDEST timestamp, so
// "stale for too long" measures the oldest unserved change.
func (s *Store) MarkStale(ctx context.Context, resources []model.Resource) error {
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
		`INSERT INTO resources (type, id, stale_seq, stale_since)
		 SELECT DISTINCT t, i, 1, now() FROM unnest($1::text[], $2::text[]) AS x(t, i)
		 ON CONFLICT (type, id) DO UPDATE
		 SET stale_seq = resources.stale_seq + 1,
		     stale_since = COALESCE(resources.stale_since, now())`,
		types, ids,
	)
	return err
}

// MarkDeleted tombstones the resource: deleted=true plus a stale mark, so the
// sweep retries an inline delete that failed or was shed. version resets to 0
// so a later re-create is never rejected as stale against the old lifecycle.
func (s *Store) MarkDeleted(ctx context.Context, resource model.Resource) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, deleted, stale_seq, stale_since)
		 VALUES ($1, $2, true, 1, now())
		 ON CONFLICT (type, id) DO UPDATE
		 SET deleted = true,
		     version = 0,
		     stale_seq = resources.stale_seq + 1,
		     stale_since = COALESCE(resources.stale_since, now())
		 RETURNING stale_seq`,
		resource.Type, resource.Id,
	).Scan(&seq)
	return seq, err
}

// BeginBuild atomically bumps the Build Sequence (ES external_gte OCC version)
// and captures the current stale_seq for the race-safe ClearStale at the end
// of the build.
func (s *Store) BeginBuild(ctx context.Context, resource model.Resource) (int64, int64, error) {
	var buildIdx, staleSeq int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, build_idx)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (type, id) DO UPDATE
		 SET build_idx = resources.build_idx + 1
		 RETURNING build_idx, stale_seq`,
		resource.Type, resource.Id,
	).Scan(&buildIdx, &staleSeq)
	if err != nil {
		return 0, 0, err
	}
	return buildIdx, staleSeq, nil
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
// before, oldest first, including delete tombstones.
func (s *Store) ListStale(ctx context.Context, before time.Time, limit int) ([]core.StaleResource, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT type, id, stale_seq, deleted FROM resources
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
		if err := rows.Scan(&e.Type, &e.Id, &e.StaleSeq, &e.Deleted); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
