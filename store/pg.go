package store

import (
	"context"

	"github.com/theleeeo/indexer/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) AddRelations(ctx context.Context, relations []Relation) error {
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

func (s *PostgresStore) GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error) {
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

func (s *PostgresStore) GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error) {
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

func (s *PostgresStore) RemoveResource(ctx context.Context, resource model.Resource) error {
	_, err := s.pool.Exec(
		ctx,
		`DELETE FROM relations WHERE resource=$1 AND resource_id=$2`,
		resource.Type, resource.Id,
	)
	return err
}

func (s *PostgresStore) AddChildResources(ctx context.Context, parent model.Resource, childs []model.Resource) error {
	var relations []Relation
	for _, child := range childs {
		relations = append(relations, Relation{
			Parent: parent,
			Child:  child,
		})
	}
	return s.AddRelations(ctx, relations)
}

// NextRebuildCounter atomically increments and returns the rebuild counter for
// a root resource. The returned value is used as the ES external version for
// OCC so older writes are rejected.
func (s *PostgresStore) NextRebuildCounter(ctx context.Context, resource model.Resource) (int64, error) {
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
func (s *PostgresStore) UpsertResource(ctx context.Context, resource model.Resource, version int64) error {
	// TODO: Always require version, set it at a higher level if omitted in the api.
	if version == 0 {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO resources (type, id) VALUES ($1, $2) ON CONFLICT (type, id) DO NOTHING`,
			resource.Type, resource.Id,
		)
		return err
	}

	tag, err := s.pool.Exec(ctx,
		`INSERT INTO resources (type, id, version) VALUES ($1, $2, $3)
		 ON CONFLICT (type, id) DO UPDATE SET version = EXCLUDED.version
		 WHERE resources.version < EXCLUDED.version`,
		resource.Type, resource.Id, version,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleVersion
	}
	return nil
}

// DeleteResource removes a resource from the resources table.
func (s *PostgresStore) DeleteResource(ctx context.Context, resource model.Resource) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM resources WHERE type=$1 AND id=$2`,
		resource.Type, resource.Id,
	)

	return err
}
