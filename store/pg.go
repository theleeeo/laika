package store

import (
	"context"

	"github.com/theleeeo/indexer/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// AddRelations upserts relation rows.
func (s *PostgresStore) AddRelations(ctx context.Context, relations []Relation) error {
	return s.addRelationsBatch(ctx, s.pool, relations)
}

func (s *PostgresStore) addRelationsBatch(ctx context.Context, sender batchSender, relations []Relation) error {
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

	br := sender.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return nil
}

// DeleteRelation removes a single relation row.
func (s *PostgresStore) DeleteRelation(ctx context.Context, relation Relation) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM relations WHERE resource=$1 AND resource_id=$2 AND related_resource=$3 AND related_resource_id=$4`,
		relation.Parent.Type, relation.Parent.Id, relation.Child.Type, relation.Child.Id,
	)
	return err
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

func (s *PostgresStore) GetChildResourcesOfType(ctx context.Context, parentResource model.Resource, childType string) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT related_resource_id FROM relations WHERE resource=$1 AND resource_id=$2 AND related_resource=$3`,
		parentResource.Type, parentResource.Id, childType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []model.Resource
	for rows.Next() {
		var childResourceId string
		if err := rows.Scan(&childResourceId); err != nil {
			return nil, err
		}
		children = append(children, model.Resource{Type: childType, Id: childResourceId})
	}
	return children, nil
}

func (s *PostgresStore) RemoveResource(ctx context.Context, resource model.Resource) error {
	return s.removeResource(ctx, s.pool, resource)
}

func (s *PostgresStore) removeResource(ctx context.Context, sender executor, resource model.Resource) error {
	_, err := sender.Exec(
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
