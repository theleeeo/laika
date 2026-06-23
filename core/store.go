package core

import (
	"context"

	"github.com/theleeeo/laika/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}

type Store interface {
	AddChildResources(ctx context.Context, parent model.Resource, childs []model.Resource) error
	AddRelations(ctx context.Context, relations []Relation) error
	AnyResourceVersionDrifted(ctx context.Context, observed []model.VersionedResource) (bool, error)
	DeleteResource(ctx context.Context, resource model.Resource) error
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	NextRebuildCounter(ctx context.Context, resource model.Resource) (int64, error)
	RemoveResource(ctx context.Context, resource model.Resource) error
	UpsertResource(ctx context.Context, resource model.Resource, version int64) error
}
