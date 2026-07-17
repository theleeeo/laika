package core

import (
	"context"
	"time"

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
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	RemoveResource(ctx context.Context, resource model.Resource) error
	UpsertResource(ctx context.Context, resource model.Resource, version int64) error

	// MarkStale durably records build intent for the given resources, along
	// with the notification metadata the eventual build must run with. The
	// metadata is stored per resource (last mark wins) so a sweep-recovered
	// build carries the same context an inline build would have.
	MarkStale(ctx context.Context, resources []model.Resource, metadata map[string]string) error
	// MarkDeleted tombstones the resource and returns its stale_seq.
	MarkDeleted(ctx context.Context, resource model.Resource) (staleSeq int64, err error)
	// BeginBuild bumps the Build Sequence and captures the current stale_seq.
	BeginBuild(ctx context.Context, resource model.Resource) (buildIdx, staleSeq int64, err error)
	// ClearStale clears the stale mark only if staleSeq still matches.
	ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error
	// DeleteResourceIfSeq hard-deletes a tombstoned row guarded by stale_seq.
	DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq int64) error
	// ListStale returns up to limit resources whose stale mark predates before.
	ListStale(ctx context.Context, before time.Time, limit int) ([]StaleResource, error)
}

// StaleResource is one entry of the stale backlog.
type StaleResource struct {
	model.Resource
	StaleSeq int64
	Deleted  bool
	// Metadata is the notification metadata stored by the most recent
	// MarkStale, replayed into the build that serves the mark.
	Metadata map[string]string
}
