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
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	RemoveResource(ctx context.Context, resource model.Resource) error

	// RegisterChanges records a batch of changes in one atomic statement:
	// each accepted item's version (or tombstone), stale mark and metadata,
	// and the stale marks of the accepted items' Parents, commit together or
	// not at all. See Registration for what is accepted. The items must name
	// distinct resources; the caller validates that.
	RegisterChanges(ctx context.Context, items []Registration) (Registered, error)

	// MarkStale durably records build intent for the given resources, along
	// with the notification metadata the eventual build must run with. The
	// metadata is stored per resource (last mark wins) so a sweep-recovered
	// build carries the same context an inline build would have.
	MarkStale(ctx context.Context, resources []model.Resource, metadata map[string]string) error
	// BeginBuild bumps the Build Sequence, captures the current stale_seq and
	// takes the build's start from the Change Sequence. Callers invoke it
	// before the build's fetches.
	BeginBuild(ctx context.Context, resource model.Resource) (BuildBegun, error)
	// NextChangeSeq takes a value of the Change Sequence: the start of a
	// Rebuild plan walk, taken before the walk fetches its first page.
	NextChangeSeq(ctx context.Context) (int64, error)
	// AnyChangedSince is the ADR 0002 drift check: it reports whether any
	// checked resource had a change accepted by RegisterChanges after the
	// check's Start, i.e. its stored change_seq exceeds Start. A resource
	// without a row has never changed.
	AnyChangedSince(ctx context.Context, checks []ChangeCheck) (bool, error)
	// ClearStale clears the stale mark only if staleSeq still matches.
	ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error
	// DeleteResourceIfSeq hard-deletes a tombstoned row guarded by stale_seq.
	DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq int64) error
	// ListStale returns up to limit resources whose stale mark predates before.
	ListStale(ctx context.Context, before time.Time, limit int) ([]StaleResource, error)
}

// BuildBegun is what BeginBuild returns for one resource.
type BuildBegun struct {
	// BuildIdx is the bumped Build Sequence, sent as the ES external_gte
	// version.
	BuildIdx int64
	// StaleSeq is the stale_seq the build's ClearStale is guarded by.
	StaleSeq int64
	// Start is a value of the Change Sequence taken before the build's
	// fetches; the drift check compares against it.
	Start int64
}

// ChangeCheck is one resource of a drift check, with the start of the build
// that fetched it.
type ChangeCheck struct {
	Resource model.Resource
	Start    int64
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

// Registration is one item of a RegisterChanges batch.
type Registration struct {
	Resource model.Resource
	// Deleted tombstones the row instead of upserting it. A delete is always
	// accepted; Version is ignored and the stored version resets to 0.
	Deleted bool
	// Version 0 means the upstream does not track versions: the item is
	// always accepted. A non-zero Version is accepted only when strictly
	// greater than the stored one; otherwise the item is stale and nothing
	// is written for it — neither its row nor its Parents' marks.
	Version int64
	// Metadata is stored on the item's own row with its mark.
	Metadata map[string]string
}

// Registered is the committed outcome of RegisterChanges.
type Registered struct {
	// Items is index-aligned with the input.
	Items []RegisteredItem
	// Parents are the rows the statement marked stale as Parents of accepted
	// items, excluding resources that are themselves accepted items of the
	// batch (their own row already carries the mark). Each appears once.
	Parents []MarkedParent
}

// RegisteredItem is one item's outcome.
type RegisteredItem struct {
	Accepted bool
	// StaleSeq is the row's stale_seq after the mark, for accepted items; a
	// delete is submitted with it (DeleteResourceIfSeq).
	StaleSeq int64
}

// MarkedParent is a Parent RegisterChanges marked stale, with the metadata
// it stored on the row: that of the last accepted child in batch order.
type MarkedParent struct {
	model.Resource
	Metadata map[string]string
}
