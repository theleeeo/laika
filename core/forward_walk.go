package core

import (
	"context"
	"errors"
	"time"
)

// ForwardWalkConfig enables the scheduled forward Rebuild walk for one
// resource type and paces it. A run walks the type from its source once per
// metadata map, so a resource created or changed without a notification is
// indexed — the other direction of the reverse sweep (ADR 0012), which finds
// what the source no longer has.
type ForwardWalkConfig struct {
	// Interval is how often the type's ForwardWalk schedule runs. Default
	// 24h.
	Interval time.Duration
	// PageSize is the listing page size each walk asks its plans for
	// (projection.BuildRequest.PageSize). Default 100.
	PageSize int
	// PageInterval is a page's rate budget: a page that finishes sooner
	// waits out the rest before the walk takes the next. Default 1s.
	PageInterval time.Duration
	// Metadata returns the metadata maps a run walks with, one RebuildWalk
	// per map. It is called at each run, so an embedder can derive them. A
	// map is the walk's actor and nothing else: an actor lists only what it
	// can see, so an embedder whose actors partition the source returns one
	// map per actor. Nil means one walk with no metadata.
	Metadata func(ctx context.Context) ([]map[string]string, error)
}

// ForwardWalkResult is what one run of a type's forward walk did.
type ForwardWalkResult struct {
	// Walks is the number of walks the run started, one per metadata map.
	Walks int
	// FailedResources is the number of resources the run's walks failed,
	// each marked stale for the sweep (RebuildMarkedFailuresError).
	FailedResources int
}

// ForwardWalkParams names the resource type one ForwardWalk run walks. Its
// metadata maps and pacing come from the worker's Config.ForwardWalks entry
// for the type, not from the schedule.
type ForwardWalkParams struct {
	ResourceType string
}

var errForwardWalkUnimplemented = errors.New("forward walk: not implemented")

// ForwardWalkNow runs one run of resourceType's forward walk in-process. It
// is the body of the ForwardWalk workflow, as RebuildNow is RebuildWalk's.
func (idx *Indexer) ForwardWalkNow(ctx context.Context, resourceType string) (ForwardWalkResult, error) {
	return ForwardWalkResult{}, errForwardWalkUnimplemented
}

// EnsureForwardWalkSchedules idempotently creates one ForwardWalk schedule
// per type in Config.ForwardWalks.
func (idx *Indexer) EnsureForwardWalkSchedules(ctx context.Context) error {
	return errForwardWalkUnimplemented
}
