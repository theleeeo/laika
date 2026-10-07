package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/theleeeo/laika/core/resource"
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

const (
	// defaultForwardWalkInterval is how often a type's ForwardWalk schedule
	// runs when its ForwardWalkConfig leaves Interval at zero.
	defaultForwardWalkInterval = 24 * time.Hour
	// defaultForwardWalkPageSize is the listing page size a walk asks for
	// when ForwardWalkConfig.PageSize is zero.
	defaultForwardWalkPageSize = 100
	// defaultForwardWalkPageInterval is a page's rate budget when
	// ForwardWalkConfig.PageInterval is zero.
	defaultForwardWalkPageInterval = time.Second
)

// forwardWalkMarkedFailuresMsg is the Warn message of a walk whose failed
// resources are all marked stale: the run records it as done.
const forwardWalkMarkedFailuresMsg = "forward walk failed resources; they are marked stale for the sweep"

// forwardWalkConfigs validates Config.ForwardWalks against the resource
// configs and applies its defaults, as reverseSweepConfigs does for the
// reverse sweep.
func forwardWalkConfigs(in map[string]ForwardWalkConfig, resources resource.Configs) (map[string]ForwardWalkConfig, error) {
	out := make(map[string]ForwardWalkConfig, len(in))
	for typ, c := range in {
		if resources.Get(typ) == nil {
			return nil, fmt.Errorf("forward walk for resource type %q: %w", typ, ErrUnknownResource)
		}
		if c.Interval < 0 || c.PageSize < 0 || c.PageInterval < 0 {
			return nil, fmt.Errorf("forward walk for resource type %q: negative interval, page size or page interval", typ)
		}
		if c.PageSize > math.MaxInt32 {
			// A listing's page size is an int32 on the provider contract.
			return nil, fmt.Errorf("forward walk for resource type %q: page size %d above %d", typ, c.PageSize, math.MaxInt32)
		}
		if c.Interval == 0 {
			c.Interval = defaultForwardWalkInterval
		}
		if c.PageSize == 0 {
			c.PageSize = defaultForwardWalkPageSize
		}
		if c.PageInterval == 0 {
			c.PageInterval = defaultForwardWalkPageInterval
		}
		out[typ] = c
	}
	return out, nil
}

// errForwardWalkNotEnabled is wrapped by the error of a forward walk of a
// type Config.ForwardWalks has no entry for. The ForwardWalkSelectors
// activity, like an unknown type, fails it without retrying.
var errForwardWalkNotEnabled = errors.New("not enabled in Config.ForwardWalks")

// ForwardWalkNow runs one run of resourceType's forward walk in-process. It
// is the body of the ForwardWalk workflow, as RebuildNow is RebuildWalk's,
// and is exported for embedders and tests that want it without Temporal.
//
// The run resolves its walks as the workflow's ForwardWalkSelectors activity
// does (forwardWalkSelectors): one per metadata map, each a selector of the
// whole type with the map as its Metadata and the config's pacing. It runs
// them one after another, in the order the Metadata func returned the maps,
// each through RebuildNowResumable without a checkpoint. A walk whose error
// is a RebuildMarkedFailuresError is done: its failed resources are marked
// stale for the sweep, so its count adds to the result's FailedResources and
// is logged. Every walk runs even if an earlier one failed; the run's error
// joins the walks' other errors.
//
// A type Resources doesn't configure is an error wrapping
// ErrUnknownResource, and one without a Config.ForwardWalks entry an error
// too; a failing Metadata func fails the run, walking nothing.
func (idx *Indexer) ForwardWalkNow(ctx context.Context, resourceType string) (ForwardWalkResult, error) {
	sels, err := idx.forwardWalkSelectors(ctx, resourceType)
	if err != nil {
		return ForwardWalkResult{}, err
	}
	return runForwardWalks(resourceType, sels, slog.Default(), func(sel ResourceSelector) error {
		return idx.RebuildNowResumable(ctx, sel, nil, nil)
	}, markedFailuresInProcess)
}

// EnsureForwardWalkSchedules idempotently creates one ForwardWalk schedule
// per type in Config.ForwardWalks, each every its type's Interval. As with
// EnsureSweepSchedule, an existing schedule is left untouched — changing a
// type's interval requires deleting its schedule in Temporal first — and a
// type removed from the config keeps its schedule until it is deleted there;
// its runs then fail without retrying (ForwardWalkSelectors). With no
// entries it creates nothing and returns nil.
func (idx *Indexer) EnsureForwardWalkSchedules(ctx context.Context) error {
	return ensureForwardWalkSchedules(ctx, idx.temporal.ScheduleClient(), idx.taskQueue, idx.forwardWalks)
}

// forwardWalkSelectors resolves one run of resourceType's forward walk: a
// selector of the whole type per map the config's Metadata func returns, in
// its order, each with the map as its Metadata and the config's PageSize and
// PageInterval as its Pacing. A nil func is one walk with no metadata; a func
// that returns no maps walks nothing this run, with a warning.
func (idx *Indexer) forwardWalkSelectors(ctx context.Context, resourceType string) ([]ResourceSelector, error) {
	if idx.resources.Get(resourceType) == nil {
		return nil, fmt.Errorf("forward walk of resource type %q: %w", resourceType, ErrUnknownResource)
	}
	cfg, ok := idx.forwardWalks[resourceType]
	if !ok {
		return nil, fmt.Errorf("forward walk of resource type %q: %w", resourceType, errForwardWalkNotEnabled)
	}

	mds := []map[string]string{nil}
	if cfg.Metadata != nil {
		var err error
		if mds, err = cfg.Metadata(ctx); err != nil {
			return nil, fmt.Errorf("forward walk of resource type %q: metadata: %w", resourceType, err)
		}
		if len(mds) == 0 {
			slog.Warn("forward walk walks nothing this run: its Metadata func returned no maps",
				slog.String("type", resourceType))
		}
	}

	sels := make([]ResourceSelector, len(mds))
	for i, md := range mds {
		sels[i] = ResourceSelector{
			ResourceType: resourceType,
			Metadata:     md,
			Pacing:       &WalkPacing{PageSize: cfg.PageSize, PageInterval: cfg.PageInterval},
		}
	}
	return sels, nil
}

// kvLogger is the logging both of a run's bodies share: slog's in-process,
// and the workflow's replay-safe Temporal logger.
type kvLogger interface {
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
}

// runForwardWalks runs one forward walk run's walks — sels, in order, each
// through walk — and accumulates its result. Every selector is a walk
// started. A walk whose error marked recognises as a marked-failures outcome
// is done: its count adds to FailedResources and is logged. Any other error
// is joined into the run's, once every walk has run. Walks are numbered from
// 1 in the log and in errors. It is the accumulation the ForwardWalk workflow
// and ForwardWalkNow share; each supplies its own walk and its own reading of
// a marked-failures error.
func runForwardWalks(resourceType string, sels []ResourceSelector, logger kvLogger, walk func(ResourceSelector) error, marked func(error) (count int, ok bool)) (ForwardWalkResult, error) {
	res := ForwardWalkResult{Walks: len(sels)}
	var errs []error
	for i, sel := range sels {
		err := walk(sel)
		if err == nil {
			continue
		}
		if n, ok := marked(err); ok {
			res.FailedResources += n
			logger.Warn(forwardWalkMarkedFailuresMsg, "type", resourceType, "walk", i+1, "walks", len(sels), "failed", n)
			continue
		}
		errs = append(errs, fmt.Errorf("forward walk %d of %d of resource type %q: %w", i+1, len(sels), resourceType, err))
	}
	logger.Info("forward walk run complete",
		"type", resourceType, "walks", res.Walks, "failed_resources", res.FailedResources, "failed_walks", len(errs))
	return res, errors.Join(errs...)
}

// markedFailuresInProcess reads an in-process walk's error as a
// marked-failures outcome: a RebuildMarkedFailuresError anywhere in its
// chain, with its count.
func markedFailuresInProcess(err error) (int, bool) {
	var m *RebuildMarkedFailuresError
	if errors.As(err, &m) {
		return m.Count, true
	}
	return 0, false
}
