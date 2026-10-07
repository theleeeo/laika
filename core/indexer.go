package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
	"go.temporal.io/sdk/client"
)

var (
	ErrUnknownResource = errors.New("unknown resource")

	// ErrStaleVersion is returned when a resource upsert is rejected because the
	// provided version is not strictly greater than the currently stored version.
	ErrStaleVersion = errors.New("stale version")
)

type InvalidArgumentError struct {
	Msg string
}

func (e *InvalidArgumentError) Error() string {
	return e.Msg
}

// Config holds the dependencies required to create an Indexer.
type Config struct {
	// Resources defines the resource types, fields, and relations.
	Resources resource.Configs

	Plans map[string][]projection.Plan

	// ES is the search backend for indexing and searching.
	ES SearchBackend

	// Store is the PostgreSQL relation-graph store.
	Store Store

	// PoolSize bounds the number of concurrent inline builds. Default 10.
	PoolSize int

	// QueueSize bounds the number of accepted-but-not-yet-running inline
	// builds. By default submission never blocks: a full queue sheds at once,
	// leaving the resource stale for the sweep — except a RegisterChanges
	// called with WaitForSlot, which waits for the queue to drop below
	// QueueHighWater. Default 10 × PoolSize.
	QueueSize int

	// QueueHighWater is the queued-task count at or above which the pool is
	// under pressure: WaitForSlot registrations wait until the queue drops
	// below it. 0 means the default, 80% of QueueSize and at least 1; any
	// other value outside [1, QueueSize] makes New fail.
	QueueHighWater int

	// RebuildChunkSize bounds the number of documents per bulk write during
	// rebuilds, so an all-of-type walk streams to Elasticsearch in bounded
	// requests instead of one unbounded payload. Default 500.
	RebuildChunkSize int

	// OwnerLease is how long a claimed build owner holds its resource
	// without renewing: a mark of a row whose owner renewed (claim, a pool
	// task's dequeue, the sweep reaching its entry, BeginBuild, BeginDelete)
	// within it doesn't submit another inline build. A holder whose renewal
	// at dequeue or at its sweep entry finds the ownership lost — the lease
	// lapsed and another owner claimed the row under a newer mark — does
	// nothing for it. Size it above an inline build's or delete's queue wait
	// plus its run: a build or delete that outlives it costs a duplicate
	// build or delete, which the Build Sequence orders — except a build that
	// a delete overtakes and that writes more than Elasticsearch's
	// index.gc_deletes after it, which brings the deleted document back
	// (seams S16). Default 30s.
	OwnerLease time.Duration

	// SweepBackoff is how long the StaleSweep leaves a resource after its
	// first failed owned build or delete in a row (Store.ReleaseFailed);
	// each further failure doubles it, up to SweepBackoffMax. A successful
	// build or delete, or a registration of the resource itself, resets it
	// (ADR 0008). Default 5m.
	SweepBackoff time.Duration
	// SweepBackoffMax caps SweepBackoff's doubling: a resource that never
	// builds is retried at this interval. Default 24h. New refuses a cap
	// below SweepBackoff, once both defaults are applied.
	SweepBackoffMax time.Duration

	// ReverseSweeps enables the reverse sweep (ADR 0012) per resource type:
	// each entry's type is swept on its own schedule
	// (EnsureReverseSweepSchedules), paced by its config. A type without an
	// entry is never swept. New rejects an entry for a type Resources doesn't
	// configure, and negative values.
	ReverseSweeps map[string]ReverseSweepConfig

	// ForwardWalks enables the scheduled forward Rebuild walk per resource
	// type: each entry's type is walked from its source on its own schedule
	// (EnsureForwardWalkSchedules), once per metadata map, paced by its
	// config. A type without an entry is walked only by an explicit rebuild.
	ForwardWalks map[string]ForwardWalkConfig

	// SearchMiddlewares wrap the search path. They run outermost-first in
	// registration order: []{A, B} executes A → B → the Indexer's own
	// validate/normalize/backend call. A middleware may authorize the request,
	// short-circuit with an error, mutate the SearchRequest, and inspect or
	// modify the SearchResponse. The chain is composed once at construction.
	SearchMiddlewares []SearchMiddleware

	// Temporal is the Temporal client used for the durable slow lane:
	// RebuildWalk workflows, the StaleSweep schedule, and the ReverseSweep
	// and ForwardWalk workflows and their per-type schedules. Required for
	// any deployment; construction does not nil-check so search-only tests
	// can omit it, but Rebuild, NewWorker, EnsureSweepSchedule,
	// EnsureReverseSweepSchedules and EnsureForwardWalkSchedules will panic
	// without it.
	Temporal client.Client

	// TaskQueue is the Temporal task queue for the Indexer's workflows.
	// Default DefaultTaskQueue.
	TaskQueue string

	// FederatedSearchMiddlewares wrap the federated search path, independently
	// of SearchMiddlewares — neither chain ever runs on the other path. They
	// run outermost-first in registration order: []{A, B} executes A → B → the
	// Indexer's own validate/group-build/backend call, so a middleware also
	// observes validation failures. A middleware may deny the request with an
	// error, mutate the FederatedSearchRequest, and inspect or modify the
	// response. The chain is composed once at construction.
	FederatedSearchMiddlewares []FederatedSearchMiddleware
}

// Indexer is the core indexing engine. It receives change notifications,
// determines which search documents are affected, rebuilds them from
// authoritative source data, and writes them to Elasticsearch.
type Indexer struct {
	st Store
	es SearchBackend

	plans map[string][]projection.Plan

	pool *buildPool

	rebuildChunkSize int

	ownerLease time.Duration

	// sweepBackoff is Config.SweepBackoff and SweepBackoffMax with their
	// defaults applied: every Store.ReleaseFailed gets it.
	sweepBackoff SweepBackoff

	// reverseSweeps is Config.ReverseSweeps with its defaults applied.
	reverseSweeps map[string]ReverseSweepConfig
	// forwardWalks is Config.ForwardWalks with its defaults applied.
	forwardWalks map[string]ForwardWalkConfig
	// poolBackoff is how long a paced walk — the reverse sweep, or a paced
	// rebuild walk — waits before checking the pool's pressure again; tests
	// shorten it.
	poolBackoff time.Duration
	// waitPageInterval waits out the rest of a paced rebuild walk's page
	// interval (sleepCtx); tests record the waits instead.
	waitPageInterval func(ctx context.Context, d time.Duration) error

	temporal  client.Client
	taskQueue string

	resources resource.Configs

	// searchChain is the composed search handler: the registered middlewares
	// wrapped around searchBase. When no middlewares are registered it equals
	// searchBase, so the search path has zero overhead.
	searchChain SearchHandler

	// federatedSearchChain is the composed federated search handler: the
	// registered FederatedSearchMiddlewares wrapped around federatedSearchBase.
	// When no middlewares are registered it equals federatedSearchBase.
	federatedSearchChain FederatedSearchHandler
}

const (
	defaultPoolSize         = 10
	defaultRebuildChunkSize = 500
	defaultOwnerLease       = 30 * time.Second
	defaultSweepBackoff     = 5 * time.Minute
	defaultSweepBackoffMax  = 24 * time.Hour
)

// New creates a new Indexer with the given configuration.
//
// New is the validation boundary for resource configs: it applies defaults
// and validates the set, and returns an error rather than construct an
// Indexer over an invalid one. All code past this point assumes the config
// invariants hold (every resource has at least one version, ReadVersion
// resolves, relations are consistent). The caller must not mutate the
// resource configs after passing them in — that would bypass this boundary.
func New(cfg Config) (*Indexer, error) {
	if err := finalizeResourceConfigs(cfg.Resources); err != nil {
		return nil, err
	}

	idx := &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		resources: cfg.Resources,
		plans:     cfg.Plans,
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultPoolSize
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = poolSize * 10
	}
	highWater := cfg.QueueHighWater
	if highWater == 0 {
		highWater = max(1, queueSize*8/10)
	}
	if highWater < 1 || highWater > queueSize {
		return nil, fmt.Errorf("queue high water %d outside [1, %d] (the queue size)", highWater, queueSize)
	}
	idx.pool = newBuildPool(poolSize, queueSize, highWater)

	idx.rebuildChunkSize = cfg.RebuildChunkSize
	if idx.rebuildChunkSize <= 0 {
		idx.rebuildChunkSize = defaultRebuildChunkSize
	}

	idx.ownerLease = cfg.OwnerLease
	if idx.ownerLease <= 0 {
		idx.ownerLease = defaultOwnerLease
	}

	idx.sweepBackoff = SweepBackoff{Base: cfg.SweepBackoff, Max: cfg.SweepBackoffMax}
	if idx.sweepBackoff.Base <= 0 {
		idx.sweepBackoff.Base = defaultSweepBackoff
	}
	if idx.sweepBackoff.Max <= 0 {
		idx.sweepBackoff.Max = defaultSweepBackoffMax
	}
	if idx.sweepBackoff.Max < idx.sweepBackoff.Base {
		return nil, fmt.Errorf("sweep backoff max %s is below the sweep backoff %s", idx.sweepBackoff.Max, idx.sweepBackoff.Base)
	}

	sweeps, err := reverseSweepConfigs(cfg.ReverseSweeps, cfg.Resources)
	if err != nil {
		return nil, err
	}
	idx.reverseSweeps = sweeps
	walks, err := forwardWalkConfigs(cfg.ForwardWalks, cfg.Resources)
	if err != nil {
		return nil, err
	}
	idx.forwardWalks = walks
	idx.poolBackoff = defaultPoolBackoff
	idx.waitPageInterval = sleepCtx

	taskQueue := cfg.TaskQueue
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	idx.temporal = cfg.Temporal
	idx.taskQueue = taskQueue

	mws := make([]SearchMiddleware, 0, len(cfg.SearchMiddlewares)+2)
	mws = append(mws, cfg.SearchMiddlewares...) // user middleware runs first (outermost); nothing precedes it
	mws = append(mws, idx.deriveNestedPath)     // fill NestedPath for denormalized-many relation fields, before referenceResolve strips reference filters
	mws = append(mws, idx.referenceResolve)     // innermost: route filters by path, run child searches, fold terms

	idx.searchChain = chain(idx.searchBase, mws)
	idx.federatedSearchChain = chain(idx.federatedSearchBase, cfg.FederatedSearchMiddlewares)
	return idx, nil
}

// finalizeResourceConfigs applies defaults and validates a resource config
// set at the library boundary (New, SetPlans). An empty set is allowed: an
// Indexer with nothing configured is legal, and rejecting it is app policy.
// A valid set is then checked for versions with no primary-tier field, which
// are warned about, not rejected.
func finalizeResourceConfigs(resources resource.Configs) error {
	for _, rc := range resources {
		rc.ApplyDefaults()
	}
	if err := resources.Validate(); err != nil {
		return fmt.Errorf("invalid resource config: %w", err)
	}
	resources.WarnMissingPrimaryTier(slog.Default())
	return nil
}

// Shutdown stops accepting inline work and waits for in-flight builds until
// ctx ends. Unfinished work stays stale and is recovered by the sweep.
func (idx *Indexer) Shutdown(ctx context.Context) error {
	return idx.pool.shutdown(ctx)
}

// WaitForIdle blocks until the inline pool has fully settled, including
// cascaded parent builds and drift re-builds. Intended for tests and
// embedders that need a quiescence point.
func (idx *Indexer) WaitForIdle(ctx context.Context) error {
	return idx.pool.waitIdle(ctx)
}

// SetPlans replaces the aggregation plans and resource configuration.
// This is primarily used by the standalone application with YAML DSL;
// library users typically set these once at construction via Config.
//
// Like New, SetPlans is a validation boundary: an invalid resource config
// set is rejected without being applied, and the caller must not mutate the
// configs after passing them in.
func (idx *Indexer) SetPlans(plans map[string][]projection.Plan, resources resource.Configs) error {
	if err := finalizeResourceConfigs(resources); err != nil {
		return err
	}
	idx.plans = plans
	idx.resources = resources
	return nil
}

func (idx *Indexer) verifyResourceConfig(n Notification) error {
	if n.ResourceType == "" {
		return &InvalidArgumentError{Msg: "resource_type required"}
	}

	if n.ResourceID == "" {
		return &InvalidArgumentError{Msg: "resource_id required"}
	}

	r := idx.resources.Get(n.ResourceType)
	if r == nil {
		return ErrUnknownResource
	}

	return nil
}
