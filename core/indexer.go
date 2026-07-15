package core

import (
	"context"
	"errors"
	"fmt"

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
	// builds. Submission never blocks: a full queue sheds immediately,
	// leaving the resource stale for the sweep. Default 10 × PoolSize.
	QueueSize int

	// SearchMiddlewares wrap the search path. They run outermost-first in
	// registration order: []{A, B} executes A → B → the Indexer's own
	// validate/normalize/backend call. A middleware may authorize the request,
	// short-circuit with an error, mutate the SearchRequest, and inspect or
	// modify the SearchResponse. The chain is composed once at construction.
	SearchMiddlewares []SearchMiddleware

	// Temporal is the Temporal client used for the durable slow lane:
	// RebuildWalk workflows and the StaleSweep schedule. Required for any
	// deployment; construction does not nil-check so search-only tests can
	// omit it, but Rebuild/NewWorker/EnsureSweepSchedule will panic without it.
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

const defaultPoolSize = 10

// New creates a new Indexer with the given configuration.
func New(cfg Config) *Indexer {
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
	idx.pool = newBuildPool(poolSize, queueSize)

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
	return idx
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
func (idx *Indexer) SetPlans(plans map[string][]projection.Plan, resources resource.Configs) {
	idx.plans = plans
	idx.resources = resources

}

func (idx *Indexer) verifyResourceConfig(n Notification) error {
	if n.ResourceType == "" {
		return fmt.Errorf("resource_type required")
	}

	if n.ResourceID == "" {
		return fmt.Errorf("resource_id required")
	}

	r := idx.resources.Get(n.ResourceType)
	if r == nil {
		return ErrUnknownResource
	}

	return nil
}
