package core

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
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

	// RiverClient is the River job queue client for enqueuing rebuild/delete
	// and full-rebuild jobs. It may be left nil at construction and assigned
	// later via [Indexer.SetRiverClient]; this lets callers wire workers
	// that reference the Indexer before the client is created.
	RiverClient *river.Client[pgx.Tx]

	// SearchMiddlewares wrap the search path. They run outermost-first in
	// registration order: []{A, B} executes A → B → the Indexer's own
	// validate/normalize/backend call. A middleware may authorize the request,
	// short-circuit with an error, mutate the SearchRequest, and inspect or
	// modify the SearchResponse. The chain is composed once at construction.
	SearchMiddlewares []SearchMiddleware

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

	river *river.Client[pgx.Tx]

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

// New creates a new Indexer with the given configuration.
func New(cfg Config) *Indexer {
	idx := &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		river:     cfg.RiverClient,
		resources: cfg.Resources,
		plans:     cfg.Plans,
	}
	mws := make([]SearchMiddleware, 0, len(cfg.SearchMiddlewares)+2)
	mws = append(mws, cfg.SearchMiddlewares...) // user middleware runs first (outermost); nothing precedes it
	mws = append(mws, idx.deriveNestedPath)     // fill NestedPath for denormalized-many relation fields, before referenceResolve strips reference filters
	mws = append(mws, idx.referenceResolve)     // innermost: route filters by path, run child searches, fold terms

	idx.searchChain = chain(idx.searchBase, mws)
	idx.federatedSearchChain = chain(idx.federatedSearchBase, cfg.FederatedSearchMiddlewares)
	return idx
}

// SetRiverClient assigns the River client used to enqueue jobs. It is
// intended for the wiring sequence where workers (which reference the
// Indexer) must be constructed before the River client itself.
func (idx *Indexer) SetRiverClient(c *river.Client[pgx.Tx]) {
	idx.river = c
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
