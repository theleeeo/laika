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
	idx.searchChain = chainSearch(idx.searchBase, cfg.SearchMiddlewares)
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
