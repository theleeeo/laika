package core

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
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

	// ES is the Elasticsearch client for indexing and searching.
	ES *es.Client

	// Store is the PostgreSQL relation-graph store.
	Store Store

	// RiverClient is the River job queue client for enqueuing rebuild/delete
	// and full-rebuild jobs. It may be left nil at construction and assigned
	// later via [Indexer.SetRiverClient]; this lets callers wire workers
	// that reference the Indexer before the client is created.
	RiverClient *river.Client[pgx.Tx]
}

// Indexer is the core indexing engine. It receives change notifications,
// determines which search documents are affected, rebuilds them from
// authoritative source data, and writes them to Elasticsearch.
type Indexer struct {
	st Store
	es *es.Client

	plans map[string][]projection.Plan

	river *river.Client[pgx.Tx]

	resources resource.Configs
}

// New creates a new Indexer with the given configuration.
func New(cfg Config) *Indexer {
	return &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		river:     cfg.RiverClient,
		resources: cfg.Resources,
		plans:     cfg.Plans,
	}
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
