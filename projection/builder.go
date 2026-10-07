package projection

import (
	"context"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
)

// BuildRequest is the request parameter for the aggregation plan.
type BuildRequest struct {
	ResourceType string
	ResourceID   string
	Metadata     map[string]string

	// PageToken starts an all-of-type walk (ResourceID == "") mid-listing:
	// the walk's first fetch uses it in place of the first page. Empty means
	// the beginning. Ignored for single-resource builds. Set by core when
	// resuming a rebuild walk from a RebuildCursor.
	PageToken string

	// PageSize asks an all-of-type walk's listing for pages of this many
	// resources; 0 leaves the size to the plan. A plan that ignores it is
	// correct, but a paced walk's rate budget then counts its own pages.
	PageSize int
}

// BuildDoc is the intermediate document flowing through the aggregation plan.
// It carries the final doc, the resolved data for chained relations, and root info.
type BuildDoc struct {
	Root model.Resource
	// Metadata is the fetch context flowing down the plan: the request's,
	// which relation fetches decorate with. It is not the resource's own.
	Metadata map[string]string
	// ResourceMetadata is the resource's own metadata as the plan reports it
	// — the actor a later build of the resource fetches as — or nil when the
	// plan doesn't know. Core stores it as the resource's row metadata when
	// the row has none; see core.Store.ReplaceEdges.
	ResourceMetadata map[string]string

	Doc      map[string]any
	Resolved map[string][]map[string]any

	// Relations are the forward edges discovered during the build — the
	// children this document references. Core persists them in the Relation
	// graph.
	Relations []model.Resource

	// Parents are the reverse edges derived from the root's own data — the
	// Parents that should also be built so they include this resource. The Plan
	// populates them after fetching the root; core enqueues a Build for each.
	// This bootstraps a Parent edge for a brand-new Child that has no persisted
	// edge yet. See ADR 0006.
	Parents []model.Resource
}

// TODO: NewPlan builder
// Plan is an aggregation executor that produces BuildDoc results.
type Plan struct {
	Version  int
	Executer aggregation.Executer[BuildRequest, BuildDoc]

	// Probe, when set, reports which of ids the plan's root fetch would find
	// when fetching with metadata: it returns exactly those ids, in any
	// order, and fails where that fetch would fail — as a fetch with no
	// actor does in a source that needs one. It may use ids as it likes,
	// filtering them in place included: the caller doesn't read them back.
	// The reverse sweep (core.Indexer.ReverseSweepNow) calls it with one
	// actor's ids at a time and marks each id it doesn't return for an owned
	// build, which rebuilds or deletes it, so a probe that misses an id its
	// root fetch would find costs a needless build, and one that returns an
	// id its root fetch would not find misses a delete. Write it beside the
	// plan, on the clients its root fetch uses: a by-ids call where the
	// source has one, a call per id otherwise. An error fails the whole call;
	// nothing of it is used. Existence belongs to the resource, not to a
	// Schema Version, so the sweep uses the first of a type's plans that has
	// a Probe (ADR 0012).
	Probe func(ctx context.Context, ids []string, metadata map[string]string) (present []string, err error)
}

// TODO: Abstract away
func (p Plan) Execute(ctx context.Context, req BuildRequest) <-chan aggregation.ExecutionResult[BuildDoc] {
	return p.Executer.Execute(ctx, req)
}
