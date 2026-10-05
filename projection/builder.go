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
}

// TODO: Abstract away
func (p Plan) Execute(ctx context.Context, req BuildRequest) <-chan aggregation.ExecutionResult[BuildDoc] {
	return p.Executer.Execute(ctx, req)
}
