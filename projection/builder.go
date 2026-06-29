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
}

// BuildDoc is the intermediate document flowing through the aggregation plan.
// It carries the final doc, the resolved data for chained relations, and root info.
type BuildDoc struct {
	Root     model.Resource
	Metadata map[string]string

	Doc      map[string]any
	Resolved map[string][]map[string]any

	// Relations are the forward edges discovered during the build — the
	// children this document references. Core persists them in the Relation
	// graph.
	Relations []model.VersionedResource

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
