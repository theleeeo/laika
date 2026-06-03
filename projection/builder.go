package projection

import (
	"context"

	"github.com/theleeeo/indexer/aggregation"
	"github.com/theleeeo/indexer/model"
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

	Relations []model.VersionedResource
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
