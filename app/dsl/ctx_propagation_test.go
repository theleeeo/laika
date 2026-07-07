package dsl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/source"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
)

// The ctx given to Plan.Execute must reach every provider call — build jobs
// are cancelled via ctx (e.g. River job timeouts), and a plan that fetches
// with context.Background() keeps hammering the provider after its job died.

func customerRelationConfig() *resource.VersionConfig {
	return &resource.VersionConfig{
		Fields: []resource.FieldConfig{{Name: "number"}},
		Relations: []resource.RelationConfig{{
			Resource: "customer",
			Join:     resource.JoinConfig{Local: "id", Foreign: "order_id"},
			Fields:   []resource.FieldConfig{{Name: "name"}},
		}},
	}
}

func TestPlanExecute_PropagatesContextToProvider_SingleResource(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1"}
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	plan := buildPlanForVersion(prov, "order", customerRelationConfig(), nil)

	ctx := context.WithValue(context.Background(), ctxMarkerKey{}, "marker")
	for r := range plan.Execute(ctx, projection.BuildRequest{ResourceType: "order", ResourceID: "1"}) {
		require.NoError(t, r.Err)
	}

	require.Equal(t, "marker", prov.lastCtxValues["FetchResource"])
	require.Equal(t, "marker", prov.lastCtxValues["FetchRelated"])
}

func TestPlanExecute_PropagatesContextToProvider_ListAll(t *testing.T) {
	prov := newMockProvider()
	prov.listed["order"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "number": "ORD-1"}},
	}
	prov.related["customer|1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	plan := buildPlanForVersion(prov, "order", customerRelationConfig(), nil)

	ctx := context.WithValue(context.Background(), ctxMarkerKey{}, "marker")
	for r := range plan.Execute(ctx, projection.BuildRequest{ResourceType: "order"}) {
		require.NoError(t, r.Err)
	}

	require.Equal(t, "marker", prov.lastCtxValues["ListResources"])
	require.Equal(t, "marker", prov.lastCtxValues["FetchRelated"])
}
