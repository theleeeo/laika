package dsl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// childParents builds plans from the config, runs the child's plan for the
// given id (with childData as its fetched source data), and returns the Parents
// the plan derived onto the resulting BuildDoc.
func childParents(t *testing.T, resources resource.Configs, childType, childID string, childData map[string]any) []model.Resource {
	t.Helper()

	prov := newMockProvider()
	prov.resources[childType+"|"+childID] = childData

	plans := BuildPlansFromConfig(prov, resources)
	childPlans := plans[childType]
	require.NotEmpty(t, childPlans)

	ch := childPlans[0].Execute(context.Background(), projection.BuildRequest{
		ResourceType: childType,
		ResourceID:   childID,
	})

	var doc projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		if len(r.Items) > 0 {
			doc = r.Items[0]
		}
	}
	return doc.Parents
}

// child declares a child resource type with a single version and no relations.
func child(name string) *resource.Config {
	return &resource.Config{
		Resource: name,
		Versions: []resource.VersionConfig{{Version: 1}},
	}
}

// On `a`, relation to `b` joins a.id == b.a_id. Building child `b` should
// derive parent `a` from b's own `a_id` field.
func TestPlanParents_InvertibleRelationEmitsParent(t *testing.T) {
	resources := resource.Configs{
		{
			Resource: "a",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Relations: []resource.RelationConfig{
					{Resource: "b", Join: resource.JoinConfig{Local: "id", Foreign: "a_id"}},
				},
			}},
		},
		child("b"),
	}

	parents := childParents(t, resources, "b", "b1", map[string]any{"id": "b1", "a_id": "a1"})

	require.Equal(t, []model.Resource{{Type: "a", Id: "a1"}}, parents)
}

// When the parent holds the foreign key (a.b_id == b.id), the parent's resource
// id is not derivable from the child, so building `b` derives nothing — and does
// not error (acceptance criterion #4).
func TestPlanParents_ParentHoldingFKEmitsNothing(t *testing.T) {
	resources := resource.Configs{
		{
			Resource: "a",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Relations: []resource.RelationConfig{
					{Resource: "b", Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Cardinality: "one"},
				},
			}},
		},
		child("b"),
	}

	parents := childParents(t, resources, "b", "b1", map[string]any{"id": "b1"})

	require.Empty(t, parents)
}

// A chained join reads its local value from a sibling relation, not the parent's
// own identity, so it is not invertible to the parent and derives nothing.
func TestPlanParents_ChainedJoinEmitsNothing(t *testing.T) {
	resources := resource.Configs{
		{
			Resource: "a",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Relations: []resource.RelationConfig{
					{Resource: "customer", Join: resource.JoinConfig{Local: "id", Foreign: "a_id"}},
					{Resource: "address", Join: resource.JoinConfig{Local: "address_id", Foreign: "id", From: "customer"}},
				},
			}},
		},
		child("customer"),
		child("address"),
	}

	require.Empty(t, childParents(t, resources, "address", "addr9", map[string]any{"id": "addr9"}))
}

// The same relation declared across multiple versions of a parent must not
// produce duplicate parents.
func TestPlanParents_DedupesAcrossVersions(t *testing.T) {
	resources := resource.Configs{
		{
			Resource: "a",
			Versions: []resource.VersionConfig{
				{Version: 1, Relations: []resource.RelationConfig{
					{Resource: "b", Join: resource.JoinConfig{Local: "id", Foreign: "a_id"}},
				}},
				{Version: 2, Relations: []resource.RelationConfig{
					{Resource: "b", Join: resource.JoinConfig{Local: "id", Foreign: "a_id"}},
				}},
			},
		},
		child("b"),
	}

	parents := childParents(t, resources, "b", "b1", map[string]any{"id": "b1", "a_id": "a1"})

	require.Equal(t, []model.Resource{{Type: "a", Id: "a1"}}, parents)
}

func TestBuildReverseMapSkipsReference(t *testing.T) {
	resources := resource.Configs{
		{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Relations: []resource.RelationConfig{
			// invertible by shape (Local==id, From=="") but reference -> must be skipped
			{Resource: "a", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "id", Foreign: "b_id"}, Fields: []resource.FieldConfig{{Name: "n"}}},
		}}}},
		{Resource: "a", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "n"}}}}},
	}
	rev := buildReverseMap(resources)
	if len(rev["a"]) != 0 {
		t.Fatalf("reference relation must not appear in reverse map, got %+v", rev["a"])
	}
}

// A child whose foreign field is absent or empty yields no parent and no error.
func TestPlanParents_MissingForeignFieldEmitsNothing(t *testing.T) {
	resources := resource.Configs{
		{
			Resource: "a",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Relations: []resource.RelationConfig{
					{Resource: "b", Join: resource.JoinConfig{Local: "id", Foreign: "a_id"}},
				},
			}},
		},
		child("b"),
	}

	require.Empty(t, childParents(t, resources, "b", "b1", map[string]any{"id": "b1"}))
	require.Empty(t, childParents(t, resources, "b", "b1", map[string]any{"id": "b1", "a_id": ""}))
}
