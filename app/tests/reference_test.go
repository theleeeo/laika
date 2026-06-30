package tests

import (
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
)

// Test_ReferenceRelation_EndToEnd exercises the reference strategy for the
// C→A→B scenario:
//   - c denormalizes a (carrying b_id as a field)
//   - c references b keyed from: a, local: b_id, foreign: id
//
// Assertions:
//  1. Building c1 writes no c→b parent edge (reference, not denormalize).
//  2. Searching c by b.name=acme returns c1 (two-phase reference join).
//  3. Changing b1 does not rebuild c1 (no fanout edge).
func (t *TestSuite) Test_ReferenceRelation_EndToEnd() {
	referenceConfig := resource.Configs{
		{
			Resource: "c",
			Versions: []resource.VersionConfig{
				{
					Version: 1,
					Fields: []resource.FieldConfig{
						{Name: "title"},
					},
					Relations: []resource.RelationConfig{
						{
							// denormalize a into c; carry b_id so the reference key is reachable
							Resource: "a",
							Join:     resource.JoinConfig{Local: "id", Foreign: "c_id"},
							Fields: []resource.FieldConfig{
								{Name: "b_id"},
							},
						},
						{
							// reference b at search time via the b_id propagated from a
							Resource: "b",
							Strategy: resource.StrategyReference,
							Join:     resource.JoinConfig{Local: "b_id", From: "a", Foreign: "id"},
							Fields:   []resource.FieldConfig{{Name: "name"}},
						},
					},
				},
			},
		},
		{
			Resource: "a",
			Versions: []resource.VersionConfig{
				{
					Version: 1,
					Fields: []resource.FieldConfig{
						{Name: "c_id"},
						{Name: "b_id"},
					},
				},
			},
		},
		{
			Resource: "b",
			Versions: []resource.VersionConfig{
				{
					Version: 1,
					Fields: []resource.FieldConfig{
						{Name: "name"},
					},
				},
			},
		},
	}

	for _, cfg := range referenceConfig {
		cfg.ApplyDefaults()
	}
	t.Require().NoError(referenceConfig.Validate())

	t.setResourceConfig(referenceConfig)

	// Seed provider data.
	// c1 root resource
	t.fakeProvider.SetResource("c", "c1", map[string]any{
		"id":    "c1",
		"title": "doc one",
	})
	// a1 is related to c1 via c_id; it carries b_id=b1 which the reference relation needs
	t.fakeProvider.SetRelated("a", []string{"c1"}, []map[string]any{
		{"id": "a1", "c_id": "c1", "b_id": "b1"},
	})
	// b1 root resource
	t.fakeProvider.SetResource("b", "b1", map[string]any{
		"id":   "b1",
		"name": "acme",
	})

	// Build c1 (enqueues async build via River)
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "c",
		ResourceID:   "c1",
		Kind:         core.ChangeCreated,
	}))
	// Build b1 so it exists in ES for the reference search
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "b",
		ResourceID:   "b1",
		Kind:         core.ChangeCreated,
	}))
	t.worker.Drain(t.T().Context())

	// Assertion 1: no c→b edge — reference relations must not create parent edges.
	t.Run("no c->b edge persisted", func() {
		parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "b", Id: "b1"})
		t.Require().NoError(err)
		for _, p := range parents {
			t.Require().NotEqual("c", p.Type, "reference relation must not create a c->b parent edge")
		}
	})

	// Assertion 2: search c by b.name = "acme" returns c1 via the two-phase reference join.
	t.Run("search c by b.name returns c1", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "c",
			Filters:  []core.Filter{{Field: "b.name", Op: core.FilterOpEq, Value: "acme"}},
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("c1", resp.Hits[0].ID)
	})

	// Assertion 3: changing b1 does not rebuild c1 (no fanout edge exists).
	t.Run("b1 change does not rebuild c1", func() {
		beforeCounter := t.resourceRebuildCounter("c", "c1")

		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "b",
			ResourceID:   "b1",
			Kind:         core.ChangeUpdated,
		}))
		t.worker.Drain(t.T().Context())

		afterCounter := t.resourceRebuildCounter("c", "c1")
		t.Require().Equal(beforeCounter, afterCounter,
			"changing referenced b must not trigger a c rebuild (no fanout edge)")
	})
}
