package tests

import (
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// Test_BrandNewChild_BootstrapsParentEdge exercises reverse-relation discovery
// (ADR 0006 / issue #7). A Child created before its Parent has any persisted
// Relation edge would, without this feature, never reach its Parent: the
// RegisterChange fanout has no edge to follow. Reverse discovery resolves the
// Parent from the Child's own built document and enqueues the Parent's Build —
// no upstream Parent Notification required.
//
// Distinct from Test_Create_ParentRelation_Already_Exists, which covers the
// case where the edge already exists.
func (t *TestSuite) Test_BrandNewChild_BootstrapsParentEdge() {
	t.setResourceConfig(DefaultResourceConfig)

	// The relation on `a` is a.id == b.a_id, so b/1 (carrying a_id="1") is a
	// child of a/1. We deliberately never build a/1 first, so no a->b edge
	// exists when b/1 is created.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "a_id": "1", "field1": "b_val"})
	// When a/1 is built it fetches its related b's keyed by a's id ("1").
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "a_id": "1", "field1": "b_val"},
	})

	// Precondition: no Parent edge for b/1 yet.
	parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "b", Id: "1"})
	t.Require().NoError(err)
	t.Require().Empty(parents)

	// Create the brand-new Child. RegisterChange finds no parents to fan out to,
	// so only b is enqueued; reverse discovery during b's Build must bootstrap
	// the Build of a/1.
	err = t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "b", ResourceID: "1", Kind: core.ChangeCreated,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	t.Run("parent is rebuilt to include the new child (AC#1)", func() {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].ID)

		rels := sourceList(resp.Hits[0].Source, "b")
		t.Require().Len(rels, 1)
		t.Require().Equal("1", fieldStr(rels[0], "id"))
	})

	t.Run("bootstrap established the parent edge (AC#2)", func() {
		parents, err := t.st.GetParentResources(t.T().Context(), model.Resource{Type: "b", Id: "1"})
		t.Require().NoError(err)
		t.Require().Contains(parents, model.Resource{Type: "a", Id: "1"})
	})

	t.Run("subsequent child update converges the parent via the edge (AC#2)", func() {
		t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "a_id": "1", "field1": "b_val_updated"})
		t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
			{"id": "1", "a_id": "1", "field1": "b_val_updated"},
		})

		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "b", ResourceID: "1", Kind: core.ChangeUpdated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)

		rels := sourceList(resp.Hits[0].Source, "b")
		t.Require().Len(rels, 1)
		t.Require().Equal("b_val_updated", fieldStr(rels[0], "field1"))
	})
}
