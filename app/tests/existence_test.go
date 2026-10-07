package tests

import (
	"context"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/app/dsl"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/projection"
)

// nilForID wraps a plan's Executer and answers nil for one resource: it runs
// the plan as it is and replaces each item rooted at id with one without a
// document, the shape of a plan whose source has no data for it.
type nilForID struct {
	inner aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
	id    string
}

func (e nilForID) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	out := make(chan aggregation.ExecutionResult[projection.BuildDoc])
	go func() {
		defer close(out)
		for r := range e.inner.Execute(ctx, req) {
			for i, item := range r.Items {
				if item.Root.Id == e.id {
					r.Items[i] = projection.BuildDoc{Root: item.Root}
				}
			}
			out <- r
		}
	}()
	return out
}

// Test_Existence_NilVersionDeletesOnlyItsOwnDocument: a plan's nil is its own
// Schema Version's answer (ADR 0013). p/X is built with both versions' plans,
// so each version holds its document and its edges (v1's child b/xX, v2's
// c/oldX). Then v2's plan stops returning X — a new version that filters it
// out — while v1's still does. A notification of X writes v1's fresh
// document, deletes v2's and empties v2's edge set, keeps X's row and settles
// it, and a search through the alias, which reads v1, still finds X.
func (t *TestSuite) Test_Existence_NilVersionDeletesOnlyItsOwnDocument() {
	for _, c := range EdgesVersionedConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(EdgesVersionedConfig.Validate())
	t.setResourceConfig(EdgesVersionedConfig)
	ctx := t.T().Context()
	v1Index, v2Index := core.IndexName("p", 1), core.IndexName("p", 2)

	t.fakeProvider.SetResource("p", "X", map[string]any{"id": "X", "f1": "before"})
	t.fakeProvider.SetRelated("b", []string{"X"}, []map[string]any{{"id": "xX", "f1": "bx"}})
	t.fakeProvider.SetRelated("c", []string{"X"}, []map[string]any{{"id": "oldX", "f1": "cold"}})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{ResourceType: "p", ResourceID: "X", Kind: core.ChangeCreated}))
	t.worker.Drain(ctx)
	_, ok := t.docFields(v2Index, "X")
	t.Require().True(ok, "setup: v2 must hold X while its plan returns it")
	t.Require().Equal([]string{"c/oldX"}, t.versionEdges("p", "X", 2), "setup: v2's edges must be its plan's")

	plans := dsl.BuildPlansFromConfig(t.fakeProvider, EdgesVersionedConfig)
	for i, p := range plans["p"] {
		if p.Version == 2 {
			plans["p"][i].Executer = nilForID{inner: p.Executer, id: "X"}
		}
	}
	x := t.newIndexer(EdgesVersionedConfig, core.Config{Plans: plans})

	t.fakeProvider.SetResource("p", "X", map[string]any{"id": "X", "f1": "after"})
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{ResourceType: "p", ResourceID: "X", Kind: core.ChangeUpdated}))
	t.Require().NoError(x.WaitForIdle(ctx))

	f1, ok := t.docFields(v1Index, "X")
	t.Require().True(ok, "v1's plan returns X: v1 must keep its document")
	t.Require().Equal("after", f1["f1"], "v1's document must be the build's")
	_, ok = t.docFields(v2Index, "X")
	t.Require().False(ok, "v2's plan returns nil for X: v2's document must be deleted")

	t.Require().Equal([]string{"b/xX"}, t.versionEdges("p", "X", 1), "v1's edges must stay its plan's")
	t.Require().Empty(t.versionEdges("p", "X", 2), "v2's edge set must be replaced with an empty one")

	t.Require().True(t.resourceTracked("p", "X"), "X's row must stay while a version has its document")
	t.Require().Nil(t.staleSince("p", "X"), "the build must settle X, not leave it stale")

	resp, err := x.Search(ctx, core.SearchRequest{Resource: "p", Query: "after"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1, "the alias reads v1, which holds X")
	t.Require().Equal("X", resp.Hits[0].ID)
}
