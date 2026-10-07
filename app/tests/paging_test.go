package tests

import (
	"fmt"
	"slices"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
)

// The paging cases page a filter-only search (no query, no caller sort), so
// every hit ties on its score. Without a tiebreaker Elasticsearch orders tied
// hits by their place in the shard, and a rebuild moves its document to the
// end of that order (an update is a delete plus an append): a page taken after
// the rebuild repeats one hit and skips another. The cases rebuild the first
// hit of page 1 between page 1 and page 2, with its data unchanged, and require
// the three pages to hold each matching id exactly once.

// pagedIDs are the six documents of type "a" the paging filter matches, and
// pagedOutsider is one it does not; pagedFilter is that filter.
var (
	pagedIDs      = []string{"p1", "p2", "p3", "p4", "p5", "p6"}
	pagedOutsider = "q1"
	pagedFilter   = core.Filter{Field: "fields.field2", Op: core.FilterOpEq, Value: "pagedset"}
)

// seedPaged builds the paging fixture through the real build path. It
// registers and drains one resource at a time, in id order, so the documents
// enter the index in id order.
func (t *TestSuite) seedPaged() {
	t.setResourceConfig(DefaultResourceConfig)
	for i, id := range pagedIDs {
		t.fakeProvider.SetResource("a", id, map[string]any{
			"id": id, "field1": fmt.Sprintf("item%d", i+1), "field2": "pagedset",
		})
	}
	t.fakeProvider.SetResource("a", pagedOutsider, map[string]any{
		"id": pagedOutsider, "field1": "outsider", "field2": "otherset",
	})
	for _, id := range append(slices.Clone(pagedIDs), pagedOutsider) {
		t.buildA(id, core.ChangeCreated)
	}
}

// buildA registers a change to resource a/id with no source version (always
// accepted) and drains the build it starts.
func (t *TestSuite) buildA(id string, kind core.ChangeKind) {
	t.T().Helper()
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "a", ResourceID: id, Kind: kind,
	}))
	t.worker.Drain(t.T().Context())
}

// pageAcrossRebuild takes three pages of two through search; after page 1 it
// rebuilds page 1's first hit with unchanged data. It returns the pages' ids.
func (t *TestSuite) pageAcrossRebuild(search func(page int32) []string) [][]string {
	t.T().Helper()
	first := search(0)
	t.Require().NotEmpty(first, "page 1 is empty")
	t.buildA(first[0], core.ChangeUpdated)
	return [][]string{first, search(1), search(2)}
}

// requireEachOnce fails unless the pages together hold every paged id exactly
// once.
func (t *TestSuite) requireEachOnce(pages [][]string, label string) {
	t.T().Helper()
	got := slices.Sorted(slices.Values(slices.Concat(pages...)))
	t.Require().Equalf(pagedIDs, got,
		"%s: pages %v must hold each of %v exactly once", label, pages, pagedIDs)
}

// Test_Paging_SingleSearch_StableAcrossRebuild pages a single-resource Search
// with a filter and no query or sort across a rebuild of page 1's first hit.
func (t *TestSuite) Test_Paging_SingleSearch_StableAcrossRebuild() {
	t.seedPaged()

	pages := t.pageAcrossRebuild(func(page int32) []string {
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "a",
			Filters:  []core.Filter{pagedFilter},
			Page:     page,
			PageSize: 2,
		})
		t.Require().NoErrorf(err, "page %d", page+1)
		t.Require().EqualValuesf(len(pagedIDs), resp.Total, "page %d total", page+1)
		ids := make([]string, 0, len(resp.Hits))
		for _, h := range resp.Hits {
			ids = append(ids, h.ID)
		}
		return ids
	})
	t.requireEachOnce(pages, "single-resource search")
}

// Test_Paging_FederatedSearch_StableAcrossRebuild pages a FederatedSearch over
// the one type, with a global filter and no query, across a rebuild of page
// 1's first hit — under each execution mode. The modes share one fixture, so
// each runs on the order the modes before it left. The fan-out runs first, on
// the fixture as seeded: it already breaks ties by id when it merges, so
// whether a rebuild shows through depends on how the stored order lines up
// with the ids.
func (t *TestSuite) Test_Paging_FederatedSearch_StableAcrossRebuild() {
	t.seedPaged()

	for _, mode := range []elasticsearch.FederatedExecution{
		elasticsearch.FederatedFanout,
		elasticsearch.FederatedSingle,
		elasticsearch.FederatedSingleDFS, // the default
	} {
		t.Run(string(mode), func() {
			idx, err := core.New(core.Config{
				Resources: DefaultResourceConfig,
				ES:        elasticsearch.New(t.esClient, true, elasticsearch.WithFederatedExecution(mode)),
			})
			t.Require().NoErrorf(err, "mode %s", mode)

			pages := t.pageAcrossRebuild(func(page int32) []string {
				resp, err := idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
					Resources: []string{"a"},
					Filters:   []core.Filter{pagedFilter},
					Page:      page,
					PageSize:  2,
				})
				t.Require().NoErrorf(err, "mode %s page %d", mode, page+1)
				t.Require().EqualValuesf(len(pagedIDs), resp.Total, "mode %s page %d total", mode, page+1)
				ids := make([]string, 0, len(resp.Hits))
				for _, h := range resp.Hits {
					t.Require().Equalf("a", h.Resource, "mode %s page %d", mode, page+1)
					ids = append(ids, h.ID)
				}
				return ids
			})
			t.requireEachOnce(pages, fmt.Sprintf("federated search, mode %s", mode))
		})
	}
}
