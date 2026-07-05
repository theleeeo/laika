package tests

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
)

// indexRaw writes a document directly into a concrete index and refreshes, so a
// federated query test can control the standardized search fields (search /
// search_scoped) without driving the full Build pipeline (covered separately).
func (t *TestSuite) indexRaw(index, id string, doc map[string]any) {
	b, err := json.Marshal(doc)
	t.Require().NoError(err)
	res, err := t.esClient.Index(
		index,
		bytes.NewReader(b),
		t.esClient.Index.WithDocumentID(id),
		t.esClient.Index.WithRefresh("true"),
	)
	t.Require().NoError(err)
	defer res.Body.Close()
	t.Require().Falsef(res.IsError(), "index error: %s", res.String())
}

// Test_FederatedSearch_CrossTypeRankedAndCounts exercises the end-to-end
// federated path: one query across two Types returns a single relevance-ranked
// list with per-Type counts, and paging bounds the hits without changing counts.
func (t *TestSuite) Test_FederatedSearch_CrossTypeRankedAndCounts() {
	t.setResourceConfig(DefaultResourceConfig)

	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{"search": "red widget deluxe"})
	t.indexRaw(core.IndexName("b", 1), "b1", map[string]any{"search": "widget"})
	t.indexRaw(core.IndexName("b", 1), "b2", map[string]any{"search": "blue gadget"})

	resp, err := t.idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
		Query:         "widget",
		Resources:     []string{"a", "b"},
		IncludeSource: true,
	})
	t.Require().NoError(err)

	// a1 and b1 match "widget"; b2 ("gadget") does not.
	t.Require().EqualValues(2, resp.Total)
	t.Require().Len(resp.Hits, 2)

	// Hits span both Types and are ordered best-first; b1's exact single-token
	// field outranks a1's longer field under BM25 (dfs makes scores comparable).
	t.Require().Equal("b", resp.Hits[0].Resource)
	t.Require().Equal("b1", resp.Hits[0].ID)
	t.Require().Equal("a", resp.Hits[1].Resource)
	t.Require().GreaterOrEqual(resp.Hits[0].Score, resp.Hits[1].Score)

	// include_source populated the source.
	t.Require().Equal("widget", resp.Hits[0].Source["search"])

	// Per-Type counts, one entry per requested Type in request order.
	t.Require().Equal([]core.ResourceCount{{Resource: "a", Count: 1}, {Resource: "b", Count: 1}}, resp.Counts)

	t.Run("paging bounds hits but not counts", func() {
		paged, err := t.idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
			Query:     "widget",
			Resources: []string{"a", "b"},
			PageSize:  1,
		})
		t.Require().NoError(err)
		t.Require().EqualValues(2, paged.Total)
		t.Require().Len(paged.Hits, 1)
		t.Require().Equal("b1", paged.Hits[0].ID)
		t.Require().Equal([]core.ResourceCount{{Resource: "a", Count: 1}, {Resource: "b", Count: 1}}, paged.Counts)
	})
}

type scopeCtxKey struct{}

// Test_FederatedSearch_SecondaryScopeCorrelation proves the secondary tier
// matches text only inside nested entries the caller may see: a single Document
// with two scoped entries matches its "alpha" text for tenant t1 but not its
// "beta" text (which is scoped to t2), and vice versa — the scope term and the
// text match are correlated within the same nested entry.
func (t *TestSuite) Test_FederatedSearch_SecondaryScopeCorrelation() {
	t.setResourceConfig(DefaultResourceConfig)

	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{
		"search_scoped": []any{
			map[string]any{"text": "alpha", "scope": []any{"t1"}},
			map[string]any{"text": "beta", "scope": []any{"t2"}},
		},
	})

	// A consumer middleware that supplies the caller's tenant scope from context.
	scopeMW := func(next core.SearchHandler) core.SearchHandler {
		return func(ctx context.Context, req core.SearchRequest) (core.SearchResponse, error) {
			if s, ok := ctx.Value(scopeCtxKey{}).(string); ok {
				req.SecondaryScope = s
			}
			return next(ctx, req)
		}
	}
	scopedIdx := core.New(core.Config{
		Resources:         DefaultResourceConfig,
		ES:                elasticsearch.New(t.esClient, true),
		SearchMiddlewares: []core.SearchMiddleware{scopeMW},
	})

	search := func(query, tenant string) core.FederatedSearchResponse {
		ctx := context.WithValue(t.T().Context(), scopeCtxKey{}, tenant)
		resp, err := scopedIdx.FederatedSearch(ctx, core.FederatedSearchRequest{
			Query:     query,
			Resources: []string{"a"},
		})
		t.Require().NoError(err)
		return resp
	}

	// "beta" is scoped to t2, so t1 cannot match it even though the text is there.
	t.Require().EqualValues(0, search("beta", "t1").Total)
	// The same caller CAN match "alpha" (scoped to t1) on the same Document —
	// correlation holds per entry, not per Document.
	t.Require().EqualValues(1, search("alpha", "t1").Total)
	// t2 matches "beta".
	t.Require().EqualValues(1, search("beta", "t2").Total)
}
