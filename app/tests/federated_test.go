package tests

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
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

// referenceScopeConfig mirrors the vx-fiber harness domain: an access-point
// references a population (cardinality one, strategy reference) by
// population_id == population.id, and population carries fiber_operator_id as a
// root field. The reference relation's fields are NOT indexed on the
// access-point document, so scoping access-points by population.fiber_operator_id
// requires a separate population query — exactly the case a single-index term
// filter cannot express.
var referenceScopeConfig = func() resource.Configs {
	cfgs := resource.Configs{
		{
			Resource: "pop",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Fields:  []resource.FieldConfig{{Name: "name"}, {Name: "fiber_operator_id"}},
			}},
		},
		{
			Resource: "ap",
			Versions: []resource.VersionConfig{{
				Version: 1,
				Fields:  []resource.FieldConfig{{Name: "population_id"}},
				Relations: []resource.RelationConfig{{
					Resource:    "pop",
					Join:        resource.JoinConfig{Local: "population_id", Foreign: "id"},
					Cardinality: "one",
					Strategy:    resource.StrategyReference,
					Fields:      []resource.FieldConfig{{Name: "fiber_operator_id"}},
				}},
			}},
		},
	}
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	return cfgs
}()

// Test_FederatedSearch_ResolvesReferenceRelationFilter proves a federated search
// resolves a filter naming a reference relation's field by issuing a separate
// query to the referenced Type and folding the matching join keys into a terms
// filter on the parent — instead of applying the (unmapped) reference field as a
// term on the parent index, which silently matches nothing.
func (t *TestSuite) Test_FederatedSearch_ResolvesReferenceRelationFilter() {
	t.setResourceConfig(referenceScopeConfig)

	// Seed the searchable surface + join keys directly (Build pipeline covered
	// elsewhere). pop-A/ap-1 belong to operator op-A; pop-B/ap-2 to op-B.
	t.indexRaw(core.IndexName("pop", 1), "pop-A", map[string]any{
		"search": "fiber", "fields": map[string]any{"name": "alpha", "fiber_operator_id": "op-A"},
	})
	t.indexRaw(core.IndexName("pop", 1), "pop-B", map[string]any{
		"search": "fiber", "fields": map[string]any{"name": "beta", "fiber_operator_id": "op-B"},
	})
	t.indexRaw(core.IndexName("ap", 1), "ap-1", map[string]any{
		"search": "fiber", "fields": map[string]any{"population_id": "pop-A"},
	})
	t.indexRaw(core.IndexName("ap", 1), "ap-2", map[string]any{
		"search": "fiber", "fields": map[string]any{"population_id": "pop-B"},
	})

	// scopeFor builds an indexer whose consumer middleware scopes each Type to a
	// fiber operator the way authz does: population by its own root field,
	// access-point through the referenced population's field.
	scopeFor := func(operator string) *core.Indexer {
		mw := func(next core.SearchHandler) core.SearchHandler {
			return func(ctx context.Context, req core.SearchRequest) (core.SearchResponse, error) {
				switch req.Resource {
				case "pop":
					req.AddFilter(core.Filter{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: operator})
				case "ap":
					req.AddFilter(core.Filter{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: operator})
				}
				return next(ctx, req)
			}
		}
		return core.New(core.Config{
			Resources:         referenceScopeConfig,
			ES:                elasticsearch.New(t.esClient, true),
			SearchMiddlewares: []core.SearchMiddleware{mw},
		})
	}

	resp, err := scopeFor("op-A").FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
		Query:     "fiber",
		Resources: []string{"pop", "ap"},
	})
	t.Require().NoError(err)

	ids := map[string]bool{}
	for _, h := range resp.Hits {
		ids[h.ID] = true
	}
	// Only op-A's data: pop-A directly, ap-1 via the referenced pop-A. pop-B and
	// ap-2 (op-B) are excluded — ap-2 only via the separate population query.
	t.Require().Len(resp.Hits, 2)
	t.Require().True(ids["pop-A"], "pop-A matches op-A on its own field")
	t.Require().True(ids["ap-1"], "ap-1 matches via referenced pop-A (separate population query)")
	t.Require().False(ids["ap-2"], "ap-2 belongs to op-B and must be excluded")
	t.Require().EqualValues(2, resp.Total)
	t.Require().ElementsMatch(
		[]core.ResourceCount{{Resource: "pop", Count: 1}, {Resource: "ap", Count: 1}},
		resp.Counts,
	)

	t.Run("a reference matching no children excludes only that Type", func() {
		// Scope to an operator with a population (none) but... use a mixed scope:
		// pop scoped to op-A (matches pop-A), ap scoped to op-none (no population),
		// so the ap group resolves to zero children and must contribute nothing
		// while pop still returns.
		mw := func(next core.SearchHandler) core.SearchHandler {
			return func(ctx context.Context, req core.SearchRequest) (core.SearchResponse, error) {
				switch req.Resource {
				case "pop":
					req.AddFilter(core.Filter{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: "op-A"})
				case "ap":
					req.AddFilter(core.Filter{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: "op-none"})
				}
				return next(ctx, req)
			}
		}
		idx := core.New(core.Config{
			Resources:         referenceScopeConfig,
			ES:                elasticsearch.New(t.esClient, true),
			SearchMiddlewares: []core.SearchMiddleware{mw},
		})
		resp, err := idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
			Query:     "fiber",
			Resources: []string{"pop", "ap"},
		})
		t.Require().NoError(err)
		t.Require().EqualValues(1, resp.Total)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("pop-A", resp.Hits[0].ID)
		t.Require().ElementsMatch(
			[]core.ResourceCount{{Resource: "pop", Count: 1}, {Resource: "ap", Count: 0}},
			resp.Counts,
		)
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
