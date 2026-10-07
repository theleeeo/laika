package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// indexRaw writes a document directly into a concrete index and refreshes, so a
// federated query test can control the standardized search fields (search /
// search_secondary) without driving the full Build pipeline (covered separately).
func (t *TestSuite) indexRaw(index, id string, doc map[string]any) {
	// Carry resource_id as every built document does, set on a copy so the
	// caller's map stays as given.
	withID := make(map[string]any, len(doc)+1)
	maps.Copy(withID, doc)
	withID[resource.ResourceIDField] = id
	b, err := json.Marshal(withID)
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

	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{"search_primary": "red widget deluxe"})
	t.indexRaw(core.IndexName("b", 1), "b1", map[string]any{"search_primary": "widget"})
	t.indexRaw(core.IndexName("b", 1), "b2", map[string]any{"search_primary": "blue gadget"})

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
	t.Require().Equal("widget", resp.Hits[0].Source["search_primary"])

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
		"search_primary": "fiber", "fields": map[string]any{"name": "alpha", "fiber_operator_id": "op-A"},
	})
	t.indexRaw(core.IndexName("pop", 1), "pop-B", map[string]any{
		"search_primary": "fiber", "fields": map[string]any{"name": "beta", "fiber_operator_id": "op-B"},
	})
	t.indexRaw(core.IndexName("ap", 1), "ap-1", map[string]any{
		"search_primary": "fiber", "fields": map[string]any{"population_id": "pop-A"},
	})
	t.indexRaw(core.IndexName("ap", 1), "ap-2", map[string]any{
		"search_primary": "fiber", "fields": map[string]any{"population_id": "pop-B"},
	})

	// scopeFor builds an indexer whose federated middleware scopes each Type to
	// a fiber operator the way authz does: population by its own root field,
	// access-point through the referenced population's field.
	scopeFor := func(operator string) *core.Indexer {
		mw := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
			return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
				req.ResourceFilters = map[string][]core.Filter{
					"pop": {{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: operator}},
					"ap":  {{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: operator}},
				}
				return next(ctx, req)
			}
		}
		idx, err := core.New(core.Config{
			Resources:                  referenceScopeConfig,
			ES:                         elasticsearch.New(t.esClient, true),
			FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{mw},
		})
		t.Require().NoError(err)
		return idx
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
		mw := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
			return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
				req.ResourceFilters = map[string][]core.Filter{
					"pop": {{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: "op-A"}},
					"ap":  {{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: "op-none"}},
				}
				return next(ctx, req)
			}
		}
		idx, err := core.New(core.Config{
			Resources:                  referenceScopeConfig,
			ES:                         elasticsearch.New(t.esClient, true),
			FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{mw},
		})
		t.Require().NoError(err)
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
		"search_secondary": []any{
			map[string]any{"text": "alpha", "scope": []any{"t1"}},
			map[string]any{"text": "beta", "scope": []any{"t2"}},
		},
	})

	// A consumer federated middleware that supplies the caller's tenant scope
	// from context.
	scopeMW := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
		return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
			if s, ok := ctx.Value(scopeCtxKey{}).(string); ok {
				req.SecondaryScope = s
			}
			return next(ctx, req)
		}
	}
	scopedIdx, err := core.New(core.Config{
		Resources:                  DefaultResourceConfig,
		ES:                         elasticsearch.New(t.esClient, true),
		FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{scopeMW},
	})
	t.Require().NoError(err)

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

// Test_FederatedSearch_InfixMatches proves the federated query matches any
// substring of the indexed text ("*query*" semantics): the query is shredded
// with the same n-gram analysis the index side uses, and a document matches
// when it contains every gram of the query (spec D15). Whole words keep
// matching via the standard-analyzed .full subfield.
func (t *TestSuite) Test_FederatedSearch_InfixMatches() {
	t.setResourceConfig(DefaultResourceConfig)

	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{"search_primary": "Aichkirchen 2"})
	t.indexRaw(core.IndexName("a", 1), "a2", map[string]any{"search_primary": "Neustadtl an der Donau"})

	search := func(query string) core.FederatedSearchResponse {
		resp, err := t.idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
			Query:     query,
			Resources: []string{"a"},
		})
		t.Require().NoError(err)
		return resp
	}

	// Prefix, infix, and suffix fragments longer than the gram size all match.
	for _, q := range []string{"Aich", "Aichk", "chkirch", "kirchen", "Aichkirchen"} {
		resp := search(q)
		t.Require().EqualValuesf(1, resp.Total, "query %q", q)
		t.Require().Equalf("a1", resp.Hits[0].ID, "query %q", q)
	}

	// A fragment that is not a substring of any document matches nothing.
	t.Require().EqualValues(0, search("Aichx").Total)

	// A multi-word query still matches word-wise: each word hits its document
	// even though no single document contains both.
	both := search("Aichkirchen Neustadtl")
	t.Require().EqualValues(2, both.Total)

	t.Run("secondary tier matches infix", func() {
		t.indexRaw(core.IndexName("b", 1), "b1", map[string]any{
			"search_secondary": []any{
				map[string]any{"text": "Grieskirchen depot", "scope": []any{}},
			},
		})
		resp, err := t.idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
			Query:     "rieskirch",
			Resources: []string{"b"},
		})
		t.Require().NoError(err)
		t.Require().EqualValues(1, resp.Total)
		t.Require().Equal("b1", resp.Hits[0].ID)
	})
}

// Test_FederatedSearch_ExecutionModeParity runs the same federated query under
// all three backend execution modes — the default single multi-index query with
// DFS term statistics, the same query with index-local statistics, and the
// per-Type fan-out merged client-side — and asserts they agree on hit
// membership, per-Type counts, and totals. Only scores may differ between
// modes, and paging must walk a stable, non-overlapping ranking in each.
func (t *TestSuite) Test_FederatedSearch_ExecutionModeParity() {
	t.setResourceConfig(DefaultResourceConfig)

	t.indexRaw(core.IndexName("a", 1), "a1", map[string]any{"search_primary": "red widget deluxe"})
	t.indexRaw(core.IndexName("a", 1), "a2", map[string]any{"search_primary": "widget assembly kit"})
	t.indexRaw(core.IndexName("b", 1), "b1", map[string]any{"search_primary": "widget"})
	t.indexRaw(core.IndexName("b", 1), "b2", map[string]any{"search_primary": "blue gadget"})

	type outcome struct {
		ids    map[string]bool
		counts []core.ResourceCount
		total  int64
	}
	results := map[elasticsearch.FederatedExecution]outcome{}

	for _, mode := range []elasticsearch.FederatedExecution{
		elasticsearch.FederatedSingleDFS,
		elasticsearch.FederatedSingle,
		elasticsearch.FederatedFanout,
	} {
		idx, err := core.New(core.Config{
			Resources: DefaultResourceConfig,
			ES:        elasticsearch.New(t.esClient, true, elasticsearch.WithFederatedExecution(mode)),
		})
		t.Require().NoErrorf(err, "mode %s", mode)

		resp, err := idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
			Query:     "widget",
			Resources: []string{"a", "b"},
		})
		t.Require().NoErrorf(err, "mode %s", mode)

		ids := map[string]bool{}
		for _, h := range resp.Hits {
			ids[h.Resource+"/"+h.ID] = true
		}
		results[mode] = outcome{ids: ids, counts: resp.Counts, total: resp.Total}

		// Page through with size 1: the pages must be disjoint and cover
		// exactly the unpaged set (deterministic ranking, no boundary drift).
		seen := map[string]bool{}
		for page := int32(0); ; page++ {
			pr, err := idx.FederatedSearch(t.T().Context(), core.FederatedSearchRequest{
				Query:     "widget",
				Resources: []string{"a", "b"},
				Page:      page,
				PageSize:  1,
			})
			t.Require().NoErrorf(err, "mode %s page %d", mode, page)
			if len(pr.Hits) == 0 {
				break
			}
			t.Require().Lenf(pr.Hits, 1, "mode %s page %d", mode, page)
			key := pr.Hits[0].Resource + "/" + pr.Hits[0].ID
			t.Require().Falsef(seen[key], "mode %s: hit %s appeared on two pages", mode, key)
			seen[key] = true
		}
		t.Require().Equalf(ids, seen, "mode %s: paged stream diverges from unpaged set", mode)
	}

	base := results[elasticsearch.FederatedSingleDFS]
	t.Require().EqualValues(3, base.total)
	t.Require().Equal([]core.ResourceCount{{Resource: "a", Count: 2}, {Resource: "b", Count: 1}}, base.counts)
	for mode, got := range results {
		t.Require().Equalf(base.ids, got.ids, "mode %s membership", mode)
		t.Require().Equalf(base.counts, got.counts, "mode %s counts", mode)
		t.Require().Equalf(base.total, got.total, "mode %s total", mode)
	}
}
