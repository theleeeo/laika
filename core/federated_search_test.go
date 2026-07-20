package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

func TestFederatedSearch_EmptyResources_InvalidArgument(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{Query: "x"})
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError for empty resource set, got %v", err)
	}
	if backend.fedCalled {
		t.Error("backend must not be called for an invalid request")
	}
}

func TestFederatedSearch_UnknownResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{Query: "x", Resources: []string{"nope"}})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}

func TestFederatedSearch_WiresGroupsScopeAndPaging(t *testing.T) {
	backend := &recordingBackend{}
	// A federated middleware scopes each Type and sets the caller's secondary
	// scope — the per-Type channel that replaced collect mode.
	scope := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			req.SecondaryScope = "tenant-7"
			req.ResourceFilters = make(map[string][]Filter, len(req.Resources))
			for _, r := range req.Resources {
				req.ResourceFilters[r] = []Filter{{Field: "fields.tenant_id", Op: FilterOpEq, Value: "tenant-7"}}
			}
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"}, scope)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"product", "order"},
		Filters:   []Filter{{Field: "fields.region", Op: FilterOpEq, Value: "eu"}},
		PageSize:  0, // exercises normalization
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if !backend.fedCalled {
		t.Fatal("expected backend.FederatedSearch to be called")
	}
	p := backend.fedParams
	if p.SecondaryScope != "tenant-7" {
		t.Errorf("SecondaryScope = %q, want tenant-7", p.SecondaryScope)
	}
	if len(p.FilterGroups) != 2 {
		t.Fatalf("expected 2 collected filter groups, got %d", len(p.FilterGroups))
	}
	for _, g := range p.FilterGroups {
		if g.Alias != AliasName(g.Resource) {
			t.Errorf("group %s alias = %q, want %q", g.Resource, g.Alias, AliasName(g.Resource))
		}
		if len(g.Filters) != 1 || g.Filters[0].Field != "fields.tenant_id" {
			t.Errorf("group %s: expected collected visibility filter, got %+v", g.Resource, g.Filters)
		}
	}
	if len(p.Filters) != 1 || p.Filters[0].Field != "fields.region" {
		t.Errorf("global filters not forwarded, got %+v", p.Filters)
	}
	if p.PageSize != 25 {
		t.Errorf("PageSize = %d, want normalized 25", p.PageSize)
	}
}

func TestFederatedSearch_MapsIndexToResourceAndCounts(t *testing.T) {
	backend := &recordingBackend{
		fedResponse: FederatedSearchResult{
			Total: 3,
			Hits: []FederatedRawHit{
				{Index: IndexName("product", 1), ID: "p1", Score: 9.0, Source: map[string]any{"n": "a"}},
				{Index: IndexName("order", 1), ID: "o1", Score: 4.0},
			},
			IndexCounts: map[string]int64{
				IndexName("product", 1): 2,
				IndexName("order", 1):   1,
			},
		},
	}
	idx := newFederatedIndexerMW(backend, []string{"product", "order", "invoice"})

	resp, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"product", "order", "invoice"},
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if resp.Total != 3 {
		t.Errorf("Total = %d, want 3", resp.Total)
	}
	if len(resp.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(resp.Hits))
	}
	if resp.Hits[0].Resource != "product" || resp.Hits[0].ID != "p1" {
		t.Errorf("hit[0] = %+v, want product/p1", resp.Hits[0])
	}
	if resp.Hits[1].Resource != "order" {
		t.Errorf("hit[1] resource = %q, want order", resp.Hits[1].Resource)
	}
	// One count per requested Type, in request order, zeros included.
	want := []ResourceCount{
		{Resource: "product", Count: 2},
		{Resource: "order", Count: 1},
		{Resource: "invoice", Count: 0},
	}
	if len(resp.Counts) != len(want) {
		t.Fatalf("counts = %+v, want %+v", resp.Counts, want)
	}
	for i, w := range want {
		if resp.Counts[i] != w {
			t.Errorf("count[%d] = %+v, want %+v", i, resp.Counts[i], w)
		}
	}
}

// newFederatedIndexerMW builds an Indexer with the given resource types (each
// a single-version config with one text field and one keyword field) and
// federated search middlewares.
func newFederatedIndexerMW(backend SearchBackend, resources []string, mws ...FederatedSearchMiddleware) *Indexer {
	cfgs := make(resource.Configs, 0, len(resources))
	for _, name := range resources {
		cfg := &resource.Config{
			Resource: name,
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{
					{Name: "name", Type: "text"},
					{Name: "region"},
				}},
			},
		}
		cfg.ApplyDefaults()
		cfgs = append(cfgs, cfg)
	}
	return New(Config{
		Resources:                  cfgs,
		ES:                         backend,
		FederatedSearchMiddlewares: mws,
	})
}

func TestFederatedSearch_MiddlewareOrdering(t *testing.T) {
	backend := &recordingBackend{}
	var order []string
	tag := func(name string) FederatedSearchMiddleware {
		return func(next FederatedSearchHandler) FederatedSearchHandler {
			return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
				order = append(order, name)
				return next(ctx, req)
			}
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, tag("A"), tag("B"))

	if _, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	}); err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Fatalf("expected call order [A B], got %v", order)
	}
	if !backend.fedCalled {
		t.Fatal("expected backend.FederatedSearch to be called")
	}
}

func TestFederatedSearch_MiddlewareShortCircuit(t *testing.T) {
	backend := &recordingBackend{}
	denied := errors.New("denied")
	deny := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			return FederatedSearchResponse{}, denied
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, deny)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	})
	if !errors.Is(err, denied) {
		t.Fatalf("expected denied error, got %v", err)
	}
	if backend.fedCalled {
		t.Fatal("backend must not be called when a middleware short-circuits")
	}
}

func TestFederatedSearch_MiddlewareResponseModification(t *testing.T) {
	backend := &recordingBackend{fedResponse: FederatedSearchResult{Total: 1}}
	bump := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			resp, err := next(ctx, req)
			if err != nil {
				return resp, err
			}
			resp.Total += 100
			return resp, nil
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, bump)

	resp, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if resp.Total != 101 {
		t.Fatalf("expected modified Total 101, got %d", resp.Total)
	}
}

// The chain wraps the base handler INCLUDING its validation, so an observing
// middleware sees validation failures too (spec: validation-in-base).
func TestFederatedSearch_MiddlewareObservesValidationError(t *testing.T) {
	backend := &recordingBackend{}
	var seen error
	observe := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			resp, err := next(ctx, req)
			seen = err
			return resp, err
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, observe)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{Query: "q"}) // no Resources
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError for empty resource set, got %v", err)
	}
	if !errors.As(seen, &inv) {
		t.Fatalf("middleware should observe the validation error, saw %v", seen)
	}
	if backend.fedCalled {
		t.Fatal("backend must not be called for an invalid request")
	}
}

func TestFederatedSearch_MiddlewareNarrowsResources(t *testing.T) {
	backend := &recordingBackend{}
	narrow := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			req.Resources = []string{"product"}
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"}, narrow)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product", "order"},
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if len(backend.fedParams.FilterGroups) != 1 || backend.fedParams.FilterGroups[0].Resource != "product" {
		t.Fatalf("expected only the narrowed Type's group, got %+v", backend.fedParams.FilterGroups)
	}
}

func TestFederatedSearch_RelationFieldGlobalFilterRejected(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "x",
		Resources: []string{"product"},
		Filters:   []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError for relation-field global filter, got %v", err)
	}
	if !strings.Contains(err.Error(), "root fields") {
		t.Fatalf("error should explain the root-only rule, got %q", err.Error())
	}
	if backend.fedCalled {
		t.Error("backend must not be called for an invalid federated filter")
	}
}

func TestBuildIndexFilterGroups_TagsFiltersWithAlias(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"})

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"product", "order"}, map[string][]Filter{
		"product": {{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"}},
		// "order" has no entry: it must get an unfiltered group.
	}, "")
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Resource != "product" || groups[0].Alias != AliasName("product") {
		t.Errorf("group[0] = %+v, want product with its alias", groups[0])
	}
	if len(groups[0].Filters) != 1 || groups[0].Filters[0].Field != "fields.tenant_id" {
		t.Errorf("product group filters = %+v, want the per-Type filter", groups[0].Filters)
	}
	if groups[1].Resource != "order" || len(groups[1].Filters) != 0 || groups[1].MatchNothing {
		t.Errorf("order group = %+v, want unfiltered (Type membership only)", groups[1])
	}
	if backend.called || backend.fedCalled {
		t.Error("group building alone must not hit the backend for plain filters")
	}
}

func TestBuildIndexFilterGroups_UnknownResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.buildIndexFilterGroups(context.Background(), []string{"nope"}, nil, "")
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}

// newReferenceIndexerMW mirrors the harness domain: "ap" references "pop"
// (population_id == pop.id), so a per-Type filter on pop.fiber_operator_id
// must resolve via a child search into a terms filter on fields.population_id.
func newReferenceIndexerMW(backend SearchBackend, mws ...FederatedSearchMiddleware) *Indexer {
	pop := &resource.Config{
		Resource: "pop",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "name"}, {Name: "fiber_operator_id"}},
		}},
	}
	ap := &resource.Config{
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
	}
	pop.ApplyDefaults()
	ap.ApplyDefaults()
	return New(Config{
		Resources:                  resource.Configs{pop, ap},
		ES:                         backend,
		FederatedSearchMiddlewares: mws,
	})
}

func TestBuildIndexFilterGroups_ResolvesReferenceFilter(t *testing.T) {
	backend := &recordingBackend{response: SearchResponse{
		Total: 1,
		Hits:  []SearchHit{{ID: "pop-A"}},
	}}
	idx := newReferenceIndexerMW(backend)

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"ap"}, map[string][]Filter{
		"ap": {{Field: "pop.fiber_operator_id", Op: FilterOpEq, Value: "op-A"}},
	}, "")
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if !backend.called {
		t.Fatal("expected a child search against the referenced Type")
	}
	if len(groups) != 1 || groups[0].MatchNothing {
		t.Fatalf("expected one live group, got %+v", groups)
	}
	fs := groups[0].Filters
	if len(fs) != 1 || fs[0].Field != "fields.population_id" || fs[0].Op != FilterOpIn {
		t.Fatalf("expected folded terms filter on fields.population_id, got %+v", fs)
	}
	if len(fs[0].Values) != 1 || fs[0].Values[0] != "pop-A" {
		t.Fatalf("expected folded child key [pop-A], got %v", fs[0].Values)
	}
}

func TestFederatedSearch_ScopedBlock_InjectsScopeOrMatchNothing(t *testing.T) {
	idx := &Indexer{resources: resource.Configs{{
		Resource: "population", ReadVersion: 1,
		Versions: []resource.VersionConfig{{
			Version: 1, Fields: []resource.FieldConfig{{Name: "name"}},
			NestedBlocks: []resource.NestedBlockConfig{{
				Name: "operator_data", ScopeKey: "fiber_operator_id",
				Fields: []resource.FieldConfig{{Name: "available_products"}},
			}},
		}},
	}}}

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"population"}, nil, "op-1")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.False(t, groups[0].MatchNothing)
	require.Len(t, groups[0].Filters, 1)
	require.Equal(t, "operator_data.fiber_operator_id", groups[0].Filters[0].Field)
	require.Equal(t, "operator_data", groups[0].Filters[0].NestedPath)
	require.Equal(t, "op-1", groups[0].Filters[0].Value)

	empty, err := idx.buildIndexFilterGroups(context.Background(), []string{"population"}, nil, "")
	require.NoError(t, err)
	require.True(t, empty[0].MatchNothing, "empty scope must fail closed on the federated path")
}

func TestBuildIndexFilterGroups_ReferenceZeroChildrenMatchNothing(t *testing.T) {
	backend := &recordingBackend{} // child search returns no hits
	idx := newReferenceIndexerMW(backend)

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"ap", "pop"}, map[string][]Filter{
		"ap": {{Field: "pop.fiber_operator_id", Op: FilterOpEq, Value: "op-none"}},
	}, "")
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if !groups[0].MatchNothing {
		t.Errorf("ap group should be MatchNothing when the reference matches no children, got %+v", groups[0])
	}
	if groups[1].MatchNothing || groups[1].Resource != "pop" {
		t.Errorf("pop group must stay live, got %+v", groups[1])
	}
}
