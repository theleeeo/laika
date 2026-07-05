package core

import (
	"context"
	"errors"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

// newFederatedIndexer builds an Indexer with the given resource types (each a
// single-version config with one text field) and search middlewares.
func newFederatedIndexer(backend SearchBackend, resources []string, mws ...SearchMiddleware) *Indexer {
	cfgs := make(resource.Configs, 0, len(resources))
	for _, name := range resources {
		cfg := &resource.Config{
			Resource: name,
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{{Name: "name", Type: "text"}}},
			},
		}
		cfg.ApplyDefaults()
		cfgs = append(cfgs, cfg)
	}
	return New(Config{
		Resources:         cfgs,
		ES:                backend,
		SearchMiddlewares: mws,
	})
}

func TestCollectIndexFilterGroups_CapturesFilterWithAlias(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"}, appendFilter("tenant_id"))

	groups, _, err := idx.collectIndexFilterGroups(context.Background(), []string{"product"}, SearchRequest{})
	if err != nil {
		t.Fatalf("collectIndexFilterGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	g := groups[0]
	if g.Resource != "product" {
		t.Errorf("Resource = %q, want %q", g.Resource, "product")
	}
	if g.Alias != AliasName("product") {
		t.Errorf("Alias = %q, want %q", g.Alias, AliasName("product"))
	}
	if len(g.Filters) != 1 || g.Filters[0].Field != "tenant_id" {
		t.Fatalf("expected collected filter on tenant_id, got %+v", g.Filters)
	}
	if backend.called {
		t.Error("backend must not be called in collect-only mode")
	}
}

func TestCollectIndexFilterGroups_PerTypeFiltersIndependent(t *testing.T) {
	backend := &recordingBackend{}
	// A middleware that tags each Type with a filter naming that Type, so we can
	// prove filters are collected per-Type and not shared across Types.
	perType := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.Filters = append(req.Filters, Filter{Field: req.Resource + "_scope", Op: FilterOpEq, Value: "x"})
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexer(backend, []string{"product", "order"}, perType)

	groups, _, err := idx.collectIndexFilterGroups(context.Background(), []string{"product", "order"}, SearchRequest{})
	if err != nil {
		t.Fatalf("collectIndexFilterGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	for _, g := range groups {
		if len(g.Filters) != 1 {
			t.Fatalf("%s: expected 1 filter, got %+v", g.Resource, g.Filters)
		}
		if want := g.Resource + "_scope"; g.Filters[0].Field != want {
			t.Errorf("%s: filter field = %q, want %q (filters must be scoped per-Type)", g.Resource, g.Filters[0].Field, want)
		}
		if g.Alias != AliasName(g.Resource) {
			t.Errorf("%s: alias = %q, want %q", g.Resource, g.Alias, AliasName(g.Resource))
		}
	}
}

func TestCollectIndexFilterGroups_DeniedTypeFailsClosed(t *testing.T) {
	backend := &recordingBackend{}
	denied := errors.New("permission denied")
	deny := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			if req.Resource == "order" {
				return SearchResponse{}, denied
			}
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexer(backend, []string{"product", "order"}, deny)

	groups, _, err := idx.collectIndexFilterGroups(context.Background(), []string{"product", "order"}, SearchRequest{})
	if !errors.Is(err, denied) {
		t.Fatalf("expected denied error to propagate, got %v", err)
	}
	if groups != nil {
		t.Errorf("expected no groups on denial, got %+v", groups)
	}
	if backend.called {
		t.Error("backend must not be called when a Type fails closed")
	}
}

func TestFederatedSearch_EmptyResources_InvalidArgument(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"})

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
	idx := newFederatedIndexer(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{Query: "x", Resources: []string{"nope"}})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}

func TestFederatedSearch_WiresCollectedGroupsScopeAndPaging(t *testing.T) {
	backend := &recordingBackend{}
	setScope := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.SecondaryScope = "tenant-7"
			req.Filters = append(req.Filters, Filter{Field: "fields.tenant_id", Op: FilterOpEq, Value: "tenant-7"})
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexer(backend, []string{"product", "order"}, setScope)

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
	idx := newFederatedIndexer(backend, []string{"product", "order", "invoice"})

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

func TestCollectIndexFilterGroups_HarvestsSecondaryScope(t *testing.T) {
	backend := &recordingBackend{}
	// A middleware that sets the caller's tenant scope value on the request —
	// the dedicated channel Federated Search harvests for the secondary tier.
	setScope := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.SecondaryScope = "tenant-42"
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexer(backend, []string{"product", "order"}, setScope)

	_, scope, err := idx.collectIndexFilterGroups(context.Background(), []string{"product", "order"}, SearchRequest{})
	if err != nil {
		t.Fatalf("collectIndexFilterGroups: %v", err)
	}
	if scope != "tenant-42" {
		t.Errorf("harvested scope = %q, want %q", scope, "tenant-42")
	}
}

func TestCollectIndexFilterGroups_NoScopeChannel_Empty(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"}, appendFilter("tenant_id"))

	_, scope, err := idx.collectIndexFilterGroups(context.Background(), []string{"product"}, SearchRequest{})
	if err != nil {
		t.Fatalf("collectIndexFilterGroups: %v", err)
	}
	if scope != "" {
		t.Errorf("expected empty scope when no middleware sets it, got %q", scope)
	}
}

func TestCollectIndexFilterGroups_UnknownResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"})

	_, _, err := idx.collectIndexFilterGroups(context.Background(), []string{"nope"}, SearchRequest{})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}
