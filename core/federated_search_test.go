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

	groups, err := idx.collectIndexFilterGroups(context.Background(), []string{"product"}, SearchRequest{})
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

	groups, err := idx.collectIndexFilterGroups(context.Background(), []string{"product", "order"}, SearchRequest{})
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

	groups, err := idx.collectIndexFilterGroups(context.Background(), []string{"product", "order"}, SearchRequest{})
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

func TestCollectIndexFilterGroups_UnknownResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"})

	_, err := idx.collectIndexFilterGroups(context.Background(), []string{"nope"}, SearchRequest{})
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}
