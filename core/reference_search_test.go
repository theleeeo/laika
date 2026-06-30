package core

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

type fakeBackend struct {
	childHits  map[string][]SearchHit // alias -> hits
	gotPrimary SearchRequest
	calls      int
}

func (f *fakeBackend) Upsert(context.Context, string, string, any, int64) error { return nil }
func (f *fakeBackend) BulkUpsert(context.Context, []BulkItem) error             { return nil }
func (f *fakeBackend) Delete(context.Context, string, string) error             { return nil }
func (f *fakeBackend) Search(_ context.Context, req SearchRequest, alias string, _ *resource.VersionConfig) (SearchResponse, error) {
	f.calls++
	if hits, ok := f.childHits[alias]; ok {
		return SearchResponse{Total: int64(len(hits)), Hits: hits}, nil
	}
	f.gotPrimary = req // primary search (alias not in childHits)
	return SearchResponse{Total: 1, Hits: []SearchHit{{ID: "c1"}}}, nil
}

func TestReferenceExecuteFoldsTermsFilter(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{
		AliasName("b"): {{ID: "b1"}, {ID: "b2"}},
	}}
	idx := New(Config{Resources: refResources(), ES: be})

	resp, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 1 || len(resp.Hits) != 1 || resp.Hits[0].ID != "c1" {
		t.Fatalf("unexpected primary response: %+v", resp)
	}
	// primary must carry the folded terms filter on the parent key path
	var found *Filter
	for i := range be.gotPrimary.Filters {
		if be.gotPrimary.Filters[i].Field == "a.b_id" {
			found = &be.gotPrimary.Filters[i]
		}
	}
	if found == nil {
		t.Fatalf("expected folded terms filter on a.b_id, got %+v", be.gotPrimary.Filters)
	}
	if found.Op != FilterOpIn || len(found.Values) != 2 || found.NestedPath != "a" {
		t.Fatalf("bad folded filter: %+v", found)
	}
}

func TestReferenceExecuteShortCircuitsOnNoMatch(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {}}}
	idx := New(Config{Resources: refResources(), ES: be})

	resp, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "nope"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 0 || len(resp.Hits) != 0 {
		t.Fatalf("expected empty response when reference matches nothing, got %+v", resp)
	}
	// exactly one backend call (the child); primary must not have been issued
	if be.calls != 1 {
		t.Fatalf("expected exactly 1 backend call (child only), got %d", be.calls)
	}
}

// TestReferenceExecuteMultiTargetShortCircuit checks that when a request
// resource has two reference relations and the FIRST target matches nothing,
// the second child search AND the primary search are never issued.
func TestReferenceExecuteMultiTargetShortCircuit(t *testing.T) {
	// refResources2 has resource "e" with two reference relations: "b" first,
	// then "d". The first (b) returns empty hits; the second (d) and the primary
	// must therefore never be called.
	resources := refResources2()
	be := &fakeBackend{childHits: map[string][]SearchHit{
		AliasName("b"): {}, // first target: no matches → short-circuit
		AliasName("d"): {{ID: "d1"}},
	}}
	idx := New(Config{Resources: resources, ES: be})

	resp, err := idx.Search(context.Background(), SearchRequest{
		Resource: "e",
		Filters: []Filter{
			{Field: "b.name", Op: FilterOpEq, Value: "nope"},
			{Field: "d.code", Op: FilterOpEq, Value: "x"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 0 || len(resp.Hits) != 0 {
		t.Fatalf("expected empty response on multi-target short-circuit, got %+v", resp)
	}
	// exactly 1 call: only the first child ("b") was searched; "d" and primary were not
	if be.calls != 1 {
		t.Fatalf("expected exactly 1 backend call (first child only), got %d — second child or primary was issued unexpectedly", be.calls)
	}
}

// A root-field filter added by a middleware ("fields.tenant_id") scopes the
// primary search ONLY; it is never copied onto the referenced child search.
func TestRootFilterScopesPrimaryOnly(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {{ID: "b1"}}}}
	var childReq SearchRequest
	be2 := &captureChild{fakeBackend: be, onChild: func(r SearchRequest) { childReq = r }}

	tenant := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.AddFilter(Filter{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"})
			return next(ctx, req)
		}
	}
	idx := New(Config{Resources: refResources(), ES: be2, SearchMiddlewares: []SearchMiddleware{tenant}})

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c", Filters: []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFilter(be.gotPrimary.Filters, "fields.tenant_id") {
		t.Fatalf("primary search must carry the root tenant filter, got %+v", be.gotPrimary.Filters)
	}
	if hasFilter(childReq.Filters, "fields.tenant_id") {
		t.Fatalf("referenced child search must NOT carry the primary's root filter, got %+v", childReq.Filters)
	}
}

// A middleware scopes a referenced child by naming that child's field
// explicitly ("b.tenant_id"). The filter routes to the child search (rewritten
// to "fields.tenant_id") and is removed from the primary.
func TestReferenceFieldFilterScopesChildOnly(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {{ID: "b1"}}}}
	var childReq SearchRequest
	be2 := &captureChild{fakeBackend: be, onChild: func(r SearchRequest) { childReq = r }}

	tenant := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.AddFilter(Filter{Field: "b.tenant_id", Op: FilterOpEq, Value: "t1"})
			return next(ctx, req)
		}
	}
	idx := New(Config{Resources: refResources(), ES: be2, SearchMiddlewares: []SearchMiddleware{tenant}})

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c", Filters: []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFilter(childReq.Filters, "fields.tenant_id") {
		t.Fatalf("referenced child search must carry b.tenant_id rewritten to fields.tenant_id, got %+v", childReq.Filters)
	}
	if hasFilter(be.gotPrimary.Filters, "fields.tenant_id") || hasFilter(be.gotPrimary.Filters, "b.tenant_id") {
		t.Fatalf("primary search must NOT carry the reference child's filter, got %+v", be.gotPrimary.Filters)
	}
}

func hasFilter(fs []Filter, field string) bool {
	for _, f := range fs {
		if f.Field == field {
			return true
		}
	}
	return false
}

// captureChild wraps fakeBackend to capture the child request.
type captureChild struct {
	*fakeBackend
	onChild func(SearchRequest)
}

func (c *captureChild) Search(ctx context.Context, req SearchRequest, alias string, vc *resource.VersionConfig) (SearchResponse, error) {
	if _, ok := c.childHits[alias]; ok {
		c.onChild(req)
	}
	return c.fakeBackend.Search(ctx, req, alias, vc)
}

func refResources() resource.Configs {
	return resource.Configs{
		{Resource: "c", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{Resource: "a", Join: resource.JoinConfig{Local: "id", Foreign: "c_id"}, Fields: []resource.FieldConfig{{Name: "b_id"}}},
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", From: "a", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
			}}}},
		{Resource: "a", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "b_id"}}}}},
		{Resource: "b", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}}}},
	}
}

// refResources2 builds a config with resource "e" that has TWO reference
// relations — "b" first (via root field b_id → b.id), then "d" (via root field
// d_code → d.code). This is used by TestReferenceExecuteMultiTargetShortCircuit.
// Config order matters: referenceResolve executes targets in Relations config
// order, so "b" is always the first child search issued.
func refResources2() resource.Configs {
	return resource.Configs{
		{Resource: "e", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}, {Name: "d_code"}},
			Relations: []resource.RelationConfig{
				// first reference — b_id → b.id
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
				// second reference — d_code → d.code
				{Resource: "d", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "d_code", Foreign: "code"}, Fields: []resource.FieldConfig{{Name: "code"}}},
			}}}},
		{Resource: "b", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}}}},
		{Resource: "d", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "code"}}}}},
	}
}

// TestNewReferenceTarget covers the parent-key-path derivation directly: a key
// sourced from a denormalized "many" sibling lives nested at <from>.<local>.
func TestNewReferenceTarget(t *testing.T) {
	vc := refResources().Get("c").ReadVersionConfig()
	rel := vc.GetRelation("b")
	if rel == nil {
		t.Fatal("relation b not found")
	}
	tgt := newReferenceTarget(*rel, vc)
	if tgt.Resource != "b" || tgt.ForeignField != "id" {
		t.Fatalf("bad target identity: %+v", tgt)
	}
	if tgt.ParentKeyField != "a.b_id" || tgt.ParentKeyNestedPath != "a" {
		t.Fatalf("bad parent key path: field=%q nested=%q", tgt.ParentKeyField, tgt.ParentKeyNestedPath)
	}
}

// TestReferenceFilterRouting checks the resolver routes a native filter to the
// primary and a reference-relation filter (rewritten to fields.name) to the
// child — and that the native filter never leaks onto the child.
func TestReferenceFilterRouting(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {{ID: "b1"}}}}
	var childReq SearchRequest
	be2 := &captureChild{fakeBackend: be, onChild: func(r SearchRequest) { childReq = r }}
	idx := New(Config{Resources: refResources(), ES: be2})

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c",
		Filters: []Filter{
			{Field: "fields.b_id", Op: FilterOpEq, Value: "keep"}, // native field, stays primary
			{Field: "b.name", Op: FilterOpEq, Value: "acme"},      // reference, moves to child
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !hasFilter(be.gotPrimary.Filters, "fields.b_id") {
		t.Fatalf("native filter must remain on primary, got %+v", be.gotPrimary.Filters)
	}
	if hasFilter(be.gotPrimary.Filters, "b.name") {
		t.Fatalf("reference filter must not stay on primary, got %+v", be.gotPrimary.Filters)
	}
	if len(childReq.Filters) != 1 || childReq.Filters[0].Field != "fields.name" || childReq.Filters[0].Value != "acme" {
		t.Fatalf("child filter must be rewritten to fields.name, got %+v", childReq.Filters)
	}
}
