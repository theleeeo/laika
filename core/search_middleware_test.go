package core

import (
	"context"
	"errors"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

// recordingBackend is a fake SearchBackend that records the SearchRequest it
// receives and returns a canned SearchResponse. Only Search is exercised; the
// write methods exist to satisfy the interface.
type recordingBackend struct {
	called   bool
	gotReq   SearchRequest
	response SearchResponse

	// Federated search capture + canned result.
	fedCalled   bool
	fedParams   FederatedSearchParams
	fedResponse FederatedSearchResult
}

func (b *recordingBackend) Upsert(ctx context.Context, index, docID string, doc any, version int64) error {
	return nil
}
func (b *recordingBackend) BulkUpsert(ctx context.Context, items []BulkItem) error { return nil }
func (b *recordingBackend) Delete(ctx context.Context, index, docID string) error  { return nil }
func (b *recordingBackend) Search(ctx context.Context, req SearchRequest, indexAlias string, vc *resource.VersionConfig) (SearchResponse, error) {
	b.called = true
	b.gotReq = req
	return b.response, nil
}
func (b *recordingBackend) FederatedSearch(ctx context.Context, params FederatedSearchParams) (FederatedSearchResult, error) {
	b.fedCalled = true
	b.fedParams = params
	return b.fedResponse, nil
}

// newSearchIndexer builds an Indexer with a single "product" resource, the
// given backend, and the given middlewares.
func newSearchIndexer(backend SearchBackend, mws ...SearchMiddleware) *Indexer {
	cfg := &resource.Config{
		Resource: "product",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{{Name: "title", Type: "text"}}},
		},
	}
	cfg.ApplyDefaults()
	return New(Config{
		Resources:         resource.Configs{cfg},
		ES:                backend,
		SearchMiddlewares: mws,
	})
}

// appendFilter returns a middleware that appends a marker equality filter on
// the named field before calling next.
func appendFilter(field string) SearchMiddleware {
	return func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.Filters = append(req.Filters, Filter{Field: field, Op: FilterOpEq, Value: "x"})
			return next(ctx, req)
		}
	}
}

func TestSearch_MiddlewareOrdering(t *testing.T) {
	backend := &recordingBackend{}
	idx := newSearchIndexer(backend, appendFilter("A"), appendFilter("B"))

	if _, err := idx.Search(context.Background(), SearchRequest{Resource: "product"}); err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(backend.gotReq.Filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(backend.gotReq.Filters))
	}
	if backend.gotReq.Filters[0].Field != "A" || backend.gotReq.Filters[1].Field != "B" {
		t.Fatalf("expected filter order [A B], got [%s %s]",
			backend.gotReq.Filters[0].Field, backend.gotReq.Filters[1].Field)
	}
}

func TestSearch_RequestMutation(t *testing.T) {
	backend := &recordingBackend{}
	idx := newSearchIndexer(backend, appendFilter("tenant_id"))

	if _, err := idx.Search(context.Background(), SearchRequest{Resource: "product"}); err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(backend.gotReq.Filters) != 1 || backend.gotReq.Filters[0].Field != "tenant_id" {
		t.Fatalf("expected backend to receive appended tenant_id filter, got %+v", backend.gotReq.Filters)
	}
}

func TestSearch_ShortCircuit(t *testing.T) {
	backend := &recordingBackend{}
	denied := errors.New("denied")
	deny := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			return SearchResponse{}, denied
		}
	}
	idx := newSearchIndexer(backend, deny)

	_, err := idx.Search(context.Background(), SearchRequest{Resource: "product"})
	if !errors.Is(err, denied) {
		t.Fatalf("expected denied error, got %v", err)
	}
	if backend.called {
		t.Fatal("backend should not be called when a middleware short-circuits")
	}
}

func TestSearch_ResponseModification(t *testing.T) {
	backend := &recordingBackend{response: SearchResponse{Total: 1}}
	bump := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			resp, err := next(ctx, req)
			if err != nil {
				return resp, err
			}
			resp.Total += 100
			return resp, nil
		}
	}
	idx := newSearchIndexer(backend, bump)

	resp, err := idx.Search(context.Background(), SearchRequest{Resource: "product"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.Total != 101 {
		t.Fatalf("expected modified Total 101, got %d", resp.Total)
	}
}

func TestSearch_EmptyPassthrough_Normalizes(t *testing.T) {
	backend := &recordingBackend{}
	idx := newSearchIndexer(backend)

	if _, err := idx.Search(context.Background(), SearchRequest{Resource: "product", PageSize: 0}); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if backend.gotReq.PageSize != 25 {
		t.Fatalf("expected normalized PageSize 25, got %d", backend.gotReq.PageSize)
	}
}

func TestSearch_EmptyPassthrough_ValidatesResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newSearchIndexer(backend)

	if _, err := idx.Search(context.Background(), SearchRequest{Resource: ""}); err == nil {
		t.Fatal("expected error for empty resource")
	}
	if _, err := idx.Search(context.Background(), SearchRequest{Resource: "nope"}); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
	if backend.called {
		t.Fatal("backend should not be called when validation fails")
	}
}
