package core

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

// newNestedPathIndexer builds an Indexer whose single "a" resource has the two
// denormalized relation shapes the middleware must distinguish: a
// many-cardinality relation ("c", ES nested → needs a path) and a
// one-cardinality relation ("b", ES object → no path). No middlewares are
// registered beyond the built-in chain, so Search runs deriveNestedPath then
// referenceResolve.
func newNestedPathIndexer(backend SearchBackend) *Indexer {
	cfg := &resource.Config{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields:  []resource.FieldConfig{{Name: "searchField"}},
				Relations: []resource.RelationConfig{
					{
						Resource:    "c",
						Cardinality: "many",
						Join:        resource.JoinConfig{Local: "id", Foreign: "a_id"},
						Fields:      []resource.FieldConfig{{Name: "number", Type: "integer"}},
					},
					{
						Resource:    "b",
						Cardinality: "one",
						Join:        resource.JoinConfig{Local: "b_id", Foreign: "id"},
						Fields:      []resource.FieldConfig{{Name: "name"}},
					},
				},
			},
		},
	}
	cfg.ApplyDefaults()
	return New(Config{Resources: resource.Configs{cfg}, ES: backend})
}

// nestedPathOf returns the NestedPath the backend received for the filter on
// the given field, or ("", false) if no such filter reached the backend.
func nestedPathOf(req SearchRequest, field string) (string, bool) {
	for _, f := range req.Filters {
		if f.Field == field {
			return f.NestedPath, true
		}
	}
	return "", false
}

func TestDeriveNestedPath_DenormalizedMany(t *testing.T) {
	backend := &recordingBackend{}
	idx := newNestedPathIndexer(backend)

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "a",
		Filters:  []Filter{{Field: "c.number", Op: FilterOpEq, Value: "7"}},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	got, ok := nestedPathOf(backend.gotReq, "c.number")
	if !ok {
		t.Fatal("filter on c.number did not reach the backend")
	}
	if got != "c" {
		t.Fatalf("expected NestedPath=c for denormalized-many field, got %q", got)
	}
}

func TestDeriveNestedPath_DenormalizedOne(t *testing.T) {
	backend := &recordingBackend{}
	idx := newNestedPathIndexer(backend)

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "a",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	got, ok := nestedPathOf(backend.gotReq, "b.name")
	if !ok {
		t.Fatal("filter on b.name did not reach the backend")
	}
	if got != "" {
		t.Fatalf("expected empty NestedPath for denormalized-one (object) field, got %q", got)
	}
}

func TestDeriveNestedPath_RootField(t *testing.T) {
	backend := &recordingBackend{}
	idx := newNestedPathIndexer(backend)

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "a",
		Filters:  []Filter{{Field: "fields.searchField", Op: FilterOpEq, Value: "x"}},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	got, ok := nestedPathOf(backend.gotReq, "fields.searchField")
	if !ok {
		t.Fatal("filter on fields.searchField did not reach the backend")
	}
	if got != "" {
		t.Fatalf("expected empty NestedPath for root field, got %q", got)
	}
}

func TestDeriveNestedPath_PreservesExplicit(t *testing.T) {
	backend := &recordingBackend{}
	idx := newNestedPathIndexer(backend)

	// "b" is denormalized-one (would derive ""), but an explicit path must win.
	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "a",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme", NestedPath: "explicit"}},
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	got, ok := nestedPathOf(backend.gotReq, "b.name")
	if !ok {
		t.Fatal("filter on b.name did not reach the backend")
	}
	if got != "explicit" {
		t.Fatalf("expected caller-supplied NestedPath to be preserved, got %q", got)
	}
}
