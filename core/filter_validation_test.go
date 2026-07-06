package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

// newTypedIndexer builds an Indexer with one "doc" resource covering the
// validation surface: typed root fields, a denormalized many-relation and a
// reference relation.
func newTypedIndexer(backend SearchBackend) *Indexer {
	docCfg := &resource.Config{
		Resource: "doc",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{
				{Name: "name"},               // keyword
				{Name: "size", Type: "long"}, // numeric
				{Name: "body", Type: "text"}, // fulltext: no filter ops
				{Name: "child_id"},           // join key for the reference relation
			},
			Relations: []resource.RelationConfig{
				{
					Resource:    "tag",
					Cardinality: "many",
					Join:        resource.JoinConfig{Local: "id", Foreign: "doc_id"},
					Fields:      []resource.FieldConfig{{Name: "label"}},
				},
				{
					Resource:    "child",
					Strategy:    resource.StrategyReference,
					Cardinality: "many",
					Join:        resource.JoinConfig{Local: "child_id", Foreign: "id"},
					Fields:      []resource.FieldConfig{{Name: "state"}},
				},
			},
		}},
	}
	childCfg := &resource.Config{
		Resource: "child",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "state"}},
		}},
	}
	docCfg.ApplyDefaults()
	childCfg.ApplyDefaults()
	return New(Config{
		Resources: resource.Configs{docCfg, childCfg},
		ES:        backend,
	})
}

func searchWithFilter(t *testing.T, f Filter) error {
	t.Helper()
	backend := &recordingBackend{}
	idx := newTypedIndexer(backend)
	_, err := idx.Search(context.Background(), SearchRequest{Resource: "doc", Filters: []Filter{f}})
	return err
}

func requireInvalidArgument(t *testing.T, err error, fragment string) {
	t.Helper()
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error %q should mention %q", err.Error(), fragment)
	}
}

func TestValidateFilters_ValidPass(t *testing.T) {
	valid := []Filter{
		{Field: "fields.name", Op: FilterOpEq, Value: "x"},
		{Field: "fields.name", Op: FilterOpPrefix, Value: "x"},
		{Field: "fields.size", Op: FilterOpGte, Value: "10"},
		{Field: "fields.size", Op: FilterOpLt, Value: "100"},
		{Field: "fields.name", Op: FilterOpNotIn, Values: []string{"a", "b"}},
		{Field: "fields.name", Op: FilterOpExists},
		{Field: "tag.label", Op: FilterOpNeq, Value: "x"},   // denormalized relation: negation OK
		{Field: "child.state", Op: FilterOpEq, Value: "on"}, // reference relation: positive OK
		{Field: "", Op: FilterOpEq},                         // empty field is skipped everywhere
	}
	for _, f := range valid {
		if err := searchWithFilter(t, f); err != nil {
			t.Errorf("filter %+v: unexpected error %v", f, err)
		}
	}
}

func TestValidateFilters_UnknownField(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.nope", Op: FilterOpEq, Value: "x"}), "fields.nope")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "nope.f", Op: FilterOpEq, Value: "x"}), "nope.f")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "bare", Op: FilterOpEq, Value: "x"}), "bare")
}

func TestValidateFilters_OpNotAllowedForType(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.size", Op: FilterOpPrefix, Value: "1"}), "prefix")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpGt, Value: "a"}), "gt")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.body", Op: FilterOpEq, Value: "a"}), "eq")
}

func TestValidateFilters_NegationOnReferenceField(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "child.state", Op: FilterOpNeq, Value: "on"}), "neq")
}

func TestValidateFilters_OperandShape(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpEq}), "requires value")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpIn}), "requires values")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpEq, Value: "a", Values: []string{"b"}}), "not values")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpExists, Value: "a"}), "takes no value")
}

// Middleware-appended filters are trusted code and bypass validation; this is
// the documented boundary that lets consumers scope by internal fields.
func TestValidateFilters_MiddlewareFiltersExempt(t *testing.T) {
	backend := &recordingBackend{}
	idx := newTypedIndexer(backend)
	mw := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.Filters = append(req.Filters, Filter{Field: "not_a_field", Op: FilterOpEq, Value: "x"})
			return next(ctx, req)
		}
	}
	idx2 := New(Config{
		Resources:         idx.resources,
		ES:                backend,
		SearchMiddlewares: []SearchMiddleware{mw},
	})
	if _, err := idx2.Search(context.Background(), SearchRequest{Resource: "doc"}); err != nil {
		t.Fatalf("middleware-added filter must bypass validation, got %v", err)
	}
	if !backend.called {
		t.Fatal("backend should have been called")
	}
}

func TestFederatedSearch_InvalidGlobalFilter(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "x",
		Resources: []string{"product"},
		Filters:   []Filter{{Field: "fields.unknown", Op: FilterOpEq, Value: "v"}},
	})
	requireInvalidArgument(t, err, "product")
	if backend.fedCalled {
		t.Error("backend must not be called for an invalid federated filter")
	}
}
