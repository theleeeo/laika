package elasticsearch

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"testing"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// captureSearch executes Search and returns the decoded JSON body that was
// sent to Elasticsearch, together with the returned response / error.
func captureSearch(t *testing.T, req core.SearchRequest, vc *resource.VersionConfig) (body map[string]any, _ core.SearchResponse, _ error) {
	t.Helper()
	var captured map[string]any

	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(raw, &captured); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}

		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				`{"hits":{"total":{"value":0},"hits":[]}}`,
			)),
			Header: headers,
		}, nil
	})

	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	c := New(esClient, false)
	resp, callErr := c.Search(context.Background(), req, "idx", vc)
	return captured, resp, callErr
}

// vcFlatOnly returns a VersionConfig with two flat keyword fields and no relations.
func vcFlatOnly() *resource.VersionConfig {
	return &resource.VersionConfig{
		Version: 1,
		Fields: []resource.FieldConfig{
			{Name: "name"},
			{Name: "status"},
		},
	}
}

// vcWithNestedRelation returns a VersionConfig that has a flat field and one
// many-cardinality (nested) relation with a searchable field.
func vcWithNestedRelation() *resource.VersionConfig {
	return &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "title"}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "tags",
				Cardinality: "many",
				Key:         resource.KeyConfig{Source: "root", Field: "id"},
				Fields:      []resource.FieldConfig{{Name: "label"}},
			},
		},
	}
}

// vcWithObjectRelation returns a VersionConfig that has one one-cardinality
// (object-mapped) relation with a searchable field.
func vcWithObjectRelation() *resource.VersionConfig {
	return &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "title"}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "owner",
				Cardinality: "one",
				Key:         resource.KeyConfig{Source: "root", Field: "id"},
				Fields:      []resource.FieldConfig{{Name: "email"}},
			},
		},
	}
}

// getPath is a small helper to dig into a nested map by dot-separated key parts.
func getPath(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}

// ---- query body construction tests ----

func TestSearch_NoQuery_EmptyMustAndFilter(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{PageSize: 10}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	if len(must) != 0 {
		t.Errorf("expected empty must, got %v", must)
	}

	filter := getPath(body, "query", "bool", "filter").([]any)
	if len(filter) != 0 {
		t.Errorf("expected empty filter, got %v", filter)
	}
}

func TestSearch_FlatFieldsOnly_MultiMatch(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "hello",
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	if len(must) != 1 {
		t.Fatalf("expected 1 must clause, got %d", len(must))
	}

	mm := getPath(must[0].(map[string]any), "multi_match").(map[string]any)
	if mm["query"] != "hello" {
		t.Errorf("expected query=hello, got %v", mm["query"])
	}

	fields := mm["fields"].([]any)
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d: %v", len(fields), fields)
	}
	for _, f := range fields {
		if !strings.HasPrefix(f.(string), "fields.") {
			t.Errorf("flat field should be prefixed with 'fields.', got %q", f)
		}
	}
}

// Nested relation (IsMany=true) must produce a nested query wrapper.
func TestSearch_NestedRelation_WrappedInNestedQuery(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "golang",
	}, vcWithNestedRelation())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	if len(must) != 1 {
		t.Fatalf("expected 1 must clause, got %d", len(must))
	}

	// Two relations + flat → should wrapper
	should := getPath(must[0].(map[string]any), "bool", "should").([]any)
	if len(should) != 2 {
		t.Fatalf("expected 2 should clauses (flat + nested), got %d", len(should))
	}

	// Find the nested clause
	var nestedClause map[string]any
	for _, s := range should {
		m := s.(map[string]any)
		if _, ok := m["nested"]; ok {
			nestedClause = m
		}
	}
	if nestedClause == nil {
		t.Fatal("expected a nested clause, found none")
	}

	nested := nestedClause["nested"].(map[string]any)
	if nested["path"] != "tags" {
		t.Errorf("expected path=tags, got %v", nested["path"])
	}

	innerFields := getPath(nested, "query", "multi_match", "fields").([]any)
	if len(innerFields) != 1 || innerFields[0] != "tags.label" {
		t.Errorf("expected fields=[tags.label], got %v", innerFields)
	}
}

// Object relation (IsMany=false / cardinality=one) must NOT be wrapped in nested.
func TestSearch_ObjectRelation_NotWrappedInNested(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "alice",
	}, vcWithObjectRelation())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	should := getPath(must[0].(map[string]any), "bool", "should").([]any)

	for _, s := range should {
		if _, ok := s.(map[string]any)["nested"]; ok {
			t.Error("object relation should not produce a nested query wrapper")
		}
	}

	// one of the should clauses must target owner.email
	found := false
	for _, s := range should {
		fields, _ := getPath(s.(map[string]any), "multi_match", "fields").([]any)
		for _, f := range fields {
			if f == "owner.email" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected owner.email in a multi_match fields list")
	}
}

// A field with search=false must not appear in the query.
func TestSearch_SearchDisabledField_ExcludedFromQuery(t *testing.T) {
	vc := &resource.VersionConfig{
		Version: 1,
		Fields: []resource.FieldConfig{
			{Name: "visible"},
			{Name: "hidden", Query: resource.QueryConfig{Search: new(false)}},
		},
	}

	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "x",
	}, vc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	mm := getPath(must[0].(map[string]any), "multi_match").(map[string]any)
	fields := mm["fields"].([]any)

	for _, f := range fields {
		if f.(string) == "fields.hidden" {
			t.Error("search-disabled field 'hidden' should not appear in multi_match fields")
		}
	}
	if len(fields) != 1 || fields[0] != "fields.visible" {
		t.Errorf("expected only fields.visible, got %v", fields)
	}
}

// A nested relation whose only field has search=false should produce no nested clause.
func TestSearch_NestedRelation_AllFieldsSearchDisabled_NoClause(t *testing.T) {
	vc := &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "title"}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "meta",
				Cardinality: "many",
				Key:         resource.KeyConfig{Source: "root", Field: "id"},
				Fields: []resource.FieldConfig{
					{Name: "internal", Query: resource.QueryConfig{Search: new(false)}},
				},
			},
		},
	}

	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "x",
	}, vc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	// Only one clause — the flat multi_match — no should wrapper needed.
	if _, ok := must[0].(map[string]any)["multi_match"]; !ok {
		t.Errorf("expected a plain multi_match when nested relation has no searchable fields, got %v", must[0])
	}
	if _, ok := must[0].(map[string]any)["nested"]; ok {
		t.Error("unexpected nested clause when relation has no searchable fields")
	}
}

// ---- filter tests ----

func TestSearch_Filter_EQ(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpEq, Value: "active"},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	if len(filters) != 1 {
		t.Fatalf("expected 1 filter clause, got %d", len(filters))
	}

	term := getPath(filters[0].(map[string]any), "term", "fields.status")
	if term != "active" {
		t.Errorf("expected term value=active, got %v", term)
	}
}

func TestSearch_Filter_IN(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpIn, Values: []string{"a", "b", "c"}},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	if len(filters) != 1 {
		t.Fatalf("expected 1 filter clause, got %d", len(filters))
	}

	terms := getPath(filters[0].(map[string]any), "terms", "fields.status").([]any)
	if len(terms) != 3 {
		t.Errorf("expected 3 IN values, got %v", terms)
	}
}

func TestSearch_Filter_Nested(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{
				Field:      "tags.label",
				Op:         core.FilterOpEq,
				Value:      "go",
				NestedPath: "tags",
			},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	nested := filters[0].(map[string]any)["nested"].(map[string]any)

	if nested["path"] != "tags" {
		t.Errorf("expected nested path=tags, got %v", nested["path"])
	}
	innerTerm := getPath(nested, "query", "term", "tags.label")
	if innerTerm != "go" {
		t.Errorf("expected inner term=go, got %v", innerTerm)
	}
}

func TestSearch_Filter_EQ_MissingValue_Error(t *testing.T) {
	_, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpEq, Value: ""},
		},
	}, vcFlatOnly())
	if err == nil {
		t.Fatal("expected error for EQ filter with empty value")
	}
}

func TestSearch_Filter_IN_NoValues_Error(t *testing.T) {
	_, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpIn, Values: nil},
		},
	}, vcFlatOnly())
	if err == nil {
		t.Fatal("expected error for IN filter with no values")
	}
}

func TestSearch_Filter_UnknownOp_Error(t *testing.T) {
	_, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOp(99), Value: "x"},
		},
	}, vcFlatOnly())
	if err == nil {
		t.Fatal("expected error for unknown filter op")
	}
}

// Nil or empty-field filters should be silently skipped.
func TestSearch_Filter_NilOrEmptyField_Skipped(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			core.Filter{},
			{Field: "", Op: core.FilterOpEq, Value: "x"},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	if len(filters) != 0 {
		t.Errorf("expected nil/empty filters to be skipped, got %v", filters)
	}
}

// Multiple filters should all appear in the filter array.
func TestSearch_MultipleFilters(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpEq, Value: "active"},
			{Field: "fields.name", Op: core.FilterOpIn, Values: []string{"alice", "bob"}},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	if len(filters) != 2 {
		t.Errorf("expected 2 filter clauses, got %d", len(filters))
	}
}

// Query + filter must both be present simultaneously.
func TestSearch_QueryAndFilter_Both(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Query:    "hello",
		Filters: []core.Filter{
			{Field: "fields.status", Op: core.FilterOpEq, Value: "active"},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	must := getPath(body, "query", "bool", "must").([]any)
	if len(must) != 1 {
		t.Errorf("expected 1 must clause, got %d", len(must))
	}

	filters := getPath(body, "query", "bool", "filter").([]any)
	if len(filters) != 1 {
		t.Errorf("expected 1 filter clause, got %d", len(filters))
	}
}

// ---- sort / pagination tests ----

func TestSearch_Sort_AscAndDesc(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Sort: []core.SortOption{
			{Field: "fields.name", Desc: false},
			{Field: "fields.status", Desc: true},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sortRaw, ok := body["sort"]
	if !ok {
		t.Fatal("expected sort key in body")
	}
	sorts := sortRaw.([]any)
	if len(sorts) != 2 {
		t.Fatalf("expected 2 sort clauses, got %d", len(sorts))
	}

	first := sorts[0].(map[string]any)["fields.name"].(map[string]any)
	if first["order"] != "asc" {
		t.Errorf("expected order=asc, got %v", first["order"])
	}

	second := sorts[1].(map[string]any)["fields.status"].(map[string]any)
	if second["order"] != "desc" {
		t.Errorf("expected order=desc, got %v", second["order"])
	}
}

func TestSearch_Sort_EmptyField_Skipped(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		PageSize: 10,
		Sort: []core.SortOption{
			{Field: ""},
		},
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := body["sort"]; ok {
		t.Error("expected no sort key when all sort fields are empty/nil")
	}
}

func TestSearch_Pagination_FromAndSize(t *testing.T) {
	body, _, err := captureSearch(t, core.SearchRequest{
		Page:     3,
		PageSize: 5,
	}, vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if body["from"] != float64(15) { // page=3, size=5 → from=15
		t.Errorf("expected from=15, got %v", body["from"])
	}
	if body["size"] != float64(5) {
		t.Errorf("expected size=5, got %v", body["size"])
	}
}

// ---- response handling tests ----

func TestSearch_404_ReturnsEmpty(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{"error":"index_not_found_exception"}`)),
			Header:     headers,
		}, nil
	})
	esClient, _ := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	c := New(esClient, false)

	resp, err := c.Search(context.Background(), core.SearchRequest{PageSize: 10}, "missing_idx", vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Total != 0 || len(resp.Hits) != 0 {
		t.Errorf("expected empty response for 404, got %+v", resp)
	}
}

func TestSearch_ErrorResponse_ReturnsError(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader(`{"error":"oops"}`)),
			Header:     headers,
		}, nil
	})
	esClient, _ := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	c := New(esClient, false)

	_, err := c.Search(context.Background(), core.SearchRequest{PageSize: 10}, "idx", vcFlatOnly())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestSearch_HitsDecoded(t *testing.T) {
	payload := `{"hits":{"total":{"value":1},"hits":[{"_id":"42","_score":1.5,"_source":{"fields":{"name":"alice"}}}]}}`
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(payload)),
			Header:     headers,
		}, nil
	})
	esClient, _ := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	c := New(esClient, false)

	resp, err := c.Search(context.Background(), core.SearchRequest{PageSize: 10}, "idx", vcFlatOnly())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Total != 1 {
		t.Errorf("expected total=1, got %d", resp.Total)
	}
	if len(resp.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(resp.Hits))
	}
	hit := resp.Hits[0]
	if hit.ID != "42" {
		t.Errorf("expected id=42, got %q", hit.ID)
	}
	if hit.Score != 1.5 {
		t.Errorf("expected score=1.5, got %f", hit.Score)
	}
	fields, _ := hit.Source["fields"].(map[string]any)
	if v, _ := fields["name"].(string); v != "alice" {
		t.Errorf("expected name=alice, got %q", v)
	}
}
