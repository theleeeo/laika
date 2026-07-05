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
			{Name: "name", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
			{Name: "status", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
		},
	}
}

// vcWithNestedRelation returns a VersionConfig that has a flat field and one
// many-cardinality (nested) relation with a searchable field.
func vcWithNestedRelation() *resource.VersionConfig {
	return &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "title", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "tags",
				Cardinality: "many",
				Join:        resource.JoinConfig{Local: "id", Foreign: "root_id"},
				Fields:      []resource.FieldConfig{{Name: "label", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
			},
		},
	}
}

// vcWithObjectRelation returns a VersionConfig that has one one-cardinality
// (object-mapped) relation with a searchable field.
func vcWithObjectRelation() *resource.VersionConfig {
	return &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "title", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "owner",
				Cardinality: "one",
				Join:        resource.JoinConfig{Local: "id", Foreign: "root_id"},
				Fields:      []resource.FieldConfig{{Name: "email", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
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
			{Name: "visible", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
			{Name: "hidden", Query: resource.QueryConfig{Search: resource.SearchTierNone}},
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
		Fields:  []resource.FieldConfig{{Name: "title", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
		Relations: []resource.RelationConfig{
			{
				Resource:    "meta",
				Cardinality: "many",
				Join:        resource.JoinConfig{Local: "id", Foreign: "root_id"},
				Fields: []resource.FieldConfig{
					{Name: "internal", Query: resource.QueryConfig{Search: resource.SearchTierNone}},
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

func TestBuildIndexFilterGroups_ScopesFiltersToIndex(t *testing.T) {
	groups := []core.IndexFilterGroup{
		{
			Resource: "product",
			Alias:    "product_search",
			Filters:  []core.Filter{{Field: "fields.tenant_id", Op: core.FilterOpEq, Value: "t1"}},
		},
		{
			Resource: "order",
			Alias:    "order_search",
			Filters:  nil, // unscoped Type: only the _index constraint
		},
	}

	clause, err := buildIndexFilterGroups(groups)
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}

	boolQ, ok := clause.(map[string]any)["bool"].(map[string]any)
	if !ok {
		t.Fatalf("expected bool clause, got %#v", clause)
	}
	if boolQ["minimum_should_match"] != 1 {
		t.Errorf("minimum_should_match = %v, want 1", boolQ["minimum_should_match"])
	}
	should, ok := boolQ["should"].([]any)
	if !ok || len(should) != 2 {
		t.Fatalf("expected 2 should clauses, got %#v", boolQ["should"])
	}

	// Each should-clause pins its group to its own _index alias and carries only
	// that group's filters.
	assertIndexTerm := func(leg any, wantIndex string, wantFilters int) {
		t.Helper()
		filter, ok := leg.(map[string]any)["bool"].(map[string]any)["filter"].([]any)
		if !ok {
			t.Fatalf("leg missing bool.filter: %#v", leg)
		}
		term, ok := filter[0].(map[string]any)["term"].(map[string]any)
		if !ok || term["_index"] != wantIndex {
			t.Errorf("first clause = %#v, want term _index=%q", filter[0], wantIndex)
		}
		if got := len(filter) - 1; got != wantFilters {
			t.Errorf("index %s: got %d filters, want %d", wantIndex, got, wantFilters)
		}
	}
	assertIndexTerm(should[0], "product_search", 1)
	assertIndexTerm(should[1], "order_search", 0)
}

func TestBuildSecondaryClause_CorrelatesScopeWithinEntry(t *testing.T) {
	clause := buildSecondaryClause("acme", "tenant-1")

	nested, ok := clause.(map[string]any)["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested clause, got %#v", clause)
	}
	if nested["path"] != "search_scoped" {
		t.Errorf("nested path = %v, want search_scoped", nested["path"])
	}

	// The text match and the scope term must live in the SAME nested query's
	// bool.must — that is what correlates them per entry: an entry contributes
	// its text only when its own scope[] contains the caller.
	must, ok := nested["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	if !ok || len(must) != 2 {
		t.Fatalf("expected 2 must clauses (text + scope), got %#v", nested["query"])
	}

	var sawTextMatch, sawScopeTerm bool
	for _, c := range must {
		m := c.(map[string]any)
		if mm, ok := m["multi_match"].(map[string]any); ok {
			fields, _ := mm["fields"].([]any)
			if len(fields) == 0 || fields[0] != "search_scoped.text.full^3" {
				t.Errorf("text match fields = %#v, want boosted search_scoped.text.full first", fields)
			}
			sawTextMatch = true
		}
		if term, ok := m["term"].(map[string]any); ok {
			if term["search_scoped.scope"] != "tenant-1" {
				t.Errorf("scope term = %#v, want search_scoped.scope=tenant-1", term)
			}
			sawScopeTerm = true
		}
	}
	if !sawTextMatch || !sawScopeTerm {
		t.Fatalf("expected both text match and scope term inside the nested clause (text=%v scope=%v)", sawTextMatch, sawScopeTerm)
	}
}

func TestBuildSecondaryClause_EmptyScopeUnscoped(t *testing.T) {
	clause := buildSecondaryClause("acme", "")

	must := clause.(map[string]any)["nested"].(map[string]any)["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	if len(must) != 1 {
		t.Fatalf("expected only the text match when scope is empty, got %#v", must)
	}
	if _, ok := must[0].(map[string]any)["multi_match"]; !ok {
		t.Errorf("expected text multi_match, got %#v", must[0])
	}
	// No scope term anywhere.
	for _, c := range must {
		if _, ok := c.(map[string]any)["term"]; ok {
			t.Errorf("unscoped secondary must carry no scope term, got %#v", c)
		}
	}
}

// captureFederated runs FederatedSearch against a mock transport, returning the
// decoded request body, the request URL (for search_type / index path), and the
// parsed result. The transport replies with a fixed two-index response so both
// the built query and the response parsing can be asserted.
func captureFederated(t *testing.T, p core.FederatedSearchParams) (body map[string]any, url string, _ core.FederatedSearchResult) {
	t.Helper()
	var captured map[string]any
	var capturedURL string

	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedURL = r.URL.String()
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
			Header:     headers,
			Body: io.NopCloser(strings.NewReader(`{
				"hits": {"total": {"value": 2}, "hits": [
					{"_index": "product_search_v1", "_id": "p1", "_score": 9.0, "_source": {"n": "x"}},
					{"_index": "order_search_v1", "_id": "o1", "_score": 4.0}
				]},
				"aggregations": {"per_index": {"buckets": [
					{"key": "product_search_v1", "doc_count": 5},
					{"key": "order_search_v1", "doc_count": 3}
				]}}
			}`)),
		}, nil
	})

	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	res, err := New(esClient, false).FederatedSearch(context.Background(), p)
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	return captured, capturedURL, res
}

func fedGroups() []core.IndexFilterGroup {
	return []core.IndexFilterGroup{
		{Resource: "product", Alias: "product_search", Filters: []core.Filter{{Field: "fields.tenant_id", Op: core.FilterOpEq, Value: "t1"}}},
		{Resource: "order", Alias: "order_search"},
	}
}

func TestFederatedSearch_QueryShapeAndSearchType(t *testing.T) {
	body, url, _ := captureFederated(t, core.FederatedSearchParams{
		Query:        "acme",
		FilterGroups: fedGroups(),
		Filters:      []core.Filter{{Field: "fields.region", Op: core.FilterOpEq, Value: "eu"}},
		Page:         0,
		PageSize:     25,
	})

	// dfs_query_then_fetch for comparable cross-index scores (D13).
	if !strings.Contains(url, "search_type=dfs_query_then_fetch") {
		t.Errorf("expected dfs_query_then_fetch in URL, got %q", url)
	}
	// Multi-index over both aliases.
	if !strings.Contains(url, "product_search") || !strings.Contains(url, "order_search") {
		t.Errorf("expected both aliases in index path, got %q", url)
	}

	boolQ := body["query"].(map[string]any)["bool"].(map[string]any)

	// must = a single should(primary, secondary) with minimum_should_match:1.
	must := boolQ["must"].([]any)
	if len(must) != 1 {
		t.Fatalf("expected 1 must clause, got %#v", must)
	}
	should := must[0].(map[string]any)["bool"].(map[string]any)
	if should["minimum_should_match"] != float64(1) {
		t.Errorf("minimum_should_match = %v, want 1", should["minimum_should_match"])
	}
	shoulds := should["should"].([]any)
	if len(shoulds) != 2 {
		t.Fatalf("expected primary + secondary should clauses, got %#v", shoulds)
	}
	primaryFields := shoulds[0].(map[string]any)["multi_match"].(map[string]any)["fields"].([]any)
	if primaryFields[0] != "search.full^9" || primaryFields[1] != "search^3" {
		t.Errorf("primary fields = %#v, want [search.full^9 search^3]", primaryFields)
	}
	if _, ok := shoulds[1].(map[string]any)["nested"]; !ok {
		t.Errorf("expected secondary nested clause, got %#v", shoulds[1])
	}

	// filter = [global region filter, per-index groups].
	filter := boolQ["filter"].([]any)
	if len(filter) != 2 {
		t.Fatalf("expected [global filter, groups], got %#v", filter)
	}
	if _, ok := filter[0].(map[string]any)["term"]; !ok {
		t.Errorf("expected global term filter first, got %#v", filter[0])
	}
	if _, ok := filter[1].(map[string]any)["bool"].(map[string]any)["should"]; !ok {
		t.Errorf("expected index-group should clause, got %#v", filter[1])
	}

	// Per-resource counts aggregation on _index.
	aggField := body["aggs"].(map[string]any)["per_index"].(map[string]any)["terms"].(map[string]any)["field"]
	if aggField != "_index" {
		t.Errorf("agg field = %v, want _index", aggField)
	}
}

func TestFederatedSearch_IncludeSourceToggle(t *testing.T) {
	off, _, _ := captureFederated(t, core.FederatedSearchParams{Query: "q", FilterGroups: fedGroups(), PageSize: 25})
	if src, ok := off["_source"]; !ok || src != false {
		t.Errorf("expected _source:false when IncludeSource is false, got %v (present=%v)", src, ok)
	}
	on, _, _ := captureFederated(t, core.FederatedSearchParams{Query: "q", FilterGroups: fedGroups(), PageSize: 25, IncludeSource: true})
	if _, ok := on["_source"]; ok {
		t.Error("expected no _source key when IncludeSource is true")
	}
}

func TestFederatedSearch_ParsesHitsAndCounts(t *testing.T) {
	_, _, res := captureFederated(t, core.FederatedSearchParams{Query: "q", FilterGroups: fedGroups(), PageSize: 25})
	if res.Total != 2 {
		t.Errorf("Total = %d, want 2", res.Total)
	}
	if len(res.Hits) != 2 || res.Hits[0].Index != "product_search_v1" || res.Hits[0].ID != "p1" {
		t.Fatalf("hits parsed wrong: %#v", res.Hits)
	}
	if res.IndexCounts["product_search_v1"] != 5 || res.IndexCounts["order_search_v1"] != 3 {
		t.Errorf("index counts = %#v, want product=5 order=3", res.IndexCounts)
	}
}

func TestFederatedSearch_EmptyQueryOmitsMust(t *testing.T) {
	body, _, _ := captureFederated(t, core.FederatedSearchParams{Query: "", FilterGroups: fedGroups(), PageSize: 25})
	boolQ := body["query"].(map[string]any)["bool"].(map[string]any)
	if _, ok := boolQ["must"]; ok {
		t.Errorf("expected no must clause for empty query (filter-only browse), got %#v", boolQ["must"])
	}
	if _, ok := boolQ["filter"]; !ok {
		t.Error("expected filter groups even with empty query")
	}
}
