package elasticsearch

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"strings"
	"testing"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
)

// fanoutLeg is one decoded NDJSON pair of a captured _msearch request.
type fanoutLeg struct {
	header map[string]any
	body   map[string]any
}

// captureFanout runs FederatedSearch in fan-out mode against a mock transport
// that replies with msearchBody, returning the decoded NDJSON legs, the request
// URL, and the call's result/error. An empty msearchBody fails the test if the
// transport is reached at all.
func captureFanout(t *testing.T, p core.FederatedSearchParams, msearchBody string) ([]fanoutLeg, string, core.FederatedSearchResult, error) {
	t.Helper()
	var legs []fanoutLeg
	var capturedURL string

	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if msearchBody == "" {
			t.Fatal("unexpected HTTP call")
		}
		capturedURL = r.URL.String()
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines)%2 != 0 {
			t.Fatalf("msearch body has odd line count %d: %q", len(lines), raw)
		}
		for i := 0; i < len(lines); i += 2 {
			var leg fanoutLeg
			if err := json.Unmarshal([]byte(lines[i]), &leg.header); err != nil {
				t.Fatalf("unmarshal msearch header line: %v", err)
			}
			if err := json.Unmarshal([]byte(lines[i+1]), &leg.body); err != nil {
				t.Fatalf("unmarshal msearch body line: %v", err)
			}
			legs = append(legs, leg)
		}
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader(msearchBody)),
		}, nil
	})

	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	res, callErr := New(esClient, false, WithFederatedExecution(FederatedFanout)).
		FederatedSearch(context.Background(), p)
	return legs, capturedURL, res, callErr
}

// legResponse builds one _msearch item with the given total and hits, each hit
// given as "<id>:<score>" and attributed to index (a concrete index name, as ES
// reports even when the leg queried an alias).
func legResponse(index string, total int, hits ...string) string {
	hs := make([]string, 0, len(hits))
	for _, h := range hits {
		id, score, _ := strings.Cut(h, ":")
		hs = append(hs, fmt.Sprintf(`{"_index":%q,"_id":%q,"_score":%s,"_source":{"n":%q}}`, index, id, score, id))
	}
	return fmt.Sprintf(`{"hits":{"total":{"value":%d},"hits":[%s]}}`, total, strings.Join(hs, ","))
}

func msearchResponse(items ...string) string {
	return fmt.Sprintf(`{"responses":[%s]}`, strings.Join(items, ","))
}

func TestFederatedFanout_LegShapePerType(t *testing.T) {
	legs, url, _, err := captureFanout(t, core.FederatedSearchParams{
		Query:        "acme",
		FilterGroups: fedGroups(),
		Filters:      []core.Filter{{Field: "fields.region", Op: core.FilterOpEq, Value: "eu"}},
		Page:         0,
		PageSize:     25,
	}, msearchResponse(legResponse("product_search_v1", 0), legResponse("order_search_v1", 0)))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}

	if !strings.Contains(url, "_msearch") {
		t.Errorf("expected _msearch endpoint, got %q", url)
	}
	if len(legs) != 2 {
		t.Fatalf("expected 2 legs, got %d", len(legs))
	}
	if legs[0].header["index"] != "product_search" || legs[1].header["index"] != "order_search" {
		t.Errorf("leg headers = %v / %v, want the two aliases", legs[0].header, legs[1].header)
	}

	for i, wantFilters := range []int{2, 1} { // product: global + tenant; order: global only
		body := legs[i].body
		if body["from"] != float64(0) || body["size"] != float64(25) {
			t.Errorf("leg %d paging = from %v size %v, want 0/25", i, body["from"], body["size"])
		}
		if v, ok := body["track_total_hits"]; ok {
			t.Errorf("leg %d sets track_total_hits = %v; federated counts accept the ES default cap", i, v)
		}
		if _, ok := body["aggs"]; ok {
			t.Errorf("leg %d carries aggs; counts come from hits.total", i)
		}
		if body["_source"] != false {
			t.Errorf("leg %d _source = %v, want false", i, body["_source"])
		}

		boolQ := body["query"].(map[string]any)["bool"].(map[string]any)
		filter := boolQ["filter"].([]any)
		if len(filter) != wantFilters {
			t.Errorf("leg %d: got %d filters, want %d", i, len(filter), wantFilters)
		}
		// The global filter is applied to every leg, first.
		term := filter[0].(map[string]any)["term"].(map[string]any)
		if term["fields.region"] != "eu" {
			t.Errorf("leg %d: first filter = %#v, want global region term", i, filter[0])
		}

		// The text query is the shared two-tier federated clause.
		must := boolQ["must"].([]any)
		if len(must) != 1 {
			t.Fatalf("leg %d: expected 1 must clause, got %#v", i, must)
		}
		shoulds := must[0].(map[string]any)["bool"].(map[string]any)["should"].([]any)
		if len(shoulds) != 3 {
			t.Errorf("leg %d: expected primary word + primary infix + secondary clauses, got %d", i, len(shoulds))
		}

		// No _index pinning and no cross-Type groups: that is the point. The
		// only _index a leg names is its sort's tiebreaker, which
		// TestFederatedFanout_LegSortMatchesMerge asserts exactly.
		rest := maps.Clone(body)
		delete(rest, "sort")
		raw, _ := json.Marshal(rest)
		if strings.Contains(string(raw), "_index") {
			t.Errorf("leg %d body pins _index outside its sort: %s", i, raw)
		}
	}
}

func TestFederatedFanout_IncludeSourceOmitsSourceFalse(t *testing.T) {
	legs, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), PageSize: 25, IncludeSource: true,
	}, msearchResponse(legResponse("product_search_v1", 0), legResponse("order_search_v1", 0)))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	for i, leg := range legs {
		if _, ok := leg.body["_source"]; ok {
			t.Errorf("leg %d: _source should be omitted when IncludeSource is set, got %v", i, leg.body["_source"])
		}
	}
}

func TestFederatedFanout_EmptyQueryOmitsMust(t *testing.T) {
	legs, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "", FilterGroups: fedGroups(), PageSize: 25,
	}, msearchResponse(legResponse("product_search_v1", 0), legResponse("order_search_v1", 0)))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	for i, leg := range legs {
		boolQ := leg.body["query"].(map[string]any)["bool"].(map[string]any)
		if _, ok := boolQ["must"]; ok {
			t.Errorf("leg %d: expected no must clause for empty query, got %#v", i, boolQ["must"])
		}
	}
}

func TestFederatedFanout_MergesPagesAndCounts(t *testing.T) {
	// Page 1 of size 2: every leg must over-fetch the full merged window (4).
	legs, _, res, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), Page: 1, PageSize: 2,
	}, msearchResponse(
		legResponse("product_search_v1", 3, "p1:9.0", "p2:7.0", "p3:5.0"),
		legResponse("order_search_v1", 3, "o1:8.0", "o2:6.0", "o3:4.0"),
	))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}

	for i, leg := range legs {
		if leg.body["from"] != float64(0) || leg.body["size"] != float64(4) {
			t.Errorf("leg %d window = from %v size %v, want 0/4 ((page+1)*page_size)", i, leg.body["from"], leg.body["size"])
		}
	}

	// Merged order is 9,8,7,6,5,4 → page 1 is [p2:7, o2:6].
	if len(res.Hits) != 2 || res.Hits[0].ID != "p2" || res.Hits[1].ID != "o2" {
		t.Errorf("page 1 hits = %+v, want [p2 o2]", res.Hits)
	}
	if res.Total != 6 {
		t.Errorf("Total = %d, want 6", res.Total)
	}
	// Counts are per-leg exact totals, keyed by the group's alias.
	if res.IndexCounts["product_search"] != 3 || res.IndexCounts["order_search"] != 3 {
		t.Errorf("IndexCounts = %v, want product_search:3 order_search:3", res.IndexCounts)
	}
	// Hits keep the concrete index names ES reported.
	if res.Hits[0].Index != "product_search_v1" {
		t.Errorf("hit index = %q, want concrete index name", res.Hits[0].Index)
	}
}

// Each leg sorts as the merge does — score, index, resource_id — so the last
// hit of a leg's window is the one the merge would rank there, with or
// without a text query.
func TestFederatedFanout_LegSortMatchesMerge(t *testing.T) {
	for _, query := range []string{"q", ""} {
		legs, _, _, err := captureFanout(t, core.FederatedSearchParams{
			Query: query, FilterGroups: fedGroups(), PageSize: 25,
		}, msearchResponse(legResponse("product_search_v1", 0), legResponse("order_search_v1", 0)))
		if err != nil {
			t.Fatalf("FederatedSearch: %v", err)
		}
		if len(legs) != 2 {
			t.Fatalf("expected 2 legs, got %d", len(legs))
		}
		for i, leg := range legs {
			require.Equal(t, wantFederatedSort, leg.body["sort"], "query %q leg %d", query, i)
		}
	}
}

func TestFederatedFanout_TieBreakIsDeterministic(t *testing.T) {
	_, _, res, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), PageSize: 25,
	}, msearchResponse(
		legResponse("product_search_v1", 2, "p2:5.0", "p1:5.0"),
		legResponse("order_search_v1", 1, "o1:5.0"),
	))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	// Equal scores order by (index, id): order_search_v1 < product_search_v1.
	want := []string{"o1", "p1", "p2"}
	if len(res.Hits) != 3 {
		t.Fatalf("expected 3 hits, got %+v", res.Hits)
	}
	for i, id := range want {
		if res.Hits[i].ID != id {
			t.Errorf("hit[%d] = %q, want %q", i, res.Hits[i].ID, id)
		}
	}
}

func TestFederatedFanout_SkipsMatchNothingGroups(t *testing.T) {
	groups := []core.IndexFilterGroup{
		{Resource: "product", Alias: "product_search"},
		{Resource: "order", Alias: "order_search", MatchNothing: true},
	}
	legs, _, res, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: groups, PageSize: 25,
	}, msearchResponse(legResponse("product_search_v1", 1, "p1:2.0")))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if len(legs) != 1 || legs[0].header["index"] != "product_search" {
		t.Fatalf("expected only the product leg, got %+v", legs)
	}
	if res.IndexCounts["order_search"] != 0 {
		t.Errorf("excluded Type should report a zero count, got %v", res.IndexCounts)
	}
	if res.Total != 1 || len(res.Hits) != 1 {
		t.Errorf("res = %+v, want the product hit only", res)
	}
}

func TestFederatedFanout_AllGroupsMatchNothingSkipsES(t *testing.T) {
	groups := []core.IndexFilterGroup{
		{Resource: "product", Alias: "product_search", MatchNothing: true},
		{Resource: "order", Alias: "order_search", MatchNothing: true},
	}
	// Empty msearchBody makes the transport fail the test if reached.
	_, _, res, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: groups, PageSize: 25,
	}, "")
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if res.Total != 0 || len(res.Hits) != 0 {
		t.Errorf("res = %+v, want empty", res)
	}
	if res.IndexCounts["product_search"] != 0 || res.IndexCounts["order_search"] != 0 {
		t.Errorf("IndexCounts = %v, want explicit zeros", res.IndexCounts)
	}
}

func TestFederatedFanout_LegErrorFailsRequest(t *testing.T) {
	_, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), PageSize: 25,
	}, msearchResponse(
		legResponse("product_search_v1", 0),
		`{"error":{"type":"search_phase_execution_exception","reason":"boom"},"status":500}`,
	))
	if err == nil {
		t.Fatal("expected an error when a leg fails")
	}
	if !strings.Contains(err.Error(), "order_search") {
		t.Errorf("error should name the failing leg's alias, got %v", err)
	}
}

func TestFederatedFanout_MissingIndexLegIsEmpty(t *testing.T) {
	_, _, res, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), PageSize: 25,
	}, msearchResponse(
		legResponse("product_search_v1", 1, "p1:2.0"),
		`{"error":{"type":"index_not_found_exception","reason":"no such index [order_search]"},"status":404}`,
	))
	if err != nil {
		t.Fatalf("a missing index must empty its own leg only, got %v", err)
	}
	if res.Total != 1 || len(res.Hits) != 1 || res.Hits[0].ID != "p1" {
		t.Errorf("res = %+v, want the product hit only", res)
	}
	if res.IndexCounts["order_search"] != 0 {
		t.Errorf("missing index should count 0, got %v", res.IndexCounts)
	}
}

func TestFederatedFanout_PagingWindowCap(t *testing.T) {
	_, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), Page: 100, PageSize: 100,
	}, "") // transport must not be reached
	if err == nil || !strings.Contains(err.Error(), "max_result_window") {
		t.Fatalf("expected paging window cap error, got %v", err)
	}
	requirePagingRefused(t, err, 100, 100, 10100)
}

func TestFederatedFanout_PagingWindowCapSurvivesInt32Overflow(t *testing.T) {
	// (Page+1)*PageSize wraps in int32; the guard must still refuse the window.
	_, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), Page: math.MaxInt32, PageSize: 100,
	}, "") // transport must not be reached
	if err == nil || !strings.Contains(err.Error(), "max_result_window") {
		t.Fatalf("expected paging window cap error, got %v", err)
	}
	requirePagingRefused(t, err, math.MaxInt32, 100, 214748364800)
}

func TestFederatedFanout_LastPageInsideWindow(t *testing.T) {
	// (99+1)*100 = 10000 = index.max_result_window: every leg over-fetches the
	// whole window, and that is still allowed.
	legs, _, _, err := captureFanout(t, core.FederatedSearchParams{
		Query: "q", FilterGroups: fedGroups(), Page: 99, PageSize: 100,
	}, msearchResponse(
		legResponse("product_search_v1", 0),
		legResponse("order_search_v1", 0),
	))
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	for i, leg := range legs {
		if leg.body["from"] != float64(0) || leg.body["size"] != float64(10000) {
			t.Errorf("leg %d window = from %v size %v, want 0/10000", i, leg.body["from"], leg.body["size"])
		}
	}
}

func TestFederatedSearch_UnknownModeIsRefused(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Error("unexpected HTTP call")
		return nil, fmt.Errorf("unexpected HTTP call")
	})
	esClient, err := esv8.NewClient(esv8.Config{Addresses: []string{"http://example.invalid"}, Transport: rt})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	_, err = New(esClient, false, WithFederatedExecution("bogus")).
		FederatedSearch(context.Background(), core.FederatedSearchParams{Query: "q", FilterGroups: fedGroups(), PageSize: 25})
	if err == nil || !strings.Contains(err.Error(), "unknown federated execution mode") {
		t.Fatalf("expected an unknown-mode error, got %v", err)
	}
}

func TestFederatedSearch_SingleModeDropsDFS(t *testing.T) {
	var capturedURL string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedURL = r.URL.String()
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader(`{"hits":{"total":{"value":0},"hits":[]}}`)),
		}, nil
	})
	esClient, err := esv8.NewClient(esv8.Config{Addresses: []string{"http://example.invalid"}, Transport: rt})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	_, err = New(esClient, false, WithFederatedExecution(FederatedSingle)).
		FederatedSearch(context.Background(), core.FederatedSearchParams{Query: "q", FilterGroups: fedGroups(), PageSize: 25})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if !strings.Contains(capturedURL, "product_search,order_search") || !strings.Contains(capturedURL, "_search") {
		t.Errorf("expected a single multi-index _search, got %q", capturedURL)
	}
	if strings.Contains(capturedURL, "search_type") {
		t.Errorf("single mode must not set a search_type, got %q", capturedURL)
	}
}

func TestParseFederatedExecution(t *testing.T) {
	for in, want := range map[string]FederatedExecution{
		"":           FederatedSingleDFS,
		"single-dfs": FederatedSingleDFS,
		"single":     FederatedSingle,
		"fanout":     FederatedFanout,
	} {
		got, err := ParseFederatedExecution(in)
		if err != nil || got != want {
			t.Errorf("ParseFederatedExecution(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseFederatedExecution("bogus"); err == nil {
		t.Error("expected an error for an unknown mode")
	}
}
