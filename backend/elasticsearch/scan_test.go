package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// scanCall is one request a scan sent, as the mock transport saw it.
type scanCall struct {
	Method string
	Path   string
	Query  url.Values
	Body   []byte
}

// bodyMap decodes the call's JSON body.
func (sc scanCall) bodyMap(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(sc.Body, &m), "body: %s", sc.Body)
	return m
}

// rawField returns the raw JSON of one top-level body field, byte for byte.
func (sc scanCall) rawField(t *testing.T, name string) string {
	t.Helper()
	var m map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(sc.Body, &m), "body: %s", sc.Body)
	return string(m[name])
}

// scanReply is one canned answer of the mock transport.
type scanReply struct {
	status int
	body   string
}

// scanTransport is a mock Elasticsearch that answers by endpoint — opening a
// point in time (POST /<index>/_pit), searching (/_search), closing a point in
// time (DELETE /_pit) — each from its own queue of replies, the last of which
// repeats, and records every request in order.
type scanTransport struct {
	t      *testing.T
	open   []scanReply
	search []scanReply
	close  []scanReply
	calls  []scanCall
}

func (st *scanTransport) next(q *[]scanReply, what string) scanReply {
	st.t.Helper()
	if len(*q) == 0 {
		st.t.Fatalf("unexpected %s request", what)
	}
	r := (*q)[0]
	if len(*q) > 1 {
		*q = (*q)[1:]
	}
	return r
}

func (st *scanTransport) client(opts ...Option) *Client {
	st.t.Helper()
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body []byte
		if r.Body != nil {
			body, _ = io.ReadAll(r.Body)
		}
		st.calls = append(st.calls, scanCall{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Body: body})
		var reply scanReply
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/_pit":
			reply = st.next(&st.close, "close point in time")
		case strings.HasSuffix(r.URL.Path, "/_pit"):
			reply = st.next(&st.open, "open point in time")
		case strings.HasSuffix(r.URL.Path, "/_search"):
			reply = st.next(&st.search, "search")
		default:
			st.t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		headers.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: reply.status,
			Body:       io.NopCloser(strings.NewReader(reply.body)),
			Header:     headers,
		}, nil
	})
	es, err := esv8.NewClient(esv8.Config{Addresses: []string{"http://example.invalid"}, Transport: rt})
	require.NoError(st.t, err)
	return New(es, false, opts...)
}

// callsTo returns the recorded calls whose path ends in suffix (and, if
// method is non-empty, use it).
func (st *scanTransport) callsTo(method, suffix string) []scanCall {
	var out []scanCall
	for _, c := range st.calls {
		if strings.HasSuffix(c.Path, suffix) && (method == "" || c.Method == method) {
			out = append(out, c)
		}
	}
	return out
}

func (st *scanTransport) searches() []scanCall { return st.callsTo("", "/_search") }

func (st *scanTransport) closes() []scanCall { return st.callsTo(http.MethodDelete, "/_pit") }

const pinnedIndex = "users_search_v3"

func ok(body string) scanReply { return scanReply{status: http.StatusOK, body: body} }

var closedReply = ok(`{"succeeded":true,"num_freed":1}`)

func openReply(id string) scanReply { return ok(`{"id":"` + id + `"}`) }

// scanPage renders a point-in-time search response; total < 0 leaves
// hits.total out, as track_total_hits:false does.
func scanPage(pitID string, total int, shardsFailed int, hits ...string) string {
	tot := ""
	if total >= 0 {
		tot = `"total":{"value":` + itoa(total) + `,"relation":"eq"},`
	}
	return `{"pit_id":"` + pitID + `","took":1,"timed_out":false,` +
		`"_shards":{"total":1,"successful":1,"skipped":0,"failed":` + itoa(shardsFailed) + `},` +
		`"hits":{` + tot + `"max_score":null,"hits":[` + strings.Join(hits, ",") + `]}}`
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

// scanHit renders one hit of index with the given raw sort array.
func scanHit(index, id, sort string) string {
	return `{"_index":"` + index + `","_id":"` + id + `","_score":null,"sort":` + sort + `}`
}

func scanReq(pageSize int32, token string) core.SearchRequest {
	return core.SearchRequest{
		Scan:      true,
		PageSize:  pageSize,
		PageToken: token,
		Filters:   []core.Filter{{Field: "fields.status", Op: core.FilterOpEq, Value: "active"}},
		Sort:      []core.SortOption{{Field: "fields.name"}, {Field: "fields.count", Desc: true}},
	}
}

func TestScan_FirstPageOpensPointInTimeOnPinnedIndex(t *testing.T) {
	st := &scanTransport{t: t,
		open: []scanReply{openReply("pit-1")},
		search: []scanReply{ok(scanPage("pit-1", 5, 0,
			scanHit(pinnedIndex, "a", `["x",1,"a",0]`),
			scanHit(pinnedIndex, "b", `["y",2,"b",1]`)))},
	}
	c := st.client()

	resp, err := c.Search(context.Background(), scanReq(2, ""), pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	require.Equal(t, int64(5), resp.Total)
	require.Len(t, resp.Hits, 2)
	require.Equal(t, "a", resp.Hits[0].ID)
	require.NotEmpty(t, resp.NextPageToken)

	opens := st.callsTo(http.MethodPost, "/_pit")
	require.Len(t, opens, 1)
	require.Equal(t, "/"+pinnedIndex+"/_pit", opens[0].Path)
	require.Equal(t, "60000ms", opens[0].Query.Get("keep_alive"))

	searches := st.searches()
	require.Len(t, searches, 1)
	s := searches[0]
	require.Equal(t, "/_search", s.Path, "a point-in-time search names no index")
	require.Equal(t, "false", s.Query.Get("allow_partial_search_results"))

	body := s.bodyMap(t)
	require.Equal(t, map[string]any{"id": "pit-1", "keep_alive": "60000ms"}, body["pit"])
	require.Equal(t, false, body["_source"])
	require.Equal(t, true, body["track_total_hits"])
	require.Equal(t, float64(2), body["size"])
	require.NotContains(t, body, "search_after")
	require.NotContains(t, body, "allow_partial_search_results", "ES rejects it in the body")

	sorts := body["sort"].([]any)
	require.Len(t, sorts, 3)
	require.Contains(t, sorts[0], "fields.name")
	require.Equal(t, map[string]any{"order": "desc"}, sorts[1].(map[string]any)["fields.count"])
	last := sorts[len(sorts)-1].(map[string]any)
	require.Contains(t, last, resource.ResourceIDField, "the sort ends in resource_id")

	filters := getPath(body, "query", "bool", "filter").([]any)
	require.Contains(t, filters, map[string]any{"term": map[string]any{"_index": pinnedIndex}})
	require.Contains(t, filters, map[string]any{"term": map[string]any{"fields.status": "active"}},
		"the scan keeps the paged search's filters")
	require.Empty(t, st.closes(), "a full page keeps the point in time open")
}

// The scope of a scoped block is enforced on a scan exactly as on a paged
// search: one correlated nested clause per block of the config it's given.
func TestScan_KeepsScopedBlockClauses(t *testing.T) {
	st := &scanTransport{t: t, open: []scanReply{openReply("pit-1")}, search: []scanReply{ok(scanPage("pit-1", 0, 0))},
		close: []scanReply{closedReply}}
	_, err := st.client().Search(context.Background(),
		core.SearchRequest{Scan: true, PageSize: 10, Scope: "op-1"}, pinnedIndex, vcScopedBlock())
	require.NoError(t, err)

	filters := getPath(st.searches()[0].bodyMap(t), "query", "bool", "filter").([]any)
	require.Len(t, filters, 2)
	nested := getPath(filters[0].(map[string]any), "nested", "path")
	require.Equal(t, "operator_data", nested)
	require.Equal(t, map[string]any{"term": map[string]any{"_index": pinnedIndex}}, filters[1])
}

func TestScan_LaterPageSendsLastSortValuesAndLatestPointInTime(t *testing.T) {
	// The caller sorts on a keyword the last document lacks (null) and a long
	// above 2^53; a point-in-time search appends _shard_doc, one value more
	// than the sort clauses.
	lastSort := `[null,9007199254740993,"b",-9223372036854775808,4294967306]`
	st := &scanTransport{t: t,
		open: []scanReply{openReply("pit-1")},
		search: []scanReply{
			ok(scanPage("pit-2", 5, 0,
				scanHit(pinnedIndex, "a", `["x",1,"a",0,4294967300]`),
				scanHit(pinnedIndex, "b", lastSort))),
			ok(scanPage("pit-3", -1, 0,
				scanHit(pinnedIndex, "c", `["z",3,"c",0,4294967311]`),
				scanHit(pinnedIndex, "d", `["z",4,"d",0,4294967312]`))),
		},
	}
	c := st.client()
	ctx := context.Background()

	page1, err := c.Search(ctx, scanReq(2, ""), pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	require.NotEmpty(t, page1.NextPageToken)

	page2, err := c.Search(ctx, scanReq(2, page1.NextPageToken), pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	require.Equal(t, int64(5), page2.Total, "a later page's total is the first page's")
	require.Equal(t, []string{"c", "d"}, []string{page2.Hits[0].ID, page2.Hits[1].ID})
	require.NotEmpty(t, page2.NextPageToken)
	require.NotEqual(t, page1.NextPageToken, page2.NextPageToken)

	require.Len(t, st.callsTo(http.MethodPost, "/_pit"), 1, "a later page never opens a point in time")
	searches := st.searches()
	require.Len(t, searches, 2)
	later := searches[1]
	require.Equal(t, "/_search", later.Path)
	require.Equal(t, "false", later.Query.Get("allow_partial_search_results"))
	require.Equal(t, lastSort, later.rawField(t, "search_after"), "search_after is the last hit's sort, byte for byte")
	body := later.bodyMap(t)
	require.Equal(t, map[string]any{"id": "pit-2", "keep_alive": "60000ms"}, body["pit"],
		"the point-in-time id of the previous response, keep-alive renewed")
	require.Equal(t, false, body["track_total_hits"])
	require.Equal(t, false, body["_source"])
	require.Contains(t, getPath(body, "query", "bool", "filter").([]any),
		map[string]any{"term": map[string]any{"_index": pinnedIndex}})

	// Page 3 goes on from page 2's response: its point in time and last sort.
	st.search = []scanReply{ok(scanPage("pit-3", -1, 0))}
	st.close = []scanReply{closedReply}
	page3, err := c.Search(ctx, scanReq(2, page2.NextPageToken), pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	require.Equal(t, int64(5), page3.Total)
	third := st.searches()[2]
	require.Equal(t, `["z",4,"d",0,4294967312]`, third.rawField(t, "search_after"))
	require.Equal(t, "pit-3", third.bodyMap(t)["pit"].(map[string]any)["id"])
}

func TestScan_KeepAliveOption(t *testing.T) {
	st := &scanTransport{t: t,
		open: []scanReply{openReply("pit-1")},
		search: []scanReply{ok(scanPage("pit-1", 4, 0,
			scanHit(pinnedIndex, "a", `["a",0]`), scanHit(pinnedIndex, "b", `["b",1]`)))},
	}
	c := st.client(WithScanKeepAlive(90 * time.Second))
	ctx := context.Background()
	req := core.SearchRequest{Scan: true, PageSize: 2}

	page1, err := c.Search(ctx, req, pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	req.PageToken = page1.NextPageToken
	_, err = c.Search(ctx, req, pinnedIndex, vcFlatOnly())
	require.NoError(t, err)

	opens := st.callsTo(http.MethodPost, "/_pit")
	require.Len(t, opens, 1)
	require.Equal(t, "90000ms", opens[0].Query.Get("keep_alive"))
	require.Len(t, st.searches(), 2)
	for i, s := range st.searches() {
		require.Equal(t, "90000ms", s.bodyMap(t)["pit"].(map[string]any)["keep_alive"], "search %d", i)
	}
}

func TestScan_BackstopsFailThePage(t *testing.T) {
	cases := map[string]string{
		"hit from another index": scanPage("pit-1", 2, 0,
			scanHit(pinnedIndex, "a", `["a",0]`), scanHit("users_search_v4", "b", `["b",1]`)),
		"failed shard": scanPage("pit-1", 2, 1,
			scanHit(pinnedIndex, "a", `["a",0]`), scanHit(pinnedIndex, "b", `["b",1]`)),
	}
	for name, page := range cases {
		t.Run(name, func(t *testing.T) {
			st := &scanTransport{t: t, open: []scanReply{openReply("pit-1")}, search: []scanReply{ok(page)}}
			resp, err := st.client().Search(context.Background(),
				core.SearchRequest{Scan: true, PageSize: 10}, pinnedIndex, vcFlatOnly())
			require.ErrorIs(t, err, core.ErrScanFault)
			require.Empty(t, resp.Hits)
			require.Empty(t, resp.NextPageToken)
			require.Empty(t, st.closes(), "a scan ended by an error leaves its point in time to expire")
		})
	}
}

func TestScan_CursorWithoutPointInTimeOrSortIsInvalid(t *testing.T) {
	cases := map[string]string{
		"no point-in-time id": encodeScanCursor(scanCursor{SearchAfter: jsontext.Value(`["a",0]`), Total: 3}),
		"no sort values":      encodeScanCursor(scanCursor{PITID: "pit-1", Total: 3}),
		"empty sort values":   encodeScanCursor(scanCursor{PITID: "pit-1", SearchAfter: jsontext.Value(`[]`), Total: 3}),
		"not base64":          "%%%",
		"not a cursor":        "bm90IGpzb24",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			st := &scanTransport{t: t}
			_, err := st.client().Search(context.Background(),
				core.SearchRequest{Scan: true, PageSize: 10, PageToken: token}, pinnedIndex, vcFlatOnly())
			require.ErrorIs(t, err, core.ErrInvalidPageToken)
			require.Empty(t, st.calls, "an invalid cursor reaches no endpoint")
		})
	}
}

func TestScan_404IsCursorExpired(t *testing.T) {
	expiredPIT := `{"error":{"root_cause":[{"type":"search_context_missing_exception","reason":"No search context found for id [7]"}],` +
		`"type":"search_phase_execution_exception","reason":"all shards failed","phase":"query","grouped":true},"status":404}`
	indexGone := `{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [users_search_v3]"}],` +
		`"type":"index_not_found_exception","reason":"no such index [users_search_v3]"},"status":404}`
	laterToken := encodeScanCursor(scanCursor{PITID: "pit-1", SearchAfter: jsontext.Value(`["a",0]`), Total: 3})

	cases := []struct {
		name   string
		token  string
		open   []scanReply
		search []scanReply
	}{
		{name: "opening the point in time", open: []scanReply{{http.StatusNotFound, indexGone}}},
		{name: "first search", open: []scanReply{openReply("pit-1")}, search: []scanReply{{http.StatusNotFound, indexGone}}},
		{name: "later page, point in time gone", token: laterToken, search: []scanReply{{http.StatusNotFound, expiredPIT}}},
		{name: "later page, index gone", token: laterToken, search: []scanReply{{http.StatusNotFound, indexGone}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &scanTransport{t: t, open: tc.open, search: tc.search}
			resp, err := st.client().Search(context.Background(),
				core.SearchRequest{Scan: true, PageSize: 10, PageToken: tc.token}, pinnedIndex, vcFlatOnly())
			require.ErrorIs(t, err, core.ErrCursorExpired)
			require.Empty(t, resp.Hits)
			require.Empty(t, resp.NextPageToken)
			require.Empty(t, st.closes())
		})
	}
}

func TestScan_ErrorStatusIsAnError(t *testing.T) {
	st := &scanTransport{t: t, open: []scanReply{openReply("pit-1")}, search: []scanReply{{http.StatusInternalServerError, `{"error":"oops"}`}}}
	_, err := st.client().Search(context.Background(), core.SearchRequest{Scan: true, PageSize: 10}, pinnedIndex, vcFlatOnly())
	require.Error(t, err)
	require.False(t, errors.Is(err, core.ErrCursorExpired))
	require.Empty(t, st.closes())
}

func TestScan_ShortPageClosesPointInTime(t *testing.T) {
	for name, closeReply := range map[string]scanReply{
		"close succeeds": closedReply,
		"close fails":    {http.StatusInternalServerError, `{"error":"oops"}`},
	} {
		t.Run(name, func(t *testing.T) {
			st := &scanTransport{t: t,
				open: []scanReply{openReply("pit-1")},
				search: []scanReply{ok(scanPage("pit-2", 1, 0,
					scanHit(pinnedIndex, "a", `["a",0]`)))},
				close: []scanReply{closeReply},
			}
			var logs bytes.Buffer
			ctx := core.WithLogger(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)))

			resp, err := st.client().Search(ctx, core.SearchRequest{Scan: true, PageSize: 2}, pinnedIndex, vcFlatOnly())
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.Total)
			require.Len(t, resp.Hits, 1)
			require.Empty(t, resp.NextPageToken, "the last page has no cursor")

			closes := st.closes()
			require.Len(t, closes, 1)
			require.Equal(t, "pit-2", closes[0].bodyMap(t)["id"], "the latest point-in-time id is closed")
			if closeReply.status != http.StatusOK {
				require.Contains(t, logs.String(), "pit-2", "a failed close is logged")
			}
		})
	}
}

// An empty page on a later request ends the scan too.
func TestScan_EmptyLaterPageClosesPointInTime(t *testing.T) {
	st := &scanTransport{t: t,
		search: []scanReply{ok(scanPage("pit-2", -1, 0))},
		close:  []scanReply{closedReply},
	}
	token := encodeScanCursor(scanCursor{PITID: "pit-1", SearchAfter: jsontext.Value(`["a",0]`), Total: 7})
	resp, err := st.client().Search(context.Background(),
		core.SearchRequest{Scan: true, PageSize: 2, PageToken: token}, pinnedIndex, vcFlatOnly())
	require.NoError(t, err)
	require.Equal(t, int64(7), resp.Total)
	require.Empty(t, resp.Hits)
	require.Empty(t, resp.NextPageToken)
	require.Len(t, st.closes(), 1)
}
