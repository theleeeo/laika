package elasticsearch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"encoding/json/v2"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestUpsert_UsesExternalVersion(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPut {
			t.Fatalf("expected PUT method, got %s", req.Method)
		}

		q := req.URL.Query()
		if got := q.Get("version"); got != "7" {
			t.Fatalf("expected version=7, got %q", got)
		}
		if got := q.Get("version_type"); got != "external_gte" {
			t.Fatalf("expected version_type=external_gte, got %q", got)
		}

		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"result":"created"}`)),
			Header:     headers,
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
	if err := c.Upsert(context.Background(), "idx", "1", map[string]any{"id": "1"}, 7); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
}

func TestBulkUpsert_UsesExternalVersionPerItem(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", req.Method)
		}

		if !strings.Contains(req.URL.Path, "/_bulk") {
			t.Fatalf("expected bulk path, got %s", req.URL.Path)
		}

		b, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 NDJSON lines, got %d", len(lines))
		}

		var meta map[string]map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &meta); err != nil {
			t.Fatalf("unmarshal meta: %v", err)
		}

		indexMeta := meta["index"]
		if indexMeta == nil {
			t.Fatal("missing index meta")
		}
		if got := indexMeta["version"]; got != float64(9) {
			t.Fatalf("expected version=9, got %v", got)
		}
		if got := indexMeta["version_type"]; got != "external_gte" {
			t.Fatalf("expected version_type=external_gte, got %v", got)
		}

		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"errors":false}`)),
			Header:     headers,
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
	failures, err := c.BulkUpsert(context.Background(), []core.BulkItem{{
		Index:   "idx",
		ID:      "1",
		Doc:     map[string]any{"id": "1"},
		Version: 9,
	}})
	if err != nil {
		t.Fatalf("bulk upsert failed: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("expected no failures, got %v", failures)
	}
}

func TestUpsert_VersionConflict_ReturnsSentinel(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusConflict,
			Body: io.NopCloser(strings.NewReader(
				`{"error":{"type":"version_conflict_engine_exception","reason":"[1]: version conflict"},"status":409}`)),
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
	err = c.Upsert(context.Background(), "idx", "1", map[string]any{"id": "1"}, 3)
	if !errors.Is(err, core.ErrVersionConflict) {
		t.Fatalf("a 409 must surface as core.ErrVersionConflict so callers can treat OCC losses as benign, got: %v", err)
	}
}

// bulkClient builds a Client whose transport serves the given bulk response
// body with HTTP 200 — the status ES uses even when individual items fail.
func bulkClient(t *testing.T, responseBody string) *Client {
	t.Helper()
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Header:     headers,
		}, nil
	})
	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	return New(esClient, false)
}

func TestBulkUpsert_VersionConflictIsNotAFailure(t *testing.T) {
	c := bulkClient(t, `{
		"took": 1,
		"errors": true,
		"items": [
			{"index": {"_index": "a_search_v1", "_id": "1", "status": 409,
				"error": {"type": "version_conflict_engine_exception",
					"reason": "[1]: version conflict, current version [5] is higher or equal to the one provided [3]"}}},
			{"index": {"_index": "a_search_v2", "_id": "1", "status": 200, "result": "updated"}}
		]
	}`)

	failures, err := c.BulkUpsert(context.Background(), []core.BulkItem{
		{Index: "a_search_v1", ID: "1", Doc: map[string]any{"title": "t"}, Version: 3},
		{Index: "a_search_v2", ID: "1", Doc: map[string]any{"title": "t"}, Version: 3},
	})
	if err != nil {
		t.Fatalf("an OCC loss must not be a request-level error: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("an OCC loss means a newer build already wrote fresher data — not a failure: %v", failures)
	}
}

func TestBulkUpsert_ReturnsPerItemFailures(t *testing.T) {
	c := bulkClient(t, `{
		"took": 1,
		"errors": true,
		"items": [
			{"index": {"_index": "a_search_v1", "_id": "1", "status": 201, "result": "created"}},
			{"index": {"_index": "a_search_v2", "_id": "1", "status": 400,
				"error": {"type": "document_parsing_exception", "reason": "failed to parse field [price]"}}}
		]
	}`)

	failures, err := c.BulkUpsert(context.Background(), []core.BulkItem{
		{Index: "a_search_v1", ID: "1", Doc: map[string]any{"title": "t"}, Version: 3},
		{Index: "a_search_v2", ID: "1", Doc: map[string]any{"price": "not-a-number"}, Version: 3},
	})
	if err != nil {
		t.Fatalf("item-level rejections must not be a request-level error: %v", err)
	}
	if len(failures) != 1 {
		t.Fatalf("expected exactly the rejected item as failure, got %v", failures)
	}
	f := failures[0]
	if f.Index != "a_search_v2" || f.ID != "1" || f.Status != 400 {
		t.Fatalf("failure must identify the rejected doc: %+v", f)
	}
	if !strings.Contains(f.Reason, "failed to parse field") {
		t.Fatalf("failure must carry the ES reason: %+v", f)
	}
}

// statusClient builds a Client whose transport answers every request with the
// given status and body, and records the last request it served.
func statusClient(t *testing.T, status int, responseBody string) (*Client, **http.Request) {
	t.Helper()
	var last *http.Request
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		last = req
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Header:     headers,
		}, nil
	})
	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	return New(esClient, false), &last
}

func TestDelete_UsesExternalVersion(t *testing.T) {
	c, last := statusClient(t, http.StatusOK, `{"result":"deleted"}`)
	if err := c.Delete(context.Background(), "idx", "1", 7); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	req := *last
	if req == nil {
		t.Fatal("no request sent")
	}
	if req.Method != http.MethodDelete {
		t.Fatalf("expected DELETE method, got %s", req.Method)
	}
	q := req.URL.Query()
	if got := q.Get("version"); got != "7" {
		t.Fatalf("expected version=7, got %q", got)
	}
	if got := q.Get("version_type"); got != "external_gte" {
		t.Fatalf("expected version_type=external_gte, got %q", got)
	}
}

func TestDelete_ResponseStatus(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{
			name:   "deleted",
			status: http.StatusOK,
			body:   `{"result":"deleted"}`,
		},
		{
			// A newer build holds the document at a higher version: it stays,
			// as an OCC loss does for a write.
			name:   "version conflict keeps the newer document",
			status: http.StatusConflict,
			body:   `{"error":{"type":"version_conflict_engine_exception","reason":"[1]: version conflict"},"status":409}`,
		},
		{
			name:   "missing document",
			status: http.StatusNotFound,
			body:   `{"result":"not_found"}`,
		},
		{
			name:    "other error",
			status:  http.StatusInternalServerError,
			body:    `{"error":"boom"}`,
			wantErr: true,
		},
		{
			name:    "bad request",
			status:  http.StatusBadRequest,
			body:    `{"error":"bad"}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := statusClient(t, tc.status, tc.body)
			err := c.Delete(context.Background(), "idx", "1", 3)
			if tc.wantErr && err == nil {
				t.Fatalf("status %d must return an error", tc.status)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("status %d must not be an error, got: %v", tc.status, err)
			}
		})
	}
}

func TestDelete_RejectsNonPositiveVersion(t *testing.T) {
	c, last := statusClient(t, http.StatusOK, `{"result":"deleted"}`)
	if err := c.Delete(context.Background(), "idx", "1", 0); err == nil {
		t.Fatal("a delete without a Build Sequence must be rejected, not sent")
	}
	if *last != nil {
		t.Fatal("no request may be sent for an invalid version")
	}
}

// bodyClient builds a Client whose transport answers every request with HTTP
// 200 and the given body, and records each request body it served.
func bodyClient(t *testing.T, responseBody string) (*Client, *[]string) {
	t.Helper()
	var bodies []string
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		bodies = append(bodies, string(b))
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Header:     headers,
		}, nil
	})
	esClient, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("new es client: %v", err)
	}
	return New(esClient, false), &bodies
}

// Every search sorts on resource_id last, so every written document carries
// it, equal to its document id — and the caller's map, which a plan may share
// across ids and workers, is never written into.
func TestUpsert_SetsResourceIDOnACopy(t *testing.T) {
	c, bodies := bodyClient(t, `{"result":"created"}`)

	doc := map[string]any{"fields": map[string]any{"title": "t"}}
	require.NoError(t, c.Upsert(context.Background(), "idx", "42", doc, 7))

	require.Len(t, *bodies, 1)
	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte((*bodies)[0]), &sent))
	require.Equal(t, "42", sent[resource.ResourceIDField])
	require.Equal(t, map[string]any{"title": "t"}, sent["fields"])
	require.NotContains(t, doc, resource.ResourceIDField, "the caller's map must not be written into")
}

// A resource_id already in the document is overwritten with the document id:
// the id the build writes under is the one every search sorts on.
func TestUpsert_ResourceIDOverwritesDocKey(t *testing.T) {
	c, bodies := bodyClient(t, `{"result":"created"}`)

	doc := map[string]any{resource.ResourceIDField: "stale"}
	require.NoError(t, c.Upsert(context.Background(), "idx", "42", doc, 7))

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte((*bodies)[0]), &sent))
	require.Equal(t, "42", sent[resource.ResourceIDField])
	require.Equal(t, "stale", doc[resource.ResourceIDField])
}

func TestUpsert_NonMapDocIsAnError(t *testing.T) {
	c, bodies := bodyClient(t, `{"result":"created"}`)

	err := c.Upsert(context.Background(), "idx", "42", struct{ Title string }{"t"}, 7)
	require.Error(t, err)
	require.Empty(t, *bodies, "no request is sent for a document that cannot carry resource_id")
}

func TestBulkUpsert_SetsResourceIDPerItemOnCopies(t *testing.T) {
	c, bodies := bodyClient(t, `{"errors":false}`)

	// One map shared by two items, as a plan's fixture may return it for
	// several ids: each item's document carries its own id.
	shared := map[string]any{"fields": map[string]any{"title": "t"}}
	_, err := c.BulkUpsert(context.Background(), []core.BulkItem{
		{Index: "idx", ID: "1", Doc: shared, Version: 3},
		{Index: "idx", ID: "2", Doc: shared, Version: 3},
	})
	require.NoError(t, err)

	require.Len(t, *bodies, 1)
	lines := strings.Split(strings.TrimSpace((*bodies)[0]), "\n")
	require.Len(t, lines, 4, "two meta lines and two document lines")
	for i, wantID := range []string{"1", "2"} {
		var sent map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[2*i+1]), &sent))
		require.Equal(t, wantID, sent[resource.ResourceIDField], "item %d", i)
		require.Equal(t, map[string]any{"title": "t"}, sent["fields"], "item %d", i)
	}
	require.NotContains(t, shared, resource.ResourceIDField, "the caller's map must not be written into")
}

// A document that is not a map is a programming error for the whole call,
// like an invalid version: nothing is sent.
func TestBulkUpsert_NonMapDocFailsTheCall(t *testing.T) {
	c, bodies := bodyClient(t, `{"errors":false}`)

	_, err := c.BulkUpsert(context.Background(), []core.BulkItem{
		{Index: "idx", ID: "1", Doc: map[string]any{"fields": map[string]any{}}, Version: 3},
		{Index: "idx", ID: "2", Doc: "not a map", Version: 3},
	})
	require.Error(t, err)
	require.Empty(t, *bodies, "no request is sent when any item cannot carry resource_id")
}
