package elasticsearch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"encoding/json/v2"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/theleeeo/indexer/core"
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
	err = c.BulkUpsert(context.Background(), []core.BulkItem{{
		Index:   "idx",
		ID:      "1",
		Doc:     map[string]any{"id": "1"},
		Version: 9,
	}})
	if err != nil {
		t.Fatalf("bulk upsert failed: %v", err)
	}
}
