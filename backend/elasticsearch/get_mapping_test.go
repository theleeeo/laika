package elasticsearch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	esv8 "github.com/elastic/go-elasticsearch/v8"
)

func esClientReturning(t *testing.T, status int, body string, assertReq func(*http.Request)) *Client {
	t.Helper()
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if assertReq != nil {
			assertReq(req)
		}
		headers := make(http.Header)
		headers.Set("X-Elastic-Product", "Elasticsearch")
		headers.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     headers,
		}, nil
	})
	es, err := esv8.NewClient(esv8.Config{
		Addresses: []string{"http://example.invalid"},
		Transport: rt,
	})
	require.NoError(t, err)
	return New(es, false)
}

func TestGetMapping_UnwrapsIndexAndMappings(t *testing.T) {
	body := `{"my_index":{"mappings":{"properties":{"fields":{"properties":{"name":{"type":"keyword"}}}}}}}`
	c := esClientReturning(t, http.StatusOK, body, func(req *http.Request) {
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "/my_index/_mapping", req.URL.Path)
	})

	m, exists, err := c.GetMapping(context.Background(), "my_index")
	require.NoError(t, err)
	require.True(t, exists)

	// Returned value is the inner "mappings" object (has "properties" at top).
	props, ok := m["properties"].(map[string]any)
	require.True(t, ok, "expected a properties key, got %v", m)
	require.Contains(t, props, "fields")
}

func TestGetMapping_NotFoundReturnsFalse(t *testing.T) {
	c := esClientReturning(t, http.StatusNotFound, `{"error":{"type":"index_not_found_exception"},"status":404}`, nil)

	m, exists, err := c.GetMapping(context.Background(), "missing")
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, m)
}

func TestGetMapping_ServerErrorReturnsError(t *testing.T) {
	c := esClientReturning(t, http.StatusInternalServerError, `{"error":"boom"}`, nil)

	_, exists, err := c.GetMapping(context.Background(), "idx")
	require.Error(t, err)
	require.False(t, exists)
}
