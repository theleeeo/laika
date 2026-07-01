package server

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/gen/search/v1/searchconnect"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// fakeBackend is a core.SearchBackend that returns a canned response from
// Search and no-ops the write methods. It lets us exercise the full HTTP +
// Connect-JSON path without Elasticsearch.
type fakeBackend struct {
	gotReq   core.SearchRequest
	gotAlias string
}

func (b *fakeBackend) Upsert(context.Context, string, string, any, int64) error { return nil }
func (b *fakeBackend) BulkUpsert(context.Context, []core.BulkItem) error        { return nil }
func (b *fakeBackend) Delete(context.Context, string, string) error             { return nil }

func (b *fakeBackend) Search(_ context.Context, req core.SearchRequest, alias string, _ *resource.VersionConfig) (core.SearchResponse, error) {
	b.gotReq = req
	b.gotAlias = alias
	return core.SearchResponse{
		Total: 1,
		Hits: []core.SearchHit{
			{ID: "p1", Score: 1.5, Source: map[string]any{"name": "Widget"}},
		},
	}, nil
}

// TestSearch_ConnectJSONContract exercises the exact web-client contract: a
// plain HTTP POST with a JSON body to the Connect route, and a JSON response.
func TestSearch_ConnectJSONContract(t *testing.T) {
	backend := &fakeBackend{}
	idx := core.New(core.Config{
		ES: backend,
		Resources: resource.Configs{
			{
				Resource:    "product",
				ReadVersion: 1,
				Versions: []resource.VersionConfig{
					{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}},
				},
			},
		},
	})

	mux := http.NewServeMux()
	mux.Handle(searchconnect.NewSearchServiceHandler(NewSearcher(idx)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := `{"resource":"product","query":"widget","pageSize":20}`
	resp, err := http.Post(
		srv.URL+searchconnect.SearchServiceSearchProcedure,
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)

	var got struct {
		Total string `json:"total"` // proto3 JSON encodes int64 as a string
		Hits  []struct {
			ID     string         `json:"id"`
			Score  float64        `json:"score"`
			Source map[string]any `json:"source"`
		} `json:"hits"`
	}
	require.NoError(t, json.Unmarshal(raw, &got))

	require.Equal(t, "1", got.Total)
	require.Len(t, got.Hits, 1)
	require.Equal(t, "p1", got.Hits[0].ID)
	require.Equal(t, 1.5, got.Hits[0].Score)
	require.Equal(t, "Widget", got.Hits[0].Source["name"])

	// The request shaping reached the backend intact.
	require.Equal(t, "product", backend.gotReq.Resource)
	require.Equal(t, "widget", backend.gotReq.Query)
	require.Equal(t, int32(20), backend.gotReq.PageSize)
}
