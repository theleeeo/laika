package webui_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/gen/search/v1/searchconnect"
	"github.com/theleeeo/laika/app/server"
	"github.com/theleeeo/laika/app/webui"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// stubBackend satisfies core.SearchBackend; GetCapabilities never touches it.
type stubBackend struct{}

func (stubBackend) Upsert(context.Context, string, string, any, int64) error { return nil }
func (stubBackend) BulkUpsert(context.Context, []core.BulkItem) error        { return nil }
func (stubBackend) Delete(context.Context, string, string) error             { return nil }
func (stubBackend) Search(context.Context, core.SearchRequest, string, *resource.VersionConfig) (core.SearchResponse, error) {
	return core.SearchResponse{}, nil
}

// TestUI_ServedAndCoexistsWithServiceRoutes verifies the exact wiring main.go
// uses: the demo console is served at "/" while the Connect service routes stay
// reachable on their own longer prefixes.
func TestUI_ServedAndCoexistsWithServiceRoutes(t *testing.T) {
	idx := core.New(core.Config{
		ES: stubBackend{},
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
	mux.Handle(searchconnect.NewSearchServiceHandler(server.NewSearcher(idx)))
	mux.Handle("/", webui.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. GET "/" serves the self-contained console HTML.
	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), "<title>laika")
	// The client hardcodes the procedure paths; assert they're present so the
	// UI and server can never silently drift apart.
	require.Contains(t, string(body), searchconnect.SearchServiceGetCapabilitiesProcedure)
	require.Contains(t, string(body), searchconnect.SearchServiceSearchProcedure)

	// 2. The GetCapabilities procedure still routes to the service, not the
	//    file server, and returns the configured resource as JSON.
	cresp, err := http.Post(
		srv.URL+searchconnect.SearchServiceGetCapabilitiesProcedure,
		"application/json",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	cbody, _ := io.ReadAll(cresp.Body)
	cresp.Body.Close()
	require.Equal(t, http.StatusOK, cresp.StatusCode, "body: %s", cbody)
	require.Contains(t, string(cbody), `"product"`)
	require.Contains(t, string(cbody), `"fields.name"`)
}
