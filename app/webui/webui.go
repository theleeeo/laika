// Package webui serves a self-contained, single-file demo client for the
// search API. It reads GetCapabilities to render the filterable surface of each
// resource and POSTs Search requests over the Connect-JSON protocol — the same
// handlers the gRPC server exposes, on the same origin, so no CORS is involved.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html
var content embed.FS

// Handler returns an http.Handler that serves the demo UI. Mount it on "/"; the
// ServeMux longest-prefix rule keeps the Connect service routes ("/search.v1.*",
// "/index.v1.*") on their own handlers.
func Handler() http.Handler {
	sub, err := fs.Sub(content, ".")
	if err != nil {
		// The embedded FS is compiled in, so this cannot fail at runtime.
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
