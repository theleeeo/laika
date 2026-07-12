package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/theleeeo/laika/app/gen/index/v1/indexconnect"
	"github.com/theleeeo/laika/app/gen/search/v1/searchconnect"
)

// TestNewServeMuxes_Routing asserts the read/write surfaces land on the right
// listener: search on public, indexing on admin, and neither leaking onto the
// other port. Unimplemented handlers are enough — this only checks routing, not
// behavior, via ServeMux.Handler's matched pattern.
func TestNewServeMuxes_Routing(t *testing.T) {
	public, admin := newServeMuxes(
		indexconnect.UnimplementedIndexServiceHandler{},
		searchconnect.UnimplementedSearchServiceHandler{},
	)

	const (
		searchPath = "/" + searchconnect.SearchServiceName + "/Search"
		indexPath  = "/" + indexconnect.IndexServiceName + "/NotifyChange"
	)

	routed := func(mux *http.ServeMux, path string) bool {
		_, pattern := mux.Handler(httptest.NewRequest(http.MethodPost, path, nil))
		return pattern != ""
	}

	for _, tc := range []struct {
		name string
		got  bool
		want bool
	}{
		{"public serves search", routed(public, searchPath), true},
		{"public rejects index", routed(public, indexPath), false},
		{"admin serves index", routed(admin, indexPath), true},
		{"admin rejects search", routed(admin, searchPath), false},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: routed=%v, want %v", tc.name, tc.got, tc.want)
		}
	}
}
