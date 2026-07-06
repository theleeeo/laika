package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWithCORS_Preflight verifies an OPTIONS preflight is answered directly
// with 204 and the expected Access-Control-Allow-* headers, without ever
// reaching the wrapped handler.
func TestWithCORS_Preflight(t *testing.T) {
	called := false
	h := withCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))

	req := httptest.NewRequest(http.MethodOptions, "/search.v1.SearchService/Search", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.False(t, called, "preflight must be short-circuited, not forwarded")
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")
	require.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "Content-Type")
}

// TestWithCORS_PassThrough verifies a normal request reaches the wrapped
// handler and still carries the cross-origin header on the response.
func TestWithCORS_PassThrough(t *testing.T) {
	h := withCORS(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/search.v1.SearchService/Search", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
}
