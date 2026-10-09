package elasticsearch

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetAliasTargets_ReturnsEveryTargetSorted(t *testing.T) {
	body := `{"users_search_v3":{"aliases":{"users_search":{}}},` +
		`"users_search_v1":{"aliases":{"users_search":{}}},` +
		`"users_search_v2":{"aliases":{"users_search":{}}}}`
	c := esClientReturning(t, http.StatusOK, body, func(req *http.Request) {
		require.Equal(t, "/_alias/users_search", req.URL.Path)
	})

	targets, err := c.GetAliasTargets(context.Background(), "users_search")
	require.NoError(t, err)
	require.Equal(t, []string{"users_search_v1", "users_search_v2", "users_search_v3"}, targets)
}

func TestGetAliasTargets_NotFoundIsNone(t *testing.T) {
	c := esClientReturning(t, http.StatusNotFound, `{"error":"alias [users_search] missing","status":404}`, nil)

	targets, err := c.GetAliasTargets(context.Background(), "users_search")
	require.NoError(t, err)
	require.Empty(t, targets)
}

func TestGetAliasTargets_ErrorStatusIsAnError(t *testing.T) {
	c := esClientReturning(t, http.StatusInternalServerError, `{"error":"oops"}`, nil)

	_, err := c.GetAliasTargets(context.Background(), "users_search")
	require.Error(t, err)
}
