package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/gen/search/v1"
	"github.com/theleeeo/laika/core"
)

// newSearcher builds a SearcherServer over an Indexer with no configured
// resources. This is enough to exercise the request-shaping and error-mapping
// in the Connect handler: searchBase rejects an unknown resource before it
// touches the (nil) search backend.
func newSearcher() *SearcherServer {
	return NewSearcher(core.New(core.Config{}))
}

func TestSearch_UnknownResourceMapsToFailedPrecondition(t *testing.T) {
	srv := newSearcher()

	_, err := srv.Search(context.Background(), connect.NewRequest(&search.SearchRequest{
		Resource: "does-not-exist",
		Query:    "anything",
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// FederatedSearch is a stub until the adapter + validation land in #17; assert
// it advertises itself as unimplemented rather than pretending to succeed.
func TestFederatedSearch_NotYetImplemented(t *testing.T) {
	srv := newSearcher()

	_, err := srv.FederatedSearch(context.Background(), connect.NewRequest(&search.FederatedSearchRequest{
		Query:     "anything",
		Resources: []string{"a"},
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
}

func TestGetCapabilities_EmptyConfigReturnsNoResources(t *testing.T) {
	srv := newSearcher()

	resp, err := srv.GetCapabilities(context.Background(), connect.NewRequest(&search.GetCapabilitiesRequest{}))

	require.NoError(t, err)
	require.Empty(t, resp.Msg.Resources)
}
