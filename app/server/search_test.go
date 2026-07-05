package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/app/gen/search/v1"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
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

func TestFederatedSearch_EmptyResourcesInvalidArgument(t *testing.T) {
	srv := newSearcher()

	_, err := srv.FederatedSearch(context.Background(), connect.NewRequest(&search.FederatedSearchRequest{
		Query:     "anything",
		Resources: nil,
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// federatedSearcher builds a SearcherServer over two resources ("a" has a
// "region" field, "b" does not) wired to the given backend.
func federatedSearcher(backend core.SearchBackend) *SearcherServer {
	cfgA := &resource.Config{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{{Name: "region"}, {Name: "name"}}},
		},
	}
	cfgB := &resource.Config{
		Resource: "b",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}},
		},
	}
	cfgA.ApplyDefaults()
	cfgB.ApplyDefaults()
	return NewSearcher(core.New(core.Config{
		ES:        backend,
		Resources: resource.Configs{cfgA, cfgB},
	}))
}

func TestFederatedSearch_FilterFieldMissingFromTypeInvalidArgument(t *testing.T) {
	srv := federatedSearcher(&fakeBackend{})

	// "fields.region" exists on "a" but not "b"; filtering across both is invalid.
	_, err := srv.FederatedSearch(context.Background(), connect.NewRequest(&search.FederatedSearchRequest{
		Query:     "x",
		Resources: []string{"a", "b"},
		Filters:   []*search.Filter{{Field: "fields.region", Op: search.FilterOp_FILTER_OP_EQ, Value: "eu"}},
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestFederatedSearch_CommonFilterAndResponseMapping(t *testing.T) {
	backend := &fakeBackend{
		fedResponse: core.FederatedSearchResult{
			Total: 2,
			Hits: []core.FederatedRawHit{
				{Index: core.IndexName("a", 1), ID: "a1", Score: 9, Source: map[string]any{"name": "acme"}},
				{Index: core.IndexName("b", 1), ID: "b1", Score: 4},
			},
			IndexCounts: map[string]int64{core.IndexName("a", 1): 1, core.IndexName("b", 1): 1},
		},
	}
	srv := federatedSearcher(backend)

	// "fields.name" is common to both a and b, so the filter is accepted.
	resp, err := srv.FederatedSearch(context.Background(), connect.NewRequest(&search.FederatedSearchRequest{
		Query:     "acme",
		Resources: []string{"a", "b"},
		Filters:   []*search.Filter{{Field: "fields.name", Op: search.FilterOp_FILTER_OP_EQ, Value: "acme"}},
	}))
	require.NoError(t, err)

	// Global filter forwarded to core.
	require.Len(t, backend.fedParams.Filters, 1)
	require.Equal(t, "fields.name", backend.fedParams.Filters[0].Field)

	msg := resp.Msg
	require.EqualValues(t, 2, msg.Total)
	require.Len(t, msg.Hits, 2)
	require.Equal(t, "a", msg.Hits[0].Resource)
	require.Equal(t, "a1", msg.Hits[0].Id)
	require.Equal(t, "acme", msg.Hits[0].Source.Fields["name"].GetStringValue())
	require.Equal(t, "b", msg.Hits[1].Resource)
	require.Nil(t, msg.Hits[1].Source) // no source when the hit carried none
	require.Equal(t, []*search.ResourceCount{
		{Resource: "a", Count: 1},
		{Resource: "b", Count: 1},
	}, msg.Counts)
}

func TestGetCapabilities_EmptyConfigReturnsNoResources(t *testing.T) {
	srv := newSearcher()

	resp, err := srv.GetCapabilities(context.Background(), connect.NewRequest(&search.GetCapabilitiesRequest{}))

	require.NoError(t, err)
	require.Empty(t, resp.Msg.Resources)
}
