package tests

import (
	"context"
	"time"

	"github.com/theleeeo/laika/core"
)

// Test_ForwardWalk_IndexesResourcesCreatedWithoutANotification: a resource
// its source has but no notification ever named is indexed by the type's
// forward walk — its row in Postgres, its document in Elasticsearch — across
// the listing's pages, each listing asking for the configured page size.
func (t *TestSuite) Test_ForwardWalk_IndexesResourcesCreatedWithoutANotification() {
	t.setResourceConfig(DefaultResourceConfig)
	x := t.newIndexer(DefaultResourceConfig, core.Config{
		ForwardWalks: map[string]core.ForwardWalkConfig{"a": {PageSize: 2, PageInterval: time.Millisecond}},
	})

	t.fakeProvider.SetPageSize(1)
	for _, id := range []string{"1", "2", "3"} {
		t.fakeProvider.SetResource("a", id, map[string]any{"id": id, "field1": "unnotified" + id})
	}

	res, err := x.ForwardWalkNow(t.T().Context(), "a")
	t.Require().NoError(err)
	t.Require().Equal(core.ForwardWalkResult{Walks: 1}, res)

	for _, id := range []string{"1", "2", "3"} {
		t.Require().Truef(t.resourceTracked("a", id), "a/%s must have a row in Postgres", id)
		t.Require().Nilf(t.staleSince("a", id), "a/%s must be settled, not left stale", id)
		t.Require().Truef(t.docExists("a", id), "a/%s must have a document in Elasticsearch", id)
	}
	calls := t.fakeProvider.ListCalls()
	t.Require().Len(calls, 3, "one listing per page of one")
	for i, c := range calls {
		t.Require().EqualValuesf(2, c.PageSize, "listing %d must ask for the configured page size", i)
	}
}

// Test_ForwardWalk_WalksOncePerMetadataMap: a run walks the type once per
// map its Metadata func returns, in that order, each walk listing with its
// own map.
func (t *TestSuite) Test_ForwardWalk_WalksOncePerMetadataMap() {
	t.setResourceConfig(DefaultResourceConfig)
	tenants := []map[string]string{{"tenant": "t1"}, {"tenant": "t2"}}
	x := t.newIndexer(DefaultResourceConfig, core.Config{
		ForwardWalks: map[string]core.ForwardWalkConfig{"a": {
			Metadata: func(context.Context) ([]map[string]string, error) { return tenants, nil },
		}},
	})
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "unnotified"})

	res, err := x.ForwardWalkNow(t.T().Context(), "a")
	t.Require().NoError(err)
	t.Require().Equal(core.ForwardWalkResult{Walks: 2}, res)

	var got []map[string]string
	for _, c := range t.fakeProvider.ListCalls() {
		if c.ResourceType == "a" {
			got = append(got, c.Metadata)
		}
	}
	t.Require().Equal(tenants, got, "each walk lists with its own map, in the func's order")
	t.Require().True(t.resourceTracked("a", "1"))
	t.Require().True(t.docExists("a", "1"))
}
