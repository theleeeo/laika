package tests

import (
	"connectrpc.com/connect"

	"github.com/theleeeo/laika/app/gen/index/v1"
	"github.com/theleeeo/laika/app/server"
	"github.com/theleeeo/laika/core"
)

// NotifyChangeBatch commits a batch in one statement and reports a status per
// notification: a stale entry is rejected alone, the others are indexed.
func (t *TestSuite) Test_NotifyChangeBatch_StaleEntry_IndexesTheOthers() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "first"})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 2,
	}))
	t.worker.Drain(ctx)
	t.Require().Equal(int64(1), t.resourceRebuildCounter("a", "1"))

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "stale"})
	t.fakeProvider.SetResource("a", "2", map[string]any{"id": "2", "field1": "second"})
	t.fakeProvider.SetResource("a", "3", map[string]any{"id": "3", "field1": "third"})

	resp, err := server.NewIndexer(t.idx).NotifyChangeBatch(ctx, connect.NewRequest(&index.NotifyChangeBatchRequest{
		Notifications: []*index.ChangeNotification{
			{Kind: index.ChangeKind_CHANGE_KIND_CREATED, ResourceType: "a", ResourceId: "2", Version: 1},
			{Kind: index.ChangeKind_CHANGE_KIND_UPDATED, ResourceType: "a", ResourceId: "1", Version: 2},
			{Kind: index.ChangeKind_CHANGE_KIND_CREATED, ResourceType: "a", ResourceId: "3"},
		},
	}))
	t.Require().NoError(err)
	t.Require().Equal([]index.ChangeStatus{
		index.ChangeStatus_CHANGE_STATUS_ACCEPTED,
		index.ChangeStatus_CHANGE_STATUS_STALE,
		index.ChangeStatus_CHANGE_STATUS_ACCEPTED,
	}, resp.Msg.Statuses)
	t.worker.Drain(ctx)

	t.Require().True(t.docExists("a", "2"))
	t.Require().True(t.docExists("a", "3"))
	t.Require().Equal(int64(1), t.resourceVersion("a", "2"))

	// The stale entry wrote and scheduled nothing: same version, no rebuild.
	t.Require().Equal(int64(2), t.resourceVersion("a", "1"))
	t.Require().Equal(int64(1), t.resourceRebuildCounter("a", "1"))
	hits, err := t.idx.Search(ctx, core.SearchRequest{Resource: "a", Query: "stale"})
	t.Require().NoError(err)
	t.Require().Empty(hits.Hits)
}
