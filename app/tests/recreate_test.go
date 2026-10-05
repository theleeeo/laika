package tests

// Delete and recreate of one resource: a resource is hard-deleted (row and
// document) and then created again under the same id. The recreate gets a new
// row, and its builds must still win in Elasticsearch over whatever the
// delete left behind there.

import (
	"github.com/theleeeo/laika/core"
)

// Test_Recreate_WithinGCDeletes_IndexesNewData: a/1 is created and built
// twice, deleted (row hard-deleted, document removed), and recreated with new
// data immediately — well inside Elasticsearch's index.gc_deletes window (60s
// by default), during which ES remembers a deleted document's version. The
// recreate's build must land the new document and clear the mark.
//
// Before L2.1, build_idx and stale_seq were per-row counters: the hard delete
// dropped the row, so the recreated row restarted both at 1. The recreate's
// build wrote with version_type=external_gte at that build_idx, lower than the
// version ES remembered for the deleted document, so the write lost with a
// version conflict. The build treated the conflict as a harmless loss to a
// newer write, its finish cleared the mark, and a/1 was left unindexed.
func (t *TestSuite) Test_Recreate_WithinGCDeletes_IndexesNewData() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "old"})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	}))
	t.worker.Drain(ctx)
	// A second build lifts the document's version further above 1. Even after
	// one build, the unversioned delete leaves its tombstone at version 2.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "older"})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated, Version: 2,
	}))
	t.worker.Drain(ctx)
	fields, ok := t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "the original a/1 is indexed")
	t.Require().Equal("older", fields["field1"])

	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.worker.Drain(ctx)
	t.Require().Equal(0, t.resourceRowCount("a", "1"), "the delete hard-deletes the row")
	t.Require().False(t.docExists("a", "1"), "the delete removes the document")

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "new"})
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 3,
	}))
	t.worker.Drain(ctx)

	t.Require().Nil(t.staleSince("a", "1"), "the recreate's build clears the mark")
	fields, ok = t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "the recreate within gc_deletes must index its document")
	t.Require().Equal("new", fields["field1"], "the recreated document carries the new data")
}
