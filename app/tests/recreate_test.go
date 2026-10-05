package tests

// Delete and recreate of one resource: a resource is hard-deleted (row and
// document) and then created again under the same id. The recreate gets a new
// row, and its builds must still win in Elasticsearch over whatever the
// delete left behind there.

import (
	"context"
	"errors"
	"time"

	"github.com/theleeeo/laika/backend/elasticsearch"
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

// Test_Recreate_LateBuildPathDelete_KeepsRecreatedDocument: a/1 with child b/x
// is created and built ("old"). It is then removed at the source and an
// update — not a delete notification — is registered on X, so the delete runs
// on the build path. The interleaving, forced through the gated search
// backend:
//
//  1. X claims a/1 and its owned build A begins (Build Sequence n), fetches
//     a/1, gets nil, and is held before its ES delete.
//  2. a/1 is recreated at the source ("new", child b/x still related) and its
//     ChangeCreated is registered: A owns the row, so the registration only
//     marks it and submits nothing.
//  3. Build B, a direct idx.Build that owns nothing, begins after A (Build
//     Sequence above n), fetches a/1, writes the "new" document and stores
//     the edge set a/1 -> b/x at its sequence.
//  4. A is released: its ES delete and its edge removal run, late.
//  5. Any follow-up A's finish hands on fails at its fetch, so it writes and
//     removes nothing (see below).
//
// Before L2.1 the build path passed 0 to handleDelete: the ES delete was a
// plain, unversioned delete that removed B's newer document, and
// RemoveResource dropped every edge set whatever build stored it, so B's
// a/1 -> b/x went too — a recreated resource left unindexed and cut off from
// its child's fanout. With A's Build Sequence on both, Elasticsearch rejects
// the delete against B's higher-versioned document and the Store keeps the
// set B stamped above it.
func (t *TestSuite) Test_Recreate_LateBuildPathDelete_KeepsRecreatedDocument() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	backend := &gatingBackend{SearchBackend: elasticsearch.New(t.esClient, true)}
	x := t.newIndexer(DefaultResourceConfig, core.Config{ES: backend})
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	bx := map[string]any{"id": "x", "a_id": "1", "field1": "bx"}
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "old"})
	t.fakeProvider.SetResource("b", "x", bx)
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{bx})
	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "b", ResourceID: "x", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(x.RegisterChange(ctx, n))
	}
	t.Require().NoError(x.WaitForIdle(waitCtx))
	fields, ok := t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "the original a/1 is indexed")
	t.Require().Equal("old", fields["field1"])
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"), "the build stored the edge a/1 -> b/x")

	// 1. A: the build path finds a/1 gone and is held before its ES delete.
	reached := backend.armDelete("1")
	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated, Version: 2,
	}))
	t.awaitGate(reached, "A's build-path delete of a/1 to reach its ES delete")
	ownerSeq, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(ownerSeq, "A's build owns a/1")

	// 2. The recreate only marks a/1: A owns it.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "new"})
	buildsBefore := t.resourceRebuildCounter("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 3,
	}))
	afterRecreate, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(afterRecreate)
	t.Require().Equal(*ownerSeq, *afterRecreate, "the recreate's mark did not claim a/1: A still owns it")
	t.Require().Equal(buildsBefore, t.resourceRebuildCounter("a", "1"), "the recreate submits nothing while A owns a/1")

	// 3. B: a direct build, owning nothing, begins after A and writes "new".
	t.Require().NoError(t.idx.Build(ctx, core.BuildArgs{ResourceType: "a", ResourceIds: []string{"1"}}))
	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "B began after A")
	fields, ok = t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "B indexed the recreated a/1")
	t.Require().Equal("new", fields["field1"])
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"), "B stored the edge a/1 -> b/x")

	// 5, armed before 4. The final state must show only what A's late delete
	// did. The recreate moved stale_seq past A's, so should A's FinishOwned
	// still own the row it re-claims it for a follow-up, which would rebuild
	// a/1 from the source and re-write the document and the edge, hiding a
	// lost one. (Today B's ClearStale settled the mark and dropped A's
	// ownership with it, so A's finish hands on nothing; the guard keeps the
	// case meaningful if that changes.) The follow-up's fetch fails instead,
	// and a build whose fetch fails writes and removes nothing: its plan
	// errors before any write, and the build releases its ownership and
	// leaves the mark for the sweep.
	t.fakeProvider.SetError("a", "1", errors.New("follow-up fetch held off by the test"))

	// 4. A's ES delete and edge removal run, after B's write.
	backend.releaseDelete()
	t.Require().NoError(x.WaitForIdle(waitCtx))
	t.fakeProvider.SetError("a", "1", nil)

	fields, ok = t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "A's late delete, at a lower Build Sequence than B's write, must leave the recreated document")
	t.Require().Equal("new", fields["field1"], "the recreated document keeps B's data")
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"),
		"A's edge removal must leave the edge set B stored at a higher Build Sequence")
}
