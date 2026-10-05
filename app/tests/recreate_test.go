package tests

// Delete and recreate of one resource: a resource is hard-deleted (row and
// document) and then created again under the same id. The recreate gets a new
// row, and its builds must still win in Elasticsearch over whatever the
// delete left behind there.

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
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

// createAWithChildBX creates a/1 ("old") with child b/x at the source,
// registers both on x and requires a/1's document and its edge a/1 -> b/x.
// The source keeps relating b/x to a/1 until the test changes it.
func (t *TestSuite) createAWithChildBX(x *core.Indexer, waitCtx context.Context) {
	ctx := t.T().Context()
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

	t.createAWithChildBX(x, waitCtx)

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
	fields, ok := t.docFields(core.IndexName("a", 1), "1")
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

// Test_Recreate_LateNotifiedDelete_KeepsRecreatedDocument: a/1 with child b/x
// is created and built ("old"), then removed at the source and its
// ChangeDeleted registered on X. The interleaving, forced through the gated
// search backend:
//
//  1. X claims the tombstone and its owned delete D runs: it takes its Build
//     Sequence (BeginDelete, n) and is held before its ES delete.
//  2. a/1 is recreated at the source ("new", child b/x still related) and its
//     ChangeCreated is registered: D owns the row, so the registration only
//     marks it and submits nothing.
//  3. Build B, a direct idx.Build that owns nothing, begins after D's bump
//     (Build Sequence above n), fetches a/1, writes the "new" document and
//     stores the edge set a/1 -> b/x at its sequence.
//  4. D is released: its ES delete and its edge removal run, late, and its
//     finish (DeleteResourceIfSeq) finds stale_seq moved, so it keeps the
//     row.
//  5. Any follow-up D's finish hands on fails at its fetch, so it writes and
//     removes nothing (see below).
//
// Unlike Test_Owner_DeleteHeldBeforeESDelete_RecreateBuildsAfterIt, where the
// recreate's build is the delete's follow-up and runs after it, B writes
// before the late delete. Before L2.1 the notified delete took no Build
// Sequence: its ES delete was a plain, unversioned delete that removed B's
// newer document, and RemoveResource dropped every edge set whatever build
// stored it, so B's a/1 -> b/x went too — a recreated resource left unindexed
// and cut off from its child's fanout. With D's Build Sequence on both,
// Elasticsearch rejects the delete against B's higher-versioned document and
// the Store keeps the set B stamped above it.
func (t *TestSuite) Test_Recreate_LateNotifiedDelete_KeepsRecreatedDocument() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	backend := &gatingBackend{SearchBackend: elasticsearch.New(t.esClient, true)}
	x := t.newIndexer(DefaultResourceConfig, core.Config{ES: backend})
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	t.createAWithChildBX(x, waitCtx)

	// 1. D: the owned delete of the tombstone is held before its ES delete.
	reached := backend.armDelete("1")
	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.awaitGate(reached, "D, the owned delete of a/1, to reach its ES delete")
	ownerSeq, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(ownerSeq, "D owns the tombstone")

	// 2. The recreate only marks a/1: D owns it.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "new"})
	buildsBefore := t.resourceRebuildCounter("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 2,
	}))
	afterRecreate, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(afterRecreate)
	t.Require().Equal(*ownerSeq, *afterRecreate, "the recreate's mark did not claim a/1: D still owns it")
	t.Require().Equal(buildsBefore, t.resourceRebuildCounter("a", "1"), "the recreate submits nothing while D owns a/1")

	// 3. B: a direct build, owning nothing, begins after D's bump and writes
	// "new".
	t.Require().NoError(t.idx.Build(ctx, core.BuildArgs{ResourceType: "a", ResourceIds: []string{"1"}}))
	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "B began")
	fields, ok := t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "B indexed the recreated a/1")
	t.Require().Equal("new", fields["field1"])
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"), "B stored the edge a/1 -> b/x")

	// 5, armed before 4. The final state must show only what D's late delete
	// did. The recreate moved stale_seq past D's, so should D still own the
	// row at its finish, DeleteResourceIfSeq re-claims it for a follow-up
	// build, which would rebuild a/1 from the source and re-write the
	// document and the edge, hiding a lost one. (Today B's ClearStale settled
	// the mark and dropped D's ownership with it, so D's finish keeps the row
	// and hands on nothing; the guard keeps the case meaningful if that
	// changes.) The follow-up's fetch fails instead, and a build whose fetch
	// fails writes and removes nothing: its plan errors before any write, and
	// the build releases its ownership and leaves the mark for the sweep.
	t.fakeProvider.SetError("a", "1", errors.New("follow-up fetch held off by the test"))

	// 4. D's ES delete, edge removal and finish run, after B's write.
	backend.releaseDelete()
	t.Require().NoError(x.WaitForIdle(waitCtx))
	t.fakeProvider.SetError("a", "1", nil)

	t.Require().Equal(1, t.resourceRowCount("a", "1"), "D's finish must not hard-delete the recreated row")
	fields, ok = t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "D's late delete, at a lower Build Sequence than B's write, must leave the recreated document")
	t.Require().Equal("new", fields["field1"], "the recreated document keeps B's data")
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"),
		"D's edge removal must leave the edge set B stored at a higher Build Sequence")
}

// deleteCountingBackend is a SearchBackend that counts Delete calls per
// docID before delegating.
type deleteCountingBackend struct {
	core.SearchBackend

	mu      sync.Mutex
	deletes map[string]int
}

// Delete implements [core.SearchBackend].
func (b *deleteCountingBackend) Delete(ctx context.Context, index, docID string, version int64) error {
	b.mu.Lock()
	if b.deletes == nil {
		b.deletes = map[string]int{}
	}
	b.deletes[docID]++
	b.mu.Unlock()
	return b.SearchBackend.Delete(ctx, index, docID, version)
}

// deleteCalls returns how many Delete calls of docID ran on b.
func (b *deleteCountingBackend) deleteCalls(docID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deletes[docID]
}

// Test_Recreate_DeleteLeaseLapsedBeforeBump_AbortsAndKeepsDocument: a/1 with
// child b/x is created and built ("old") on X, then removed at the source and
// its ChangeDeleted registered on X. a/1's Build Sequence is now N. The
// interleaving, forced through the gated store:
//
//  1. X claims the tombstone and submits its owned delete D. D's pool task
//     renews D's lease at dequeue, and D is held on entry to BeginDelete —
//     before its Build Sequence bump.
//  2. D's lease lapses (owner_since is backdated past it).
//  3. a/1 is recreated at the source ("new", child b/x still related) and its
//     ChangeCreated is registered on the suite's indexer: the lapsed lease
//     lets the mark claim the row under a new owner token, and the
//     recreate's owned build R begins at N+1, writes the "new" document,
//     stores the edge set a/1 -> b/x and finishes, clearing the mark.
//  4. D is released into BeginDelete: the row is no longer a tombstone, so
//     D is superseded and aborts. It issues no ES delete and removes no
//     edges, and as it no longer owns the row, its finish neither re-claims
//     it nor submits a follow-up.
//
// Without the abort D would bump the Build Sequence to N+2, above R's N+1,
// and its versioned ES delete and bounded edge removal would both be
// accepted: the recreated document and R's edge set would go, with the mark
// already cleared — lost until the next change. The recreate is registered on
// the suite's indexer, not X, because X.WaitForIdle would wait on the held
// D. No sweep runs: the final state is what the inline work alone left.
func (t *TestSuite) Test_Recreate_DeleteLeaseLapsedBeforeBump_AbortsAndKeepsDocument() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()
	a1 := model.Resource{Type: "a", Id: "1"}

	gated := newGatingStore(t.store)
	backend := &deleteCountingBackend{SearchBackend: elasticsearch.New(t.esClient, true)}
	x := t.newIndexer(DefaultResourceConfig, core.Config{ES: backend, Store: gated})
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	t.createAWithChildBX(x, waitCtx)

	// 1. D: the owned delete is held on entry to BeginDelete. The deferred
	// release frees a D still held when the test fails, before AfterTest and
	// X's shutdown.
	reached := gated.armBeginDelete(a1)
	defer gated.releaseBeginDelete()
	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.awaitGate(reached, "D, the owned delete of a/1, to reach BeginDelete")
	dToken, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(dToken, "D owns the tombstone")

	// 2. D's lease lapses.
	_, err := t.pool.Exec(ctx,
		`UPDATE resources SET owner_since = now() - interval '1 hour' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)

	// 3. R: the recreate claims the row and is built.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "new"})
	buildsBefore := t.resourceRebuildCounter("a", "1")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 2,
	}))
	rToken, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(rToken, "the recreate's mark claimed a/1")
	t.Require().NotEqual(*dToken, *rToken, "the recreate claimed a/1 under a new owner token: D's lease had lapsed")
	t.worker.Drain(ctx)
	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "the recreate's build began")
	fields, ok := t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "the recreate's build indexed a/1")
	t.Require().Equal("new", fields["field1"])
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"), "the recreate's build stored the edge a/1 -> b/x")

	// 4. D enters BeginDelete, superseded.
	gated.releaseBeginDelete()
	t.Require().NoError(x.WaitForIdle(waitCtx))

	t.Require().Equal(0, backend.deleteCalls("1"), "a superseded delete issues no ES delete")
	t.Require().Equal(0, gated.removeCalls(a1), "a superseded delete removes no edges")
	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "D's finish submits no follow-up")
	t.Require().Equal(1, t.resourceRowCount("a", "1"), "the recreated row stays")
	t.Require().Nil(t.staleSince("a", "1"), "nothing is left stale for a sweep")
	fields, ok = t.docFields(core.IndexName("a", 1), "1")
	t.Require().True(ok, "the delete, superseded before its bump, must leave the recreated document")
	t.Require().Equal("new", fields["field1"], "the recreated document keeps the recreate's data")
	t.Require().Equal([]string{"b/x"}, t.childEdges("a", "1"), "the delete must leave the recreate's edge set")
}
