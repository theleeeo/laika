package tests

import (
	"context"
	"sync"
	"time"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// Build ownership (one Build owner per resource): a mark claims a row that has
// no live owner, only a claimed row is submitted, and the owner's finish runs
// at most one follow-up for the changes it kept from submitting. These cases
// pin the interleavings ownership exists for; every race among them is forced
// through provider gates or a gated search backend, never by timing.

// gatingBackend is a SearchBackend whose Delete of one armed docID closes
// reached on entry and then blocks until release is closed, before
// delegating.
type gatingBackend struct {
	core.SearchBackend

	mu      sync.Mutex
	docID   string
	reached chan struct{}
	release chan struct{}
}

// armDelete arms the gate for docID and returns its reached channel.
func (g *gatingBackend) armDelete(docID string) <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.docID = docID
	g.reached = make(chan struct{})
	g.release = make(chan struct{})
	return g.reached
}

// releaseDelete lets the held Delete through and disarms the gate.
func (g *gatingBackend) releaseDelete() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		close(g.release)
	}
	g.docID, g.reached, g.release = "", nil, nil
}

// Delete implements [core.SearchBackend].
func (g *gatingBackend) Delete(ctx context.Context, index, docID string, version int64) error {
	g.mu.Lock()
	var reached, release chan struct{}
	if g.docID != "" && g.docID == docID {
		reached, release = g.reached, g.release
		g.docID = "" // only the first Delete of the doc is held
	}
	g.mu.Unlock()

	if reached != nil {
		close(reached)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.SearchBackend.Delete(ctx, index, docID, version)
}

// ownerColumns reads the resource's owner_seq and owner_since; nil means no
// owner.
func (t *TestSuite) ownerColumns(resourceType, id string) (*int64, *time.Time) {
	var seq *int64
	var since *time.Time
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT owner_seq, owner_since FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&seq, &since)
	t.Require().NoError(err)
	return seq, since
}

// staleSeq reads the resource's stale_seq, which every mark bumps.
func (t *TestSuite) staleSeq(resourceType, id string) int64 {
	var seq int64
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT stale_seq FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&seq)
	t.Require().NoError(err)
	return seq
}

// relationField1 maps each denormalized relation of a hit to its field1
// value, by id.
func relationField1(hit core.SearchHit, relation string) map[string]string {
	out := map[string]string{}
	for _, r := range sourceList(hit.Source, relation) {
		out[fieldStr(r, "id")] = fieldStr(r, "field1")
	}
	return out
}

// Test_Owner_TwoInstances_OneOwnerBuildsWithOneFollowUp: instance A (the
// suite's indexer) registers a/1, claims it and builds it; the build fetches
// a/1 and is held in FetchRelated(b) after snapshotting b/x at bxv1. While it
// is held, the source moves b/x to bxv2 and instance B registers b/x's change:
// B claims and builds b/x itself, and marks Parent a/1 without claiming it, as
// A owns it — so B never fetches a/1. B settles before A is released.
// Released, A writes bxv1, its FinishOwned finds stale_seq moved and re-claims
// a/1 for exactly one follow-up, which fetches a/1 and b/x at bxv2. Without
// ownership B would have built a/1 concurrently with A's held build.
func (t *TestSuite) Test_Owner_TwoInstances_OneOwnerBuildsWithOneFollowUp() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "av1"})
	bx1 := map[string]any{"id": "x", "a_id": "1", "field1": "bxv1"}
	t.fakeProvider.SetResource("b", "x", bx1)
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{bx1})
	for _, n := range []core.Notification{
		{ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1},
		{ResourceType: "b", ResourceID: "x", Kind: core.ChangeCreated, Version: 1},
	} {
		t.Require().NoError(t.idx.RegisterChange(ctx, n))
	}
	t.worker.Drain(ctx)

	parents, err := t.st.GetParentResources(ctx, model.Resource{Type: "b", Id: "x"})
	t.Require().NoError(err)
	t.Require().Contains(parents, model.Resource{Type: "a", Id: "1"}, "the edge a/1 -> b/x must exist")
	t.Require().Nil(t.staleSince("a", "1"))

	instB := t.newIndexer(DefaultResourceConfig, core.Config{})

	fetchesBefore := t.fakeProvider.FetchCount("a", "1")
	buildsBefore := t.resourceRebuildCounter("a", "1")
	bxBuildsBefore := t.resourceRebuildCounter("b", "x")

	gate := t.fakeProvider.SetFetchGate("a1-related-b")
	t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeUpdated, Version: 2,
		Metadata: map[string]string{"test_related_gate": "a1-related-b", "test_related_gate_resource": "b"},
	}))
	t.awaitGate(gate, "A's build of a/1 to reach its FetchRelated(b) gate")
	heldIdx := t.resourceRebuildCounter("a", "1")
	t.Require().Equal(buildsBefore+1, heldIdx, "A's owned build of a/1 has begun")
	ownerSeq, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(ownerSeq, "A's build owns a/1")
	heldSeq := t.staleSeq("a", "1")

	bx2 := map[string]any{"id": "x", "a_id": "1", "field1": "bxv2"}
	t.fakeProvider.SetResource("b", "x", bx2)
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{bx2})
	t.Require().NoError(instB.RegisterChange(ctx, core.Notification{
		ResourceType: "b", ResourceID: "x", Kind: core.ChangeUpdated, Version: 2,
	}))
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	t.Require().NoError(instB.WaitForIdle(waitCtx))

	t.Require().Greater(t.staleSeq("a", "1"), heldSeq, "B's registration marked a/1")
	afterB, _ := t.ownerColumns("a", "1")
	t.Require().Equal(*ownerSeq, *afterB, "B's mark did not claim a/1: A still owns it")
	t.Require().Equal(bxBuildsBefore+1, t.resourceRebuildCounter("b", "x"), "B built b/x itself")
	t.Require().Equal(heldIdx, t.resourceRebuildCounter("a", "1"), "B submitted no build of a/1: A owns it")
	t.Require().Equal(1, t.fakeProvider.FetchCount("a", "1")-fetchesBefore, "only A's held build has fetched a/1")

	t.fakeProvider.ReleaseFetchGate("a1-related-b")
	t.worker.Drain(ctx)
	t.Require().NoError(instB.WaitForIdle(waitCtx))

	t.Require().Equal(2, t.fakeProvider.FetchCount("a", "1")-fetchesBefore, "A's build plus exactly one follow-up fetched a/1")
	t.Require().Equal(buildsBefore+2, t.resourceRebuildCounter("a", "1"), "A's build plus exactly one follow-up")
	t.Require().Nil(t.staleSince("a", "1"))
	seq, since := t.ownerColumns("a", "1")
	t.Require().Nil(seq, "the follow-up's finish drops the ownership")
	t.Require().Nil(since)
	t.Require().Equal(map[string]string{"x": "bxv2"}, relationField1(t.onlyHit("a"), "b"),
		"only the follow-up could deliver bxv2: A's build snapshotted bxv1")
}

// Test_Owner_DeleteHeldBeforeESDelete_RecreateBuildsAfterIt: a/1 is created
// and built on X. Its delete is registered on X, which claims the tombstone
// and submits the owned delete; the delete is held in the search backend
// before its ES delete. While it is held, a/1 is recreated at the source (rv2)
// and its ChangeCreated is registered on X: the row has a live owner (the
// delete), so nothing is submitted. Released, the delete removes the document,
// DeleteResourceIfSeq finds stale_seq moved, keeps the row and re-claims it,
// and exactly one follow-up build writes rv2. Without ownership the recreate's
// build ran during the held delete, and the delete then removed its document.
func (t *TestSuite) Test_Owner_DeleteHeldBeforeESDelete_RecreateBuildsAfterIt() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	backend := &gatingBackend{SearchBackend: elasticsearch.New(t.esClient, true)}
	x := t.newIndexer(DefaultResourceConfig, core.Config{ES: backend})
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "rv1"})
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 1,
	}))
	t.Require().NoError(x.WaitForIdle(waitCtx))
	t.Require().True(t.docExists("a", "1"))

	reached := backend.armDelete("1")
	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.awaitGate(reached, "the owned delete of a/1 to reach its ES delete")
	ownerSeq, _ := t.ownerColumns("a", "1")
	t.Require().NotNil(ownerSeq, "the delete owns the tombstone")

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "rv2"})
	buildsBefore := t.resourceRebuildCounter("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeCreated, Version: 2,
	}))
	afterRecreate, _ := t.ownerColumns("a", "1")
	t.Require().Equal(*ownerSeq, *afterRecreate, "the recreate's mark did not claim the row, so it submitted nothing")
	t.Require().Equal(buildsBefore, t.resourceRebuildCounter("a", "1"), "the recreate submits nothing while the delete owns the row")

	backend.releaseDelete()
	t.Require().NoError(x.WaitForIdle(waitCtx))

	t.Require().Equal(buildsBefore+1, t.resourceRebuildCounter("a", "1"), "exactly one follow-up build")
	t.Require().Equal(1, t.resourceRowCount("a", "1"), "the recreate keeps the row")
	t.Require().Nil(t.staleSince("a", "1"))
	t.Require().True(t.docExists("a", "1"), "the follow-up build lands the recreated document")
	fields, _ := t.onlyHit("a").Source["fields"].(map[string]any)
	t.Require().Equal("rv2", fields["field1"])
}

// Test_Owner_DeletedParentMarkedByItsChild_IsDeletedByItsDelete: a/1 with
// child b/x is created and built on X, then removed at the source and its
// ChangeDeleted registered on X. Its edge a/1 -> b/x stays until a delete
// removes it, so every change of b/x marks the tombstone as b/x's Parent. The
// interleaving, forced through the gated store:
//
//  1. X claims the tombstone and its owned delete D is held on entry to
//     BeginDelete.
//  2. b/x changes, and its registration marks a/1 through the edge: stale_seq
//     moves, the row stays a tombstone, and D still owns it.
//  3. D is released; the gate is re-armed for the next BeginDelete of a/1.
//     D deletes a/1's document and its edges at its bump, and its finish
//     (DeleteResourceIfSeq) sees the moved mark and hands on a follow-up
//     delete F, held on entry to BeginDelete. At this point a/1 must be gone
//     from Elasticsearch and its edges removed: D did the delete.
//  4. b/x changes again, marking a/1 again, and F is released: F deletes
//     again at its own bump, hands on one more follow-up, and that one,
//     with no change after it, hard-deletes the row.
//
// Before, a delete whose tombstone's mark moved was superseded and deleted
// nothing, edges included: under steady child traffic every delete round was
// superseded by the next Parent mark and a/1 stayed searchable.
func (t *TestSuite) Test_Owner_DeletedParentMarkedByItsChild_IsDeletedByItsDelete() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()
	a1 := model.Resource{Type: "a", Id: "1"}

	gated := newGatingStore(t.store)
	x := t.newIndexer(DefaultResourceConfig, core.Config{Store: gated})
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	t.createAWithChildBX(x, waitCtx)

	// childChanges registers a change of b/x on the suite's indexer, not X,
	// whose WaitForIdle would wait on the held delete, and requires it to
	// have marked the tombstone a/1 as its Parent, leaving it to its owner.
	bxVersion := int64(1)
	childChanges := func() {
		before := t.staleSeq("a", "1")
		owner, _ := t.ownerColumns("a", "1")
		bxVersion++
		t.Require().NoError(t.idx.RegisterChange(ctx, core.Notification{
			ResourceType: "b", ResourceID: "x", Kind: core.ChangeUpdated, Version: bxVersion,
		}))
		t.worker.Drain(ctx)
		t.Require().Greater(t.staleSeq("a", "1"), before, "b/x's registration marks a/1 through the edge a/1 -> b/x")
		var deleted bool
		t.Require().NoError(t.pool.QueryRow(ctx,
			`SELECT deleted FROM resources WHERE type='a' AND id='1'`).Scan(&deleted))
		t.Require().True(deleted, "the Parent mark keeps a/1 a tombstone")
		after, _ := t.ownerColumns("a", "1")
		t.Require().Equal(owner, after, "the Parent mark leaves a/1 to the delete that owns it")
	}

	// 1. D is held on entry to BeginDelete. The deferred release frees a
	// delete still held when the test fails, before AfterTest and X's
	// shutdown.
	reached := gated.armBeginDelete(a1)
	defer gated.releaseBeginDelete()
	t.fakeProvider.DeleteResource("a", "1")
	t.Require().NoError(x.RegisterChange(ctx, core.Notification{
		ResourceType: "a", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.awaitGate(reached, "D, the owned delete of a/1, to reach BeginDelete")

	// 2. The child marks the tombstone.
	childChanges()

	// 3. D runs; its follow-up F is held.
	reached = gated.passBeginDelete()
	t.awaitGate(reached, "F, D's follow-up delete of a/1, to reach BeginDelete")
	t.Require().False(t.docExists("a", "1"), "D, whose tombstone a child marked, must still delete a/1's document")
	_, ok := t.docFields(core.IndexName("a", 1), "1")
	t.Require().False(ok, "D must delete a/1's document from every version's index")
	t.Require().Empty(t.childEdges("a", "1"), "D must remove a/1's edges")
	t.Require().Equal(1, gated.removeCalls(a1), "D removed a/1's edges")
	t.Require().Equal(1, t.resourceRowCount("a", "1"), "D's finish keeps the tombstone for F: its mark moved")

	// 4. The child marks the tombstone again; F runs, and the follow-up it
	// hands on finishes the delete.
	childChanges()
	gated.releaseBeginDelete()
	t.Require().NoError(x.WaitForIdle(waitCtx))

	t.Require().Equal(3, gated.removeCalls(a1), "F and its follow-up each removed a/1's edges again")
	t.Require().Equal(0, t.resourceRowCount("a", "1"), "the last follow-up, with no change after it, hard-deletes a/1's row")
	t.Require().False(t.docExists("a", "1"), "a/1 stays deleted")
	t.Require().Empty(t.childEdges("a", "1"), "a/1's edges stay removed")
	var relations int
	t.Require().NoError(t.pool.QueryRow(ctx,
		`SELECT count(*) FROM relations WHERE resource='a' AND resource_id='1'`).Scan(&relations))
	t.Require().Zero(relations, "no relations row of a/1 is left")
}

// Test_Owner_SweepSkipsLiveLease_BuildsOnceExpired: an instance claims a/1
// (MarkStale with a lease) and crashes before submitting. A sweep while the
// claim's lease is live skips a/1: it builds nothing. Once the lease has
// expired (owner_since backdated past it), the next sweep claims a/1 and
// builds it, and the build's finish clears the mark and the ownership.
func (t *TestSuite) Test_Owner_SweepSkipsLiveLease_BuildsOnceExpired() {
	t.setResourceConfig(DefaultResourceConfig)
	ctx := t.T().Context()

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "field1": "sv1"})
	owned, err := t.st.MarkStale(ctx, []model.Resource{{Type: "a", Id: "1"}}, time.Minute)
	t.Require().NoError(err)
	t.Require().Len(owned, 1, "the crashed instance's mark claimed a/1")
	t.Require().NotZero(owned[0].Token)

	n, err := t.idx.SweepStale(ctx, 0, 100)
	t.Require().NoError(err)
	t.Require().Equal(0, n, "the sweep must skip a row under a live lease")
	t.Require().Equal(int64(0), t.resourceRebuildCounter("a", "1"))
	t.Require().False(t.docExists("a", "1"))
	t.Require().NotNil(t.staleSince("a", "1"))

	_, err = t.pool.Exec(ctx,
		`UPDATE resources SET owner_since = now() - interval '1 hour' WHERE type='a' AND id='1'`)
	t.Require().NoError(err)

	n, err = t.idx.SweepStale(ctx, 0, 100)
	t.Require().NoError(err)
	t.Require().Equal(1, n, "the sweep claims a/1 once its lease has expired")
	t.worker.Drain(ctx)

	t.Require().True(t.docExists("a", "1"))
	t.Require().Nil(t.staleSince("a", "1"))
	t.Require().Equal(int64(1), t.resourceRebuildCounter("a", "1"))
	seq, since := t.ownerColumns("a", "1")
	t.Require().Nil(seq, "the sweep build's finish drops the ownership")
	t.Require().Nil(since)
}
