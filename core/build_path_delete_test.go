package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// A build path that finds every plan nil deletes the resource's documents
// and removes its row (L2.2), guarded by the stale_seq its BeginBuild
// captured, so no tombstone and no row of a resource the source doesn't
// have is left behind, and a change that moved the mark keeps the row.

// newDeletePathIndexer serves "product" over two Schema Versions, both from
// ex, on recordingStore.
func newDeletePathIndexer(st *recordingStore, ex *staticExecuter) (*Indexer, *captureBackend) {
	be := &captureBackend{}
	return mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}, {Version: 2, Executer: ex}}},
		ES:        be,
		Store:     st,
		PoolSize:  1,
		QueueSize: 8,
	}), be
}

// afterBothWalks checks a two-plan walk took both plans' walk starts and made
// call only after the second: a multi-plan walk deletes an id every plan
// finds gone only once its last plan's nil has arrived.
func afterBothWalks(t *testing.T, calls []string, call string) {
	t.Helper()
	starts, at := callIndexes(calls, "NextChangeSeq"), slices.Index(calls, call)
	if len(starts) != 2 || at < starts[1] {
		t.Fatalf("want %q after both plans' walk starts: %v", call, calls)
	}
}

// bothVersionsAt is the two Schema Versions' deletes of id at seq.
func bothVersionsAt(id string, seq int64) []string {
	return []string{fmt.Sprintf("product_search_v1/%s@%d", id, seq), fmt.Sprintf("product_search_v2/%s@%d", id, seq)}
}

// A tombstone a child's registration marks as its Parent is claimed and
// submitted as a build, not a delete. Its plans all return nil, so its
// documents are deleted at the build's Build Sequence and the build finishes
// with the guarded row delete rather than FinishOwned, which would leave an
// unmarked tombstone nothing lists again.
func TestBuildPathDelete_TombstoneClaimedByAParentMark_RemovesTheRow(t *testing.T) {
	P, C := product("P"), product("C")
	st := &recordingStore{parentsOf: map[model.Resource][]model.Resource{C: {P}}, buildIdx: 41}
	// P is a tombstone whose delete never ran: marked, unowned.
	deleted := true
	st.mu.Lock()
	st.markLocked(P, &deleted)
	st.mu.Unlock()
	ex := &staticExecuter{byID: map[string][]projection.BuildDoc{"P": {nilDoc("P")}, "C": {productDoc("C")}}}
	idx, be := newDeletePathIndexer(st, ex)

	mustRegister(t, idx, notify("C", ChangeUpdated, nil))
	waitIdle(t, idx)

	begins := callsWithPrefix(st, "BeginBuild:product/P:")
	if len(begins) != 1 || st.count("BeginDelete:product/P") != 0 {
		t.Fatalf("the Parent mark must claim P and submit one build of it, not a delete: %v", st.callsSnapshot())
	}
	var tok int64
	if _, err := fmt.Sscanf(begins[0], "BeginBuild:product/P:%d", &tok); err != nil || tok == 0 {
		t.Fatalf("P's build must be owned, got %q", begins[0])
	}
	// C's build begins first (42); P's at 43.
	if got := deletesOf(be, "P"); !slices.Equal(got, bothVersionsAt("P", 43)) {
		t.Fatalf("P's documents must be deleted from every version at its build's sequence: got %v", got)
	}
	inOrder(t, st, "RemoveResource:product/P:43", fmt.Sprintf("DeleteResourceIfSeq:product/P:%d:%d", tok, tok))
	if st.count("FinishOwned:product/P") != 0 {
		t.Fatalf("a build that deleted P must not finish as a build: %v", st.callsSnapshot())
	}
	if r, ok := st.row(P); ok {
		t.Fatalf("P's row must be removed, got %+v", r)
	}
}

// A change registered while an owned build is fetching moves the mark: the
// build still deletes the resource's documents, but its guarded row delete
// keeps the row and re-claims it, and the follow-up — finding the resource
// still gone — removes it.
func TestBuildPathDelete_ChangeDuringTheBuild_KeepsTheRowForTheFollowUp(t *testing.T) {
	R := product("R")
	st := &recordingStore{}
	ex := &staticExecuter{byID: map[string][]projection.BuildDoc{"R": {nilDoc("R")}}}
	idx, _ := newDeletePathIndexer(st, ex)
	var once sync.Once
	ex.onExecute = func() {
		once.Do(func() {
			if err := idx.RegisterChange(context.Background(), notify("R", ChangeUpdated, nil)); err != nil {
				t.Error(err)
			}
		})
	}

	mustRegister(t, idx, notify("R", ChangeUpdated, nil))
	waitIdle(t, idx)

	fus := st.followUpsOf(R)
	if len(fus) != 1 || fus[0].Deleted {
		t.Fatalf("the moved mark must be handed on as one build follow-up, got %v: %v", fus, st.callsSnapshot())
	}
	tok := fus[0].Token
	inOrder(t, st,
		"DeleteResourceIfSeq:product/R:1:1",
		fmt.Sprintf("BeginBuild:product/R:%d", tok),
		fmt.Sprintf("DeleteResourceIfSeq:product/R:%d:%d", tok, tok))
	if r, ok := st.row(R); ok {
		t.Fatalf("the follow-up must remove R's row, got %+v", r)
	}
}

// A by-ids rebuild and a rebuild walk own nothing, and BeginBuild inserts a
// row for an id without one. An id whose plans all return nil has its
// documents deleted and the row its BeginBuild inserted removed. Each runs
// every plan: a rebuild that selects versions never removes the row, and
// leaves such an id marked for the sweep (ADR 0013).
func TestBuildPathDelete_RebuildOfAnIDWithoutARow_LeavesNoRow(t *testing.T) {
	t.Run("by ids", func(t *testing.T) {
		st := &recordingStore{buildIdx: 41}
		idx, be := newDeletePathIndexer(st, &staticExecuter{byID: map[string][]projection.BuildDoc{"X": {nilDoc("X")}}})

		if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: []string{"X"}}}); err != nil {
			t.Fatal(err)
		}

		if got := deletesOf(be, "X"); !slices.Equal(got, bothVersionsAt("X", 42)) {
			t.Fatalf("X's documents must be deleted at its BeginBuild's sequence: got %v", got)
		}
		inOrder(t, st, "BeginBuild:product/X:0", "RemoveResource:product/X:42", "DeleteResourceIfSeq:product/X:0:0")
		if r, ok := st.row(product("X")); ok {
			t.Fatalf("the rebuild must leave no row for X, got %+v", r)
		}
	})

	t.Run("walk", func(t *testing.T) {
		st := &recordingStore{buildIdx: 41}
		idx, be := newDeletePathIndexer(st, &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), nilDoc("2")}})

		if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product"}}); err != nil {
			t.Fatal(err)
		}

		// v1's walk begins 1 at 42 and 2 at 43; v2's nil for 2 is its last
		// outcome, so 2 settles on nils alone and is deleted.
		if got := deletesOf(be, "2"); !slices.Equal(got, bothVersionsAt("2", 43)) {
			t.Fatalf("2's documents must be deleted at its BeginBuild's sequence: got %v", got)
		}
		inOrder(t, st, "BeginBuild:product/2:0", "RemoveResource:product/2:43", "AnyChangedSince:1", "DeleteResourceIfSeq:product/2:0:0")
		afterBothWalks(t, st.callsSnapshot(), "RemoveResource:product/2:43")
		if n := st.count("BeginBuild:product/2:"); n != 1 {
			t.Fatalf("2 is begun once, by v1's nil, got %d: %v", n, st.callsSnapshot())
		}
		if r, ok := st.row(product("2")); ok {
			t.Fatalf("the walk must leave no row for 2, got %+v", r)
		}
		if _, ok := st.row(product("1")); !ok {
			t.Fatal("the walk must keep the row of a resource it built")
		}
	})
}

// A walk root deleted as listed without data is drift-checked against the
// walk start after its delete. A hit re-marks it — it was recreated after
// the walk fetched its page — which moves stale_seq, so the guarded row
// delete keeps the row, and the re-build the mark claimed writes it again.
// The walk runs both plans; its delete waits for v2's nil.
func TestBuildPathDelete_WalkRootRemarkedByItsDriftCheck_KeepsItsRow(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true) // the root's check, the walk's first, hits
	idx, be := newDeletePathIndexer(st, &staticExecuter{
		docs: []projection.BuildDoc{nilDoc("2")},
		byID: map[string][]projection.BuildDoc{"2": {productDoc("2")}},
	})

	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product"}}); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	inOrder(t, st, "RemoveResource:product/2:1", "MarkStale:1", "DeleteResourceIfSeq:product/2:0:0")
	afterBothWalks(t, st.callsSnapshot(), "RemoveResource:product/2:1")
	r, ok := st.row(product("2"))
	if !ok {
		t.Fatalf("a root its drift check re-marked must keep its row: %v", st.callsSnapshot())
	}
	if r.stale || r.owner != 0 {
		t.Fatalf("the re-build the mark claimed must settle the root: %+v", r)
	}
	if got := esOpsOn(be, "2"); !slices.Contains(got, "upsert product_search_v1/2") {
		t.Fatalf("the re-build must write the recreated root: %v", got)
	}
}

// An owned build whose plans all return nil and whose row delete fails has
// deleted the documents but not finished: it releases its ownership through
// ReleaseFailed and keeps the mark, so the next change claims it or the
// sweep re-builds it, finds it gone and removes the row. It never finishes as
// a build.
func TestBuildPathDelete_RowDeleteFails_ReleasesAndKeepsTheMark(t *testing.T) {
	R := product("R")
	st := &recordingStore{deleteErr: errors.New("db down")}
	idx, be := newDeletePathIndexer(st, &staticExecuter{byID: map[string][]projection.BuildDoc{"R": {nilDoc("R")}}})

	mustRegister(t, idx, notify("R", ChangeUpdated, nil))
	waitIdle(t, idx)

	if len(deletesOf(be, "R")) != 2 {
		t.Fatalf("R's documents must be deleted before the row: %v", be.deletesAt())
	}
	inOrder(t, st, "DeleteResourceIfSeq:product/R:1:1", "ReleaseFailed:product/R:1")
	for _, p := range []string{"FinishOwned:", "ClearStale:"} {
		if st.count(p) != 0 {
			t.Fatalf("a failed row delete must not finish as a build: %v", st.callsSnapshot())
		}
	}
	if fus := st.followUpsOf(R); len(fus) != 0 {
		t.Fatalf("a failed row delete hands on no follow-up, got %v", fus)
	}
	r, ok := st.row(R)
	if !ok || !r.stale || r.owner != 0 {
		t.Fatalf("R must keep its row and mark, unowned: %+v (exists %v)", r, ok)
	}
}

// A rebuild whose row delete fails fails the resource: it is marked stale for
// the sweep, counted once, and the rebuild reports the failure. The walk runs
// both plans, and its delete waits for v2's nil.
func TestBuildPathDelete_RebuildRowDeleteFails_FailsTheResource(t *testing.T) {
	for name, sel := range map[string]ResourceSelector{
		"by ids": {ResourceType: "product", ResourceIDs: []string{"2"}},
		"walk":   {ResourceType: "product"},
	} {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{deleteErrs: map[string]error{"2": errors.New("db down")}}
			ex := &staticExecuter{docs: []projection.BuildDoc{nilDoc("2")}, byID: map[string][]projection.BuildDoc{"2": {nilDoc("2")}}}
			idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": {
				{Version: 1, Executer: ex}, {Version: 2, Executer: ex},
			}}, 0)

			err := idx.RebuildNow(t.Context(), []ResourceSelector{sel})
			if err == nil || !strings.Contains(err.Error(), "failed 1 resource") {
				t.Fatalf("the rebuild must report one failed resource, got %v", err)
			}
			calls := st.callsSnapshot()
			del, mark := slices.Index(calls, "DeleteResourceIfSeq:product/2:42"), slices.Index(calls, "MarkStale:product/2")
			if del == -1 || mark < del || st.count("MarkStale:product/2") != 1 {
				t.Fatalf("the failed row delete must be followed by one stale mark: %v", calls)
			}
			if st.count("BeginBuild:product/2") != 1 {
				t.Fatalf("the failed resource must not be begun again: %v", calls)
			}
			if name == "walk" {
				afterBothWalks(t, calls, "DeleteResourceIfSeq:product/2:42")
			}
		})
	}
}
