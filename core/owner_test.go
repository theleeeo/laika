package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// Build ownership (L1.4): one owned inline build per resource, plus at most
// one follow-up for the changes that arrived during it. The store is
// recordingStore, which keeps the rows' stale marks and owner tokens; every
// interleaving is forced through gates (parked Executes, onRemove, a held
// pool), never timed.

// closer returns an idempotent close of ch, also run at cleanup, so a failing
// test never leaves a parked build behind.
func closer(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	f := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(f)
	return f
}

// callsWithPrefix is every recorded call starting with prefix, in order.
func callsWithPrefix(st *recordingStore, prefix string) []string {
	var out []string
	for _, c := range st.callsSnapshot() {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func notify(id string, kind ChangeKind, md map[string]string) Notification {
	return Notification{ResourceType: "product", ResourceID: id, Kind: kind, Metadata: md}
}

func mdN(n int) map[string]string { return map[string]string{"n": strconv.Itoa(n)} }

func mustRegister(t *testing.T, idx *Indexer, n Notification, opts ...RegisterOption) {
	t.Helper()
	if err := idx.RegisterChange(context.Background(), n, opts...); err != nil {
		t.Fatal(err)
	}
}

func waitIdle(t *testing.T, idx *Indexer) {
	t.Helper()
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Ten registrations touching P while its build is blocked in its plan — as
// an item, or as a Parent through its child c — submit nothing: P's row is
// owned, and the changes ride on its mark. When the build finishes, its
// FinishOwned re-claims P and the one follow-up runs with the row's
// metadata: that of P's own last registration, the ninth. The tenth, the
// last, comes through the child and leaves P's metadata alone.
func TestOwner_RegistrationsDuringABuild_CollapseIntoOneFollowUp(t *testing.T) {
	P, c := product("P"), product("c")
	st := &recordingStore{parentsOf: map[model.Resource][]model.Resource{c: {P}}}
	idx, ex := newRecordingIndexer(st, 4, 32)
	ex.arrived = make(chan string, 64)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"P": true}
	release := closer(t, ex.proceed)

	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(0)))
	within(t, ex.arrived, "P's build, parked in its plan")

	// Odd registrations name P itself, even ones its child; the tenth, the
	// last, comes through the child.
	for i := 1; i <= 10; i++ {
		id := "c"
		if i%2 == 1 {
			id = "P"
		}
		mustRegister(t, idx, notify(id, ChangeUpdated, mdN(i)))
	}
	if begins := callsWithPrefix(st, "BeginBuild:product/P:"); len(begins) != 1 {
		t.Fatalf("registrations touching P while its build owns it must not submit P again: %v", st.callsSnapshot())
	}

	release()
	waitIdle(t, idx)

	fus := st.followUpsOf(P)
	if len(fus) != 1 {
		t.Fatalf("the finishing build must hand on exactly one follow-up, got %v: %v", fus, st.callsSnapshot())
	}
	fu := fus[0]
	reqs := ex.requestsFor("P")
	if len(reqs) != 2 {
		t.Fatalf("P must be built exactly twice — its build and one follow-up — got %d: %v", len(reqs), st.callsSnapshot())
	}
	if !maps.Equal(reqs[1].Metadata, mdN(9)) {
		t.Fatalf("the follow-up must run with P's own last registration's metadata %v, not its child's; ran with %v", mdN(9), reqs[1].Metadata)
	}
	if r, _ := st.row(P); !maps.Equal(r.metadata, mdN(9)) {
		t.Fatalf("the child's registration must leave P's metadata %v, P holds %v", mdN(9), r.metadata)
	}
	if st.indexOf(fmt.Sprintf("BeginBuild:product/P:%d", fu.Token)) == -1 ||
		st.indexOf(fmt.Sprintf("FinishOwned:product/P:%d:%d", fu.Token, fu.Token)) == -1 {
		t.Fatalf("the follow-up must build and finish under the re-claimed token %d: %v", fu.Token, st.callsSnapshot())
	}
	if r, _ := st.row(P); r.stale || r.owner != 0 {
		t.Fatalf("after the follow-up P must be clear and unowned: %+v", r)
	}
}

// A change registered while P's follow-up runs gets a follow-up of its own:
// three builds of P, with the metadata of each change in order.
func TestOwner_ChangeDuringAFollowUp_GetsAFollowUpOfItsOwn(t *testing.T) {
	P := product("P")
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 1, 4)
	ex.arrived = make(chan string, 8)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"P": true}
	release := closer(t, ex.proceed)

	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(0)))
	within(t, ex.arrived, "P's build")
	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(1)))
	ex.proceed <- struct{}{} // release the first build
	within(t, ex.arrived, "P's follow-up")
	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(2)))
	ex.proceed <- struct{}{} // release the follow-up
	within(t, ex.arrived, "the follow-up of P's follow-up")
	release()
	waitIdle(t, idx)

	reqs := ex.requestsFor("P")
	var got []map[string]string
	for _, r := range reqs {
		got = append(got, r.Metadata)
	}
	if want := []map[string]string{mdN(0), mdN(1), mdN(2)}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("P must be built three times with the changes' metadata in order, got %v want %v", got, want)
	}
	fus := st.followUpsOf(P)
	if len(fus) != 2 {
		t.Fatalf("each finished build that saw a newer change must hand on one follow-up, got %v: %v", fus, st.callsSnapshot())
	}
	for i, fu := range fus {
		if st.indexOf(fmt.Sprintf("BeginBuild:product/P:%d", fu.Token)) == -1 {
			t.Fatalf("follow-up %d must build under its re-claimed token %d: %v", i, fu.Token, st.callsSnapshot())
		}
	}
}

// A submit the pool sheds releases the ownership the registration claimed,
// through ReleaseOwners — a shed build did not fail, so it is not backed off —
// so the next registration of P claims again and builds it.
func TestOwner_ShedSubmit_ReleasesItsClaim_SoTheNextRegistrationBuilds(t *testing.T) {
	P := product("1")
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 1)
	hold := make(chan struct{})
	unhold := closer(t, hold)
	holdPoolFull(t, idx, func(context.Context) { <-hold })

	mustRegister(t, idx, notify("1", ChangeUpdated, nil))
	r, _ := st.row(P)
	tok := r.staleSeq // the claim's token is the registration's stale_seq
	if st.indexOf(fmt.Sprintf("ReleaseOwners:product/1:%d", tok)) == -1 || st.owner(P) != 0 {
		t.Errorf("a shed submit must release the ownership it claimed (token %d), owner now %d: %v", tok, st.owner(P), st.callsSnapshot())
	}
	if n := st.count("ReleaseFailed"); n != 0 {
		t.Errorf("a shed submit is not a failure and must not back the row off: %v", st.callsSnapshot())
	}
	if r, _ := st.row(P); !r.stale {
		t.Errorf("the shed build's mark must stay for the sweep: %+v", r)
	}

	unhold()
	waitIdle(t, idx)
	if n := st.count("BeginBuild:product/1:"); n != 0 {
		t.Fatalf("the shed build must not run: %v", st.callsSnapshot())
	}

	mustRegister(t, idx, notify("1", ChangeUpdated, nil))
	waitIdle(t, idx)
	r, _ = st.row(P)
	tok2 := r.staleSeq
	if st.indexOf(fmt.Sprintf("BeginBuild:product/1:%d", tok2)) == -1 ||
		st.indexOf(fmt.Sprintf("FinishOwned:product/1:%d:%d", tok2, tok2)) == -1 {
		t.Fatalf("the next registration must claim (token %d) and build P: %v", tok2, st.callsSnapshot())
	}
}

// An owned delete whose guarded hard delete finds the row moved — a change
// registered while the delete ran — and still owns it hands on one follow-up:
// a build for a recreate, a delete when the row was tombstoned again.
func TestOwner_DeleteFollowUp(t *testing.T) {
	R := product("R")
	recreate := map[string]string{"m": "recreate"}

	// setup registers the delete of R and, inside its first edge cleanup,
	// the changes during.
	setup := func(t *testing.T, during ...Notification) (*recordingStore, *recordingExecuter) {
		st := &recordingStore{}
		// One worker: everything submitted during the delete queues behind it.
		idx, ex := newRecordingIndexer(st, 1, 8)
		var once sync.Once
		st.onRemove = func(r model.Resource) {
			if r != R {
				return
			}
			once.Do(func() {
				for _, n := range during {
					if err := idx.RegisterChange(context.Background(), n); err != nil {
						t.Error(err)
					}
				}
			})
		}
		mustRegister(t, idx, notify("R", ChangeDeleted, map[string]string{"m": "delete"}))
		waitIdle(t, idx)
		return st, ex
	}

	t.Run("recreate during the delete is built once by the follow-up", func(t *testing.T) {
		st, ex := setup(t, notify("R", ChangeCreated, recreate))

		fus := st.followUpsOf(R)
		if len(fus) != 1 || fus[0].Deleted {
			t.Fatalf("the guarded delete must hand on one build follow-up, got %v: %v", fus, st.callsSnapshot())
		}
		fu := fus[0]
		begins := callsWithPrefix(st, "BeginBuild:product/R:")
		if want := fmt.Sprintf("BeginBuild:product/R:%d", fu.Token); len(begins) != 1 || begins[0] != want {
			t.Fatalf("R must be built exactly once, by the follow-up (%s), got %v: %v", want, begins, st.callsSnapshot())
		}
		if del, b := st.indexOf("DeleteResourceIfSeq:product/R:"), st.indexOf(begins[0]); del == -1 || b < del {
			t.Fatalf("the follow-up build must come after the guarded delete: %v", st.callsSnapshot())
		}
		if st.indexOf(fmt.Sprintf("FinishOwned:product/R:%d:%d", fu.Token, fu.Token)) == -1 {
			t.Fatalf("the follow-up must finish as an owned build: %v", st.callsSnapshot())
		}
		if reqs := ex.requestsFor("R"); len(reqs) != 1 || !maps.Equal(reqs[0].Metadata, recreate) {
			t.Fatalf("the one build of R must run with the recreate's metadata: %v", reqs)
		}
		if r, ok := st.row(R); !ok || r.deleted || r.stale || r.owner != 0 {
			t.Fatalf("after the follow-up R must exist, clear and unowned: %+v (exists %v)", r, ok)
		}
	})

	t.Run("recreate and re-delete during the delete: the follow-up is a delete", func(t *testing.T) {
		st, ex := setup(t,
			notify("R", ChangeCreated, recreate),
			notify("R", ChangeDeleted, map[string]string{"m": "redelete"}))

		fus := st.followUpsOf(R)
		if len(fus) != 1 || !fus[0].Deleted {
			t.Fatalf("the guarded delete must hand on one delete follow-up, got %v: %v", fus, st.callsSnapshot())
		}
		fu := fus[0]
		if st.indexOf(fmt.Sprintf("DeleteResourceIfSeq:product/R:%d:%d", fu.Token, fu.Token)) == -1 {
			t.Fatalf("the follow-up delete must be guarded by and own token %d: %v", fu.Token, st.callsSnapshot())
		}
		if reqs := ex.requestsFor("R"); len(reqs) != 0 {
			t.Fatalf("nothing may build R — every change during the delete rode on its mark: %v", st.callsSnapshot())
		}
		if r, ok := st.row(R); ok {
			t.Fatalf("the follow-up delete must hard-delete R's row, got %+v", r)
		}
	})
}

// newTwoVersionCaptureIndexer is newCaptureIndexer over two Schema Versions,
// both served by the one recordingExecuter, so a test sees a delete reach
// every version's index.
func newTwoVersionCaptureIndexer(st Store, poolSize, queueSize int) (*Indexer, *recordingExecuter, *captureBackend) {
	ex := &recordingExecuter{}
	be := &captureBackend{}
	return mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}, {Version: 2, Executer: ex}}},
		ES:        be,
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	}), ex, be
}

// deletesOf is every ES delete of product id, as "index/id@version".
func deletesOf(be *captureBackend, id string) []string {
	var out []string
	for _, d := range be.deletesAt() {
		if strings.Contains(d, "/"+id+"@") {
			out = append(out, d)
		}
	}
	return out
}

// inOrder fails unless every call in want was recorded, in that order.
func inOrder(t *testing.T, st *recordingStore, want ...string) {
	t.Helper()
	last := -1
	calls := st.callsSnapshot()
	for _, w := range want {
		i := slices.Index(calls, w)
		if i == -1 || i < last {
			t.Fatalf("want %v in that order: %v", want, calls)
		}
		last = i
	}
}

// A notified delete bumps the row's Build Sequence with BeginDelete after its
// renewal and before it deletes anything; every version's ES delete and the
// edge removal carry the bumped sequence, and the guarded hard delete
// finishes it.
func TestOwner_NotifiedDelete_DeletesAtTheSequenceBeginDeleteBumped(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		R := product("R")
		st := &recordingStore{buildIdx: 41} // BeginDelete bumps to 42
		idx, _, be := newTwoVersionCaptureIndexer(st, 1, 4)

		mustRegister(t, idx, notify("R", ChangeDeleted, nil)) // stale_seq 1, token 1
		waitIdle(t, idx)

		inOrder(t, st, "RenewOwners:product/R:1", "BeginDelete:product/R:1",
			"RemoveResource:product/R:42", "DeleteResourceIfSeq:product/R:1:1")
		if got, want := deletesOf(be, "R"), []string{"product_search_v1/R@42", "product_search_v2/R@42"}; !slices.Equal(got, want) {
			t.Fatalf("every version's delete must carry BeginDelete's Build Sequence: got %v want %v", got, want)
		}
		if n := st.count("RemoveResource:"); n != 1 {
			t.Fatalf("the edges are removed once: %v", st.callsSnapshot())
		}
		if r, ok := st.row(R); ok {
			t.Fatalf("the finished delete must hard-delete the row, got %+v", r)
		}
	})

	t.Run("sweep", func(t *testing.T) {
		gone := product("gone")
		st := &staleListingStore{entries: []StaleResource{{Resource: gone, StaleSeq: 9, Token: 10, Deleted: true}}}
		st.buildIdx = 41
		idx, _, be := newTwoVersionCaptureIndexer(st, 1, 4)

		if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
			t.Fatal(err)
		}
		waitIdle(t, idx)

		inOrder(t, &st.recordingStore, "RenewOwners:product/gone:10", "BeginDelete:product/gone:10",
			"RemoveResource:product/gone:42", "DeleteResourceIfSeq:product/gone:9:10")
		if got, want := deletesOf(be, "gone"), []string{"product_search_v1/gone@42", "product_search_v2/gone@42"}; !slices.Equal(got, want) {
			t.Fatalf("every version's delete must carry BeginDelete's Build Sequence: got %v want %v", got, want)
		}
		if r, ok := st.row(gone); ok {
			t.Fatalf("the swept delete must hard-delete the row, got %+v", r)
		}
	})
}

// A notified delete that BeginDelete reports superseded — a recreate or a
// finished delete reached the row between the delete's renewal and its bump —
// deletes nothing: no ES delete, no edge removal. It still finishes with the
// guarded hard delete under its mark and token, which hands on the follow-up
// the change is due, if the delete still owns the row. A mark that keeps the
// row deleted does not supersede it
// (TestOwner_DeleteOverAMovedMark_DeletesAndHandsOnADeleteFollowUp).
func TestOwner_SupersededDelete_DeletesNothingAndFinishesAsAMovedMark(t *testing.T) {
	R := product("R")
	recreate := map[string]string{"m": "recreate"}

	// setup registers the delete of R (stale_seq 1, token 1) and runs during
	// once, at the start of its BeginDelete.
	setup := func(t *testing.T, during func(st *recordingStore, idx *Indexer)) (*recordingStore, *recordingExecuter, *captureBackend) {
		st := &recordingStore{buildIdx: 41}
		idx, ex, be := newCaptureIndexer(st, 1, 8)
		var once sync.Once
		st.onBeginDelete = func(r model.Resource) {
			if r == R {
				once.Do(func() { during(st, idx) })
			}
		}
		mustRegister(t, idx, notify("R", ChangeDeleted, map[string]string{"m": "delete"}))
		waitIdle(t, idx)
		if begins := callsWithPrefix(st, "BeginDelete:product/R:"); !slices.Equal(begins, []string{"BeginDelete:product/R:1"}) {
			t.Fatalf("setup: the delete must begin once under its token: %v", st.callsSnapshot())
		}
		inOrder(t, st, "BeginDelete:product/R:1", "DeleteResourceIfSeq:product/R:1:1")
		return st, ex, be
	}
	register := func(t *testing.T, ns ...Notification) func(*recordingStore, *Indexer) {
		return func(_ *recordingStore, idx *Indexer) {
			for _, n := range ns {
				if err := idx.RegisterChange(context.Background(), n); err != nil {
					t.Error(err)
				}
			}
		}
	}

	t.Run("recreate: the follow-up builds R once", func(t *testing.T) {
		st, ex, be := setup(t, register(t, notify("R", ChangeCreated, recreate)))

		if ds := deletesOf(be, "R"); len(ds) != 0 {
			t.Fatalf("a superseded delete must not delete from ES: %v", ds)
		}
		if n := st.count("RemoveResource:"); n != 0 {
			t.Fatalf("a superseded delete must not remove edges: %v", st.callsSnapshot())
		}
		fus := st.followUpsOf(R)
		if len(fus) != 1 || fus[0].Deleted {
			t.Fatalf("the finish must hand on one build follow-up, got %v: %v", fus, st.callsSnapshot())
		}
		fu := fus[0]
		begins := callsWithPrefix(st, "BeginBuild:product/R:")
		if want := fmt.Sprintf("BeginBuild:product/R:%d", fu.Token); len(begins) != 1 || begins[0] != want {
			t.Fatalf("R must be built exactly once, by the follow-up (%s), got %v: %v", want, begins, st.callsSnapshot())
		}
		if st.indexOf(fmt.Sprintf("FinishOwned:product/R:%d:%d", fu.Token, fu.Token)) == -1 {
			t.Fatalf("the follow-up must finish as an owned build: %v", st.callsSnapshot())
		}
		if reqs := ex.requestsFor("R"); len(reqs) != 1 || !maps.Equal(reqs[0].Metadata, recreate) {
			t.Fatalf("the one build of R must run with the recreate's metadata: %v", reqs)
		}
		if r, ok := st.row(R); !ok || r.deleted || r.stale || r.owner != 0 {
			t.Fatalf("after the follow-up R must exist, clear and unowned: %+v (exists %v)", r, ok)
		}
	})

	// A recreate whose mark another owner claimed (the delete's lease
	// lapsed): the delete is superseded by the recreate and, no longer
	// owning the row, hands on nothing.
	t.Run("recreate under another owner: no follow-up", func(t *testing.T) {
		var stolen int64
		st, _, be := setup(t, func(st *recordingStore, _ *Indexer) { stolen = steal(st, R, false) })

		if ds := deletesOf(be, "R"); len(ds) != 0 {
			t.Fatalf("a superseded delete must not delete from ES: %v", ds)
		}
		if n := st.count("RemoveResource:"); n != 0 {
			t.Fatalf("a superseded delete must not remove edges: %v", st.callsSnapshot())
		}
		if fus := st.followUpsOf(R); len(fus) != 0 {
			t.Fatalf("a delete that lost the row hands on nothing, got %v: %v", fus, st.callsSnapshot())
		}
		if n := st.count("BeginBuild:product/R:"); n != 0 {
			t.Fatalf("the new owner builds R, not the lost delete: %v", st.callsSnapshot())
		}
		if r, ok := st.row(R); !ok || r.owner != stolen || !r.stale || r.deleted {
			t.Fatalf("the new owner (%d) must keep the row and its mark: %+v (exists %v)", stolen, r, ok)
		}
	})

	t.Run("row gone: nothing to do", func(t *testing.T) {
		st, _, be := setup(t, func(st *recordingStore, _ *Indexer) {
			st.mu.Lock()
			delete(st.rows, R)
			st.mu.Unlock()
		})

		if ds := deletesOf(be, "R"); len(ds) != 0 {
			t.Fatalf("a superseded delete must not delete from ES: %v", ds)
		}
		if n := st.count("RemoveResource:"); n != 0 {
			t.Fatalf("a superseded delete must not remove edges: %v", st.callsSnapshot())
		}
		if fus := st.followUpsOf(R); len(fus) != 0 {
			t.Fatalf("a gone row hands on nothing, got %v: %v", fus, st.callsSnapshot())
		}
		if n := st.count("BeginBuild:"); n != 0 {
			t.Fatalf("nothing may be built: %v", st.callsSnapshot())
		}
	})
}

// A notified delete whose tombstone got a newer mark that kept it deleted —
// a child's registration marking it as a Parent, or a recreate followed by a
// re-delete — between the delete's renewal and its bump is not superseded:
// it deletes the documents and the edges at its bump. Its finish, under its
// own mark and token, then sees the moved mark and hands on a delete
// follow-up, which runs its own BeginDelete, deletes at its own bump and
// hard-deletes the row.
func TestOwner_DeleteOverAMovedMark_DeletesAndHandsOnADeleteFollowUp(t *testing.T) {
	R, C := product("R"), product("C")

	// run registers the delete of R (stale_seq 1, token 1) and runs during
	// once, at the start of its BeginDelete. The pool has one worker, so the
	// delete's bump is 42.
	run := func(t *testing.T, st *recordingStore, during func(idx *Indexer)) *captureBackend {
		st.buildIdx = 41
		idx, _, be := newCaptureIndexer(st, 1, 8)
		var once sync.Once
		st.onBeginDelete = func(r model.Resource) {
			if r == R {
				once.Do(func() { during(idx) })
			}
		}
		mustRegister(t, idx, notify("R", ChangeDeleted, map[string]string{"m": "delete"}))
		waitIdle(t, idx)

		fus := st.followUpsOf(R)
		if len(fus) != 1 || !fus[0].Deleted {
			t.Fatalf("the finish must hand on one delete follow-up, got %v: %v", fus, st.callsSnapshot())
		}
		tok := fus[0].Token
		removes := callsWithPrefix(st, "RemoveResource:product/R:")
		if len(removes) != 2 || removes[0] != "RemoveResource:product/R:42" {
			t.Fatalf("the delete and its follow-up each remove R's edges, the delete at its bump 42: %v", st.callsSnapshot())
		}
		var bump int64
		if _, err := fmt.Sscanf(removes[1], "RemoveResource:product/R:%d", &bump); err != nil || bump <= 42 {
			t.Fatalf("the follow-up must remove R's edges at a bump of its own above 42, got %q: %v", removes[1], st.callsSnapshot())
		}
		inOrder(t, st,
			"BeginDelete:product/R:1", "RemoveResource:product/R:42", "DeleteResourceIfSeq:product/R:1:1",
			fmt.Sprintf("BeginDelete:product/R:%d", tok), removes[1],
			fmt.Sprintf("DeleteResourceIfSeq:product/R:%d:%d", tok, tok))
		if got, want := deletesOf(be, "R"), []string{"product_search_v1/R@42", fmt.Sprintf("product_search_v1/R@%d", bump)}; !slices.Equal(got, want) {
			t.Fatalf("the delete and its follow-up each delete R's document at their own bump: got %v want %v", got, want)
		}
		if n := st.count("BeginBuild:product/R:"); n != 0 {
			t.Fatalf("nothing may build R: %v", st.callsSnapshot())
		}
		if r, ok := st.row(R); ok {
			t.Fatalf("the follow-up delete must hard-delete R's row, got %+v", r)
		}
		return be
	}

	t.Run("a child marks R as its Parent", func(t *testing.T) {
		st := &recordingStore{parentsOf: map[model.Resource][]model.Resource{C: {R}}}
		run(t, st, func(idx *Indexer) {
			if err := idx.RegisterChange(context.Background(), notify("C", ChangeUpdated, nil)); err != nil {
				t.Error(err)
			}
			if r, _ := st.row(R); r.staleSeq == 1 || !r.deleted || r.owner != 1 {
				t.Errorf("the child's registration must move R's mark, keep it deleted and leave it to the delete: %+v", r)
			}
		})
	})

	t.Run("recreate and re-delete", func(t *testing.T) {
		st := &recordingStore{}
		run(t, st, func(idx *Indexer) {
			for _, n := range []Notification{
				notify("R", ChangeCreated, map[string]string{"m": "recreate"}),
				notify("R", ChangeDeleted, map[string]string{"m": "redelete"}),
			} {
				if err := idx.RegisterChange(context.Background(), n); err != nil {
					t.Error(err)
				}
			}
		})
	})
}

// A notified delete whose BeginDelete fails deletes nothing and does not
// finish: its ownership is released through ReleaseFailed, which backs the
// row off, and the tombstone stays for the next change or the sweep.
func TestOwner_BeginDeleteFails_DeletesNothingAndKeepsTheTombstone(t *testing.T) {
	R := product("R")
	st := &recordingStore{beginDeleteErr: errors.New("db down")}
	idx, _, be := newCaptureIndexer(st, 2, 4)

	mustRegister(t, idx, notify("R", ChangeDeleted, nil)) // stale_seq 1, token 1
	waitIdle(t, idx)

	if st.indexOf("BeginDelete:product/R:1") == -1 {
		t.Fatalf("setup: the delete must try to begin: %v", st.callsSnapshot())
	}
	if ds := deletesOf(be, "R"); len(ds) != 0 {
		t.Fatalf("a delete that could not begin must not delete from ES: %v", ds)
	}
	for _, p := range []string{"RemoveResource:", "DeleteResourceIfSeq:"} {
		if n := st.count(p); n != 0 {
			t.Fatalf("a delete that could not begin must not call %s: %v", p, st.callsSnapshot())
		}
	}
	inOrder(t, st, "BeginDelete:product/R:1", "ReleaseFailed:product/R:1")
	if n := st.count("ReleaseOwners"); n != 0 {
		t.Fatalf("a failed delete releases through ReleaseFailed only: %v", st.callsSnapshot())
	}
	if r, ok := st.row(R); !ok || !r.deleted || !r.stale || r.owner != 0 {
		t.Fatalf("the tombstone must stay, unowned: %+v (exists %v)", r, ok)
	}
}

// A failed owned build releases its ownership through ReleaseFailed, which
// backs the row off, and leaves the mark, so the next change claims or the
// sweep rebuilds once the backoff has passed.
func TestOwner_FailedOwnedBuild_ReleasesAndKeepsTheMark(t *testing.T) {
	cases := map[string]func(st *recordingStore, ex *recordingExecuter){
		"BeginBuild fails": func(st *recordingStore, _ *recordingExecuter) { st.beginErr = errors.New("db down") },
		"plan fails":       func(_ *recordingStore, ex *recordingExecuter) { ex.failIDs = map[string]bool{"1": true} },
	}
	for name, fail := range cases {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			idx, ex := newRecordingIndexer(st, 2, 4)
			fail(st, ex)

			mustRegister(t, idx, notify("1", ChangeUpdated, nil))
			waitIdle(t, idx)

			r, _ := st.row(product("1"))
			if st.indexOf(fmt.Sprintf("ReleaseFailed:product/1:%d", r.staleSeq)) == -1 || r.owner != 0 || st.count("ReleaseOwners") != 0 {
				t.Fatalf("a failed owned build must release its ownership (token %d) through ReleaseFailed, owner now %d: %v", r.staleSeq, r.owner, st.callsSnapshot())
			}
			if !r.stale {
				t.Fatalf("a failed build's mark must stay: %+v", r)
			}
			if st.count("FinishOwned") != 0 || st.count("ClearStale") != 0 {
				t.Fatalf("a failed build finishes nothing: %v", st.callsSnapshot())
			}
		})
	}
}

// A failed owned delete releases its ownership through ReleaseFailed and
// leaves the tombstone.
func TestOwner_FailedOwnedDelete_ReleasesAndKeepsTheTombstone(t *testing.T) {
	st := &recordingStore{removeErr: errors.New("db down")}
	idx, _ := newRecordingIndexer(st, 2, 4)

	mustRegister(t, idx, notify("R", ChangeDeleted, nil))
	waitIdle(t, idx)

	r, ok := st.row(product("R"))
	if !ok || !r.deleted || !r.stale {
		t.Fatalf("a failed delete must leave the tombstone: %+v (exists %v)", r, ok)
	}
	if st.indexOf(fmt.Sprintf("ReleaseFailed:product/R:%d", r.staleSeq)) == -1 || r.owner != 0 || st.count("ReleaseOwners") != 0 {
		t.Fatalf("a failed owned delete must release its ownership (token %d) through ReleaseFailed, owner now %d: %v", r.staleSeq, r.owner, st.callsSnapshot())
	}
	if st.count("DeleteResourceIfSeq") != 0 {
		t.Fatalf("a failed delete must not finish the tombstone: %v", st.callsSnapshot())
	}
}

// The sweep builds and deletes its entries under the tokens ListStale claimed
// them with. (The tokens differ from the stale seqs here only so the
// assertions can tell which value went where; a real claim makes them equal.)
func TestOwner_Sweep_BuildsAndDeletesUnderTheListedTokens(t *testing.T) {
	st := &staleListingStore{entries: []StaleResource{
		{Resource: product("1"), StaleSeq: 4, Token: 5},
		{Resource: product("gone"), StaleSeq: 9, Token: 10, Deleted: true},
	}}
	idx := newHotPathIndexer(st, 2, 4)

	if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	for _, want := range []string{"BeginBuild:product/1:5", "FinishOwned:product/1:4:5", "DeleteResourceIfSeq:product/gone:9:10"} {
		if st.indexOf(want) == -1 {
			t.Fatalf("missing %s: %v", want, st.callsSnapshot())
		}
	}
	if st.count("ClearStale:product/1:") != 0 {
		t.Fatalf("an owned sweep build finishes with FinishOwned, not ClearStale: %v", st.callsSnapshot())
	}
	// ListStale claimed the whole batch up front; each entry renews its lease
	// when the pass reaches it, so a long pass doesn't let a later entry's
	// claim lapse under it.
	for _, e := range [][2]string{{"RenewOwners:product/1:5", "BeginBuild:product/1:5"}, {"RenewOwners:product/gone:10", "DeleteResourceIfSeq:product/gone:9:10"}} {
		if renew, serve := st.indexOf(e[0]), st.indexOf(e[1]); renew == -1 || renew > serve {
			t.Fatalf("the sweep must renew %s before serving it: %v", e[0], st.callsSnapshot())
		}
	}
	if r, _ := st.row(product("1")); r.stale || r.owner != 0 {
		t.Fatalf("the swept build must clear its mark and ownership: %+v", r)
	}
}

// newCaptureIndexer is newRecordingIndexer with a captureBackend, so a test
// sees every ES write and delete.
func newCaptureIndexer(st Store, poolSize, queueSize int) (*Indexer, *recordingExecuter, *captureBackend) {
	ex := &recordingExecuter{}
	be := &captureBackend{}
	return mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        be,
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	}), ex, be
}

// esOpsOn is every ES upsert and delete captureBackend saw for product id.
func esOpsOn(be *captureBackend, id string) []string {
	be.mu.Lock()
	defer be.mu.Unlock()
	var out []string
	for _, k := range be.upserts {
		if strings.HasSuffix(k, "/"+id) {
			out = append(out, "upsert "+k)
		}
	}
	for _, k := range be.deletes {
		if strings.HasSuffix(k, "/"+id) {
			out = append(out, "delete "+k)
		}
	}
	return out
}

// steal hands res to another owner, as a change registered by another
// instance after the holder's lease lapsed does: a new mark (deleted sets
// the tombstone) that claims the row. It returns the new owner token.
func steal(st *recordingStore, res model.Resource, deleted bool) int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	r := st.markLocked(res, &deleted)
	r.metadata = map[string]string{"m": "stolen"}
	r.owner = r.staleSeq
	r.registeredSinceClaim = false
	return r.owner
}

// stealOnRenew makes the first RenewOwners naming res hand it to another
// owner just before the renew; the returned func reports the new token.
func stealOnRenew(st *recordingStore, res model.Resource, deleted bool) func() int64 {
	var once sync.Once
	var mu sync.Mutex
	var tok int64
	st.onRenew = func(owned []Owned) {
		for _, o := range owned {
			if o.Resource == res {
				once.Do(func() {
					t := steal(st, res, deleted)
					mu.Lock()
					tok = t
					mu.Unlock()
				})
			}
		}
	}
	return func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return tok
	}
}

// A sweep entry whose ownership another owner took between ListStale and the
// pass reaching it — its lease lapsed and a change claimed the row — is
// skipped: a lost tombstone deletes nothing, a lost build entry builds
// nothing. The new owner keeps the row and its mark, and the entries after
// it are still served.
func TestOwner_Sweep_SkipsAnEntryWhoseOwnershipWasLost(t *testing.T) {
	t.Run("lost tombstone", func(t *testing.T) {
		gone := product("gone")
		st := &staleListingStore{entries: []StaleResource{
			{Resource: gone, StaleSeq: 9, Token: 10, Deleted: true},
			{Resource: product("1"), StaleSeq: 4, Token: 5},
		}}
		// A recreate claims the tombstone's row.
		stolen := stealOnRenew(&st.recordingStore, gone, false)
		idx, _, be := newCaptureIndexer(st, 2, 4)

		if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
			t.Fatal(err)
		}
		waitIdle(t, idx)

		if tok := stolen(); tok == 0 {
			t.Fatalf("setup: the sweep must renew the tombstone before serving it: %v", st.callsSnapshot())
		}
		if ops := esOpsOn(be, "gone"); len(ops) != 0 {
			t.Errorf("a lost tombstone must not touch ES: %v", ops)
		}
		for _, p := range []string{"RemoveResource:product/gone", "DeleteResourceIfSeq:product/gone:"} {
			if n := st.count(p); n != 0 {
				t.Errorf("a lost tombstone must not call %s: %v", p, st.callsSnapshot())
			}
		}
		if r, ok := st.row(gone); !ok || r.owner != stolen() || !r.stale {
			t.Errorf("the new owner (%d) must keep the row and its mark: %+v (exists %v)", stolen(), r, ok)
		}
		for _, want := range []string{"BeginBuild:product/1:5", "FinishOwned:product/1:4:5"} {
			if st.indexOf(want) == -1 {
				t.Errorf("the entry after the lost one must still be served, missing %s: %v", want, st.callsSnapshot())
			}
		}
	})

	t.Run("lost build entry", func(t *testing.T) {
		lost := product("1")
		st := &staleListingStore{entries: []StaleResource{
			{Resource: lost, StaleSeq: 4, Token: 5},
			{Resource: product("gone"), StaleSeq: 9, Token: 10, Deleted: true},
			{Resource: product("2"), StaleSeq: 6, Token: 7},
		}}
		stolen := stealOnRenew(&st.recordingStore, lost, false)
		idx, ex, be := newCaptureIndexer(st, 2, 4)

		if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
			t.Fatal(err)
		}
		waitIdle(t, idx)

		if tok := stolen(); tok == 0 {
			t.Fatalf("setup: the sweep must renew the build entry before serving it: %v", st.callsSnapshot())
		}
		if n := st.count("BeginBuild:product/1:"); n != 0 {
			t.Errorf("a lost build entry must not begin a build: %v", st.callsSnapshot())
		}
		if reqs := ex.requestsFor("1"); len(reqs) != 0 {
			t.Errorf("a lost build entry must not run its plan: %v", reqs)
		}
		if ops := esOpsOn(be, "1"); len(ops) != 0 {
			t.Errorf("a lost build entry must not write to ES: %v", ops)
		}
		if r, _ := st.row(lost); r.owner != stolen() || !r.stale {
			t.Errorf("the new owner (%d) must keep the row and its mark: %+v", stolen(), r)
		}
		for _, want := range []string{"DeleteResourceIfSeq:product/gone:9:10", "BeginBuild:product/2:7", "FinishOwned:product/2:6:7"} {
			if st.indexOf(want) == -1 {
				t.Errorf("the entries after the lost one must still be served, missing %s: %v", want, st.callsSnapshot())
			}
		}
	})
}

// A sweep entry whose renew fails is not served — the sweep can't tell it
// still owns the row — and its mark or tombstone stays for the next pass. Its
// ownership is released through ReleaseOwners, without a backoff, so the next
// change or sweep needn't wait for the lease.
func TestOwner_Sweep_RenewError_SkipsTheEntryAndKeepsItsMark(t *testing.T) {
	st := &staleListingStore{entries: []StaleResource{
		{Resource: product("1"), StaleSeq: 4, Token: 5},
		{Resource: product("gone"), StaleSeq: 9, Token: 10, Deleted: true},
	}}
	st.renewErr = errors.New("db down")
	idx, ex, be := newCaptureIndexer(st, 2, 4)

	if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if st.count("RenewOwnersFailed:") == 0 {
		t.Fatalf("setup: the sweep must try to renew: %v", st.callsSnapshot())
	}
	for _, p := range []string{"BeginBuild:", "FinishOwned:", "ClearStale:", "RemoveResource:", "DeleteResourceIfSeq:"} {
		if n := st.count(p); n != 0 {
			t.Errorf("an entry whose renew failed must not be served, got %s: %v", p, st.callsSnapshot())
		}
	}
	if reqs := ex.requestsFor("1"); len(reqs) != 0 {
		t.Errorf("an entry whose renew failed must not run its plan: %v", reqs)
	}
	if ops := append(esOpsOn(be, "1"), esOpsOn(be, "gone")...); len(ops) != 0 {
		t.Errorf("an entry whose renew failed must not touch ES: %v", ops)
	}
	if r, _ := st.row(product("1")); !r.stale || r.owner != 0 {
		t.Errorf("the build entry's mark must stay and its ownership be released: %+v", r)
	}
	if r, ok := st.row(product("gone")); !ok || !r.stale || !r.deleted || r.owner != 0 {
		t.Errorf("the tombstone must stay and its ownership be released: %+v (exists %v)", r, ok)
	}
	for _, want := range []string{"ReleaseOwners:product/1:5", "ReleaseOwners:product/gone:10"} {
		if st.indexOf(want) == -1 {
			t.Errorf("missing %s: %v", want, st.callsSnapshot())
		}
	}
	if n := st.count("ReleaseFailed"); n != 0 {
		t.Errorf("a failed renewal is not a failed build and must not back the row off: %v", st.callsSnapshot())
	}
}

// The ADR 0006 cascade submits only the Parents its MarkStale claimed: p1
// already has a live owner, so only p2 is built, under its token.
func TestOwner_Cascade_SubmitsOnlyWhatMarkStaleClaimed(t *testing.T) {
	p1, p2 := product("p1"), product("p2")
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 8)
	idx.plans["product"][0].Executer.(*staticExecuter).byID = map[string][]projection.BuildDoc{"c": {{
		Root:    product("c"),
		Doc:     map[string]any{"fields": map[string]any{"title": "t"}},
		Parents: []model.Resource{p1, p2},
	}}}
	owned, err := st.MarkStale(t.Context(), []model.Resource{p1}, time.Minute)
	if err != nil || len(owned) != 1 {
		t.Fatalf("claiming p1: %v %v", owned, err)
	}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"c"}}); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if n := st.count("BeginBuild:product/p1:"); n != 0 {
		t.Fatalf("p1 has a live owner; the cascade must not submit it: %v", st.callsSnapshot())
	}
	if got := st.owner(p1); got != owned[0].Token {
		t.Fatalf("p1's owner must be untouched (%d), got %d", owned[0].Token, got)
	}
	r, _ := st.row(p2)
	tok := r.staleSeq
	begins := callsWithPrefix(st, "BeginBuild:product/p2:")
	if want := fmt.Sprintf("BeginBuild:product/p2:%d", tok); len(begins) != 1 || begins[0] != want {
		t.Fatalf("p2 must be built once under the token its mark claimed (%s), got %v: %v", want, begins, st.callsSnapshot())
	}
	if st.indexOf(fmt.Sprintf("FinishOwned:product/p2:%d:%d", tok, tok)) == -1 {
		t.Fatalf("p2's build must finish as an owned build: %v", st.callsSnapshot())
	}
}

// A drift hit re-marks the root through scheduleBuild. An unowned build's
// re-mark claims the root and submits it; an owned build's re-mark claims
// nothing — the build owns its root — and its FinishOwned yields the
// follow-up.
func TestOwner_DriftRemark(t *testing.T) {
	setup := func() (*recordingStore, *Indexer) {
		st := &recordingStore{}
		st.drift.Store(true) // the first build's drift check hits
		idx := newHotPathIndexer(st, 2, 4)
		idx.plans = map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "c1")}}},
		}}
		return st, idx
	}

	t.Run("an unowned build's re-mark claims and submits the root", func(t *testing.T) {
		st, idx := setup()
		if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
			t.Fatal(err)
		}
		waitIdle(t, idx)

		r, _ := st.row(product("1"))
		tok := r.staleSeq // the drift re-mark is the row's only mark
		begins := callsWithPrefix(st, "BeginBuild:product/1:")
		want := []string{"BeginBuild:product/1:0", fmt.Sprintf("BeginBuild:product/1:%d", tok)}
		if fmt.Sprint(begins) != fmt.Sprint(want) {
			t.Fatalf("the direct build owns nothing and its re-build owns the claimed root: got %v want %v: %v", begins, want, st.callsSnapshot())
		}
		if st.count("FinishOwned:product/1:") != 1 || st.indexOf(fmt.Sprintf("FinishOwned:product/1:%d:%d", tok, tok)) == -1 {
			t.Fatalf("the re-build must finish as an owned build: %v", st.callsSnapshot())
		}
		if st.count("ClearStale:product/1:") != 1 {
			t.Fatalf("the direct build alone finishes with ClearStale: %v", st.callsSnapshot())
		}
		if r, _ := st.row(product("1")); r.stale || r.owner != 0 {
			t.Fatalf("the root must end clear and unowned: %+v", r)
		}
	})

	t.Run("an owned build's re-mark submits nothing; its follow-up re-builds", func(t *testing.T) {
		st, idx := setup()
		mustRegister(t, idx, notify("1", ChangeUpdated, nil))
		waitIdle(t, idx)

		fus := st.followUpsOf(product("1"))
		if len(fus) != 1 {
			t.Fatalf("the owned build's FinishOwned must yield the one follow-up, got %v: %v", fus, st.callsSnapshot())
		}
		begins := callsWithPrefix(st, "BeginBuild:product/1:")
		want := []string{"BeginBuild:product/1:1", fmt.Sprintf("BeginBuild:product/1:%d", fus[0].Token)}
		if fmt.Sprint(begins) != fmt.Sprint(want) {
			t.Fatalf("exactly two builds — the registered one and its follow-up: got %v want %v: %v", begins, want, st.callsSnapshot())
		}
		if st.count("MarkStale") != 1 {
			t.Fatalf("the drift hit re-marks once: %v", st.callsSnapshot())
		}
	})
}

// A cascade submit the pool sheds releases what its MarkStale claimed.
func TestOwner_ShedScheduleBuild_ReleasesItsClaim(t *testing.T) {
	p := product("p")
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 1)
	idx.plans["product"][0].Executer.(*staticExecuter).byID = map[string][]projection.BuildDoc{"c": {{
		Root:    product("c"),
		Doc:     map[string]any{"fields": map[string]any{"title": "t"}},
		Parents: []model.Resource{p},
	}}}
	hold := make(chan struct{})
	unhold := closer(t, hold)
	holdPoolFull(t, idx, func(context.Context) { <-hold })

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"c"}}); err != nil {
		t.Fatal(err)
	}
	r, _ := st.row(p)
	if st.indexOf(fmt.Sprintf("ReleaseOwners:product/p:%d", r.staleSeq)) == -1 || r.owner != 0 {
		t.Fatalf("the shed cascade must release p's claim (token %d), owner now %d: %v", r.staleSeq, r.owner, st.callsSnapshot())
	}
	if !r.stale {
		t.Fatalf("p's mark must stay for the sweep: %+v", r)
	}
	unhold()
	waitIdle(t, idx)
}

// A follow-up is submitted with trySubmit from inside the finishing task; a
// shed follow-up releases the ownership FinishOwned re-claimed for it (R9).
func TestOwner_ShedFollowUp_ReleasesTheReclaim(t *testing.T) {
	P := product("P")
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 1, 1)
	ex.arrived = make(chan string, 4)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"P": true}
	release := closer(t, ex.proceed)

	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(0)))
	within(t, ex.arrived, "P's build on the only worker")
	mustRegister(t, idx, notify("P", ChangeUpdated, mdN(1)))
	// The worker is busy with P's build until it returns, so the queue's one
	// slot stays taken while the build submits its follow-up.
	if !idx.pool.trySubmit(func(context.Context) {}) {
		t.Fatalf("the queue's slot must be free: P's re-registration must submit nothing while P's build owns it: %v", st.callsSnapshot())
	}
	release()
	waitIdle(t, idx)

	fus := st.followUpsOf(P)
	if len(fus) != 1 {
		t.Fatalf("P's build must re-claim for one follow-up, got %v: %v", fus, st.callsSnapshot())
	}
	if st.indexOf(fmt.Sprintf("ReleaseOwners:product/P:%d", fus[0].Token)) == -1 || st.owner(P) != 0 {
		t.Fatalf("the shed follow-up must release the re-claimed token %d, owner now %d: %v", fus[0].Token, st.owner(P), st.callsSnapshot())
	}
	if r, _ := st.row(P); !r.stale {
		t.Fatalf("the shed follow-up's mark must stay for the sweep: %+v", r)
	}
	if reqs := ex.requestsFor("P"); len(reqs) != 1 {
		t.Fatalf("the shed follow-up must not build: %d builds of P", len(reqs))
	}
}

// R5: a Build whose ctx ends before an owned id is finished releases the
// unfinished ids' ownership on a context detached from the cancellation,
// through ReleaseOwners: a cancellation is not a failure, so neither the
// unreached id 2 nor id 1, whose build the cancellation cut short, is backed
// off.
func TestOwner_CancelledBuild_ReleasesUnfinishedOwnershipOnALiveCtx(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	idx.plans["product"][0].Executer.(*staticExecuter).onExecute = cancel // during the first id's plan

	owned, err := st.MarkStale(t.Context(), []model.Resource{product("1"), product("2")}, time.Minute)
	if err != nil || len(owned) != 2 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	tokens := map[string]int64{owned[0].Id: owned[0].Token, owned[1].Id: owned[1].Token}

	_ = idx.Build(ctx, BuildArgs{ResourceType: "product", ResourceIds: []string{"1", "2"}, OwnerTokens: tokens})

	for _, id := range []string{"1", "2"} {
		if st.indexOf(fmt.Sprintf("ReleaseOwners:product/%s:%d", id, tokens[id])) == -1 || st.count("ReleaseOwnersFailed:product/"+id+":") != 0 {
			t.Fatalf("the unfinished id %s must be released on a live ctx: %v", id, st.callsSnapshot())
		}
	}
	if got := st.owner(product("2")); got != 0 {
		t.Fatalf("id 2's ownership must be dropped, owner %d", got)
	}
	if n := st.count("ReleaseFailed"); n != 0 {
		t.Fatalf("a cancelled build must not back any id off: %v", st.callsSnapshot())
	}
	if n := st.count("BeginBuild:product/2:"); n != 0 {
		t.Fatalf("nothing may be built for id 2 after the cancellation: %v", st.callsSnapshot())
	}
	waitIdle(t, idx)
}

// A direct Build carries no tokens: it owns nothing, finishes with ClearStale
// and never renews, releases or finishes an ownership.
func TestOwner_DirectBuild_OwnsNothing(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)
	if _, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, 0); err != nil {
		t.Fatal(err)
	}

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if st.indexOf("BeginBuild:product/1:0") == -1 || st.indexOf("ClearStale:product/1:1") == -1 {
		t.Fatalf("a direct build begins unowned and finishes with ClearStale: %v", st.callsSnapshot())
	}
	for _, p := range []string{"FinishOwned", "RenewOwners", "ReleaseOwners"} {
		if st.count(p) != 0 {
			t.Fatalf("a direct build must never call %s: %v", p, st.callsSnapshot())
		}
	}
	if r, _ := st.row(product("1")); r.stale {
		t.Fatalf("the direct build must clear the mark: %+v", r)
	}
}

// A pool task renews its ownership when it is dequeued, before it begins.
func TestOwner_PoolTask_RenewsAtStart(t *testing.T) {
	t.Run("build", func(t *testing.T) {
		st := &recordingStore{}
		idx, _ := newRecordingIndexer(st, 2, 4)
		mustRegister(t, idx, notify("1", ChangeUpdated, nil))
		waitIdle(t, idx)

		renew, begin := st.indexOf("RenewOwners:product/1:1"), st.indexOf("BeginBuild:product/1:1")
		if renew == -1 || begin == -1 || renew > begin {
			t.Fatalf("the build task must renew its ownership before BeginBuild: %v", st.callsSnapshot())
		}
	})
	t.Run("delete", func(t *testing.T) {
		st := &recordingStore{}
		idx, _ := newRecordingIndexer(st, 2, 4)
		mustRegister(t, idx, notify("1", ChangeDeleted, nil))
		waitIdle(t, idx)

		renew, del := st.indexOf("RenewOwners:product/1:1"), st.indexOf("DeleteResourceIfSeq:product/1:1:1")
		if renew == -1 || del == -1 || renew > del {
			t.Fatalf("the delete task must renew its ownership before finishing: %v", st.callsSnapshot())
		}
	})
}

// occupyWorker holds a pool of size one busy until the returned func runs (or
// cleanup), leaving its queue free: what is submitted meanwhile queues, and is
// dequeued only after the release.
func occupyWorker(t *testing.T, idx *Indexer) func() {
	t.Helper()
	hold := make(chan struct{})
	release := closer(t, hold)
	started := make(chan struct{})
	if !idx.pool.trySubmit(func(context.Context) { close(started); <-hold }) {
		t.Fatal("failed to occupy the worker")
	}
	<-started
	return release
}

// A queued owned task whose ownership another owner took before a worker
// dequeued it does nothing for it: a build task skips the lost ids and still
// builds the ones it holds, a delete task deletes nothing. The new owner
// keeps the row and its mark.
func TestOwner_PoolTask_SkipsWhatItLostBeforeDequeue(t *testing.T) {
	t.Run("build", func(t *testing.T) {
		st := &recordingStore{}
		idx, ex, be := newCaptureIndexer(st, 1, 4)
		release := occupyWorker(t, idx)

		owned, err := st.MarkStale(t.Context(), []model.Resource{product("1"), product("2")}, time.Minute)
		if err != nil || len(owned) != 2 {
			t.Fatalf("claiming: %v %v", owned, err)
		}
		tokens := map[string]int64{owned[0].Id: owned[0].Token, owned[1].Id: owned[1].Token}
		idx.submitOwnedBuilds(t.Context(), owned) // one task: both ids are products
		if n := st.count("ReleaseOwners"); n != 0 {
			t.Fatalf("setup: the build must queue, not shed: %v", st.callsSnapshot())
		}
		stolen := steal(st, product("1"), false)
		release()
		waitIdle(t, idx)

		if st.indexOf(fmt.Sprintf("RenewOwners:product/1:%d", tokens["1"])) == -1 {
			t.Fatalf("setup: the task must renew at dequeue: %v", st.callsSnapshot())
		}
		if n := st.count("BeginBuild:product/1:"); n != 0 {
			t.Errorf("the lost id must not begin a build: %v", st.callsSnapshot())
		}
		if reqs := ex.requestsFor("1"); len(reqs) != 0 {
			t.Errorf("the lost id must not run its plan: %v", reqs)
		}
		if ops := esOpsOn(be, "1"); len(ops) != 0 {
			t.Errorf("the lost id must not be written to ES: %v", ops)
		}
		if r, _ := st.row(product("1")); r.owner != stolen || !r.stale {
			t.Errorf("the new owner (%d) must keep the lost id's row and its mark: %+v", stolen, r)
		}
		tok2 := tokens["2"]
		for _, want := range []string{fmt.Sprintf("BeginBuild:product/2:%d", tok2), fmt.Sprintf("FinishOwned:product/2:%d:%d", tok2, tok2)} {
			if st.indexOf(want) == -1 {
				t.Errorf("the id the task still holds must be built, missing %s: %v", want, st.callsSnapshot())
			}
		}
		if r, _ := st.row(product("2")); r.stale || r.owner != 0 {
			t.Errorf("the held id's build must clear its mark and ownership: %+v", r)
		}
	})

	t.Run("delete", func(t *testing.T) {
		R := product("R")
		st := &recordingStore{}
		idx, _, be := newCaptureIndexer(st, 1, 4)
		release := occupyWorker(t, idx)

		mustRegister(t, idx, notify("R", ChangeDeleted, nil))
		if n := st.count("ReleaseOwners"); n != 0 {
			t.Fatalf("setup: the delete must queue, not shed: %v", st.callsSnapshot())
		}
		stolen := steal(st, R, false) // a recreate claims the row
		release()
		waitIdle(t, idx)

		if st.count("RenewOwners:product/R:") == 0 {
			t.Fatalf("setup: the task must renew at dequeue: %v", st.callsSnapshot())
		}
		if ops := esOpsOn(be, "R"); len(ops) != 0 {
			t.Errorf("a lost delete must not touch ES: %v", ops)
		}
		for _, p := range []string{"RemoveResource:product/R", "DeleteResourceIfSeq:product/R:"} {
			if n := st.count(p); n != 0 {
				t.Errorf("a lost delete must not call %s: %v", p, st.callsSnapshot())
			}
		}
		if r, ok := st.row(R); !ok || r.owner != stolen || !r.stale {
			t.Errorf("the new owner (%d) must keep the row and its mark: %+v (exists %v)", stolen, r, ok)
		}
	})
}

// A queued owned task whose renew at dequeue fails does no work — it can't
// tell it still owns the row — and finishes nothing: the mark or tombstone
// stays for the next change or the sweep, and its ownership is released
// through ReleaseOwners, without a backoff.
func TestOwner_PoolTask_RenewError_SkipsTheWorkAndKeepsTheMark(t *testing.T) {
	failRenew := func(st *recordingStore) {
		st.mu.Lock()
		st.renewErr = errors.New("db down")
		st.mu.Unlock()
	}

	t.Run("build", func(t *testing.T) {
		st := &recordingStore{}
		idx, ex, be := newCaptureIndexer(st, 1, 4)
		release := occupyWorker(t, idx)

		mustRegister(t, idx, notify("1", ChangeUpdated, nil))
		failRenew(st)
		release()
		waitIdle(t, idx)

		if st.count("RenewOwnersFailed:product/1:") == 0 {
			t.Fatalf("setup: the task must try to renew at dequeue: %v", st.callsSnapshot())
		}
		for _, p := range []string{"BeginBuild:", "FinishOwned:", "ClearStale:"} {
			if n := st.count(p); n != 0 {
				t.Errorf("a task whose renew failed must not build or finish, got %s: %v", p, st.callsSnapshot())
			}
		}
		if reqs := ex.requestsFor("1"); len(reqs) != 0 {
			t.Errorf("a task whose renew failed must not run its plan: %v", reqs)
		}
		if ops := esOpsOn(be, "1"); len(ops) != 0 {
			t.Errorf("a task whose renew failed must not write to ES: %v", ops)
		}
		if r, _ := st.row(product("1")); !r.stale || r.owner != 0 {
			t.Errorf("the mark must stay and the ownership be released: %+v", r)
		}
		if st.indexOf("ReleaseOwners:product/1:1") == -1 || st.count("ReleaseFailed") != 0 {
			t.Errorf("the ownership must be released through ReleaseOwners only: %v", st.callsSnapshot())
		}
	})

	t.Run("delete", func(t *testing.T) {
		st := &recordingStore{}
		idx, _, be := newCaptureIndexer(st, 1, 4)
		release := occupyWorker(t, idx)

		mustRegister(t, idx, notify("R", ChangeDeleted, nil))
		failRenew(st)
		release()
		waitIdle(t, idx)

		if st.count("RenewOwnersFailed:product/R:") == 0 {
			t.Fatalf("setup: the task must try to renew at dequeue: %v", st.callsSnapshot())
		}
		if ops := esOpsOn(be, "R"); len(ops) != 0 {
			t.Errorf("a delete whose renew failed must not touch ES: %v", ops)
		}
		for _, p := range []string{"RemoveResource:", "DeleteResourceIfSeq:"} {
			if n := st.count(p); n != 0 {
				t.Errorf("a delete whose renew failed must not call %s: %v", p, st.callsSnapshot())
			}
		}
		if r, ok := st.row(product("R")); !ok || !r.stale || !r.deleted || r.owner != 0 {
			t.Errorf("the tombstone must stay and its ownership be released: %+v (exists %v)", r, ok)
		}
		if st.indexOf("ReleaseOwners:product/R:1") == -1 || st.count("ReleaseFailed") != 0 {
			t.Errorf("the ownership must be released through ReleaseOwners only: %v", st.callsSnapshot())
		}
	})
}

// Only claimed submits wait under WaitForSlot: a registration of a row with a
// live owner submits nothing, so it returns at once on a pressured pool.
func TestOwner_WaitForSlot_UnclaimedRegistrationDoesNotWait(t *testing.T) {
	P := product("1")
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 1)
	owned, err := st.MarkStale(t.Context(), []model.Resource{P}, time.Minute)
	if err != nil || len(owned) != 1 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	hold := make(chan struct{})
	unhold := closer(t, hold)
	holdPoolFull(t, idx, func(context.Context) { <-hold })

	errc := make(chan error, 1)
	go func() { errc <- idx.RegisterChange(t.Context(), notify("1", ChangeUpdated, nil), WaitForSlot()) }()
	if err := within(t, errc, "a WaitForSlot registration of an owned row on a pressured pool"); err != nil {
		t.Fatal(err)
	}

	unhold()
	waitIdle(t, idx)
	if n := st.count("BeginBuild:product/1:"); n != 0 {
		t.Fatalf("an unclaimed registration submits nothing: %v", st.callsSnapshot())
	}
	if got := st.owner(P); got != owned[0].Token {
		t.Fatalf("the live owner (%d) must keep the row, got %d", owned[0].Token, got)
	}
}

// A finishing statement that fails leaves the work unfinished: the owned
// build or delete releases its ownership through ReleaseFailed and the mark
// or tombstone stays.
func TestOwner_FailedFinish_Releases(t *testing.T) {
	cases := map[string]struct {
		kind ChangeKind
		fail func(st *recordingStore)
	}{
		"FinishOwned fails":         {ChangeUpdated, func(st *recordingStore) { st.finishErr = errors.New("db down") }},
		"DeleteResourceIfSeq fails": {ChangeDeleted, func(st *recordingStore) { st.deleteErr = errors.New("db down") }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			tc.fail(st)
			idx, _ := newRecordingIndexer(st, 2, 4)

			mustRegister(t, idx, notify("1", tc.kind, nil))
			waitIdle(t, idx)

			r, ok := st.row(product("1"))
			if !ok || !r.stale {
				t.Fatalf("the unfinished work's mark must stay: %+v (exists %v)", r, ok)
			}
			if st.indexOf(fmt.Sprintf("ReleaseFailed:product/1:%d", r.staleSeq)) == -1 || r.owner != 0 || st.count("ReleaseOwners") != 0 {
				t.Fatalf("a failed finish must release its ownership (token %d) through ReleaseFailed, owner now %d: %v", r.staleSeq, r.owner, st.callsSnapshot())
			}
		})
	}
}
