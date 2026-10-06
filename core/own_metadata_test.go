package core

import (
	"context"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// A resource's metadata is its own (L1.8): only its own registrations write
// it, and while it has none its plans' report; a mark another resource's
// change makes leaves it, and a build of the resource that owns it fetches
// with what the row holds when the build begins (BuildBegun.Metadata). A
// build that owns nothing — a rebuild walk, a direct Build — fetches with its
// caller's.

// fetchedWith fails unless req fetched with exactly want; nil want is none.
func fetchedWith(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	if !maps.Equal(got, want) || (want == nil && got != nil) {
		t.Fatalf("%s must fetch with %v, fetched with %v", what, want, got)
	}
}

// Q16, ruling R2. R's owned build fetches with md0 and finds its child C;
// during the fetch C is registered, so C's change_seq exceeds R's start and
// R's drift check fires. A registration of R with md1 commits unclaimed —
// R's build owns the row — after R's edge write and before its drift
// re-mark (the onCheck hook forces exactly that order). The re-mark leaves
// md1 on the row, and the follow-up FinishOwned hands on runs with md1.
func TestOwnMetadata_RegistrationBetweenEdgeWriteAndDriftRemark_FollowUpRunsWithIt(t *testing.T) {
	R, C := product("R"), product("C")
	md0, md1, mdC := map[string]string{"m": "0"}, map[string]string{"m": "1"}, map[string]string{"m": "C"}
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 8)
	ex.relations = map[string][]model.Resource{"R": {C}}
	ex.arrived = make(chan string, 8)
	ex.proceed = make(chan struct{})
	ex.parkIDs = map[string]bool{"R": true}
	release := closer(t, ex.proceed)

	var once sync.Once
	st.onCheck = func(checks []ChangeCheck) {
		if len(checks) == 0 || checks[0].Resource != C {
			return
		}
		once.Do(func() {
			if err := idx.RegisterChange(context.Background(), notify("R", ChangeUpdated, md1)); err != nil {
				t.Error(err)
			}
		})
	}

	mustRegister(t, idx, notify("R", ChangeUpdated, md0))
	within(t, ex.arrived, "R's build, parked in its fetch")
	mustRegister(t, idx, notify("C", ChangeUpdated, mdC))
	release()
	waitIdle(t, idx)

	// The order the hook forced: R's edge write, the md1 registration, the
	// drift check, R's drift re-mark.
	calls := st.callsSnapshot()
	edge := st.indexOf("ReplaceEdges:product/R:")
	reg := -1
	for i, c := range calls {
		if c == "RegisterChanges:1" && i > edge {
			reg = i
			break
		}
	}
	remark := -1
	for i, c := range calls {
		if strings.HasPrefix(c, "MarkStale:") && i > reg {
			remark = i
			break
		}
	}
	if edge == -1 || reg == -1 || remark == -1 {
		t.Fatalf("setup: want R's edge write, then md1's registration, then R's drift re-mark: %v", calls)
	}
	if begins := callsWithPrefix(st, "BeginBuild:product/R:"); len(begins) != 2 {
		t.Fatalf("R must be built twice — its build and one follow-up; md1's registration claims nothing: %v", calls)
	}
	if fus := st.followUpsOf(R); len(fus) != 1 {
		t.Fatalf("R's build must hand on one follow-up, got %v: %v", fus, calls)
	}
	if r, _ := st.row(R); !maps.Equal(r.metadata, md1) {
		t.Fatalf("the drift re-mark must leave md1 on R's row, it holds %v", r.metadata)
	}
	reqs := ex.requestsFor("R")
	if len(reqs) != 2 {
		t.Fatalf("R's build and its follow-up, got %d fetches: %v", len(reqs), calls)
	}
	fetchedWith(t, "R's build", reqs[0].Metadata, md0)
	fetchedWith(t, "R's follow-up", reqs[1].Metadata, md1)
}

// A child's registration marks its Parent P, which holds metadata of its own,
// mdP: the mark leaves mdP, and the P build it claims fetches with mdP, not
// the child's. A Parent without metadata builds with none.
func TestOwnMetadata_RegistrationCascade_ParentBuildsWithItsOwn(t *testing.T) {
	for name, mdP := range map[string]map[string]string{
		"the Parent holds metadata": {"m": "P"},
		"the Parent holds none":     nil,
	} {
		t.Run(name, func(t *testing.T) {
			P, C := product("P"), product("C")
			mdC := map[string]string{"m": "C"}
			st := &recordingStore{parentsOf: map[model.Resource][]model.Resource{C: {P}}}
			st.seedMetadata(P, mdP)
			idx, ex := newRecordingIndexer(st, 2, 8)

			mustRegister(t, idx, notify("C", ChangeUpdated, mdC))
			waitIdle(t, idx)

			if r, _ := st.row(P); !maps.Equal(r.metadata, mdP) {
				t.Fatalf("the child's registration must leave P's metadata %v, P holds %v", mdP, r.metadata)
			}
			c, p := ex.requestsFor("C"), ex.requestsFor("P")
			if len(c) != 1 || len(p) != 1 {
				t.Fatalf("C and its Parent P are built once each, got %d and %d: %v", len(c), len(p), st.callsSnapshot())
			}
			fetchedWith(t, "C's build", c[0].Metadata, mdC)
			fetchedWith(t, "P's build", p[0].Metadata, mdP)
		})
	}
}

// The ADR 0006 cascade: C's build derives its Parent P from its own data and
// marks it (scheduleBuild). The mark leaves P's metadata, and the P build it
// claims fetches with P's own — none when P has none — never C's build's.
func TestOwnMetadata_BuildCascade_ParentBuildsWithItsOwn(t *testing.T) {
	for name, mdP := range map[string]map[string]string{
		"the Parent holds metadata": {"m": "P"},
		"the Parent holds none":     nil,
	} {
		t.Run(name, func(t *testing.T) {
			P := product("P")
			mdC := map[string]string{"m": "C"}
			st := &recordingStore{}
			st.seedMetadata(P, mdP)
			idx, ex := newRecordingIndexer(st, 2, 8)
			ex.parents = map[string][]model.Resource{"C": {P}}

			mustRegister(t, idx, notify("C", ChangeUpdated, mdC))
			waitIdle(t, idx)

			if st.count("MarkStale:1") != 1 {
				t.Fatalf("C's build must mark its derived Parent once: %v", st.callsSnapshot())
			}
			if r, _ := st.row(P); !maps.Equal(r.metadata, mdP) {
				t.Fatalf("the cascade's mark must leave P's metadata %v, P holds %v", mdP, r.metadata)
			}
			p := ex.requestsFor("P")
			if len(p) != 1 {
				t.Fatalf("the cascade must build P once, got %d: %v", len(p), st.callsSnapshot())
			}
			if st.indexOf("FinishOwned:product/P:") == -1 {
				t.Fatalf("P's cascade build is owned: %v", st.callsSnapshot())
			}
			fetchedWith(t, "P's cascade build", p[0].Metadata, mdP)
		})
	}
}

// Ruling R2: an owned build waiting in the pool queue runs with the metadata
// its row holds when it begins. R's registration with md0 claims R and its
// build queues behind a busy worker; R's registration with md1 commits
// unclaimed. The one build of R fetches with md1, and no second build runs:
// it began after md1's mark, so its finish clears it.
func TestOwnMetadata_QueuedOwnedBuild_RunsWithMetadataRegisteredMeanwhile(t *testing.T) {
	R := product("R")
	md0, md1 := map[string]string{"m": "0"}, map[string]string{"m": "1"}
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 1, 4)
	release := occupyWorker(t, idx)

	mustRegister(t, idx, notify("R", ChangeUpdated, md0))
	if st.owner(R) == 0 || st.count("ReleaseOwners") != 0 {
		t.Fatalf("setup: md0's registration must claim R and its build queue: %v", st.callsSnapshot())
	}
	mustRegister(t, idx, notify("R", ChangeUpdated, md1))
	release()
	waitIdle(t, idx)

	reqs := ex.requestsFor("R")
	if len(reqs) != 1 {
		t.Fatalf("R must be built exactly once, got %d: %v", len(reqs), st.callsSnapshot())
	}
	fetchedWith(t, "R's queued build", reqs[0].Metadata, md1)
	if fus := st.followUpsOf(R); len(fus) != 0 {
		t.Fatalf("the build began after md1's mark and must finish without a follow-up, got %v: %v", fus, st.callsSnapshot())
	}
	if r, _ := st.row(R); r.stale || r.owner != 0 {
		t.Fatalf("R must end clear and unowned: %+v", r)
	}
}

// A direct Build with no owner token owns nothing: it fetches with its
// caller's BuildArgs.Metadata, whatever the row holds. An owner token makes
// the same Build fetch with the row's metadata instead.
func TestOwnMetadata_DirectBuild_UnownedFetchesWithItsCallers_OwnedWithTheRows(t *testing.T) {
	rowMD, callerMD := map[string]string{"m": "row"}, map[string]string{"m": "caller"}
	st := &recordingStore{}
	st.seedMetadata(product("1"), rowMD)
	idx, ex := newRecordingIndexer(st, 2, 4)

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}, Metadata: callerMD}); err != nil {
		t.Fatal(err)
	}
	owned, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, time.Minute)
	if err != nil || len(owned) != 1 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	if err := idx.Build(t.Context(), BuildArgs{
		ResourceType: "product", ResourceIds: []string{"1"}, Metadata: callerMD,
		OwnerTokens: map[string]int64{"1": owned[0].Token},
	}); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	reqs := ex.requestsFor("1")
	if len(reqs) != 2 {
		t.Fatalf("two builds, got %d: %v", len(reqs), st.callsSnapshot())
	}
	fetchedWith(t, "the build that owns nothing", reqs[0].Metadata, callerMD)
	fetchedWith(t, "the owned build", reqs[1].Metadata, rowMD)
}

// A rebuild owns nothing: the walk and a targeted rebuild fetch with the
// rebuild's Metadata, even for a row that holds metadata of its own.
func TestOwnMetadata_Rebuild_FetchesWithTheRebuildsMetadata(t *testing.T) {
	rowMD := map[string]string{"m": "row"}
	for name, ids := range map[string][]string{"plan walk": nil, "targeted": {"1"}} {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{}
			st.seedRow(product("1"), rowMD)
			exec := &requestLog{exec: &staticExecuter{
				docs: []projection.BuildDoc{productDoc("1")},
				byID: map[string][]projection.BuildDoc{"1": {productDoc("1")}},
			}}
			rebuildProducts(t, st, 0, ids, exec)

			reqID := ""
			if ids != nil {
				reqID = "1"
			}
			reqs := exec.requestsFor(reqID)
			if len(reqs) != 1 {
				t.Fatalf("one fetch, got %d: %v", len(reqs), st.callsSnapshot())
			}
			fetchedWith(t, "the rebuild", reqs[0].Metadata, walkMetadata)
			assertRowMetadata(t, st, product("1"), rowMD)
		})
	}
}
