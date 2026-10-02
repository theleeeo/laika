package core

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// holdPoolFull occupies the only worker of a (1 worker, queue 1) indexer with
// a task that runs hold, then fills the queue's slot, so the pool is both
// full and at its high-water mark (default max(1, 1*8/10) = 1). The pool
// stays full until hold returns.
func holdPoolFull(t *testing.T, idx *Indexer, hold func(context.Context)) {
	t.Helper()
	started := make(chan struct{})
	if !idx.pool.trySubmit(func(ctx context.Context) { close(started); hold(ctx) }) {
		t.Fatal("failed to occupy the worker")
	}
	<-started
	if !idx.pool.trySubmit(func(context.Context) {}) {
		t.Fatal("failed to fill the queue")
	}
	if !idx.pool.pressured() {
		t.Fatal("the pool must be pressured once the queue is full")
	}
}

// registerAsync runs RegisterChange in a goroutine and returns its result
// channel once its RegisterChanges statement — every mark and tombstone of
// the registration — has landed; a WaitForSlot call parks only after it.
func registerAsync(t *testing.T, ctx context.Context, idx *Indexer, st *recordingStore, n Notification, opts ...RegisterOption) <-chan error {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- idx.RegisterChange(ctx, n, opts...) }()
	within(t, st.marked, "the registration's mark")
	return errc
}

var productUpdated = Notification{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1}

func TestRegisterChange_WaitForSlot_WaitsForPressureToClear_ThenBuilds(t *testing.T) {
	st := &recordingStore{marked: make(chan struct{}, 8)}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	errc := registerAsync(t, t.Context(), idx, st, productUpdated, WaitForSlot())
	stillBlocked(t, errc, "RegisterChange with WaitForSlot on a pressured pool")
	if st.indexOf("BeginBuild") != -1 {
		t.Fatal("no build may start while the pool is held full")
	}

	close(release)
	if err := within(t, errc, "RegisterChange after pressure cleared"); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("ClearStale:product/1:3") == -1 {
		t.Fatalf("the waited-for build must run and clear the mark: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_WithoutWaitForSlot_ShedsOnPressuredPool(t *testing.T) {
	st := &recordingStore{marked: make(chan struct{}, 8)}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	errc := registerAsync(t, t.Context(), idx, st, productUpdated)
	if err := within(t, errc, "RegisterChange without WaitForSlot"); err != nil {
		t.Fatalf("a shed must not surface as an error: %v", err)
	}
	close(release)
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatalf("the shed build must be left to the sweep: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_WaitForSlot_Delete_WaitsThenDeletes(t *testing.T) {
	st := &recordingStore{marked: make(chan struct{}, 8)}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	errc := registerAsync(t, t.Context(), idx, st,
		Notification{ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted}, WaitForSlot())
	stillBlocked(t, errc, "a WaitForSlot delete on a pressured pool")

	close(release)
	if err := within(t, errc, "the delete after pressure cleared"); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("DeleteResourceIfSeq:product/1:7") == -1 {
		t.Fatalf("the waited-for delete must run: %v", st.callsSnapshot())
	}
}

// A delete's Parents are marked stale before the delete's own submission
// waits: a cancelled wait must neither leave them unmarked nor turn into an
// error from marking them under the dead ctx.
func TestRegisterChange_WaitForSlot_DeleteWithParents_MarksParentsBeforeWaiting(t *testing.T) {
	st := &recordingStore{
		marked:  make(chan struct{}, 8),
		parents: []MarkedParent{{Resource: model.Resource{Type: "product", Id: "parent"}}},
	}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	ctx, cancel := context.WithCancel(t.Context())
	errc := registerAsync(t, ctx, idx, st,
		Notification{ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted}, WaitForSlot())
	stillBlocked(t, errc, "a WaitForSlot delete on a pressured pool")
	if st.indexOf("RegisterChanges") == -1 {
		t.Fatalf("the Parents must be marked before the wait: %v", st.callsSnapshot())
	}

	cancel()
	if err := within(t, errc, "the delete after cancel"); err != nil {
		t.Fatalf("a wait ended by ctx must return nil — every mark already landed: %v", err)
	}
	if n := st.count("MarkStale"); n != 0 {
		t.Fatalf("the Parents' marks belong to the statement, not a later MarkStale: %v", st.callsSnapshot())
	}
	close(release)
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterChange_WaitForSlot_CtxCancel_ReturnsNilWithMarkInPlace(t *testing.T) {
	st := &recordingStore{marked: make(chan struct{}, 8)}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	ctx, cancel := context.WithCancel(t.Context())
	errc := registerAsync(t, ctx, idx, st, productUpdated, WaitForSlot())
	stillBlocked(t, errc, "RegisterChange with WaitForSlot on a pressured pool")

	cancel()
	if err := within(t, errc, "RegisterChange after cancel"); err != nil {
		t.Fatalf("a wait ended by ctx must return nil — the mark already landed: %v", err)
	}
	if st.indexOf("RegisterChanges") == -1 {
		t.Fatal("the stale mark must be in place")
	}
	close(release)
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatalf("a cancelled wait must leave the build to the sweep: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_WaitForSlot_Shutdown_ReturnsNilWithMarkInPlace(t *testing.T) {
	st := &recordingStore{marked: make(chan struct{}, 8)}
	idx := newHotPathIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	errc := registerAsync(t, t.Context(), idx, st, productUpdated, WaitForSlot())
	stillBlocked(t, errc, "RegisterChange with WaitForSlot on a pressured pool")

	// Shutdown blocks draining the held worker; the waiting producer must not.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- idx.Shutdown(t.Context()) }()
	if err := within(t, errc, "RegisterChange after Shutdown"); err != nil {
		t.Fatalf("a wait ended by shutdown must return nil — the mark already landed: %v", err)
	}
	if st.indexOf("RegisterChanges") == -1 {
		t.Fatal("the stale mark must be in place")
	}

	close(release)
	if err := within(t, shutdownDone, "Shutdown"); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatalf("a wait ended by shutdown must leave the build to the sweep: %v", st.callsSnapshot())
	}
}

// Cascades submitted from inside a running build must shed, never wait. The
// interleaving is forced, not timed:
//
//  1. The only worker runs task W, which parks until the test says go.
//  2. The queue's single slot is filled: the pool is full and pressured.
//  3. A producer calls RegisterChange(WaitForSlot) and parks in submitWait
//     after its mark lands.
//  4. The test tells W to run a cascade path on the worker. Its submission
//     meets the same full queue; if it waited, W could never finish — the
//     worker it would wait on is W itself — and the pool would deadlock.
//  5. W must finish promptly; only then does the pool drain and the parked
//     producer submit.
func TestCascades_ShedWhileAProducerWaits(t *testing.T) {
	cascades := map[string]func(t *testing.T, idx *Indexer, st *recordingStore){
		// ADR 0006 parent schedule and the drift re-build, both in buildOne:
		// the plan emits a Parent and a drifted Relation for product/2 only,
		// so the builds that run once the pool drains settle.
		"build parents and drift": func(t *testing.T, idx *Indexer, st *recordingStore) {
			st.drift.Store(true)
			idx.plans["product"][0].Executer.(*staticExecuter).byID = map[string][]projection.BuildDoc{"2": {{
				Root:      model.Resource{Type: "product", Id: "2"},
				Doc:       map[string]any{"fields": map[string]any{"title": "t"}},
				Parents:   []model.Resource{{Type: "product", Id: "parent"}},
				Relations: []model.VersionedResource{{Resource: model.Resource{Type: "product", Id: "child"}, Version: 1}},
			}}}
		},
		// The rebuild flusher's drift re-schedule. In production the flusher
		// runs in a rebuild walk, not on the pool; running it on the only
		// worker here just proves it sheds rather than waits.
		"rebuild flusher drift": func(t *testing.T, idx *Indexer, st *recordingStore) {
			st.drift.Store(true)
		},
	}
	runCascade := map[string]func(ctx context.Context, idx *Indexer){
		"build parents and drift": func(ctx context.Context, idx *Indexer) {
			_ = idx.Build(ctx, BuildArgs{ResourceType: "product", ResourceIds: []string{"2"}})
		},
		"rebuild flusher drift": func(ctx context.Context, idx *Indexer) {
			newRebuildFlusher(idx, "product", nil).checkDrift(ctx, map[string][]ChangeCheck{
				"2": {{Resource: model.Resource{Type: "product", Id: "child"}, Start: 1}},
			})
		},
	}

	// Marks each cascade must land: the build case runs both its parent
	// schedule and its drift re-build.
	wantMarks := map[string]int{"build parents and drift": 2, "rebuild flusher drift": 1}

	for name, setup := range cascades {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{marked: make(chan struct{}, 8)}
			idx := newHotPathIndexer(st, 1, 1)
			setup(t, idx, st)

			goCascade := make(chan struct{})
			cascadeDone := make(chan struct{})
			holdPoolFull(t, idx, func(ctx context.Context) { // steps 1-2
				<-goCascade
				runCascade[name](ctx, idx) // step 4, on the worker
				close(cascadeDone)
			})

			errc := registerAsync(t, t.Context(), idx, st, productUpdated, WaitForSlot()) // step 3
			stillBlocked(t, errc, "the producer")
			marksBefore := st.count("MarkStale")

			close(goCascade)
			within(t, cascadeDone, "the cascade on a full pool") // step 5
			if got := st.count("MarkStale") - marksBefore; got < wantMarks[name] {
				t.Fatalf("the cascade must mark stale before shedding (%d marks, want %d): %v", got, wantMarks[name], st.callsSnapshot())
			}

			if err := within(t, errc, "the producer after the pool drained"); err != nil {
				t.Fatal(err)
			}
			if err := idx.WaitForIdle(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// WaitForSlot applies to every submit of a batch: the items' builds, the
// delete and the Parents' builds all wait for pressure to clear, then run.
func TestRegisterChanges_WaitForSlot_EverySubmitWaitsThenRuns(t *testing.T) {
	st := &recordingStore{
		marked:  make(chan struct{}, 8),
		parents: []MarkedParent{{Resource: model.Resource{Type: "product", Id: "p"}}},
	}
	idx, ex := newRecordingIndexer(st, 1, 1)
	release := make(chan struct{})
	holdPoolFull(t, idx, func(context.Context) { <-release })

	errc := make(chan error, 1)
	go func() {
		_, err := idx.RegisterChanges(t.Context(), []Notification{
			{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
			{ResourceType: "product", ResourceID: "2", Kind: ChangeDeleted},
		}, WaitForSlot())
		errc <- err
	}()
	within(t, st.marked, "the batch's statement")
	stillBlocked(t, errc, "a WaitForSlot batch on a pressured pool")

	close(release)
	if err := within(t, errc, "the batch after pressure cleared"); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	built := ex.metadataByID()
	if _, ok := built["1"]; !ok {
		t.Fatalf("the item's waited-for build must run: %v", built)
	}
	if _, ok := built["p"]; !ok {
		t.Fatalf("the Parent's waited-for build must run: %v", built)
	}
	if st.indexOf("DeleteResourceIfSeq:product/2:8") == -1 {
		t.Fatalf("the waited-for delete must run: %v", st.callsSnapshot())
	}
}
