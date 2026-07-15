package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// saturate occupies every worker of p with a task blocked on the returned
// channel. It waits for each task to be picked up before submitting the next,
// so on return all workers are busy and the queue is empty.
func saturate(t *testing.T, p *buildPool, workers int) chan struct{} {
	t.Helper()
	release := make(chan struct{})
	started := make(chan struct{})
	for range workers {
		if !p.trySubmit(func(context.Context) {
			started <- struct{}{}
			<-release
		}) {
			t.Fatal("saturating submit must be accepted")
		}
		<-started
	}
	return release
}

func TestPool_RunsSubmittedTask(t *testing.T) {
	p := newBuildPool(2, 4)
	var ran atomic.Bool
	if !p.trySubmit(func(context.Context) { ran.Store(true) }) {
		t.Fatal("submit must succeed on an empty pool")
	}
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("task did not run")
	}
}

func TestPool_ShedsImmediatelyWhenQueueFull(t *testing.T) {
	p := newBuildPool(1, 1)
	release := saturate(t, p, 1)
	if !p.trySubmit(func(context.Context) {}) {
		t.Fatal("queue slot must absorb a submit while the worker is busy")
	}

	start := time.Now()
	ok := p.trySubmit(func(context.Context) {
		t.Error("shed task must never run")
	})
	elapsed := time.Since(start)

	if ok {
		t.Fatal("submit with a full queue must shed")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("shed must return immediately, took %v", elapsed)
	}
	close(release)
	_ = p.waitIdle(t.Context())
}

func TestPool_QueueAbsorbsBurstBeyondWorkerCount(t *testing.T) {
	p := newBuildPool(2, 8)
	release := saturate(t, p, 2)

	var ran atomic.Int64
	for i := range 8 {
		if !p.trySubmit(func(context.Context) { ran.Add(1) }) {
			t.Fatalf("queued submit %d must be accepted", i)
		}
	}
	close(release)
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 8 {
		t.Fatalf("all queued tasks must run, got %d/8", got)
	}
}

func TestPool_WaitIdle_CoversCascadedSubmits(t *testing.T) {
	p := newBuildPool(2, 4)
	var childRan atomic.Bool
	p.trySubmit(func(context.Context) {
		// A task submits follow-up work (parent cascade) before finishing.
		p.trySubmit(func(context.Context) {
			time.Sleep(20 * time.Millisecond)
			childRan.Store(true)
		})
	})
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !childRan.Load() {
		t.Fatal("waitIdle returned before the cascaded task finished")
	}
}

func TestPool_TaskRunsUnderLivePoolContext(t *testing.T) {
	p := newBuildPool(1, 1)
	got := make(chan error, 1)
	p.trySubmit(func(taskCtx context.Context) {
		got <- taskCtx.Err()
	})
	if err := <-got; err != nil {
		t.Fatalf("pool ctx must be live while the pool runs, got %v", err)
	}
	_ = p.waitIdle(t.Context())
}

func TestPool_ShutdownDrainsQueuedAndRejects(t *testing.T) {
	p := newBuildPool(1, 2)
	release := saturate(t, p, 1)

	var queuedRan atomic.Bool
	if !p.trySubmit(func(context.Context) { queuedRan.Store(true) }) {
		t.Fatal("queued submit must be accepted")
	}

	close(release)
	if err := p.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !queuedRan.Load() {
		t.Fatal("shutdown must drain queued tasks, not only in-flight ones")
	}
	if p.trySubmit(func(context.Context) {}) {
		t.Fatal("submit after shutdown must be rejected")
	}
}

// Regression: a submit racing shutdown must never be accepted without the
// task completing before shutdown returns. The queue has room, so the racing
// submit sits in the pending-register/closed-recheck window — the exact spot
// where an accepted task could otherwise be missed by shutdown's drain.
// Every interleaving is legal except accepted-but-not-run.
func TestPool_ShutdownRace_AcceptedSubmitAlwaysDrained(t *testing.T) {
	for i := range 50 {
		p := newBuildPool(1, 1)
		release := saturate(t, p, 1)

		var ran atomic.Bool
		accepted := make(chan bool, 1)
		go func() {
			accepted <- p.trySubmit(func(context.Context) { ran.Store(true) })
		}()
		close(release)
		if err := p.shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if <-accepted && !ran.Load() {
			t.Fatalf("iteration %d: trySubmit returned true but shutdown returned before the task completed", i)
		}
	}
}
