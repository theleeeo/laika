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
	p := newBuildPool(2, 4, 4)
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
	p := newBuildPool(1, 1, 1)
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
	p := newBuildPool(2, 8, 8)
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
	p := newBuildPool(2, 4, 4)
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
	p := newBuildPool(1, 1, 1)
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
	p := newBuildPool(1, 2, 2)
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
		p := newBuildPool(1, 1, 1)
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

// stillBlocked asserts that nothing arrives on ch within a short window. It
// is the negative half of a blocking assertion: the positive half — the
// release that must unblock ch — is asserted with a deadline by the caller.
func stillBlocked[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s must still be blocked, but returned %v", what, v)
	case <-time.After(50 * time.Millisecond):
	}
}

// within receives from ch or fails after a generous deadline. Used where the
// test asserts something returns promptly once its trigger has fired.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not return promptly", what)
		var zero T
		return zero
	}
}

func TestPool_Pressured_TracksQueueAgainstHighWater(t *testing.T) {
	p := newBuildPool(1, 4, 2)
	release := saturate(t, p, 1)

	if p.pressured() {
		t.Fatal("an empty queue is not under pressure")
	}
	p.trySubmit(func(context.Context) {})
	if p.pressured() {
		t.Fatal("one queued task is below a high-water mark of 2")
	}
	p.trySubmit(func(context.Context) {})
	if !p.pressured() {
		t.Fatal("a queue at the high-water mark is under pressure")
	}
	p.trySubmit(func(context.Context) {})
	if !p.pressured() {
		t.Fatal("a queue above the high-water mark is under pressure")
	}

	close(release)
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if p.pressured() {
		t.Fatal("a drained queue is not under pressure")
	}
}

func TestPool_SubmitWait_WaitsWhilePressured_ThenSubmits(t *testing.T) {
	// High water 1 below capacity 2: the queue has room, so a wait here is
	// the pressure rule at work, not a full channel.
	p := newBuildPool(1, 2, 1)
	release := saturate(t, p, 1)
	if !p.trySubmit(func(context.Context) {}) {
		t.Fatal("filler must be accepted")
	}

	var ran atomic.Bool
	result := make(chan bool, 1)
	go func() { result <- p.submitWait(t.Context(), func(context.Context) { ran.Store(true) }) }()
	stillBlocked(t, result, "submitWait on a pressured queue")

	close(release) // worker finishes, dequeues the filler, the queue drops below the mark
	if !within(t, result, "submitWait after pressure cleared") {
		t.Fatal("submitWait must submit once the queue drops below the high-water mark")
	}
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("a task accepted by submitWait must run")
	}
}

func TestPool_TrySubmit_StillShedsWhileAProducerWaits(t *testing.T) {
	p := newBuildPool(1, 1, 1)
	release := saturate(t, p, 1)
	p.trySubmit(func(context.Context) {}) // queue full

	result := make(chan bool, 1)
	go func() { result <- p.submitWait(t.Context(), func(context.Context) {}) }()
	stillBlocked(t, result, "submitWait on a full queue")

	if p.trySubmit(func(context.Context) {}) {
		t.Fatal("trySubmit must shed on a full queue even while a producer waits")
	}
	close(release)
	within(t, result, "submitWait after release")
	_ = p.waitIdle(t.Context())
}

// Dequeues that outpace the waiters coalesce into one wakeup token; the
// waiter that takes it must pass it on while the queue stays below the mark,
// or the others sleep through free slots. The coalescing is forced: the test
// takes three tasks off the queue itself (as three quick dequeues would) and
// drops a single token.
func TestPool_SubmitWait_CoalescedDequeues_WakeEveryWaiter(t *testing.T) {
	p := newBuildPool(1, 4, 4)
	release := saturate(t, p, 1)
	defer close(release)
	for range 4 {
		p.trySubmit(func(context.Context) {})
	}

	const waiters = 3
	results := make(chan bool, waiters)
	for range waiters {
		go func() { results <- p.submitWait(t.Context(), func(context.Context) {}) }()
	}
	stillBlocked(t, results, "waiters on a full queue")

	for range waiters {
		<-p.queue
		p.pending.Add(-1)
	}
	p.signalSlotFreed()
	for range waiters {
		if !within(t, results, "a waiter after coalesced dequeues") {
			t.Fatal("every waiter must submit into the freed slots")
		}
	}
}

func TestPool_SubmitWait_ManyWaiters_AllEventuallySubmit(t *testing.T) {
	// Several producers parked on one slot: every dequeue must eventually
	// reach a waiter.
	p := newBuildPool(1, 1, 1)
	release := saturate(t, p, 1)
	p.trySubmit(func(context.Context) {})

	const waiters = 5
	var ran atomic.Int64
	results := make(chan bool, waiters)
	for range waiters {
		go func() { results <- p.submitWait(t.Context(), func(context.Context) { ran.Add(1) }) }()
	}
	close(release)
	for range waiters {
		if !within(t, results, "a parked submitWait") {
			t.Fatal("every waiter must submit once the pool drains")
		}
	}
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != waiters {
		t.Fatalf("ran %d tasks, want %d", ran.Load(), waiters)
	}
}

func TestPool_SubmitWait_CtxCancel_ReturnsFalsePromptly(t *testing.T) {
	p := newBuildPool(1, 1, 1)
	release := saturate(t, p, 1)
	defer close(release)
	p.trySubmit(func(context.Context) {})

	ctx, cancel := context.WithCancel(t.Context())
	var ran atomic.Bool
	result := make(chan bool, 1)
	go func() { result <- p.submitWait(ctx, func(context.Context) { ran.Store(true) }) }()
	stillBlocked(t, result, "submitWait on a full queue")

	cancel()
	if within(t, result, "submitWait after cancel") {
		t.Fatal("a wait ended by ctx must return false")
	}
	if ran.Load() {
		t.Fatal("a task whose wait was cancelled must not run")
	}
}

func TestPool_SubmitWait_Shutdown_ReturnsFalsePromptly(t *testing.T) {
	p := newBuildPool(1, 1, 1)
	release := saturate(t, p, 1)
	p.trySubmit(func(context.Context) {})

	result := make(chan bool, 1)
	go func() { result <- p.submitWait(t.Context(), func(context.Context) {}) }()
	stillBlocked(t, result, "submitWait on a full queue")

	// shutdown blocks draining the saturated worker; the waiter must not.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.shutdown(t.Context()) }()
	if within(t, result, "submitWait after shutdown") {
		t.Fatal("a wait ended by shutdown must return false")
	}

	close(release)
	if err := within(t, shutdownDone, "shutdown"); err != nil {
		t.Fatal(err)
	}
}

// The shutdown-drain guarantee must hold for tasks accepted via submitWait,
// including a waiter woken by the release racing shutdown's close:
// accepted-but-not-run is the one illegal interleaving.
func TestPool_ShutdownRace_SubmitWaitAcceptedAlwaysDrained(t *testing.T) {
	for i := range 50 {
		p := newBuildPool(1, 1, 1)
		release := saturate(t, p, 1)
		p.trySubmit(func(context.Context) {}) // queue full: the waiter parks

		var ran atomic.Bool
		accepted := make(chan bool, 1)
		go func() {
			accepted <- p.submitWait(t.Context(), func(context.Context) { ran.Store(true) })
		}()
		close(release)
		if err := p.shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if within(t, accepted, "submitWait racing shutdown") && !ran.Load() {
			t.Fatalf("iteration %d: submitWait returned true but shutdown returned before the task completed", i)
		}
	}
}
