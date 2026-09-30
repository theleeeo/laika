package core

import (
	"context"
	"sync/atomic"
	"time"
)

// buildPool is the bounded in-process executor for inline builds and deletes.
// It is an accelerator, not a guarantee: callers must durably mark work
// (stale mark / tombstone) BEFORE submitting, so anything the pool sheds or
// loses to a crash is recovered by the stale sweep. See ADR 0008.
//
// A fixed set of worker goroutines, started at construction, consume tasks
// from a bounded queue. trySubmit never blocks: the queue's buffer is the
// burst absorber, and a full queue sheds immediately, so producers
// (RegisterChange RPCs) feel no backpressure from a saturated pool by
// default. submitWait is the opt-in exception for callers outside the pool
// (RegisterChange with WaitForSlot): it waits while the queue is at or above
// the high-water mark, until ctx ends or the pool shuts down.
type buildPool struct {
	// queue carries accepted tasks to the workers. It is never close()d — a
	// submitter racing shutdown could panic on send. Rejection is the closed
	// flag; worker exit is baseCtx cancellation.
	queue chan func(context.Context)

	// pending counts accepted-but-unfinished tasks: queued + running. The
	// queue cannot provide this — len(queue) sees waiting tasks but not the
	// ones workers are executing, which may be mid-flight and about to
	// cascade — and waitIdle/shutdown need exactly this span: pending reaches
	// zero only when every accepted task has finished. A WaitGroup is not a
	// substitute: with persistent workers the counter returns to zero between
	// bursts while waitIdle may be blocked waiting, so Add(1) would race a
	// blocked Wait at counter zero — the documented WaitGroup misuse, which
	// panics.
	pending atomic.Int64
	closed  atomic.Bool

	// highWater is the queued-task count at or above which the pool is
	// pressured. New guarantees 1 <= highWater <= cap(queue).
	highWater int

	// slotFreed carries one wakeup token for submitWait. Workers drop a token
	// (non-blocking) each time they dequeue, the only event that lowers
	// len(queue). The buffer of one coalesces bursts of dequeues into a
	// single token, so a waiter that consumes a token and leaves the queue
	// below the mark passes a token on: otherwise a second waiter could sleep
	// through a free slot.
	slotFreed chan struct{}

	// done is closed by shutdown so waiters return without polling.
	done chan struct{}

	// baseCtx is the context tasks run under. It is independent of any
	// caller's context — inline work must outlive the RPC that triggered it —
	// and is cancelled only when shutdown stops waiting.
	baseCtx context.Context
	cancel  context.CancelFunc
}

func newBuildPool(workers, queueSize, highWater int) *buildPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &buildPool{
		queue:     make(chan func(context.Context), queueSize),
		highWater: highWater,
		slotFreed: make(chan struct{}, 1),
		done:      make(chan struct{}),
		baseCtx:   ctx,
		cancel:    cancel,
	}
	for range workers {
		go p.worker()
	}
	return p
}

// worker runs queued tasks until the pool's context is cancelled. During a
// graceful shutdown workers keep draining the queue: cancel fires only after
// pending reaches zero or the shutdown deadline expires.
func (p *buildPool) worker() {
	for {
		select {
		case task := <-p.queue:
			p.signalSlotFreed()
			task(p.baseCtx)
			p.pending.Add(-1)
		case <-p.baseCtx.Done():
			return
		}
	}
}

// trySubmit enqueues task for the workers. It never blocks: it returns false
// — without running the task — when the queue is full or the pool is shut
// down. Shedding is safe because callers mark work stale before submitting;
// a shed row is recovered by the sweep. Tasks may call trySubmit themselves
// (cascades); because submission never blocks, a full queue sheds instead of
// deadlocking.
func (p *buildPool) trySubmit(task func(context.Context)) bool {
	if p.closed.Load() {
		return false
	}

	// Register in pending BEFORE re-checking closed. shutdown sets closed and
	// then drains by waiting for pending to reach zero, so this ordering is
	// what makes the drain cover concurrently-accepted submits: a submit that
	// incremented pending before shutdown's drain read is awaited (its task
	// runs or it undoes itself just below), and a submit that increments
	// after shutdown set closed must observe closed here and reject.
	// Incrementing only after this recheck would leave a window where
	// shutdown sees pending==0 and returns while an accepted task sits unrun
	// in the queue.
	p.pending.Add(1)
	if p.closed.Load() {
		p.pending.Add(-1)
		return false
	}

	select {
	case p.queue <- task:
		return true
	default:
		p.pending.Add(-1)
		return false
	}
}

// submitWait enqueues task like trySubmit, but first waits while the pool
// is under pressure (see pressured). It returns false without running the
// task when ctx ends or the pool shuts down before a slot frees; both return
// promptly. Only callers outside the pool may use it: a task that waited on
// its own pool could deadlock it.
//
// Acceptance goes through trySubmit, so a task submitWait accepts is covered
// by shutdown's drain exactly like one trySubmit accepts. A trySubmit that
// fails while the pool is not pressured lost a race for the last slot; the
// wait continues rather than shedding.
func (p *buildPool) submitWait(ctx context.Context, task func(context.Context)) bool {
	for {
		if p.closed.Load() {
			return false
		}
		if !p.pressured() && p.trySubmit(task) {
			if !p.pressured() {
				p.signalSlotFreed() // pass the wakeup on; see slotFreed
			}
			return true
		}
		select {
		case <-p.slotFreed:
		case <-ctx.Done():
			return false
		case <-p.done:
			return false
		}
	}
}

// signalSlotFreed wakes one submitWait waiter, if any; it never blocks.
func (p *buildPool) signalSlotFreed() {
	select {
	case p.slotFreed <- struct{}{}:
	default:
	}
}

// pressured reports whether the queue is at or above the high-water mark.
// WaitForSlot registrations and the reverse sweep pace on it. It takes no
// lock: len of a channel is a single read, and a stale answer only shifts a
// wait by one dequeue.
func (p *buildPool) pressured() bool {
	return len(p.queue) >= p.highWater
}

// waitIdle blocks until no tasks are queued or running. Tasks submit their
// follow-up work (parent cascades, drift re-builds) before finishing, so
// pending only reaches zero once a whole cascade has settled.
func (p *buildPool) waitIdle(ctx context.Context) error {
	for {
		if p.pending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// shutdown stops accepting work, then drains — workers keep pulling queued
// tasks until pending reaches zero or ctx ends, at which point any stragglers
// are cancelled. Unfinished work stays stale and is swept.
//
// Draining is pending-based rather than WaitGroup-based on purpose: trySubmit
// registers in pending before its closed recheck, so once closed is set,
// pending hitting zero proves every accepted task — queued or running — has
// finished. See the pending field comment for why a WaitGroup cannot do this
// job.
func (p *buildPool) shutdown(ctx context.Context) error {
	if p.closed.CompareAndSwap(false, true) {
		close(p.done)
	}
	err := p.waitIdle(ctx)
	p.cancel()
	return err
}
