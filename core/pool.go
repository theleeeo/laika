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
type buildPool struct {
	sem     chan struct{}
	pending atomic.Int64
	closed  atomic.Bool

	// baseCtx is the context tasks run under. It is independent of any
	// caller's context — inline work must outlive the RPC that triggered it —
	// and is cancelled only when shutdown stops waiting.
	baseCtx context.Context
	cancel  context.CancelFunc
}

func newBuildPool(size int) *buildPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &buildPool{
		sem:     make(chan struct{}, size),
		baseCtx: ctx,
		cancel:  cancel,
	}
}

// trySubmit runs task on the pool if a slot frees up within wait. It returns
// false — without running the task — when the pool stays saturated, the
// caller's ctx ends, or the pool is shut down. Tasks may call trySubmit
// themselves (cascades); because this never blocks past wait, a full pool
// sheds instead of deadlocking.
func (p *buildPool) trySubmit(ctx context.Context, wait time.Duration, task func(context.Context)) bool {
	if p.closed.Load() {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case p.sem <- struct{}{}:
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}

	// Register the task BEFORE re-checking closed. shutdown sets closed and
	// then drains by waiting for pending to reach zero, so this ordering is
	// what makes the drain cover concurrently-accepted submits: a submit that
	// incremented pending before shutdown's drain read is awaited (it either
	// runs its task or undoes itself just below), and a submit that
	// increments after shutdown set closed must observe closed here and
	// reject. Incrementing only after this recheck would leave a window where
	// shutdown sees pending==0 and returns while an accepted task is still
	// about to start.
	p.pending.Add(1)
	if p.closed.Load() {
		p.pending.Add(-1)
		<-p.sem
		return false
	}

	go func() {
		defer func() {
			<-p.sem
			p.pending.Add(-1)
		}()
		task(p.baseCtx)
	}()
	return true
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

// shutdown stops accepting work and waits for in-flight tasks until ctx ends,
// then cancels any stragglers. Unfinished work stays stale and is swept.
//
// Draining is pending-based rather than WaitGroup-based on purpose: trySubmit
// registers in pending before its closed recheck, so once closed is set,
// pending hitting zero proves every accepted task has finished. A WaitGroup
// with the same register-before-recheck ordering would instead risk
// wg.Add(1) racing a blocked wg.Wait at counter zero — the documented
// "Add called concurrently with Wait" misuse, which panics.
func (p *buildPool) shutdown(ctx context.Context) error {
	p.closed.Store(true)
	err := p.waitIdle(ctx)
	p.cancel()
	return err
}
