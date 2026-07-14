package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// buildPool is the bounded in-process executor for inline builds and deletes.
// It is an accelerator, not a guarantee: callers must durably mark work
// (stale mark / tombstone) BEFORE submitting, so anything the pool sheds or
// loses to a crash is recovered by the stale sweep. See ADR 0008.
type buildPool struct {
	sem     chan struct{}
	wg      sync.WaitGroup
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

	if p.closed.Load() {
		<-p.sem
		return false
	}

	p.pending.Add(1)
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.pending.Add(-1)
			p.wg.Done()
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
func (p *buildPool) shutdown(ctx context.Context) error {
	p.closed.Store(true)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
