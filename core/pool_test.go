package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_RunsSubmittedTask(t *testing.T) {
	p := newBuildPool(2)
	var ran atomic.Bool
	ok := p.trySubmit(context.Background(), time.Second, func(context.Context) {
		ran.Store(true)
	})
	if !ok {
		t.Fatal("submit must succeed on an empty pool")
	}
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("task did not run")
	}
}

func TestPool_ShedsWhenSaturated(t *testing.T) {
	p := newBuildPool(1)
	block := make(chan struct{})
	if !p.trySubmit(context.Background(), time.Second, func(context.Context) { <-block }) {
		t.Fatal("first submit must succeed")
	}
	ok := p.trySubmit(context.Background(), 20*time.Millisecond, func(context.Context) {
		t.Error("shed task must never run")
	})
	if ok {
		t.Fatal("submit on a saturated pool must shed after the wait budget")
	}
	close(block)
	_ = p.waitIdle(t.Context())
}

func TestPool_WaitIdle_CoversCascadedSubmits(t *testing.T) {
	p := newBuildPool(2)
	var childRan atomic.Bool
	p.trySubmit(context.Background(), time.Second, func(ctx context.Context) {
		// A task submits follow-up work (parent cascade) before finishing.
		p.trySubmit(context.Background(), time.Second, func(context.Context) {
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

func TestPool_TaskRunsOnPoolContextNotCallerContext(t *testing.T) {
	p := newBuildPool(1)
	callerCtx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	p.trySubmit(callerCtx, time.Second, func(taskCtx context.Context) {
		cancel() // caller's ctx dies (RPC returned) while the task runs
		got <- taskCtx.Err()
	})
	if err := <-got; err != nil {
		t.Fatalf("task ctx must outlive the caller's ctx, got %v", err)
	}
	_ = p.waitIdle(t.Context())
}

func TestPool_ShutdownDrainsAndRejects(t *testing.T) {
	p := newBuildPool(1)
	var ran atomic.Bool
	p.trySubmit(context.Background(), time.Second, func(context.Context) {
		time.Sleep(30 * time.Millisecond)
		ran.Store(true)
	})
	if err := p.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("shutdown must wait for in-flight tasks")
	}
	if p.trySubmit(context.Background(), time.Second, func(context.Context) {}) {
		t.Fatal("submit after shutdown must be rejected")
	}
}
