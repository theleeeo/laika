package core

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// DefaultTaskQueue is the Temporal task queue the Indexer's worker polls
	// and its workflows run on.
	DefaultTaskQueue = "laika-indexer"

	sweepScheduleID = "laika-stale-sweep"

	staleSweepWorkflowName  = "StaleSweep"
	rebuildWalkWorkflowName = "RebuildWalk"
	sweepActivityName       = "SweepStale"
	rebuildActivityName     = "RunRebuild"
)

// SweepParams configures one stale-sweep pass.
type SweepParams struct {
	// Threshold: only resources stale for longer than this are swept.
	Threshold time.Duration
	// BatchSize is the maximum number of resources per sweep activity.
	BatchSize int
}

// temporalActivities hosts the Indexer-backed activity implementations.
type temporalActivities struct {
	idx *Indexer
}

func (a *temporalActivities) SweepStale(ctx context.Context, p SweepParams) (int, error) {
	return a.idx.SweepStale(ctx, p.Threshold, p.BatchSize)
}

// RunRebuild executes one rebuild selector synchronously, heartbeating so a
// dead worker is detected and the activity retried on another instance.
// Restart-from-scratch is correct: rebuilds are idempotent.
func (a *temporalActivities) RunRebuild(ctx context.Context, sel ResourceSelector) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return a.idx.RebuildNow(ctx, []ResourceSelector{sel})
}

// StaleSweepWorkflow drains the stale backlog in batches until a pass returns
// fewer than a full batch. Bounded per run; the next scheduled run (overlap
// policy: skip) picks up any remainder.
func StaleSweepWorkflow(ctx workflow.Context, p SweepParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})
	for range 100 {
		var n int
		if err := workflow.ExecuteActivity(ctx, sweepActivityName, p).Get(ctx, &n); err != nil {
			return err
		}
		if n < p.BatchSize {
			return nil
		}
	}
	return nil
}

// RebuildWalkWorkflow runs one rebuild selector as a single long-running,
// heartbeating activity.
func RebuildWalkWorkflow(ctx workflow.Context, sel ResourceSelector) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	return workflow.ExecuteActivity(ctx, rebuildActivityName, sel).Get(ctx, nil)
}

// NewWorker creates a Temporal worker hosting the Indexer's workflows and
// activities. The caller starts and stops it. Every embedder runs the same
// worker, so the sweep safety net has exactly one implementation.
func (idx *Indexer) NewWorker() worker.Worker {
	w := worker.New(idx.temporal, idx.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(StaleSweepWorkflow, workflow.RegisterOptions{Name: staleSweepWorkflowName})
	w.RegisterWorkflowWithOptions(RebuildWalkWorkflow, workflow.RegisterOptions{Name: rebuildWalkWorkflowName})
	a := &temporalActivities{idx: idx}
	w.RegisterActivityWithOptions(a.SweepStale, activity.RegisterOptions{Name: sweepActivityName})
	w.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	return w
}

// scheduleCreator is the slice of client.ScheduleClient EnsureSweepSchedule
// needs; a seam for testing create-if-absent behavior.
type scheduleCreator interface {
	Create(ctx context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error)
}

// EnsureSweepSchedule idempotently creates the StaleSweep schedule. An
// existing schedule is left untouched — changing interval or params requires
// deleting the schedule in Temporal first.
func (idx *Indexer) EnsureSweepSchedule(ctx context.Context, interval time.Duration, p SweepParams) error {
	return ensureSweepSchedule(ctx, idx.temporal.ScheduleClient(), idx.taskQueue, interval, p)
}

func ensureSweepSchedule(ctx context.Context, sc scheduleCreator, taskQueue string, interval time.Duration, p SweepParams) error {
	_, err := sc.Create(ctx, client.ScheduleOptions{
		ID: sweepScheduleID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: interval}},
		},
		Action: &client.ScheduleWorkflowAction{
			Workflow:  staleSweepWorkflowName,
			Args:      []any{p},
			TaskQueue: taskQueue,
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return nil
	}
	return err
}

// temporalErrScheduleAlreadyRunning exists so tests can produce the sentinel
// without importing the temporal package themselves.
func temporalErrScheduleAlreadyRunning() error { return temporal.ErrScheduleAlreadyRunning }
