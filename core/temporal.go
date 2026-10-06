package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
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
	// reverseSweepScheduleIDPrefix, followed by the resource type, is the ID
	// of a type's ReverseSweep schedule.
	reverseSweepScheduleIDPrefix = "laika-reverse-sweep-"

	staleSweepWorkflowName   = "StaleSweep"
	rebuildWalkWorkflowName  = "RebuildWalk"
	reverseSweepWorkflowName = "ReverseSweep"
	sweepActivityName        = "SweepStale"
	rebuildActivityName      = "RunRebuild"
	reverseSweepActivityName = "RunReverseSweep"
)

// ReverseSweepParams names the resource type one ReverseSweep run sweeps. Its
// paging and pacing come from the worker's Config.ReverseSweeps entry for the
// type, not from the schedule.
type ReverseSweepParams struct {
	ResourceType string
}

// SweepParams configures one stale-sweep pass.
type SweepParams struct {
	// Threshold: only resources stale for longer than this are swept.
	Threshold time.Duration
	// BatchSize is the maximum number of resources per sweep activity.
	BatchSize int
}

// rebuildHeartbeatInterval is how often a running rebuild walk or reverse sweep
// emits a liveness heartbeat, well inside the one-minute HeartbeatTimeout
// RebuildWalkWorkflow and ReverseSweepWorkflow give their activities.
const rebuildHeartbeatInterval = 10 * time.Second

// temporalActivities hosts the Indexer-backed activity implementations.
type temporalActivities struct {
	idx *Indexer
	// heartbeatInterval overrides rebuildHeartbeatInterval for RunRebuild and
	// RunReverseSweep. Zero means the default; tests shorten it to observe a
	// liveness beat without waiting.
	heartbeatInterval time.Duration
	// reverseSweep overrides idx.ReverseSweepResumable as RunReverseSweep's
	// body. Nil means the Indexer's; tests stand in a fake to observe the
	// activity's cursor handling on its own.
	reverseSweep func(ctx context.Context, resourceType, after string, checkpoint func(after string)) (ReverseSweepResult, error)
}

func (a *temporalActivities) SweepStale(ctx context.Context, p SweepParams) (int, error) {
	return a.idx.SweepStale(ctx, p.Threshold, p.BatchSize)
}

// RunRebuild executes one rebuild selector synchronously, heartbeating so a
// dead worker is detected and the activity retried on another instance. The
// walk's cursor rides the heartbeat details: a retried attempt resumes where
// the dead one durably stopped instead of restarting from scratch (ADR 0011).
func (a *temporalActivities) RunRebuild(ctx context.Context, sel ResourceSelector) error {
	var start *RebuildCursor
	if activity.HasHeartbeatDetails(ctx) {
		var c RebuildCursor
		if err := activity.GetHeartbeatDetails(ctx, &c); err != nil {
			activity.GetLogger(ctx).Warn("unreadable rebuild heartbeat cursor; restarting walk from scratch",
				"error", err)
		} else {
			start = &c
		}
	}

	checkpoint, stop := heartbeatCursor(ctx, start, a.livenessInterval())
	defer stop()
	return a.idx.RebuildNowResumable(ctx, sel, start, checkpoint)
}

// RunReverseSweep runs one pass of a type's reverse sweep synchronously,
// heartbeating as RunRebuild does. Its cursor is the last id of the last page
// whose suspects were marked: a retried attempt resumes after it instead of
// re-probing the type from its first id (ADR 0011's pattern, ADR 0012).
func (a *temporalActivities) RunReverseSweep(ctx context.Context, p ReverseSweepParams) (ReverseSweepResult, error) {
	var start *string
	if activity.HasHeartbeatDetails(ctx) {
		var c string
		if err := activity.GetHeartbeatDetails(ctx, &c); err != nil {
			activity.GetLogger(ctx).Warn("unreadable reverse sweep heartbeat cursor; restarting sweep from the first id",
				"resource_type", p.ResourceType, "error", err)
		} else {
			start = &c
		}
	}
	var after string // empty: from the type's first id
	if start != nil {
		after = *start
	}

	sweep := a.reverseSweep
	if sweep == nil {
		sweep = a.idx.ReverseSweepResumable
	}
	checkpoint, stop := heartbeatCursor(ctx, start, a.livenessInterval())
	defer stop()
	return sweep(ctx, p.ResourceType, after, checkpoint)
}

// livenessInterval is heartbeatInterval, or rebuildHeartbeatInterval when it
// is zero.
func (a *temporalActivities) livenessInterval() time.Duration {
	if a.heartbeatInterval > 0 {
		return a.heartbeatInterval
	}
	return rebuildHeartbeatInterval
}

// heartbeatCursor keeps a resumable activity's cursor on its heartbeat (ADR
// 0011): it beats every interval until stop is called, and the checkpoint it
// returns records a new cursor at once. start is the cursor the attempt
// inherited, nil if none.
func heartbeatCursor[T any](ctx context.Context, start *T, interval time.Duration) (checkpoint func(T), stop func()) {
	var mu sync.Mutex
	// The inherited cursor seeds the beat: until this attempt checkpoints past
	// it, every liveness beat must re-record the position it resumed from.
	latest := start

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				mu.Lock()
				cur := latest
				mu.Unlock()
				// Heartbeat details replace each other wholesale, so a bare
				// liveness beat once a cursor exists would erase it — the
				// inherited one included, costing the next attempt its whole
				// run. Always re-record the latest cursor there is.
				if cur != nil {
					activity.RecordHeartbeat(ctx, *cur)
				} else {
					activity.RecordHeartbeat(ctx)
				}
			}
		}
	}()

	checkpoint = func(c T) {
		mu.Lock()
		latest = &c
		mu.Unlock()
		activity.RecordHeartbeat(ctx, c)
	}
	return checkpoint, func() { close(done) }
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

// ReverseSweepWorkflow runs one pass of a type's reverse sweep as a single
// long-running, heartbeating activity, with RebuildWalkWorkflow's options: a
// retried attempt resumes after the last page whose suspects were marked
// (ADR 0011's pattern, ADR 0012).
func ReverseSweepWorkflow(ctx workflow.Context, p ReverseSweepParams) (ReverseSweepResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var res ReverseSweepResult
	err := workflow.ExecuteActivity(ctx, reverseSweepActivityName, p).Get(ctx, &res)
	return res, err
}

// NewWorker creates a Temporal worker hosting the Indexer's workflows and
// activities. The caller starts and stops it. Every embedder runs the same
// worker, so the sweep safety net has exactly one implementation.
func (idx *Indexer) NewWorker() worker.Worker {
	w := worker.New(idx.temporal, idx.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(StaleSweepWorkflow, workflow.RegisterOptions{Name: staleSweepWorkflowName})
	w.RegisterWorkflowWithOptions(RebuildWalkWorkflow, workflow.RegisterOptions{Name: rebuildWalkWorkflowName})
	w.RegisterWorkflowWithOptions(ReverseSweepWorkflow, workflow.RegisterOptions{Name: reverseSweepWorkflowName})
	a := &temporalActivities{idx: idx}
	w.RegisterActivityWithOptions(a.SweepStale, activity.RegisterOptions{Name: sweepActivityName})
	w.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	w.RegisterActivityWithOptions(a.RunReverseSweep, activity.RegisterOptions{Name: reverseSweepActivityName})
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

// EnsureReverseSweepSchedules idempotently creates one ReverseSweep schedule
// per type in Config.ReverseSweeps, each every its type's Interval. As with
// EnsureSweepSchedule, an existing schedule is left untouched — changing a
// type's interval requires deleting its schedule in Temporal first — and a
// type removed from the config keeps its schedule until it is deleted there.
func (idx *Indexer) EnsureReverseSweepSchedules(ctx context.Context) error {
	return ensureReverseSweepSchedules(ctx, idx.temporal.ScheduleClient(), idx.taskQueue, idx.reverseSweeps)
}

func ensureReverseSweepSchedules(ctx context.Context, sc scheduleCreator, taskQueue string, sweeps map[string]ReverseSweepConfig) error {
	for _, typ := range slices.Sorted(maps.Keys(sweeps)) {
		_, err := sc.Create(ctx, client.ScheduleOptions{
			ID: reverseSweepScheduleIDPrefix + typ,
			Spec: client.ScheduleSpec{
				Intervals: []client.ScheduleIntervalSpec{{Every: sweeps[typ].Interval}},
			},
			Action: &client.ScheduleWorkflowAction{
				Workflow:  reverseSweepWorkflowName,
				Args:      []any{ReverseSweepParams{ResourceType: typ}},
				TaskQueue: taskQueue,
			},
			Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		})
		if err != nil && !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
			return fmt.Errorf("reverse sweep schedule for resource type %q: %w", typ, err)
		}
	}
	return nil
}

// temporalErrScheduleAlreadyRunning exists so tests can produce the sentinel
// without importing the temporal package themselves.
func temporalErrScheduleAlreadyRunning() error { return temporal.ErrScheduleAlreadyRunning }
