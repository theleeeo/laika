package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestStaleSweepWorkflow_LoopsUntilBacklogDrained(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	counts := []int{100, 100, 3} // two full batches, then a partial one
	i := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, p SweepParams) (int, error) {
		n := counts[i]
		i++
		return n, nil
	}, activity.RegisterOptions{Name: sweepActivityName})

	env.ExecuteWorkflow(StaleSweepWorkflow, SweepParams{Threshold: 5 * time.Minute, BatchSize: 100})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 3, i, "workflow must loop until a pass returns less than a full batch")
}

func TestRebuildWalkWorkflow_RunsSelectorActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var got ResourceSelector
	env.RegisterActivityWithOptions(func(ctx context.Context, sel ResourceSelector) error {
		got = sel
		return nil
	}, activity.RegisterOptions{Name: rebuildActivityName})

	sel := ResourceSelector{ResourceType: "product", ResourceIDs: []string{"1", "2"}}
	env.ExecuteWorkflow(RebuildWalkWorkflow, sel)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, sel, got)
}

// fakeScheduleCreator captures EnsureSweepSchedule's create-if-absent behavior.
type fakeScheduleCreator struct {
	opts []client.ScheduleOptions
	err  error
}

func (f *fakeScheduleCreator) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	f.opts = append(f.opts, options)
	return nil, f.err
}

func TestEnsureSweepSchedule_CreatesWithSkipOverlap(t *testing.T) {
	f := &fakeScheduleCreator{}
	err := ensureSweepSchedule(context.Background(), f, "laika-indexer", time.Minute, SweepParams{Threshold: 5 * time.Minute, BatchSize: 500})
	require.NoError(t, err)
	require.Len(t, f.opts, 1)
	require.Equal(t, sweepScheduleID, f.opts[0].ID)
	require.Equal(t, time.Minute, f.opts[0].Spec.Intervals[0].Every)
}

func TestEnsureSweepSchedule_ToleratesExisting(t *testing.T) {
	f := &fakeScheduleCreator{err: temporalErrScheduleAlreadyRunning()}
	err := ensureSweepSchedule(context.Background(), f, "laika-indexer", time.Minute, SweepParams{})
	require.NoError(t, err, "an already-existing schedule is success")
}
