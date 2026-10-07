package core

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/projection"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
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

func TestReverseSweepWorkflow_RunsSweepActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var got ReverseSweepParams
	want := ReverseSweepResult{Listed: 7, Suspects: 2, FailedProbes: 1, Unprobed: 3}
	env.RegisterActivityWithOptions(func(ctx context.Context, p ReverseSweepParams) (ReverseSweepResult, error) {
		got = p
		return want, nil
	}, activity.RegisterOptions{Name: "RunReverseSweep"})

	p := ReverseSweepParams{ResourceType: "product"}
	env.ExecuteWorkflow(ReverseSweepWorkflow, p)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, p, got)
	var res ReverseSweepResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, want, res, "the workflow returns its activity's result")
}

func TestRunRebuild_ResumesFromHeartbeatCursor(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	v2 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: v1},
		{Version: 2, Executer: v2},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	a := &temporalActivities{idx: idx}
	env.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	env.SetHeartbeatDetails(RebuildCursor{PlanVersion: 2, PageToken: "p3"})

	// A version-targeted backfill: the single-active-plan shape that resumes.
	_, err := env.ExecuteActivity(rebuildActivityName, ResourceSelector{ResourceType: "product", Versions: []int{2}})
	if err != nil {
		t.Fatal(err)
	}

	if len(v1.requests()) != 0 {
		t.Fatal("the unselected version's plan must not run")
	}
	reqs := v2.requests()
	if len(reqs) != 1 || reqs[0].PageToken != "p3" {
		t.Fatalf("a retried activity must resume its walk from the heartbeat cursor's page token, got %+v", reqs)
	}
}

func TestRunRebuild_FreshAttemptWalksFromScratch(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	v2 := &cursorPagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
		{Items: []projection.BuildDoc{productDoc("1")}},
	}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: v1},
		{Version: 2, Executer: v2},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	a := &temporalActivities{idx: idx}
	env.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})

	_, err := env.ExecuteActivity(rebuildActivityName, ResourceSelector{ResourceType: "product"})
	if err != nil {
		t.Fatal(err)
	}

	if len(v1.requests()) != 1 || v1.requests()[0].PageToken != "" {
		t.Fatalf("a fresh attempt must walk every plan from the start, v1 saw %+v", v1.requests())
	}
	if len(v2.requests()) != 1 || v2.requests()[0].PageToken != "" {
		t.Fatalf("a fresh attempt must walk every plan from the start, v2 saw %+v", v2.requests())
	}
}

// blockingExecuter holds the walk open until release is closed, so a test can
// let the activity's liveness ticker fire mid-walk, then emits one tokenless
// page — a walk that never checkpoints.
type blockingExecuter struct {
	release <-chan struct{}
	docs    []projection.BuildDoc
}

func (e *blockingExecuter) Execute(_ context.Context, _ projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	select {
	case <-e.release:
	case <-time.After(2 * time.Second): // never block the suite if no beat lands
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: e.docs}
	close(ch)
	return ch
}

func TestRunRebuild_ResumedLivenessBeatPreservesInheritedCursor(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	beat := make(chan struct{}) // closed once a heartbeat has reached the server
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &cursorPagingExecuter{}}, // not selected; never runs
		{Version: 2, Executer: &blockingExecuter{release: beat, docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	// Drive the liveness ticker rather than waiting out the production interval.
	a := &temporalActivities{idx: idx, heartbeatInterval: time.Millisecond}
	env.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	env.SetHeartbeatDetails(RebuildCursor{PlanVersion: 2, PageToken: "p3"})

	var mu sync.Mutex
	var beats []converter.EncodedValues
	var once sync.Once
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		mu.Lock()
		beats = append(beats, details)
		mu.Unlock()
		once.Do(func() { close(beat) })
	})

	_, err := env.ExecuteActivity(rebuildActivityName, ResourceSelector{ResourceType: "product", Versions: []int{2}})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, beats, "the liveness ticker must have beaten during the walk")
	// This walk emits a single tokenless page, so it never checkpoints: every
	// beat it makes carries the cursor the attempt inherited — or erases it,
	// stranding the next attempt at the start of the walk.
	for i, d := range beats {
		var got RebuildCursor
		require.NoError(t, d.Get(&got),
			"beat %d recorded no cursor: a bare liveness beat erases the position this attempt resumed from", i)
		require.Equal(t, RebuildCursor{PlanVersion: 2, PageToken: "p3"}, got, "beat %d", i)
	}
}

// reverseSweepCall is what a fake sweep body saw.
type reverseSweepCall struct {
	resourceType string
	after        string
}

// runReverseSweepEnv registers RunReverseSweep on a fresh activity test
// environment with sweep standing in for ReverseSweepResumable, so these tests
// exercise the activity's cursor handling and not the sweep body.
func runReverseSweepEnv(a *temporalActivities) *testsuite.TestActivityEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(a.RunReverseSweep, activity.RegisterOptions{Name: reverseSweepActivityName})
	return env
}

func TestRunReverseSweep_ResumesFromHeartbeatCursor(t *testing.T) {
	var got []reverseSweepCall
	want := ReverseSweepResult{Listed: 4, Suspects: 1}
	env := runReverseSweepEnv(&temporalActivities{
		reverseSweep: func(_ context.Context, typ, after string, _ func(string)) (ReverseSweepResult, error) {
			got = append(got, reverseSweepCall{typ, after})
			return want, nil
		},
	})
	env.SetHeartbeatDetails("p-41")

	val, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: "product"})
	require.NoError(t, err)

	require.Equal(t, []reverseSweepCall{{"product", "p-41"}}, got,
		"a retried activity must resume its sweep after the heartbeat cursor's id")
	var res ReverseSweepResult
	require.NoError(t, val.Get(&res))
	require.Equal(t, want, res)
}

func TestRunReverseSweep_FreshAttemptSweepsFromTheStart(t *testing.T) {
	var got []reverseSweepCall
	env := runReverseSweepEnv(&temporalActivities{
		reverseSweep: func(_ context.Context, typ, after string, _ func(string)) (ReverseSweepResult, error) {
			got = append(got, reverseSweepCall{typ, after})
			return ReverseSweepResult{}, nil
		},
	})

	_, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: "product"})
	require.NoError(t, err)

	require.Equal(t, []reverseSweepCall{{"product", ""}}, got, "a fresh attempt must sweep from the type's first id")
}

func TestRunReverseSweep_FailedAttemptLeavesItsLatestCheckpoint(t *testing.T) {
	boom := errors.New("boom")
	env := runReverseSweepEnv(&temporalActivities{
		reverseSweep: func(_ context.Context, _, _ string, checkpoint func(string)) (ReverseSweepResult, error) {
			checkpoint("p-200")
			checkpoint("p-400")
			return ReverseSweepResult{}, boom
		},
	})

	var mu sync.Mutex
	var beats []string
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var c string
		require.NoError(t, details.Get(&c), "a checkpoint beat must carry its cursor")
		mu.Lock()
		beats = append(beats, c)
		mu.Unlock()
	})

	_, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: "product"})
	require.ErrorContains(t, err, "boom")

	mu.Lock()
	defer mu.Unlock()
	// The SDK sends the first beat and buffers later ones within its throttle
	// window, flushing the newest when the attempt returns an error: the retry
	// of a failed attempt resumes from its last checkpoint.
	require.Equal(t, []string{"p-200", "p-400"}, beats, "each checkpoint must be recorded as the heartbeat's cursor")
}

func TestRunReverseSweep_ResumedLivenessBeatPreservesInheritedCursor(t *testing.T) {
	beat := make(chan struct{}) // closed once a heartbeat has reached the server
	env := runReverseSweepEnv(&temporalActivities{
		// Drive the liveness ticker rather than waiting out the production interval.
		heartbeatInterval: time.Millisecond,
		reverseSweep: func(_ context.Context, _, _ string, _ func(string)) (ReverseSweepResult, error) {
			select {
			case <-beat:
			case <-time.After(2 * time.Second): // never block the suite if no beat lands
			}
			return ReverseSweepResult{}, nil
		},
	})
	env.SetHeartbeatDetails("p-41")

	var mu sync.Mutex
	var beats []converter.EncodedValues
	var once sync.Once
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		mu.Lock()
		beats = append(beats, details)
		mu.Unlock()
		once.Do(func() { close(beat) })
	})

	_, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: "product"})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, beats, "the liveness ticker must have beaten during the sweep")
	// This sweep never checkpoints: every beat it makes carries the cursor the
	// attempt inherited — or erases it, sending the next attempt back to the
	// type's first id.
	for i, d := range beats {
		var got string
		require.NoError(t, d.Get(&got),
			"beat %d recorded no cursor: a bare liveness beat erases the position this attempt resumed from", i)
		require.Equal(t, "p-41", got, "beat %d", i)
	}
}

// With no stand-in body, RunReverseSweep drives the Indexer's own sweep: a
// retried attempt lists from after its heartbeat cursor, and its checkpoints
// reach the heartbeat.
func TestRunReverseSweep_ResumesTheIndexersSweepFromHeartbeatCursor(t *testing.T) {
	st := &recordingStore{}
	seedRows(st, nil, "a", "b", "c", "d", "e")
	p := &fakeProbe{}
	idx, _ := newSweepIndexer(st, &sweepExecuter{}, p, ReverseSweepConfig{PageSize: 2})

	env := runReverseSweepEnv(&temporalActivities{idx: idx})
	env.SetHeartbeatDetails("b")
	var mu sync.Mutex
	var beats []string
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var c string
		require.NoError(t, details.Get(&c), "a checkpoint beat must carry its cursor")
		mu.Lock()
		beats = append(beats, c)
		mu.Unlock()
	})

	val, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: "product"})
	require.NoError(t, err)

	listings := callsWithPrefix(st, "ListResources")
	require.NotEmpty(t, listings)
	require.Equal(t, "ListResources:product:b:2", listings[0], "the sweep must list from after the heartbeat cursor")
	require.Equal(t, []string{"c", "d", "e"}, p.probedIDs(), "ids at or before the cursor must not be probed again")
	var res ReverseSweepResult
	require.NoError(t, val.Get(&res))
	require.Equal(t, ReverseSweepResult{Listed: 3}, res)

	mu.Lock()
	defer mu.Unlock()
	// The SDK sends the first beat and holds later ones within its throttle
	// window: the first is the first page's checkpoint, past the cursor.
	require.NotEmpty(t, beats, "the sweep's checkpoints must reach the heartbeat")
	require.Equal(t, "d", beats[0], "the first checkpoint is the last id of the first page after the cursor")
}

// A type Resources doesn't configure, or one without a Config.ReverseSweeps
// entry, fails the activity with a non-retryable error: no retry could make
// it succeed, so a run fails once instead of retrying to its attempt limit.
func TestRunReverseSweep_UnsweepableTypeFailsNonRetryable(t *testing.T) {
	st := &recordingStore{}
	p := &fakeProbe{}
	idx := mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: &sweepExecuter{}, Probe: p.probe}}},
		ES:        &captureBackend{},
		Store:     st,
	})

	for name, typ := range map[string]string{
		"unknown type":           "ghost",
		"no ReverseSweeps entry": "product",
	} {
		t.Run(name, func(t *testing.T) {
			env := runReverseSweepEnv(&temporalActivities{idx: idx})
			_, err := env.ExecuteActivity(reverseSweepActivityName, ReverseSweepParams{ResourceType: typ})
			require.Error(t, err)
			var appErr *temporal.ApplicationError
			require.True(t, errors.As(err, &appErr), "want an application error, got %T: %v", err, err)
			require.True(t, appErr.NonRetryable(), "want a non-retryable error, got %v", err)
		})
	}
	require.Empty(t, st.callsSnapshot(), "nothing may be listed")
	require.Empty(t, p.callsSnapshot(), "nothing may be probed")
}

// fakeScheduleCreator captures EnsureSweepSchedule's create-if-absent behavior.
type fakeScheduleCreator struct {
	opts []client.ScheduleOptions
	err  error
	// errByID overrides err for the schedule IDs it names.
	errByID map[string]error
}

func (f *fakeScheduleCreator) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	f.opts = append(f.opts, options)
	if err, ok := f.errByID[options.ID]; ok {
		return nil, err
	}
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

func TestEnsureReverseSweepSchedules_OnePerTypeInSortedOrder(t *testing.T) {
	f := &fakeScheduleCreator{}
	err := ensureReverseSweepSchedules(context.Background(), f, "laika-indexer", map[string]ReverseSweepConfig{
		"product":  {Interval: time.Hour, PageSize: 200, PageInterval: time.Second},
		"category": {Interval: 2 * time.Hour, PageSize: 50, PageInterval: time.Second},
	})
	require.NoError(t, err)
	require.Len(t, f.opts, 2)

	for i, want := range []struct {
		typ   string
		every time.Duration
	}{{"category", 2 * time.Hour}, {"product", time.Hour}} {
		o := f.opts[i]
		require.Equal(t, "laika-reverse-sweep-"+want.typ, o.ID, "schedule %d", i)
		require.Equal(t, []client.ScheduleIntervalSpec{{Every: want.every}}, o.Spec.Intervals, "schedule %d", i)
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, o.Overlap, "schedule %d", i)
		require.Equal(t, &client.ScheduleWorkflowAction{
			Workflow:  "ReverseSweep",
			Args:      []any{ReverseSweepParams{ResourceType: want.typ}},
			TaskQueue: "laika-indexer",
		}, o.Action, "schedule %d", i)
	}
}

func TestEnsureReverseSweepSchedules_ToleratesExistingAndCreatesTheRest(t *testing.T) {
	f := &fakeScheduleCreator{errByID: map[string]error{
		"laika-reverse-sweep-category": temporalErrScheduleAlreadyRunning(),
	}}
	err := ensureReverseSweepSchedules(context.Background(), f, "laika-indexer", map[string]ReverseSweepConfig{
		"category": {Interval: time.Hour},
		"product":  {Interval: time.Hour},
	})
	require.NoError(t, err, "an already-existing schedule is success")
	require.Len(t, f.opts, 2, "an existing schedule must not stop the remaining types' schedules")
	require.Equal(t, "laika-reverse-sweep-product", f.opts[1].ID)
}

func TestEnsureReverseSweepSchedules_ReturnsOtherErrors(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeScheduleCreator{err: boom}
	err := ensureReverseSweepSchedules(context.Background(), f, "laika-indexer", map[string]ReverseSweepConfig{
		"product": {Interval: time.Hour},
	})
	require.ErrorIs(t, err, boom)
}

// runRebuildFailingBegins runs the RunRebuild activity over a targeted
// rebuild of products 1–3 whose BeginBuild fails for 1 and 2; markErrs sets
// which of their marks fail.
func runRebuildFailingBegins(t *testing.T, markErrs map[string]int) error {
	t.Helper()
	st := &rebuildRecordingStore{
		beginErrs: map[string]error{"1": errors.New("begin failed"), "2": errors.New("begin failed")},
		markErrs:  markErrs,
	}
	exec := &staticExecuter{byID: map[string][]projection.BuildDoc{"3": {productDoc("3")}}}
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}}, 0)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions((&temporalActivities{idx: idx}).RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	_, err := env.ExecuteActivity(rebuildActivityName, ResourceSelector{ResourceType: "product", ResourceIDs: []string{"1", "2", "3"}})
	return err
}

// A rebuild whose only failures are resources it durably marked fails the
// activity non-retryably: a retry would add nothing the sweep doesn't
// (ruling R10). The error's type names the case and its details carry the
// count.
func TestRunRebuild_FailuresAllMarkedFailNonRetryable(t *testing.T) {
	err := runRebuildFailingBegins(t, nil)
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want an application error, got %T: %v", err, err)
	require.True(t, appErr.NonRetryable(), "want a non-retryable error, got %v", err)
	require.Equal(t, RebuildMarkedFailuresErrorType, appErr.Type())
	var count int
	require.NoError(t, appErr.Details(&count))
	require.Equal(t, 2, count)
}

// A rebuild one of whose marks failed stays retryable: a resource has no
// mark, so only a retry may still serve it.
func TestRunRebuild_FailedMarkStaysRetryable(t *testing.T) {
	err := runRebuildFailingBegins(t, map[string]int{"product/2": 1})
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want an application error, got %T: %v", err, err)
	require.False(t, appErr.NonRetryable(), "want a retryable error, got %v", err)
	require.NotEqual(t, RebuildMarkedFailuresErrorType, appErr.Type())
}

// logRecord is one message a captureLogger saw.
type logRecord struct {
	level   string
	msg     string
	keyvals []any
}

// captureLogger is a Temporal log.Logger that records every message.
type captureLogger struct {
	mu   sync.Mutex
	recs []logRecord
}

func (l *captureLogger) add(level, msg string, keyvals []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, logRecord{level, msg, keyvals})
}
func (l *captureLogger) Debug(msg string, kv ...any) { l.add("debug", msg, kv) }
func (l *captureLogger) Info(msg string, kv ...any)  { l.add("info", msg, kv) }
func (l *captureLogger) Warn(msg string, kv ...any)  { l.add("warn", msg, kv) }
func (l *captureLogger) Error(msg string, kv ...any) { l.add("error", msg, kv) }

// withMsg is every record whose message is msg.
func (l *captureLogger) withMsg(msg string) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logRecord
	for _, r := range l.recs {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// rebuildCalls records the selectors a stand-in RunRebuild activity got, and
// answers each with the outcome its metadata's actor names.
type rebuildCalls struct {
	outcome map[string]error

	mu   sync.Mutex
	sels []ResourceSelector
}

func (c *rebuildCalls) run(_ context.Context, sel ResourceSelector) error {
	c.mu.Lock()
	c.sels = append(c.sels, sel)
	c.mu.Unlock()
	return c.outcome[sel.Metadata["actor"]]
}

func (c *rebuildCalls) snapshot() []ResourceSelector {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.sels)
}

// forwardWalkWorkflowEnv runs ForwardWalkWorkflow over idx's own
// ForwardWalkSelectors activity and the real RebuildWalk child, its RunRebuild
// activity stood in by calls.
func forwardWalkWorkflowEnv(idx *Indexer, calls *rebuildCalls, logger *captureLogger) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	ts.SetLogger(logger)
	env := ts.NewTestWorkflowEnvironment()
	a := &temporalActivities{idx: idx}
	env.RegisterWorkflowWithOptions(RebuildWalkWorkflow, workflow.RegisterOptions{Name: rebuildWalkWorkflowName})
	env.RegisterActivityWithOptions(a.ForwardWalkSelectors, activity.RegisterOptions{Name: forwardWalkSelectorsActivityName})
	env.RegisterActivityWithOptions(calls.run, activity.RegisterOptions{Name: rebuildActivityName})
	return env
}

// twoActorWalks configures product's forward walk with two actors' maps.
func twoActorWalks() map[string]ForwardWalkConfig {
	return map[string]ForwardWalkConfig{"product": {
		PageSize:     7,
		PageInterval: 3 * time.Second,
		Metadata: func(context.Context) ([]map[string]string, error) {
			return []map[string]string{actor("A"), actor("B")}, nil
		},
	}}
}

// A ForwardWalk run starts one RebuildWalk child per metadata map, one after
// another in the func's order, each selecting the type with the map as its
// Metadata and the config's pacing.
func TestForwardWalkWorkflow_StartsOneRebuildWalkPerMap(t *testing.T) {
	idx, _, _ := newWalkIndexer(&walkExecuter{}, twoActorWalks())
	calls := &rebuildCalls{}
	env := forwardWalkWorkflowEnv(idx, calls, &captureLogger{})

	env.ExecuteWorkflow(ForwardWalkWorkflow, ForwardWalkParams{ResourceType: "product"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	pacing := &WalkPacing{PageSize: 7, PageInterval: 3 * time.Second}
	require.Equal(t, []ResourceSelector{
		{ResourceType: "product", Metadata: actor("A"), Pacing: pacing},
		{ResourceType: "product", Metadata: actor("B"), Pacing: pacing},
	}, calls.snapshot())
	var res ForwardWalkResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, ForwardWalkResult{Walks: 2}, res)
}

// A child that failed with the marked-failures application error is a done
// walk: the run succeeds, its result counts the walk's failed resources, and
// the count is logged.
func TestForwardWalkWorkflow_MarkedFailuresAreADoneWalk(t *testing.T) {
	idx, _, _ := newWalkIndexer(&walkExecuter{}, twoActorWalks())
	marked := &RebuildMarkedFailuresError{ResourceType: "product", Count: 4}
	calls := &rebuildCalls{outcome: map[string]error{
		"A": temporal.NewNonRetryableApplicationError(marked.Error(), RebuildMarkedFailuresErrorType, marked, marked.Count),
	}}
	logger := &captureLogger{}
	env := forwardWalkWorkflowEnv(idx, calls, logger)

	env.ExecuteWorkflow(ForwardWalkWorkflow, ForwardWalkParams{ResourceType: "product"})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(), "a walk whose failures are all marked is done")
	var res ForwardWalkResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, ForwardWalkResult{Walks: 2, FailedResources: 4}, res)
	require.Len(t, calls.snapshot(), 2)
	recs := logger.withMsg(forwardWalkMarkedFailuresMsg)
	require.Len(t, recs, 1, "the walk's marked failures must be logged")
	require.Contains(t, recs[0].keyvals, 4, "the log must carry the count")
}

// A child that fails with any other error fails the run — after the remaining
// walks ran.
func TestForwardWalkWorkflow_OtherChildFailureFailsTheRunAfterTheRest(t *testing.T) {
	idx, _, _ := newWalkIndexer(&walkExecuter{}, twoActorWalks())
	calls := &rebuildCalls{outcome: map[string]error{
		"A": temporal.NewNonRetryableApplicationError("listing failed", "Other", nil),
	}}
	env := forwardWalkWorkflowEnv(idx, calls, &captureLogger{})

	env.ExecuteWorkflow(ForwardWalkWorkflow, ForwardWalkParams{ResourceType: "product"})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "listing failed")
	sels := calls.snapshot()
	require.Len(t, sels, 2, "the walk after a failed one must run")
	require.Equal(t, actor("B"), sels[1].Metadata)
}

// The selectors activity fails a type the worker's config can't walk with a
// non-retryable error, and a failing metadata func with a retryable one.
func TestForwardWalkSelectors_Errors(t *testing.T) {
	walks := map[string]ForwardWalkConfig{"product": {
		Metadata: func(context.Context) ([]map[string]string, error) { return nil, errors.New("directory down") },
	}}
	configured, _, _ := newWalkIndexer(&walkExecuter{}, walks)
	unconfigured, _, _ := newWalkIndexer(&walkExecuter{}, nil)

	for name, tc := range map[string]struct {
		idx          *Indexer
		typ          string
		nonRetryable bool
	}{
		"unknown type":          {configured, "ghost", true},
		"no ForwardWalks entry": {unconfigured, "product", true},
		"metadata func error":   {configured, "product", false},
	} {
		t.Run(name, func(t *testing.T) {
			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestActivityEnvironment()
			a := &temporalActivities{idx: tc.idx}
			env.RegisterActivityWithOptions(a.ForwardWalkSelectors, activity.RegisterOptions{Name: forwardWalkSelectorsActivityName})
			_, err := env.ExecuteActivity(forwardWalkSelectorsActivityName, ForwardWalkParams{ResourceType: tc.typ})
			require.Error(t, err)
			var appErr *temporal.ApplicationError
			require.True(t, errors.As(err, &appErr), "want an application error, got %T: %v", err, err)
			require.Equal(t, tc.nonRetryable, appErr.NonRetryable(), "non-retryable: %v", err)
		})
	}
}

func TestEnsureForwardWalkSchedules_OnePerTypeInSortedOrder(t *testing.T) {
	f := &fakeScheduleCreator{}
	err := ensureForwardWalkSchedules(context.Background(), f, "laika-indexer", map[string]ForwardWalkConfig{
		"product":  {Interval: time.Hour, PageSize: 100, PageInterval: time.Second},
		"category": {Interval: 2 * time.Hour, PageSize: 50, PageInterval: time.Second},
	})
	require.NoError(t, err)
	require.Len(t, f.opts, 2)

	for i, want := range []struct {
		typ   string
		every time.Duration
	}{{"category", 2 * time.Hour}, {"product", time.Hour}} {
		o := f.opts[i]
		require.Equal(t, "laika-forward-walk-"+want.typ, o.ID, "schedule %d", i)
		require.Equal(t, []client.ScheduleIntervalSpec{{Every: want.every}}, o.Spec.Intervals, "schedule %d", i)
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, o.Overlap, "schedule %d", i)
		require.Equal(t, &client.ScheduleWorkflowAction{
			Workflow:  "ForwardWalk",
			Args:      []any{ForwardWalkParams{ResourceType: want.typ}},
			TaskQueue: "laika-indexer",
		}, o.Action, "schedule %d", i)
	}
}

func TestEnsureForwardWalkSchedules_ToleratesExistingAndCreatesTheRest(t *testing.T) {
	f := &fakeScheduleCreator{errByID: map[string]error{
		"laika-forward-walk-category": temporalErrScheduleAlreadyRunning(),
	}}
	err := ensureForwardWalkSchedules(context.Background(), f, "laika-indexer", map[string]ForwardWalkConfig{
		"category": {Interval: time.Hour},
		"product":  {Interval: time.Hour},
	})
	require.NoError(t, err, "an already-existing schedule is success")
	require.Len(t, f.opts, 2, "an existing schedule must not stop the remaining types' schedules")
	require.Equal(t, "laika-forward-walk-product", f.opts[1].ID)
}

func TestEnsureForwardWalkSchedules_ReturnsOtherErrors(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeScheduleCreator{err: boom}
	err := ensureForwardWalkSchedules(context.Background(), f, "laika-indexer", map[string]ForwardWalkConfig{
		"product": {Interval: time.Hour},
	})
	require.ErrorIs(t, err, boom)
}

// With no ForwardWalks entries there is nothing to schedule: the app calls
// EnsureForwardWalkSchedules unconditionally at startup.
func TestEnsureForwardWalkSchedules_NoEntriesCreatesNothing(t *testing.T) {
	f := &fakeScheduleCreator{}
	require.NoError(t, ensureForwardWalkSchedules(context.Background(), f, "laika-indexer", nil))
	require.Empty(t, f.opts)
}
