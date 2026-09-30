package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// recordingStore records the order of Store calls and serves canned data.
type recordingStore struct {
	mu    sync.Mutex
	calls []string
	drift atomic.Bool // one-shot: report drift on the first AnyResourceVersionDrifted
	// marked, when set, receives (non-blocking) after every MarkStale and
	// RegisterChanges: the point past which a WaitForSlot registration may wait.
	marked chan struct{}

	// RegisterChanges serves these: parents as Registered.Parents, stale
	// resources rejected, registerErr failing the call. An accepted item i
	// gets StaleSeq 7+i. registrations records every batch it received.
	parents       []MarkedParent
	stale         map[model.Resource]bool
	registerErr   error
	registrations [][]Registration
}

func (s *recordingStore) signalMarked() {
	if s.marked == nil {
		return
	}
	select {
	case s.marked <- struct{}{}:
	default:
	}
}

func (s *recordingStore) count(prefix string) int {
	n := 0
	for _, c := range s.callsSnapshot() {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func (s *recordingStore) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *recordingStore) callsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *recordingStore) indexOf(prefix string) int {
	for i, c := range s.callsSnapshot() {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			return i
		}
	}
	return -1
}

// MarkStale fails on a done ctx, as a real store's query would.
func (s *recordingStore) MarkStale(ctx context.Context, rs []model.Resource, _ map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.record("MarkStale:%d", len(rs))
	s.signalMarked()
	return nil
}
func (s *recordingStore) BeginBuild(_ context.Context, r model.Resource) (int64, int64, error) {
	s.record("BeginBuild:%s/%s", r.Type, r.Id)
	return 1, 3, nil
}
func (s *recordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.record("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}
func (s *recordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, seq int64) error {
	s.record("DeleteResourceIfSeq:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}
func (s *recordingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
}
func (s *recordingStore) AddChildResources(context.Context, model.Resource, []model.Resource) error {
	return nil
}
func (s *recordingStore) AddRelations(context.Context, []Relation) error { return nil }
func (s *recordingStore) AnyResourceVersionDrifted(context.Context, []model.VersionedResource) (bool, error) {
	return s.drift.Swap(false), nil
}
func (s *recordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *recordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *recordingStore) RemoveResource(_ context.Context, r model.Resource) error {
	s.record("RemoveResource:%s/%s", r.Type, r.Id)
	return nil
}
func (s *recordingStore) RegisterChanges(_ context.Context, items []Registration) (Registered, error) {
	s.record("RegisterChanges:%d", len(items))
	s.mu.Lock()
	s.registrations = append(s.registrations, items)
	s.mu.Unlock()
	if s.registerErr != nil {
		return Registered{}, s.registerErr
	}
	out := Registered{Items: make([]RegisteredItem, len(items)), Parents: s.parents}
	for i, it := range items {
		if !s.stale[it.Resource] {
			out.Items[i] = RegisteredItem{Accepted: true, StaleSeq: int64(7 + i)}
		}
	}
	s.signalMarked()
	return out, nil
}

func newHotPathIndexer(st Store, poolSize, queueSize int) *Indexer {
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
	}
	return mustNew(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
		},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	})
}

func TestRegisterChange_MarksStaleBeforeBuilding_ThenClears(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	mark, begin, clear := st.indexOf("RegisterChanges"), st.indexOf("BeginBuild"), st.indexOf("ClearStale:product/1:3")
	if mark == -1 || begin == -1 || clear == -1 {
		t.Fatalf("missing calls: %v", st.callsSnapshot())
	}
	if mark > begin {
		t.Fatalf("the registration's mark must land before the build starts: %v", st.callsSnapshot())
	}
	if clear < begin {
		t.Fatalf("ClearStale must follow the build: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_PoolSaturated_ShedsButReturnsSuccess(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 1)

	// Occupy the only worker, then fill the queue's single slot.
	block := make(chan struct{})
	started := make(chan struct{})
	if !idx.pool.trySubmit(func(context.Context) { close(started); <-block }) {
		t.Fatal("failed to occupy pool")
	}
	<-started
	if !idx.pool.trySubmit(func(context.Context) {}) {
		t.Fatal("failed to fill queue")
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatalf("shed must not surface as an RPC error: %v", err)
	}
	if st.indexOf("RegisterChanges") == -1 {
		t.Fatal("the stale mark must land even when the build is shed")
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatal("no build may start on a saturated pool")
	}
	close(block)
	_ = idx.WaitForIdle(t.Context())
}

func TestRegisterChange_Delete_TombstonesAndRunsInlineDelete(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.indexOf("RegisterChanges") == -1 {
		t.Fatalf("delete must tombstone first: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/1:7") == -1 {
		t.Fatalf("inline delete must finish the tombstone with the captured seq: %v", st.callsSnapshot())
	}
}

func TestBuildOne_Drift_RemarksStale_SoGuardedClearIsNoop(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true) // first drift check reports drift
	idx := newHotPathIndexer(st, 2, 4)

	// Plan must emit relations for the drift check to run.
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
		Relations: []model.VersionedResource{
			{Resource: model.Resource{Type: "product", Id: "child"}, Version: 1},
		},
	}
	idx.plans = map[string][]projection.Plan{
		"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Drift must re-mark (MarkStale for the drift re-schedule, after the
	// notification's RegisterChanges) and trigger a second build.
	calls := st.callsSnapshot()
	marks, begins := st.count("MarkStale"), st.count("BeginBuild")
	if marks < 1 || begins < 2 {
		t.Fatalf("drift must re-mark and re-build (marks=%d begins=%d): %v", marks, begins, calls)
	}
}

// recordingExecuter serves a doc for every requested id and records each
// request, so a test sees which ids were built with which metadata.
type recordingExecuter struct {
	mu   sync.Mutex
	reqs []projection.BuildRequest
	// arrived and proceed, when set, park every Execute: it sends its id on
	// arrived and returns only once proceed is closed.
	arrived chan string
	proceed chan struct{}
}

func (e *recordingExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.reqs = append(e.reqs, req)
	e.mu.Unlock()
	if e.arrived != nil {
		e.arrived <- req.ResourceID
		<-e.proceed
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{{
		Root: model.Resource{Type: req.ResourceType, Id: req.ResourceID},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
	}}}
	close(ch)
	return ch
}

// metadataByID is the metadata each built id ran with.
func (e *recordingExecuter) metadataByID() map[string]map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]map[string]string, len(e.reqs))
	for _, r := range e.reqs {
		out[r.ResourceID] = r.Metadata
	}
	return out
}

func newRecordingIndexer(st Store, poolSize, queueSize int) (*Indexer, *recordingExecuter) {
	ex := &recordingExecuter{}
	return mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  poolSize,
		QueueSize: queueSize,
	}), ex
}

func product(id string) model.Resource { return model.Resource{Type: "product", Id: id} }

func TestRegisterChanges_InvalidBatch_FailsBeforeAnyStoreCall(t *testing.T) {
	cases := map[string]struct {
		ns   []Notification
		want func(error) bool
	}{
		"unknown resource": {
			ns: []Notification{
				{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
				{ResourceType: "nope", ResourceID: "1", Kind: ChangeUpdated},
			},
			want: func(err error) bool { return errors.Is(err, ErrUnknownResource) },
		},
		"duplicate resource": {
			ns: []Notification{
				{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1},
				{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated},
				{ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted},
			},
			want: func(err error) bool {
				_, ok := errors.AsType[*InvalidArgumentError](err)
				return ok
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			idx := newHotPathIndexer(st, 2, 4)

			statuses, err := idx.RegisterChanges(t.Context(), tc.ns)
			if !tc.want(err) {
				t.Fatalf("unexpected error %v", err)
			}
			if statuses != nil {
				t.Fatalf("a failed call returns no statuses, got %v", statuses)
			}
			if calls := st.callsSnapshot(); len(calls) != 0 {
				t.Fatalf("an invalid batch must not reach the store: %v", calls)
			}
		})
	}
}

func TestRegisterChanges_StoreError_ReturnsErrorAndSubmitsNothing(t *testing.T) {
	st := &recordingStore{registerErr: errors.New("boom")}
	idx := newHotPathIndexer(st, 2, 4)

	_, err := idx.RegisterChanges(t.Context(), []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeDeleted},
	})
	if err == nil {
		t.Fatal("a failed statement must fail the call")
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 || st.indexOf("DeleteResourceIfSeq") != -1 {
		t.Fatalf("nothing may be submitted when nothing committed: %v", st.callsSnapshot())
	}
}

func TestRegisterChanges_SubmitsEachAcceptedItemAndParentAfterTheStatement(t *testing.T) {
	st := &recordingStore{
		stale:   map[model.Resource]bool{product("2"): true},
		parents: []MarkedParent{{Resource: product("p"), Metadata: map[string]string{"m": "p"}}},
	}
	idx, ex := newRecordingIndexer(st, 4, 8)

	ns := []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 5, Metadata: map[string]string{"m": "1"}},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated, Version: 3, Metadata: map[string]string{"m": "2"}},
		{ResourceType: "product", ResourceID: "3", Kind: ChangeDeleted, Metadata: map[string]string{"m": "3"}},
		{ResourceType: "product", ResourceID: "4", Kind: ChangeCreated, Metadata: map[string]string{"m": "4"}},
	}
	statuses, err := idx.RegisterChanges(t.Context(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	want := []RegisterStatus{RegisterAccepted, RegisterStale, RegisterAccepted, RegisterAccepted}
	if fmt.Sprint(statuses) != fmt.Sprint(want) {
		t.Fatalf("statuses %v, want %v", statuses, want)
	}

	if len(st.registrations) != 1 {
		t.Fatalf("one statement per batch, got %d", len(st.registrations))
	}
	got := st.registrations[0]
	wantItems := []Registration{
		{Resource: product("1"), Version: 5, Metadata: ns[0].Metadata},
		{Resource: product("2"), Version: 3, Metadata: ns[1].Metadata},
		{Resource: product("3"), Deleted: true, Metadata: ns[2].Metadata},
		{Resource: product("4"), Metadata: ns[3].Metadata},
	}
	if fmt.Sprint(got) != fmt.Sprint(wantItems) {
		t.Fatalf("registrations %v, want %v", got, wantItems)
	}

	// Each accepted non-delete item and each Parent is built as itself with
	// its own metadata; the stale item and the delete are not built.
	wantBuilt := map[string]map[string]string{
		"1": {"m": "1"}, "4": {"m": "4"}, "p": {"m": "p"},
	}
	if built := ex.metadataByID(); fmt.Sprint(built) != fmt.Sprint(wantBuilt) {
		t.Fatalf("built %v, want %v", built, wantBuilt)
	}
	// The delete runs with the StaleSeq the store returned for it (7+2).
	if st.indexOf("DeleteResourceIfSeq:product/3:9") == -1 {
		t.Fatalf("the delete must be submitted with its own stale seq: %v", st.callsSnapshot())
	}

	ex.mu.Lock()
	nBuilds := len(ex.reqs)
	ex.mu.Unlock()
	if nBuilds != len(wantBuilt) {
		t.Fatalf("each root is built once, got %d builds", nBuilds)
	}

	// The statement is the registration's only mark and comes before every
	// submit.
	if calls := st.callsSnapshot(); calls[0] != "RegisterChanges:4" {
		t.Fatalf("the statement must come first: %v", calls)
	}
	if st.count("MarkStale") != 0 {
		t.Fatalf("registration marks nothing outside the statement: %v", st.callsSnapshot())
	}
}

// Builds are submitted per id: two items with the same metadata are two pool
// tasks and build in parallel, not one BuildArgs walked in sequence.
func TestRegisterChanges_SubmitsOneBuildPerID(t *testing.T) {
	st := &recordingStore{}
	idx, ex := newRecordingIndexer(st, 2, 4)
	ex.arrived = make(chan string, 2)
	ex.proceed = make(chan struct{})

	if _, err := idx.RegisterChanges(t.Context(), []Notification{
		{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated},
		{ResourceType: "product", ResourceID: "2", Kind: ChangeUpdated},
	}); err != nil {
		t.Fatal(err)
	}
	within(t, ex.arrived, "the first build")
	within(t, ex.arrived, "the second build, while the first is still running")
	close(ex.proceed)
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterChange_StaleItem_ReturnsErrStaleVersionAndSchedulesNothing(t *testing.T) {
	st := &recordingStore{stale: map[model.Resource]bool{product("1"): true}}
	idx := newHotPathIndexer(st, 2, 4)

	err := idx.RegisterChange(t.Context(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("want ErrStaleVersion, got %v", err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatalf("a stale item schedules nothing: %v", st.callsSnapshot())
	}
}

func TestRegisterChanges_EmptyBatch_DoesNotCallTheStore(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, 4)

	statuses, err := idx.RegisterChanges(t.Context(), nil)
	if err != nil || len(statuses) != 0 {
		t.Fatalf("got %v, %v", statuses, err)
	}
	if calls := st.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("an empty batch has nothing to commit: %v", calls)
	}
}
