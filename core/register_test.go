package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// recordingStore records the order of Store calls and serves canned data.
type recordingStore struct {
	mu      sync.Mutex
	calls   []string
	parents []model.Resource
	drift   atomic.Bool // one-shot: report drift on the first AnyResourceVersionDrifted
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

func (s *recordingStore) MarkStale(_ context.Context, rs []model.Resource) error {
	s.record("MarkStale:%d", len(rs))
	return nil
}
func (s *recordingStore) MarkDeleted(_ context.Context, r model.Resource) (int64, error) {
	s.record("MarkDeleted:%s/%s", r.Type, r.Id)
	return 7, nil
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
	return s.parents, nil
}
func (s *recordingStore) RemoveResource(_ context.Context, r model.Resource) error {
	s.record("RemoveResource:%s/%s", r.Type, r.Id)
	return nil
}
func (s *recordingStore) UpsertResource(_ context.Context, r model.Resource, v int64) error {
	s.record("UpsertResource:%s/%s:%d", r.Type, r.Id, v)
	return nil
}

func newHotPathIndexer(st Store, poolSize int, submitWait time.Duration) *Indexer {
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
	}
	return New(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
		},
		ES:         &fakeBackend{},
		Store:      st,
		PoolSize:   poolSize,
		SubmitWait: submitWait,
	})
}

func TestRegisterChange_MarksStaleBeforeBuilding_ThenClears(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, time.Second)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	mark, begin, clear := st.indexOf("MarkStale"), st.indexOf("BeginBuild"), st.indexOf("ClearStale:product/1:3")
	if mark == -1 || begin == -1 || clear == -1 {
		t.Fatalf("missing calls: %v", st.callsSnapshot())
	}
	if mark > begin {
		t.Fatalf("MarkStale must land before the build starts: %v", st.callsSnapshot())
	}
	if clear < begin {
		t.Fatalf("ClearStale must follow the build: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_PoolSaturated_ShedsButReturnsSuccess(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 20*time.Millisecond)

	// Occupy the only slot.
	block := make(chan struct{})
	if !idx.pool.trySubmit(context.Background(), time.Second, func(context.Context) { <-block }) {
		t.Fatal("failed to occupy pool")
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatalf("shed must not surface as an RPC error: %v", err)
	}
	if st.indexOf("MarkStale") == -1 {
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
	idx := newHotPathIndexer(st, 2, time.Second)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.indexOf("MarkDeleted:product/1") == -1 {
		t.Fatalf("delete must tombstone first: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/1:7") == -1 {
		t.Fatalf("inline delete must finish the tombstone with the captured seq: %v", st.callsSnapshot())
	}
}

func TestBuildOne_Drift_RemarksStale_SoGuardedClearIsNoop(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true) // first drift check reports drift
	idx := newHotPathIndexer(st, 2, time.Second)

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

	// Drift must re-mark (MarkStale at least twice: once for the notification,
	// once for the drift re-schedule) and trigger a second build.
	calls := st.callsSnapshot()
	var marks, begins int
	for _, c := range calls {
		if len(c) >= 9 && c[:9] == "MarkStale" {
			marks++
		}
		if len(c) >= 10 && c[:10] == "BeginBuild" {
			begins++
		}
	}
	if marks < 2 || begins < 2 {
		t.Fatalf("drift must re-mark and re-build (marks=%d begins=%d): %v", marks, begins, calls)
	}
}
