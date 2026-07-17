package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// staleListingStore returns a canned stale backlog on top of recordingStore.
type staleListingStore struct {
	recordingStore
	entries []StaleResource
}

func (s *staleListingStore) ListStale(_ context.Context, before time.Time, limit int) ([]StaleResource, error) {
	s.record("ListStale")
	if len(s.entries) > limit {
		return s.entries[:limit], nil
	}
	return s.entries, nil
}

func TestSweepStale_RebuildsMarksAndFinishesTombstones(t *testing.T) {
	st := &staleListingStore{
		entries: []StaleResource{
			{Resource: model.Resource{Type: "product", Id: "1"}, StaleSeq: 4},
			{Resource: model.Resource{Type: "product", Id: "gone"}, StaleSeq: 9, Deleted: true},
		},
	}
	idx := newHotPathIndexer(st, 2, 4)

	n, err := idx.SweepStale(context.Background(), 5*time.Minute, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("swept count: got %d want 2", n)
	}
	if st.indexOf("BeginBuild:product/1") == -1 {
		t.Fatalf("live stale entry must be rebuilt: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/gone:9") == -1 {
		t.Fatalf("tombstone must be finished with its listed seq: %v", st.callsSnapshot())
	}
}

// requestExecuter records every BuildRequest and emits one matching doc.
type requestExecuter struct {
	mu       sync.Mutex
	requests []projection.BuildRequest
}

func (e *requestExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.requests = append(e.requests, req)
	e.mu.Unlock()
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: []projection.BuildDoc{{
		Root: model.Resource{Type: req.ResourceType, Id: req.ResourceID},
		Doc:  map[string]any{"fields": map[string]any{}},
	}}}
	close(ch)
	return ch
}

// Each stale entry's stored notification metadata must be replayed into the
// build that recovers it — a sweep rebuild runs with the same context an
// inline build would have had.
func TestSweepStale_ReplaysStoredMetadataPerEntry(t *testing.T) {
	st := &staleListingStore{
		entries: []StaleResource{
			{Resource: model.Resource{Type: "product", Id: "1"}, StaleSeq: 4, Metadata: map[string]string{"fiber_operator_id": "op-1"}},
			{Resource: model.Resource{Type: "product", Id: "2"}, StaleSeq: 5, Metadata: map[string]string{"fiber_operator_id": "op-2"}},
		},
	}
	exec := &requestExecuter{}
	idx := New(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  1,
		QueueSize: 1,
	})

	if _, err := idx.SweepStale(context.Background(), 5*time.Minute, 100); err != nil {
		t.Fatal(err)
	}

	got := make(map[string]string, len(exec.requests))
	for _, req := range exec.requests {
		got[req.ResourceID] = req.Metadata["fiber_operator_id"]
	}
	want := map[string]string{"1": "op-1", "2": "op-2"}
	for id, op := range want {
		if got[id] != op {
			t.Fatalf("build for product/%s ran with fiber_operator_id %q, want %q (all: %v)", id, got[id], op, got)
		}
	}
}

func TestSweepStale_EmptyBacklog(t *testing.T) {
	st := &staleListingStore{}
	idx := newHotPathIndexer(st, 2, 4)
	n, err := idx.SweepStale(context.Background(), time.Minute, 100)
	if err != nil || n != 0 {
		t.Fatalf("got n=%d err=%v", n, err)
	}
}
