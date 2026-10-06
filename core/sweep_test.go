package core

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// staleListingStore returns a canned stale backlog on top of recordingStore.
// Listing an entry puts its row in recordingStore's state as the real
// statement leaves it: marked under the entry's StaleSeq and claimed under its
// Token, with its tombstone, and with the metadata the row held (seeded with
// seedMetadata) left alone. The real statement claims every row it returns,
// so an entry given without a Token is claimed as a real claim does, under
// its StaleSeq.
type staleListingStore struct {
	recordingStore
	entries []StaleResource
}

func (s *staleListingStore) ListStale(_ context.Context, before time.Time, limit int, _ time.Duration) ([]StaleResource, error) {
	s.record("ListStale")
	entries := append([]StaleResource(nil), s.entries...)
	if len(entries) > limit {
		entries = entries[:limit]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows == nil {
		s.rows = make(map[model.Resource]*memRow)
	}
	for i, e := range entries {
		if e.Token == 0 {
			e.Token = e.StaleSeq
			entries[i] = e
		}
		r, ok := s.rows[e.Resource]
		if !ok {
			r = &memRow{}
			s.rows[e.Resource] = r
		}
		r.staleSeq, r.stale, r.owner, r.deleted = e.StaleSeq, true, e.Token, e.Deleted
		s.seq = max(s.seq, e.StaleSeq, e.Token)
	}
	return entries, nil
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

// Each stale entry is an owned build, and it fetches with the metadata its
// row holds when the build begins (BuildBegun.Metadata) — the same a build
// the registration submitted would have run with; a row without metadata
// builds with none.
func TestSweepStale_BuildsEachEntryWithItsRowsMetadata(t *testing.T) {
	st := &staleListingStore{
		entries: []StaleResource{
			{Resource: model.Resource{Type: "product", Id: "1"}, StaleSeq: 4},
			{Resource: model.Resource{Type: "product", Id: "2"}, StaleSeq: 5},
			{Resource: model.Resource{Type: "product", Id: "3"}, StaleSeq: 6},
		},
	}
	st.seedMetadata(product("1"), map[string]string{"fiber_operator_id": "op-1"})
	st.seedMetadata(product("2"), map[string]string{"fiber_operator_id": "op-2"})
	exec := &requestExecuter{}
	idx := mustNew(Config{
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

	got := make(map[string]map[string]string, len(exec.requests))
	for _, req := range exec.requests {
		got[req.ResourceID] = req.Metadata
	}
	want := map[string]map[string]string{
		"1": {"fiber_operator_id": "op-1"},
		"2": {"fiber_operator_id": "op-2"},
		"3": nil,
	}
	if len(got) != len(want) {
		t.Fatalf("each entry must be built once, built %v", got)
	}
	for id, md := range want {
		if !maps.Equal(got[id], md) || (md == nil && got[id] != nil) {
			t.Fatalf("build for product/%s ran with %v, want %v (all: %v)", id, got[id], md, got)
		}
	}
	for _, id := range []string{"1", "2", "3"} {
		if st.indexOf(fmt.Sprintf("FinishOwned:product/%s:", id)) == -1 {
			t.Fatalf("product/%s must be built as an owned build: %v", id, st.callsSnapshot())
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

// A tombstone whose type was dropped from config must not wedge the sweep:
// ListStale serves oldest-first, so a panic (or a permanent skip) on that
// entry would starve everything behind it. The ES documents live in
// de-configured indices — cleanup's territory — but the relation edges and
// the tombstone row must still be finished.
func TestSweepStale_TombstoneOfDeconfiguredType_DoesNotWedgeSweep(t *testing.T) {
	st := &staleListingStore{
		entries: []StaleResource{
			{Resource: model.Resource{Type: "ghost", Id: "g1"}, StaleSeq: 9, Deleted: true},
			{Resource: model.Resource{Type: "product", Id: "1"}, StaleSeq: 4},
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
	if st.indexOf("RemoveResource:ghost/g1") == -1 {
		t.Fatalf("the de-configured tombstone's relation edges must still be cleaned: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:ghost/g1:9") == -1 {
		t.Fatalf("the de-configured tombstone must be finished, or the oldest-first backlog never advances: %v", st.callsSnapshot())
	}
	if st.indexOf("BeginBuild:product/1") == -1 {
		t.Fatalf("entries behind the tombstone must still be served: %v", st.callsSnapshot())
	}
}
