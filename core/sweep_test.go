package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
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

// newBackoffIndexer is newRecordingIndexer with a SweepBackoff configured.
func newBackoffIndexer(st Store, backoff SweepBackoff) (*Indexer, *recordingExecuter) {
	ex := &recordingExecuter{}
	return mustNew(Config{
		Resources:       testResources(),
		Plans:           map[string][]projection.Plan{"product": {{Version: 1, Executer: ex}}},
		ES:              &fakeBackend{},
		Store:           st,
		PoolSize:        2,
		QueueSize:       4,
		SweepBackoff:    backoff.Base,
		SweepBackoffMax: backoff.Max,
	}), ex
}

// A sweep build or delete that fails releases its row through ReleaseFailed,
// with the Indexer's configured backoff, and not through ReleaseOwners: the
// sweep leaves the row until its backoff has passed instead of serving it
// again at the head of the next pass. Its mark or tombstone stays.
func TestSweepStale_FailedBuildOrDelete_BacksOffThroughReleaseFailed(t *testing.T) {
	backoff := SweepBackoff{Base: 7 * time.Minute, Max: 3 * time.Hour}
	cases := map[string]struct {
		entry StaleResource
		fail  func(st *staleListingStore, ex *recordingExecuter)
	}{
		"plan fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5},
			func(_ *staleListingStore, ex *recordingExecuter) { ex.failIDs = map[string]bool{"1": true} },
		},
		"BeginBuild fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5},
			func(st *staleListingStore, _ *recordingExecuter) { st.beginErr = errors.New("db down") },
		},
		"FinishOwned fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5},
			func(st *staleListingStore, _ *recordingExecuter) { st.finishErr = errors.New("db down") },
		},
		"type removed from config": {
			StaleResource{Resource: model.Resource{Type: "ghost", Id: "1"}, StaleSeq: 4, Token: 5},
			func(*staleListingStore, *recordingExecuter) {},
		},
		"BeginDelete fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5, Deleted: true},
			func(st *staleListingStore, _ *recordingExecuter) { st.beginDeleteErr = errors.New("db down") },
		},
		"delete's edge removal fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5, Deleted: true},
			func(st *staleListingStore, _ *recordingExecuter) { st.removeErr = errors.New("db down") },
		},
		"delete's DeleteResourceIfSeq fails": {
			StaleResource{Resource: product("1"), StaleSeq: 4, Token: 5, Deleted: true},
			func(st *staleListingStore, _ *recordingExecuter) { st.deleteErr = errors.New("db down") },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &staleListingStore{entries: []StaleResource{tc.entry}}
			idx, ex := newBackoffIndexer(st, backoff)
			tc.fail(st, ex)

			if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
				t.Fatal(err)
			}
			waitIdle(t, idx)

			res := tc.entry.Resource
			if st.indexOf(fmt.Sprintf("ReleaseFailed:%s/%s:5", res.Type, res.Id)) == -1 {
				t.Fatalf("the failed entry must be released through ReleaseFailed: %v", st.callsSnapshot())
			}
			if n := st.count("ReleaseOwners"); n != 0 {
				t.Fatalf("a failure must not release through ReleaseOwners: %v", st.callsSnapshot())
			}
			if got := st.failedBackoffsSnapshot(); len(got) != 1 || got[0] != backoff {
				t.Fatalf("ReleaseFailed must get the configured backoff %+v, got %+v", backoff, got)
			}
			r, ok := st.row(res)
			if !ok || !r.stale || r.deleted != tc.entry.Deleted || r.owner != 0 {
				t.Fatalf("the mark or tombstone must stay, unowned: %+v (exists %v)", r, ok)
			}
			if r.attempts != 1 {
				t.Fatalf("the row must be backed off once, attempts %d", r.attempts)
			}
		})
	}
}

// A failed owned build logs its attempt count, as ReleaseFailed returns it,
// with its type, id, error and retry time, in one line: at Warn below the
// fifth attempt in a row, at Error from the fifth on.
func TestSweepStale_FailedBuild_LogsItsAttempts(t *testing.T) {
	for _, tc := range []struct {
		before    int
		wantLevel string
	}{
		{0, "WARN"},
		{3, "WARN"},
		{4, "ERROR"},
		{9, "ERROR"},
	} {
		t.Run(fmt.Sprintf("after %d failures", tc.before), func(t *testing.T) {
			logs := captureDefaultLogs(t)
			st := &staleListingStore{entries: []StaleResource{{Resource: product("1"), StaleSeq: 4, Token: 5}}}
			st.seedMetadata(product("1"), nil)
			st.seedAttempts(product("1"), tc.before)
			idx, ex := newBackoffIndexer(st, SweepBackoff{Base: time.Minute, Max: time.Hour})
			ex.failIDs = map[string]bool{"1": true}

			if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
				t.Fatal(err)
			}
			waitIdle(t, idx)

			var lines []map[string]any
			for _, rec := range logs.records(t) {
				if rec["id"] == "1" && (rec["level"] == "WARN" || rec["level"] == "ERROR") {
					lines = append(lines, rec)
				}
			}
			if len(lines) != 1 {
				t.Fatalf("the failure must be logged in one line naming product/1, got %v", lines)
			}
			rec := lines[0]
			if rec["level"] != tc.wantLevel {
				t.Errorf("level: got %v want %s (%v)", rec["level"], tc.wantLevel, rec)
			}
			if rec["type"] != "product" {
				t.Errorf("the line must name the type: %v", rec)
			}
			if got, _ := rec["attempts"].(float64); int(got) != tc.before+1 {
				t.Errorf("attempts: got %v want %d (%v)", rec["attempts"], tc.before+1, rec)
			}
			if e, _ := rec["error"].(string); !strings.Contains(e, "plan failed") {
				t.Errorf("the line must carry the build's error: %v", rec)
			}
			if _, ok := rec["retry_after"]; !ok {
				t.Errorf("the line must carry the retry time: %v", rec)
			}
		})
	}
}
