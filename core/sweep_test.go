package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
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
		r.registeredSinceClaim = false
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
	if st.indexOf("BeginBuilds:product/1") == -1 {
		t.Fatalf("live stale entry must be rebuilt: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/gone:9") == -1 {
		t.Fatalf("tombstone must be finished with its listed seq: %v", st.callsSnapshot())
	}
}

// requestExecuter records every BuildRequest and emits a doc for each asked id.
type requestExecuter struct {
	mu       sync.Mutex
	requests []projection.BuildRequest
}

func (e *requestExecuter) Execute(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	e.mu.Lock()
	e.requests = append(e.requests, req)
	e.mu.Unlock()
	docs := make([]projection.BuildDoc, 0, len(req.ResourceIDs))
	for _, id := range req.ResourceIDs {
		docs = append(docs, projection.BuildDoc{
			Root: model.Resource{Type: req.ResourceType, Id: id},
			Doc:  map[string]any{"fields": map[string]any{}},
		})
	}
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: docs}
	close(ch)
	return ch
}

// Each stale entry is an owned build, and it fetches with the metadata its
// row holds when the build begins (BuildBegun.Metadata) — the same a build
// the registration submitted would have run with; a row without metadata
// builds with none. Entries of one batch whose rows hold different metadata
// are begun together and still fetched each with its own row's metadata.
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
		for _, id := range req.ResourceIDs {
			got[id] = req.Metadata
		}
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
	if got := callsWithPrefix(&st.recordingStore, "BeginBuilds:"); !slices.Equal(got, []string{"BeginBuilds:product/1:4,product/2:5,product/3:6"}) {
		t.Fatalf("the three entries must be one batch, begun together, got %v", got)
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

// A tombstone whose type was dropped from config must still be finished, not
// left failing: a panic would take the sweep down with the entries behind it,
// and a delete that refused it would fail every time, retried by the sweep at
// a growing interval up to the backoff's cap forever. The ES documents live
// in de-configured indices — cleanup's territory — but the relation edges and
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
		t.Fatalf("the de-configured tombstone must be finished, not left failing for the sweep to retry forever: %v", st.callsSnapshot())
	}
	if st.indexOf("BeginBuilds:product/1") == -1 {
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
// again at the head of the next pass. Its mark or tombstone stays, and the
// failure is logged once, naming the entry.
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
			logs := captureDefaultLogs(t)
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
			lines := failureLines(t, logs)
			if len(lines) != 1 {
				t.Fatalf("the failure must be logged once, got %d lines: %v", len(lines), lines)
			}
			if lines[0]["type"] != res.Type || lines[0]["id"] != res.Id {
				t.Fatalf("the failure's line must name %s/%s: %v", res.Type, res.Id, lines[0])
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

// failureLines is every Warn or Error record logged so far that carries an
// error: the lines that report a failure.
func failureLines(t *testing.T, logs *capturedLogs) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, rec := range logs.records(t) {
		if _, ok := rec["error"]; ok && (rec["level"] == "WARN" || rec["level"] == "ERROR") {
			out = append(out, rec)
		}
	}
	return out
}

// An owned inline build of a type no longer configured fails for each of its
// ids, and each is logged once, with its attempt count: the pool task does
// not log Build's error again.
func TestBuildTask_UnknownType_LogsEachIDOnce(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{}
	idx, _ := newBackoffIndexer(st, SweepBackoff{Base: time.Minute, Max: time.Hour})
	ghost := func(id string) model.Resource { return model.Resource{Type: "ghost", Id: id} }
	owned, err := st.MarkStale(t.Context(), []model.Resource{ghost("1"), ghost("2")}, time.Minute)
	if err != nil || len(owned) != 2 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	args := BuildArgs{ResourceType: "ghost", ResourceIds: []string{"1", "2"}, OwnerTokens: map[string]int64{}}
	for _, o := range owned {
		args.OwnerTokens[o.Id] = o.Token
	}

	idx.buildTask(args)(t.Context(), owned)

	lines := failureLines(t, logs)
	if len(lines) != 2 {
		t.Fatalf("each id's failure must be logged once, got %d lines: %v", len(lines), lines)
	}
	for _, rec := range lines {
		if rec["type"] != "ghost" || (rec["id"] != "1" && rec["id"] != "2") || rec["attempts"] == nil {
			t.Fatalf("each line must name its id and attempts: %v", rec)
		}
	}
}

// A ReleaseFailed that itself fails releases nothing — each ownership expires
// with its lease — and each owned entry is logged with its type, id, the
// work's error and the release's.
func TestReleaseFailed_StoreError_LogsEachEntry(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &recordingStore{releaseFailedErr: errors.New("db gone")}
	idx, _ := newBackoffIndexer(st, SweepBackoff{Base: time.Minute, Max: time.Hour})
	ghost := func(id string) model.Resource { return model.Resource{Type: "ghost", Id: id} }
	owned, err := st.MarkStale(t.Context(), []model.Resource{ghost("1"), ghost("2")}, time.Minute)
	if err != nil || len(owned) != 2 {
		t.Fatalf("claiming: %v %v", owned, err)
	}
	args := BuildArgs{ResourceType: "ghost", ResourceIds: []string{"1", "2"}, OwnerTokens: map[string]int64{}}
	for _, o := range owned {
		args.OwnerTokens[o.Id] = o.Token
	}

	idx.buildTask(args)(t.Context(), owned)

	if st.count("ReleaseFailedFailed:") != 2 {
		t.Fatalf("setup: the release must be tried and fail: %v", st.callsSnapshot())
	}
	for _, o := range owned {
		if got := st.owner(o.Resource); got != o.Token {
			t.Fatalf("a failed release keeps %s's ownership until its lease expires, owner %d", o.Id, got)
		}
	}
	lines := failureLines(t, logs)
	if len(lines) != 2 {
		t.Fatalf("each entry must be logged once, got %d lines: %v", len(lines), lines)
	}
	ids := map[any]bool{}
	for _, rec := range lines {
		ids[rec["id"]] = true
		e, _ := rec["error"].(string)
		re, _ := rec["release_error"].(string)
		if rec["type"] != "ghost" || !strings.Contains(e, "unknown resource") || re != "db gone" {
			t.Fatalf("each line must name the type, the work's error and the release's: %v", rec)
		}
	}
	if !ids["1"] || !ids["2"] {
		t.Fatalf("each entry must be named: %v", lines)
	}
}

// recordingStore's ReleaseFailed backs a row off as the contract says:
// min(Base × 2^(n−1), Max) for its nth failure in a row, without the
// doubling overflowing at a high attempt count.
func TestRecordingStore_ReleaseFailed_BacksOffByTheContract(t *testing.T) {
	b := SweepBackoff{Base: time.Hour, Max: 100 * 365 * 24 * time.Hour}
	for _, before := range append(rangeInts(0, 100), 1000) {
		st := &recordingStore{}
		owned, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, time.Minute)
		if err != nil || len(owned) != 1 {
			t.Fatalf("claiming: %v %v", owned, err)
		}
		st.seedAttempts(product("1"), before)
		start := time.Now()
		got, err := st.ReleaseFailed(t.Context(), owned, b)
		if err != nil || len(got) != 1 || got[0].Attempts != before+1 {
			t.Fatalf("after %d failures: got %+v %v", before, got, err)
		}
		want := b.Max
		if d := float64(b.Base) * math.Pow(2, float64(before)); d < float64(b.Max) {
			want = time.Duration(d)
		}
		if delay := got[0].After.Sub(start); delay < want || delay > want+time.Second {
			t.Errorf("after %d failures: backed off %v, want %v", before, delay, want)
		}
	}
}

// rangeInts is lo, lo+1, …, hi.
func rangeInts(lo, hi int) []int {
	out := make([]int, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, i)
	}
	return out
}

// R7: a failed owned build of a resource whose own change was registered
// after the claim is released without a backoff — ReleaseFailed returns it
// with no attempts — so the sweep, or the change's next owner, retries it at
// once. It is logged at Warn as a change registered meanwhile, with its type,
// id and error, and never with an attempt count or at Error, however many
// times it failed before.
func TestSweepStale_FailedBuild_ChangeRegisteredMeanwhile_IsNotBackedOff(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &staleListingStore{entries: []StaleResource{{Resource: product("1"), StaleSeq: 4, Token: 5}}}
	st.seedMetadata(product("1"), nil)
	st.seedAttempts(product("1"), 7)
	var once sync.Once
	st.onRenew = func([]Owned) {
		once.Do(func() {
			if _, err := st.RegisterChanges(t.Context(), []Registration{{Resource: product("1")}}, time.Minute); err != nil {
				t.Errorf("registering: %v", err)
			}
		})
	}
	idx, ex := newBackoffIndexer(st, SweepBackoff{Base: time.Minute, Max: time.Hour})
	ex.failIDs = map[string]bool{"1": true}

	if _, err := idx.SweepStale(t.Context(), 5*time.Minute, 100); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if st.indexOf("ReleaseFailed:product/1:5") == -1 || st.count("ReleaseOwners") != 0 {
		t.Fatalf("the failure must still be released through ReleaseFailed: %v", st.callsSnapshot())
	}
	r, _ := st.row(product("1"))
	if !r.stale || r.owner != 0 || r.attempts != 0 || !r.after.IsZero() {
		t.Fatalf("the row must keep its mark, be released and not be backed off: %+v", r)
	}
	lines := failureLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("the failure must be logged once, got %d lines: %v", len(lines), lines)
	}
	rec := lines[0]
	msg, _ := rec["msg"].(string)
	e, _ := rec["error"].(string)
	if rec["level"] != "WARN" || rec["type"] != "product" || rec["id"] != "1" || !strings.Contains(e, "plan failed") || !strings.Contains(msg, "registered meanwhile") {
		t.Fatalf("the line must be a Warn naming the change registered meanwhile, the type, id and error: %v", rec)
	}
	if _, ok := rec["attempts"]; ok {
		t.Fatalf("the line must not carry an attempt count: %v", rec)
	}
}

// A pass serves its tombstones first, each on its own, and then its build
// entries in per-type batches of the type's batch size, one Build each: 250
// build entries at the default batch size of 100 are three Builds of 100, 100
// and 50 ids, each begun in one BeginBuilds under its entries' listed tokens,
// in list order. Each batch renews its ownership when the pass reaches it,
// not before the batch ahead of it has run.
func TestSweepStale_BuildsItsEntriesInBatchesOfTheTypesBatchSize(t *testing.T) {
	ids := productIDs(250)
	var entries []StaleResource
	for i, id := range ids {
		if i == 0 || i == 150 {
			entries = append(entries, StaleResource{Resource: product(fmt.Sprintf("gone%d", i)), StaleSeq: int64(5000 + i), Token: int64(6000 + i), Deleted: true})
		}
		entries = append(entries, StaleResource{Resource: product(id), StaleSeq: int64(1 + i), Token: int64(1000 + i)})
	}
	st := &staleListingStore{entries: entries}
	idx, ex, _ := newCaptureIndexer(st, 2, 4)

	n, err := idx.SweepStale(t.Context(), 5*time.Minute, 1000)
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, idx)

	if n != 252 {
		t.Fatalf("swept count: got %d want 252", n)
	}
	if got := callsWithPrefix(&st.recordingStore, "BeginDelete:"); !slices.Equal(got, []string{"BeginDelete:product/gone0:6000", "BeginDelete:product/gone150:6150"}) {
		t.Fatalf("each tombstone must be deleted once under its token, in list order, got %v", got)
	}
	begins := callsWithPrefix(&st.recordingStore, "BeginBuilds:")
	if len(begins) != 3 {
		t.Fatalf("want three Builds, each begun in one BeginBuilds, got %d: %v", len(begins), begins)
	}
	for b, want := range [][2]int{{0, 100}, {100, 200}, {200, 250}} {
		names := make([]string, 0, want[1]-want[0])
		for i := want[0]; i < want[1]; i++ {
			names = append(names, fmt.Sprintf("product/%s:%d", ids[i], 1000+i))
		}
		if got := "BeginBuilds:" + strings.Join(names, ","); begins[b] != got {
			t.Fatalf("batch %d must begin ids %d..%d under their tokens, got %s", b, want[0], want[1]-1, begins[b])
		}
	}
	if reqs := ex.requests(); len(reqs) != 3 {
		t.Fatalf("the plan must be asked once per batch, got %d requests", len(reqs))
	}
	calls := st.callsSnapshot()
	firstBuild := st.indexOf("BeginBuilds:")
	for _, d := range []string{"DeleteResourceIfSeq:product/gone0:5000:6000", "DeleteResourceIfSeq:product/gone150:5150:6150"} {
		if at := st.indexOf(d); at == -1 || at > firstBuild {
			t.Fatalf("the tombstones must be finished before the first batch is built, %s at %d, first BeginBuilds at %d: %v", d, at, firstBuild, calls)
		}
	}
	for b, first := range []int{100, 200} {
		renew := st.indexOf(fmt.Sprintf("RenewOwners:product/%s:%d", ids[first], 1000+first))
		if prev := slices.Index(calls, begins[b]); renew < prev {
			t.Fatalf("batch %d must renew after batch %d has begun (renew at %d, its BeginBuilds at %d)", b+1, b, renew, prev)
		}
	}
	for _, id := range ids {
		if r, _ := st.row(product(id)); r.stale || r.owner != 0 {
			t.Fatalf("product/%s must be built, its mark and ownership cleared: %+v", id, r)
		}
	}
}
