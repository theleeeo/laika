package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/projection"
)

// productIDs is "0" … "n-1".
func productIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	return ids
}

// newBatchedRebuildIndexer is newRebuildIndexer with a build batch size of
// batchSize (0 takes the default) and a plan per executer, v1 first.
func newBatchedRebuildIndexer(st Store, es SearchBackend, batchSize int, execs ...aggregation.Executer[projection.BuildRequest, projection.BuildDoc]) *Indexer {
	plans := make([]projection.Plan, len(execs))
	for i, e := range execs {
		plans[i] = projection.Plan{Version: i + 1, Executer: e}
	}
	return mustNew(Config{
		Resources:      twoVersionResources(),
		Plans:          map[string][]projection.Plan{"product": plans},
		ES:             es,
		Store:          st,
		BuildBatchSize: batchSize,
	})
}

// beginBuildsCalls is every BeginBuilds call recorded, each as its ids.
func beginBuildsCalls(st *rebuildRecordingStore) [][]string {
	var out [][]string
	for _, c := range st.callsSnapshot() {
		rest, ok := strings.CutPrefix(c, "BeginBuilds:")
		if !ok {
			continue
		}
		var ids []string
		for _, r := range strings.Split(rest, ",") {
			ids = append(ids, strings.TrimPrefix(r, "product/"))
		}
		out = append(out, ids)
	}
	return out
}

// A targeted rebuild of 250 ids, at the default batch size of 100, begins
// them in three BeginBuilds calls in BeginInsert — 100, 100 and 50 ids, in
// order — and asks each plan about each batch once. Each id still gets its
// own Build Sequence on its writes and settles on its own.
func TestRebuildByIDs_250IDs_BeginsAndAsksInThreeBatches(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	v1 := &requestLog{exec: &staticExecuter{docs: []projection.BuildDoc{productDoc("x")}}}
	v2 := &requestLog{exec: &staticExecuter{docs: []projection.BuildDoc{productDoc("x")}}}
	idx := newBatchedRebuildIndexer(st, es, 0, v1, v2)
	ids := productIDs(250)

	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: ids}}); err != nil {
		t.Fatal(err)
	}

	wantBatches := [][]string{ids[:100], ids[100:200], ids[200:]}
	begins := beginBuildsCalls(st)
	if !slices.EqualFunc(begins, wantBatches, slices.Equal[[]string]) {
		t.Fatalf("want three BeginBuilds of 100, 100 and 50 ids in order, got %d calls of sizes %v", len(begins), batchSizes(begins))
	}
	if n := st.count("BeginBuild:"); n != 0 {
		t.Fatalf("a batched rebuild begins no id on its own, got %d BeginBuild calls", n)
	}
	for v, log := range map[int]*requestLog{1: v1, 2: v2} {
		var asked [][]string
		for _, r := range log.reqs {
			asked = append(asked, r.ResourceIDs)
		}
		if !slices.EqualFunc(asked, wantBatches, slices.Equal[[]string]) {
			t.Fatalf("v%d must be asked about each batch once, got %d requests of sizes %v", v, len(asked), batchSizes(asked))
		}
	}

	// The fake begins in input order: id i at Build Sequence i+1.
	written := make(map[string]int)
	for _, it := range es.allBulkItems() {
		var i int
		if _, err := fmt.Sscan(it.ID, &i); err != nil || it.Version != int64(i+1) {
			t.Fatalf("%s must be written at its own Build Sequence %d, got %d", it.Index+"/"+it.ID, i+1, it.Version)
		}
		written[it.ID]++
	}
	calls := st.callsSnapshot()
	for i, id := range ids {
		if written[id] != 2 {
			t.Fatalf("%s must be written once per version, got %d", id, written[id])
		}
		if want := fmt.Sprintf("ClearStale:product/%s:%d", id, 42); !slices.Contains(calls, want) {
			t.Fatalf("%s (begun at %d) must settle on its own: no %s", id, i+1, want)
		}
	}
}

func batchSizes(batches [][]string) []int {
	out := make([]int, len(batches))
	for i, b := range batches {
		out[i] = len(b)
	}
	return out
}

// askedAbout is an executer that answers every asked id with a document,
// unless fail is set and the request asks about failOn: then the call fails.
func askedAbout(failOn string, fail error) aggregation.Executer[projection.BuildRequest, projection.BuildDoc] {
	return executerFunc(func(_ context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
		ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
		if fail != nil && slices.Contains(req.ResourceIDs, failOn) {
			ch <- aggregation.ExecutionResult[projection.BuildDoc]{Err: fail}
		} else {
			var docs []projection.BuildDoc
			for _, id := range req.ResourceIDs {
				docs = append(docs, productDoc(id))
			}
			ch <- aggregation.ExecutionResult[projection.BuildDoc]{Items: docs}
		}
		close(ch)
		return ch
	})
}

// A batch whose BeginBuilds fails, or whose plan call fails, fails every id
// of it — each marked stale for the sweep, none written or cleared — and is
// logged in one line naming the type, the count, the first ten ids and the
// error, not one line per id. The other batches still build.
func TestRebuildByIDs_AFailedBatch_FailsEachOfItsIDsWithOneLogLine(t *testing.T) {
	const batch = 12
	ids := productIDs(3 * batch)
	failed := ids[batch : 2*batch] // the second batch
	cases := map[string]struct {
		st   *rebuildRecordingStore
		exec aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
		err  string
	}{
		"BeginBuilds fails": {
			st:   &rebuildRecordingStore{beginErrs: map[string]error{failed[3]: errors.New("pg down")}},
			exec: askedAbout("", nil),
			err:  "pg down",
		},
		"the plan call fails": {
			st:   &rebuildRecordingStore{},
			exec: askedAbout(failed[3], errors.New("provider exploded")),
			err:  "provider exploded",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			logs := captureDefaultLogs(t)
			es := &captureBackend{}
			idx := newBatchedRebuildIndexer(c.st, es, batch, c.exec)

			err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: ids}})
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("failed %d resource(s)", batch)) {
				t.Fatalf("the rebuild must report the batch's %d ids failed, got %v", batch, err)
			}

			calls := c.st.callsSnapshot()
			if n := len(beginBuildsCalls(c.st)); n != 3 {
				t.Fatalf("each batch begins in one call, got %d: %v", n, calls)
			}
			for _, id := range failed {
				if countPrefix(calls, "MarkStale:product/"+id) != 1 || countPrefix(calls, "ClearStale:product/"+id+":") != 0 {
					t.Fatalf("%s of the failed batch must be marked stale once and not cleared: %v", id, calls)
				}
			}
			for _, it := range es.allBulkItems() {
				if slices.Contains(failed, it.ID) {
					t.Fatalf("no document of the failed batch may be written, got %s", it.ID)
				}
			}
			for _, id := range append(slices.Clone(ids[:batch]), ids[2*batch:]...) {
				if countPrefix(calls, "ClearStale:product/"+id+":") != 1 {
					t.Fatalf("%s of another batch must still build and settle: %v", id, calls)
				}
			}

			var lines []map[string]any
			for _, rec := range logs.records(t) {
				if e, _ := rec["error"].(string); strings.Contains(e, c.err) {
					lines = append(lines, rec)
				}
				if id, _ := rec["id"].(string); slices.Contains(failed, id) {
					t.Fatalf("the failed batch is logged once, not per id: %v", rec)
				}
			}
			if len(lines) != 1 {
				t.Fatalf("want one log line for the failed batch, got %d: %v", len(lines), lines)
			}
			line := lines[0]
			if line["type"] != "product" || line["count"] != float64(batch) {
				t.Fatalf("the line must name the type and the count %d: %v", batch, line)
			}
			var logged []string
			for _, v := range line["ids"].([]any) {
				logged = append(logged, v.(string))
			}
			if !slices.Equal(logged, failed[:10]) {
				t.Fatalf("the line must name the batch's first ten ids %v, got %v", failed[:10], logged)
			}
		})
	}
}

// begunIn is the index in calls of each begin of product id, once per time
// a call names it: a "BeginBuild:product/<id>" call, or a "BeginBuilds:" call
// listing it (with or without its token), counted once per listing.
func begunIn(calls []string, id string) []int {
	var at []int
	for i, c := range calls {
		var names []string
		if rest, ok := strings.CutPrefix(c, "BeginBuilds:"); ok {
			names = strings.Split(rest, ",")
		} else if rest, ok := strings.CutPrefix(c, "BeginBuild:"); ok {
			names = []string{rest}
		}
		for _, n := range names {
			n, _, _ = strings.Cut(n, ":")
			if n == "product/"+id {
				at = append(at, i)
			}
		}
	}
	return at
}
