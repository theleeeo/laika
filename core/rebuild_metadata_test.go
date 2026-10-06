package core

import (
	"context"
	"maps"
	"sync"
	"testing"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// A rebuild stores the metadata its plans report (BuildDoc.ResourceMetadata)
// as a resource row's metadata when the row has none, through
// Store.ReplaceEdges. The walk fetches with the rebuild's own Metadata, but
// never stores it: a drift re-mark leaves the row's metadata, and the
// re-build it claims is owned and fetches with what the row holds when it
// begins — none on a row without any.

// requestLog wraps an executer and records every request it serves, so a test
// sees which ids were built with which metadata — the walk's request
// (ResourceID "") and every single-id build's.
type requestLog struct {
	exec aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
	mu   sync.Mutex
	reqs []projection.BuildRequest
}

func (l *requestLog) Execute(ctx context.Context, req projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	l.mu.Lock()
	r := req
	r.Metadata = maps.Clone(req.Metadata)
	l.reqs = append(l.reqs, r)
	l.mu.Unlock()
	return l.exec.Execute(ctx, req)
}

// requestsFor is every request for id, in arrival order; "" is the walk's.
func (l *requestLog) requestsFor(id string) []projection.BuildRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []projection.BuildRequest
	for _, r := range l.reqs {
		if r.ResourceID == id {
			out = append(out, r)
		}
	}
	return out
}

// reporting is doc with the plan's report md as its ResourceMetadata.
func reporting(doc projection.BuildDoc, md map[string]string) projection.BuildDoc {
	doc.ResourceMetadata = md
	return doc
}

// walkMetadata is the Metadata every walk test's RebuildNow carries: it is
// the fetch context, never a resource's own.
var walkMetadata = map[string]string{"walk": "w"}

// rebuildProducts runs RebuildNow of product with walkMetadata over plans
// (Versions 1, 2, …) in chunks of chunkSize (0: the default), selecting ids
// (none: a plan walk), and waits for the re-builds it schedules.
func rebuildProducts(t *testing.T, st *rebuildRecordingStore, chunkSize int, ids []string, execs ...aggregation.Executer[projection.BuildRequest, projection.BuildDoc]) {
	t.Helper()
	plans := make([]projection.Plan, len(execs))
	for i, e := range execs {
		plans[i] = projection.Plan{Version: i + 1, Executer: e}
	}
	idx := newRebuildIndexer(st, &captureBackend{}, map[string][]projection.Plan{"product": plans}, chunkSize)
	if err := idx.RebuildNow(t.Context(), []ResourceSelector{{ResourceType: "product", ResourceIDs: ids, Metadata: walkMetadata}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// assertRowMetadata checks r's row exists and holds exactly want (nil and
// empty alike for none).
func assertRowMetadata(t *testing.T, st *rebuildRecordingStore, r model.Resource, want map[string]string) {
	t.Helper()
	got, ok := st.rowMetadata(r)
	if !ok {
		t.Fatalf("%v: the row must exist", r)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("%v: row metadata %v, want %v", r, got, want)
	}
}

// A plan walk stores each document's report on its root's row — each root
// its own — and never the walk's Metadata; a row that already holds metadata
// (a registration's) keeps it although the plan reports one.
func TestRebuildAll_StoresEachDocumentsReport_NotTheWalksMetadata(t *testing.T) {
	st := &rebuildRecordingStore{}
	st.seedRow(product("3"), map[string]string{"tenant": "registered"})
	rebuildProducts(t, st, 0, nil, &staticExecuter{docs: []projection.BuildDoc{
		reporting(productDoc("1"), map[string]string{"actor": "a1"}),
		reporting(productDoc("2"), map[string]string{"actor": "a2"}),
		reporting(productDoc("3"), map[string]string{"actor": "a3"}),
	}})

	assertRowMetadata(t, st, product("1"), map[string]string{"actor": "a1"})
	assertRowMetadata(t, st, product("2"), map[string]string{"actor": "a2"})
	assertRowMetadata(t, st, product("3"), map[string]string{"tenant": "registered"})
}

// A plan that reports no metadata leaves every row's metadata as it was: a
// registered row keeps its metadata, a row without any — or one the walk
// creates — stays without, and the walk's Metadata is stored on none.
func TestRebuildAll_PlanReportsNone_LeavesRowMetadataAsItWas(t *testing.T) {
	st := &rebuildRecordingStore{}
	st.seedRow(product("1"), map[string]string{"tenant": "registered"})
	st.seedRow(product("2"), nil)
	rebuildProducts(t, st, 0, nil, &staticExecuter{docs: []projection.BuildDoc{
		productDoc("1"), productDoc("2"), productDoc("3"),
	}})

	assertRowMetadata(t, st, product("1"), map[string]string{"tenant": "registered"})
	assertRowMetadata(t, st, product("2"), nil)
	assertRowMetadata(t, st, product("3"), nil)
}

// A walk root whose drift check fires is re-marked, and the mark leaves the
// row's metadata: the re-build it claims fetches as the row holds it when the
// re-build begins — the plan's report on a row that had none, a
// registration's on a row that held one, and none on a row left without any,
// never the walk's Metadata.
func TestRebuildAll_DriftRemark_ReBuildsWithTheRowsMetadata(t *testing.T) {
	cases := map[string]struct {
		seeded map[string]string // the row's metadata before the walk; nil: no row
		report map[string]string
		want   map[string]string
	}{
		"new row, plan reports": {
			report: map[string]string{"actor": "a1"},
			want:   map[string]string{"actor": "a1"},
		},
		"registered row, plan reports none": {
			seeded: map[string]string{"tenant": "registered"},
			want:   map[string]string{"tenant": "registered"},
		},
		"new row, plan reports none: none, not the walk's": {
			want: nil,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// The root checks itself against the walk start; budget 1: the
			// walk's check hits, the re-build's finds nothing.
			st := &rebuildRecordingStore{driftChildren: map[string]bool{"1": true}}
			st.driftBudget.Store(1)
			if tc.seeded != nil {
				st.seedRow(product("1"), tc.seeded)
			}
			doc := reporting(productDoc("1"), tc.report)
			exec := &requestLog{exec: &staticExecuter{
				docs: []projection.BuildDoc{doc},
				byID: map[string][]projection.BuildDoc{"1": {doc}},
			}}
			rebuildProducts(t, st, 0, nil, exec)

			if n := countPrefix(st.callsSnapshot(), "MarkStale:product/1"); n != 1 {
				t.Fatalf("the drift check must re-mark the root once, got %d: %v", n, st.callsSnapshot())
			}
			rebuilds := exec.requestsFor("1")
			if len(rebuilds) != 1 {
				t.Fatalf("the re-mark must claim and re-build the root once, got %d builds: %v", len(rebuilds), st.callsSnapshot())
			}
			if !maps.Equal(rebuilds[0].Metadata, tc.want) || (tc.want == nil && rebuilds[0].Metadata != nil) {
				t.Fatalf("the drift re-build must fetch as %v, fetched as %v", tc.want, rebuilds[0].Metadata)
			}
			assertRowMetadata(t, st, product("1"), tc.want)
		})
	}
}

// A targeted rebuild stores each root's report the same way: on a row
// without metadata, never over a registered one, and never the rebuild's own
// Metadata.
func TestRebuildByIDs_StoresTheReport_OnARowWithoutMetadata(t *testing.T) {
	st := &rebuildRecordingStore{}
	st.seedRow(product("2"), map[string]string{"tenant": "registered"})
	rebuildProducts(t, st, 0, []string{"1", "2"}, &staticExecuter{byID: map[string][]projection.BuildDoc{
		"1": {reporting(productDoc("1"), map[string]string{"actor": "a1"})},
		"2": {reporting(productDoc("2"), map[string]string{"actor": "a2"})},
	}})

	assertRowMetadata(t, st, product("1"), map[string]string{"actor": "a1"})
	assertRowMetadata(t, st, product("2"), map[string]string{"tenant": "registered"})
}

// With several plans, a root's documents that land in one flush make one
// ReplaceEdges, which passes the first non-empty report in chunk order (plan
// order here); an empty report is no report.
func TestRebuildAll_MultiPlan_OneFlush_PassesTheFirstNonEmptyReport(t *testing.T) {
	cases := map[string]struct {
		v1, v2, want map[string]string
	}{
		"both report: the first plan's": {
			v1: map[string]string{"a": "1"}, v2: map[string]string{"a": "2"}, want: map[string]string{"a": "1"},
		},
		"the first reports empty: the second's": {
			v1: map[string]string{}, v2: map[string]string{"a": "2"}, want: map[string]string{"a": "2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{}
			rebuildProducts(t, st, 0, nil,
				&staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), tc.v1)}},
				&staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), tc.v2)}})

			calls := st.replacedFor(product("1"))
			if len(calls) != 1 {
				t.Fatalf("both documents land in one flush: want one ReplaceEdges, got %d: %v", len(calls), st.callsSnapshot())
			}
			if !maps.Equal(calls[0].reported, tc.want) {
				t.Fatalf("ReplaceEdges must pass the first non-empty report %v, passed %v", tc.want, calls[0].reported)
			}
			assertRowMetadata(t, st, product("1"), tc.want)
		})
	}
}

// With a chunk of one document, each plan's document lands in its own flush
// and makes its own ReplaceEdges with its own report; the row keeps the
// first one stored.
func TestRebuildAll_MultiPlan_FlushPerDocument_TheFirstStoredReportWins(t *testing.T) {
	st := &rebuildRecordingStore{}
	rebuildProducts(t, st, 1, nil,
		&staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), map[string]string{"a": "1"})}},
		&staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), map[string]string{"a": "2"})}})

	calls := st.replacedFor(product("1"))
	if len(calls) != 2 {
		t.Fatalf("one ReplaceEdges per flush: want 2, got %d: %v", len(calls), st.callsSnapshot())
	}
	for i, want := range []map[string]string{{"a": "1"}, {"a": "2"}} {
		if !maps.Equal(calls[i].reported, want) {
			t.Fatalf("ReplaceEdges call %d must pass its document's report %v, passed %v", i, want, calls[i].reported)
		}
	}
	assertRowMetadata(t, st, product("1"), map[string]string{"a": "1"})
}

// The walk's nil-doc delete path drift-checks a root that made no
// ReplaceEdges and removed its row: its re-mark creates a row without
// metadata, and the re-build it claims fetches with none, not the walk's.
func TestRebuildAll_NilDocDriftRemark_ReBuildsWithNone(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"1": true}}
	st.driftBudget.Store(1)
	gone := projection.BuildDoc{Root: product("1")}
	exec := &requestLog{exec: &staticExecuter{
		docs: []projection.BuildDoc{gone},
		byID: map[string][]projection.BuildDoc{"1": {gone}},
	}}
	rebuildProducts(t, st, 0, nil, exec)

	if n := countPrefix(st.callsSnapshot(), "MarkStale:product/1"); n != 1 {
		t.Fatalf("the drift check must re-mark the root once, got %d: %v", n, st.callsSnapshot())
	}
	rebuilds := exec.requestsFor("1")
	if len(rebuilds) != 1 || rebuilds[0].Metadata != nil {
		t.Fatalf("the claimed re-build must run once and fetch with no metadata, got %+v: %v", rebuilds, st.callsSnapshot())
	}
}
