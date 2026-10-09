package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/projection"
)

// A whole-type rebuild that selects fewer than every plan with an executer
// walks only its versions' listings. Once the walk has finished, it asks each
// selected version's Probe, a page at a time, about the type's rows that
// version's listing left without an edge set of it (Store.ListUncovered): an
// id the probe leaves out is one the version excludes, and loses that
// version's document and edge set at a Build Sequence taken before the probe;
// an id it returns is marked for the sweep (ADR 0013's Q24 note).

// probePassRebuild runs sel over the product type with v1 and v2 under ctx,
// the plans' executers execs (Versions 1, 2, …) and, for each version probes
// names, that probe as its plan's Probe; each probe call also records
// "Probe:v<version>:<ids>" in st's call log. It waits for the re-builds the
// rebuild schedules.
func probePassRebuild(ctx context.Context, t *testing.T, st *rebuildRecordingStore, es *captureBackend, sel ResourceSelector, probes map[int]*fakeProbe, execs ...aggregation.Executer[projection.BuildRequest, projection.BuildDoc]) error {
	t.Helper()
	plans := make([]projection.Plan, len(execs))
	for i, e := range execs {
		v := i + 1
		plans[i] = projection.Plan{Version: v, Executer: e}
		if p := probes[v]; p != nil {
			plans[i].Probe = func(ctx context.Context, ids []string, md map[string]string) ([]string, error) {
				st.record("Probe:v%d:%s", v, strings.Join(ids, ","))
				return p.probe(ctx, ids, md)
			}
		}
	}
	idx := mustNew(Config{
		Resources: twoVersionResources(),
		Plans:     map[string][]projection.Plan{"product": plans},
		ES:        es,
		Store:     st,
	})
	err := idx.RebuildNow(ctx, []ResourceSelector{sel})
	if werr := idx.WaitForIdle(t.Context()); werr != nil {
		t.Fatal(werr)
	}
	return err
}

// probeReturning is a probe that returns those of the ids it is asked about
// that present names.
func probeReturning(present ...string) *fakeProbe {
	return &fakeProbe{answer: func(_ context.Context, ids []string, _ map[string]string) ([]string, error) {
		var out []string
		for _, id := range ids {
			if slices.Contains(present, id) {
				out = append(out, id)
			}
		}
		return out, nil
	}}
}

// uncoveredRows is a page of rows ListUncovered lists, each holding md.
func uncoveredRows(md map[string]string, ids ...string) []ListedResource {
	out := make([]ListedResource, len(ids))
	for i, id := range ids {
		out[i] = ListedResource{Resource: product(id), Metadata: maps.Clone(md)}
	}
	return out
}

// v2Only is the version-selected backfill of v2 of the product type.
var v2Only = ResourceSelector{ResourceType: "product", Versions: []int{2}}

// walkOf is a plan listing a document of each id.
func walkOf(ids ...string) *staticExecuter {
	docs := make([]projection.BuildDoc, len(ids))
	for i, id := range ids {
		docs[i] = productDoc(id)
	}
	return &staticExecuter{docs: docs}
}

// passCounts returns the probe pass's counts for version the "rebuild
// complete" log carries, and whether it carries them.
func passCounts(t *testing.T, logs *capturedLogs, version int) (map[string]int, bool) {
	t.Helper()
	for _, rec := range logs.records(t) {
		if rec["msg"] != "rebuild complete" {
			continue
		}
		pass, _ := rec["probe_pass"].(map[string]any)
		v, ok := pass[fmt.Sprintf("v%d", version)].(map[string]any)
		if !ok {
			return nil, false
		}
		out := make(map[string]int, len(v))
		for k, n := range v {
			out[k] = int(n.(float64))
		}
		return out, true
	}
	t.Fatalf("no \"rebuild complete\" log: %s", logs.buf.String())
	return nil, false
}

// assertUntouched checks the rebuild wrote nothing for id: no document
// deleted or written, no edge set replaced, no mark made or cleared.
func assertUntouched(t *testing.T, st *rebuildRecordingStore, es *captureBackend, id string) {
	t.Helper()
	for _, d := range es.deletesSnapshot() {
		if strings.HasSuffix(d, "/"+id) {
			t.Fatalf("no document of %s may be deleted, got %v", id, es.deletesSnapshot())
		}
	}
	for _, it := range es.allBulkItems() {
		if it.ID == id {
			t.Fatalf("no document of %s may be written: %+v", id, it)
		}
	}
	if got := st.replacedFor(product(id)); len(got) != 0 {
		t.Fatalf("no edge set of %s may be replaced: %+v", id, got)
	}
	calls := st.callsSnapshot()
	for _, p := range []string{"MarkStale:product/" + id, "MarkStaleFailed:product/" + id, "ClearStale:product/" + id + ":"} {
		if countPrefix(calls, p) != 0 {
			t.Fatalf("%s must be neither marked nor cleared: %v", id, calls)
		}
	}
}

// X, which v2's listing and its probe both leave out, is excluded by v2: its
// v2 document is deleted at the Build Sequence BeginBuilds took for it, before
// the probe, and its v2 edge set is replaced by an empty one at that sequence,
// declaring nothing, so v1's set and its relations stay. X isn't marked, and
// its v1 document isn't touched. Y, which the probe returns, is marked stale
// without a claim, as a rebuild hands an id to the sweep, and nothing of it
// is written.
func TestRebuildAll_VersionSelected_ProbePass_DeletesTheExcludedIDsVersion_MarksTheReturnedOne(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &rebuildRecordingStore{buildIdx: 41, uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X", "Y")}}
	es := &captureBackend{}
	if err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probeReturning("Y")}, walkOf("1", "X", "Y"), walkOf("1")); err != nil {
		t.Fatal(err)
	}

	// The walk begins 1 at 42; BeginBuilds begins X at 43, Y at 44.
	calls := st.callsSnapshot()
	begins, probes := callIndexes(calls, "BeginBuilds:product/X,product/Y"), callIndexes(calls, "Probe:v2:X,Y")
	if len(begins) != 1 || len(probes) != 1 || begins[0] > probes[0] {
		t.Fatalf("X and Y must be begun in one BeginBuilds, before one probe: %v", calls)
	}
	if got := countPrefix(calls, "ListUncovered:product:v2::100"); got != 1 {
		t.Fatalf("v2's uncovered rows must be listed from the first id in a page of 100: %v", calls)
	}

	var xDeletes []string
	for _, d := range es.deletesAt() {
		if strings.Contains(d, "/X@") {
			xDeletes = append(xDeletes, d)
		}
	}
	if !slices.Equal(xDeletes, []string{"product_search_v2/X@43"}) {
		t.Fatalf("only X's v2 document must be deleted, at X's Build Sequence 43, got %v", es.deletesAt())
	}
	assertReplaces(t, st, product("X"), 43, []EdgeSet{versionSet(2)})
	if idx := callIndexes(calls, "ReplaceEdges:product/X:43"); len(idx) != 1 || idx[0] < probes[0] {
		t.Fatalf("X's v2 edge set is emptied once, after the probe: %v", calls)
	}
	for _, p := range []string{"MarkStale:product/X", "ClearStale:product/X:", "DeleteResourceIfSeq:product/X:", "RemoveResource:product/X:"} {
		if countPrefix(calls, p) != 0 {
			t.Fatalf("X must not be marked, cleared or removed: %v", calls)
		}
	}

	if marks := callIndexes(calls, "MarkStale:product/Y"); len(marks) != 1 || marks[0] < probes[0] {
		t.Fatalf("Y must be marked stale once, after the probe: %v", calls)
	}
	if !slices.Equal(st.markLeases, []time.Duration{0}) {
		t.Fatalf("the pass's mark claims nothing (lease 0), as a rebuild hands an id to the sweep; leases %v", st.markLeases)
	}
	for _, d := range es.deletesSnapshot() {
		if strings.HasSuffix(d, "/Y") {
			t.Fatalf("no document of Y may be deleted: %v", es.deletesSnapshot())
		}
	}
	if got := st.replacedFor(product("Y")); len(got) != 0 {
		t.Fatalf("no edge set of Y may be replaced: %+v", got)
	}

	if got, ok := passCounts(t, logs, 2); !ok || !maps.Equal(got, map[string]int{"asked": 2, "excluded": 1, "marked": 1, "failed": 0}) {
		t.Fatalf("the completion log must count v2's pass, got %v", got)
	}
}

// A failed probe writes nothing and marks nothing for its ids, which stay
// uncovered for the next backfill to ask about; it is logged and counted, but
// not as the walk's failure, so the rebuild succeeds.
func TestRebuildAll_VersionSelected_ProbePass_FailedProbe_WritesNothing(t *testing.T) {
	logs := captureDefaultLogs(t)
	st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X", "Y")}}
	es := &captureBackend{}
	probe := &fakeProbe{answer: func(context.Context, []string, map[string]string) ([]string, error) {
		return nil, errors.New("source down")
	}}
	if err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probe}, walkOf("1", "X", "Y"), walkOf("1")); err != nil {
		t.Fatalf("a failed probe is not the rebuild's failure, got %v", err)
	}
	if len(probe.callsSnapshot()) != 1 {
		t.Fatalf("the page is probed once: %+v", probe.callsSnapshot())
	}
	assertUntouched(t, st, es, "X")
	assertUntouched(t, st, es, "Y")
	if got, _ := passCounts(t, logs, 2); !maps.Equal(got, map[string]int{"asked": 2, "excluded": 0, "marked": 0, "failed": 2}) {
		t.Fatalf("the failed probe's ids must be counted as failed, got %v", got)
	}
}

// A backfill with Metadata asks only about rows that hold it: the pass lists
// with the walk's metadata and probes with it, so a row of another actor, or
// one without metadata, isn't asked.
func TestRebuildAll_VersionSelected_ProbePass_AsksOnlyRowsWithTheWalksMetadata(t *testing.T) {
	actorA, actorB := map[string]string{"actor": "A"}, map[string]string{"actor": "B"}
	rows := slices.Concat(uncoveredRows(actorA, "a1"), uncoveredRows(actorB, "b1"), uncoveredRows(nil, "n1"))
	st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: rows}}
	es := &captureBackend{}
	probe := probeReturning()
	sel := ResourceSelector{ResourceType: "product", Versions: []int{2}, Metadata: actorA}
	if err := probePassRebuild(t.Context(), t, st, es, sel, map[int]*fakeProbe{2: probe}, walkOf("1"), walkOf("1")); err != nil {
		t.Fatal(err)
	}
	for _, md := range st.uncoveredMetadata {
		if !maps.Equal(md, actorA) {
			t.Fatalf("the pass must list with the walk's metadata, got %v", st.uncoveredMetadata)
		}
	}
	calls := probe.callsSnapshot()
	if len(calls) != 1 || !slices.Equal(calls[0].ids, []string{"a1"}) || !maps.Equal(calls[0].metadata, actorA) {
		t.Fatalf("only a1 is asked, with the walk's metadata, got %+v", calls)
	}
	assertUntouched(t, st, es, "b1")
	assertUntouched(t, st, es, "n1")
	if got := es.deletesAt(); !slices.Equal(got, []string{"product_search_v2/a1@2"}) {
		t.Fatalf("a1, excluded, loses its v2 document, got %v", got)
	}
}

// The pass lists in pages of the walk's page size — the pacing's, 100 when
// the walk has none — and probes each page in one call. It ends after a
// short page.
func TestRebuildAll_VersionSelected_ProbePass_ProbesOncePerPage(t *testing.T) {
	ids := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("u%03d", i)
		}
		return out
	}
	for name, tc := range map[string]struct {
		pacing *WalkPacing
		rows   int
		limit  int
		pages  []int
	}{
		"unpaced: pages of 100":    {nil, 150, 100, []int{100, 50}},
		"paced: the pacing's size": {&WalkPacing{PageSize: 2}, 5, 2, []int{2, 2, 1}},
		"a full last page":         {&WalkPacing{PageSize: 2}, 4, 2, []int{2, 2}},
	} {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, ids(tc.rows)...)}}
			probe := probeReturning()
			sel := ResourceSelector{ResourceType: "product", Versions: []int{2}, Pacing: tc.pacing}
			if err := probePassRebuild(t.Context(), t, st, &captureBackend{}, sel, map[int]*fakeProbe{2: probe}, walkOf("1"), walkOf("1")); err != nil {
				t.Fatal(err)
			}
			var sizes []int
			var asked []string
			for _, c := range probe.callsSnapshot() {
				sizes = append(sizes, len(c.ids))
				asked = append(asked, c.ids...)
			}
			if !slices.Equal(sizes, tc.pages) || !slices.Equal(asked, ids(tc.rows)) {
				t.Fatalf("want one probe per page of sizes %v covering every row once, got sizes %v", tc.pages, sizes)
			}
			for _, c := range st.callsSnapshot() {
				if strings.HasPrefix(c, "ListUncovered:") && !strings.HasSuffix(c, fmt.Sprintf(":%d", tc.limit)) {
					t.Fatalf("every listing asks for %d rows: %v", tc.limit, st.callsSnapshot())
				}
			}
		})
	}
}

// An id BeginBuilds didn't begin — gone or tombstoned since its listing — is
// neither probed nor written; a page with none begun probes nothing.
func TestRebuildAll_VersionSelected_ProbePass_NotBegunIDIsNeitherProbedNorWritten(t *testing.T) {
	t.Run("one not begun", func(t *testing.T) {
		st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X", "Y")}, notBegun: map[string]bool{"X": true}}
		es := &captureBackend{}
		probe := probeReturning()
		if err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probe}, walkOf("1"), walkOf("1")); err != nil {
			t.Fatal(err)
		}
		if got := probe.probedIDs(); !slices.Equal(got, []string{"Y"}) {
			t.Fatalf("only Y, begun, is probed, got %v", got)
		}
		assertUntouched(t, st, es, "X")
	})
	t.Run("none begun", func(t *testing.T) {
		st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X")}, notBegun: map[string]bool{"X": true}}
		es := &captureBackend{}
		probe := probeReturning()
		if err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probe}, walkOf("1"), walkOf("1")); err != nil {
			t.Fatal(err)
		}
		if got := probe.callsSnapshot(); len(got) != 0 {
			t.Fatalf("a page with no id begun probes nothing, got %+v", got)
		}
		assertUntouched(t, st, es, "X")
	})
}

// A failed delete or edge-set write of an excluded id, or a failed mark of a
// returned one, writes nothing more for those ids and isn't the walk's
// failure: the rebuild succeeds, the other ids are still served, and the
// failures are counted.
func TestRebuildAll_VersionSelected_ProbePass_FailedWrites_AreCountedNotTheWalksFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(st *rebuildRecordingStore, es *captureBackend)
		check func(t *testing.T, st *rebuildRecordingStore, es *captureBackend)
	}{
		"X's v2 delete fails": {
			func(_ *rebuildRecordingStore, es *captureBackend) {
				es.deleteErrs = map[string]error{"product_search_v2/X": errors.New("es down")}
			},
			func(t *testing.T, st *rebuildRecordingStore, _ *captureBackend) {
				if got := st.replacedFor(product("X")); len(got) != 0 {
					t.Fatalf("a failed delete writes no edge set: %+v", got)
				}
			},
		},
		"X's edge-set write fails": {
			func(st *rebuildRecordingStore, _ *captureBackend) {
				st.replaceErrs = map[string]error{"X": errors.New("pg down")}
			},
			func(*testing.T, *rebuildRecordingStore, *captureBackend) {},
		},
		"Y's mark fails": {
			func(st *rebuildRecordingStore, _ *captureBackend) {
				st.markErrs = map[string]int{"product/Y": 1}
			},
			func(t *testing.T, st *rebuildRecordingStore, _ *captureBackend) {
				if calls := st.callsSnapshot(); countPrefix(calls, "MarkStaleFailed:product/Y") != 1 || countPrefix(calls, "MarkStale:product/Y") != 0 {
					t.Fatalf("Y's failed mark isn't retried: %v", calls)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureDefaultLogs(t)
			st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X", "Y", "Z")}}
			es := &captureBackend{}
			tc.setup(st, es)
			if err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probeReturning("Y")}, walkOf("1"), walkOf("1")); err != nil {
				t.Fatalf("the pass's failures aren't the rebuild's, got %v", err)
			}
			calls := st.callsSnapshot()
			if countPrefix(calls, "MarkStale:product/X") != 0 || countPrefix(calls, "MarkStaleFailed:product/X") != 0 {
				t.Fatalf("X, excluded, is never marked: %v", calls)
			}
			// Z, excluded too, is served whatever befell X or Y. The walk
			// begins 1 at 1; BeginBuilds begins X at 2, Y at 3, Z at 4.
			if !slices.Contains(es.deletesAt(), "product_search_v2/Z@4") {
				t.Fatalf("Z's v2 document must still be deleted at 4, got %v", es.deletesAt())
			}
			assertReplaces(t, st, product("Z"), 4, []EdgeSet{versionSet(2)})
			tc.check(t, st, es)
			if got, _ := passCounts(t, logs, 2); got["failed"] != 1 || got["asked"] != 3 {
				t.Fatalf("one id failed of three asked, got %v", got)
			}
		})
	}
}

// A failed listing or begin ends the pass with an error the rebuild returns,
// so RunRebuild retries it; a failed begin probes nothing.
func TestRebuildAll_VersionSelected_ProbePass_FailedListingOrBegin_FailsTheRebuild(t *testing.T) {
	for name, setup := range map[string]func(st *rebuildRecordingStore){
		"listing": func(st *rebuildRecordingStore) { st.uncoveredErr = errors.New("pg down") },
		"begin":   func(st *rebuildRecordingStore) { st.beginBuildsErr = errors.New("pg down") },
	} {
		t.Run(name, func(t *testing.T) {
			st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X")}}
			setup(st)
			es := &captureBackend{}
			probe := probeReturning()
			err := probePassRebuild(t.Context(), t, st, es, v2Only, map[int]*fakeProbe{2: probe}, walkOf("1"), walkOf("1"))
			if err == nil || !strings.Contains(err.Error(), "pg down") {
				t.Fatalf("the rebuild must return the pass's error, got %v", err)
			}
			var marked *RebuildMarkedFailuresError
			if errors.As(err, &marked) {
				t.Fatalf("the pass's error must stay retryable, got %v", err)
			}
			if got := probe.callsSnapshot(); len(got) != 0 {
				t.Fatalf("nothing is probed, got %+v", got)
			}
			assertUntouched(t, st, es, "X")
		})
	}
}

// No pass runs for a rebuild that aborts or is cancelled mid-walk, one that
// runs every plan with an executer, or one by ids. A selected version whose
// plan has no Probe is skipped and logged at Info.
func TestRebuild_ProbePass_RunsOnlyAfterAWholeVersionSelectedWalk(t *testing.T) {
	probes := func() map[int]*fakeProbe { return map[int]*fakeProbe{1: probeReturning(), 2: probeReturning()} }
	for name, tc := range map[string]struct {
		sel     ResourceSelector
		v2      func(cancel context.CancelFunc) aggregation.Executer[projection.BuildRequest, projection.BuildDoc]
		wantErr bool
	}{
		"walk aborts": {v2Only, func(context.CancelFunc) aggregation.Executer[projection.BuildRequest, projection.BuildDoc] {
			return &failingExecuter{err: errors.New("listing failed")}
		}, true},
		"walk cancelled": {v2Only, func(cancel context.CancelFunc) aggregation.Executer[projection.BuildRequest, projection.BuildDoc] {
			return &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}, onExecute: cancel}
		}, true},
		"every plan, no versions":   {ResourceSelector{ResourceType: "product"}, nil, false},
		"every plan, both versions": {ResourceSelector{ResourceType: "product", Versions: []int{1, 2}}, nil, false},
		"by ids":                    {ResourceSelector{ResourceType: "product", Versions: []int{2}, ResourceIDs: []string{"1"}}, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			v2 := aggregation.Executer[projection.BuildRequest, projection.BuildDoc](walkOf("1"))
			if tc.v2 != nil {
				v2 = tc.v2(cancel)
			}
			st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{1: uncoveredRows(nil, "X"), 2: uncoveredRows(nil, "X")}}
			ps := probes()
			err := probePassRebuild(ctx, t, st, &captureBackend{}, tc.sel, ps, walkOf("1"), v2)
			if (err != nil) != tc.wantErr {
				t.Fatalf("want error %v, got %v", tc.wantErr, err)
			}
			if calls := st.callsSnapshot(); countPrefix(calls, "ListUncovered:") != 0 || countPrefix(calls, "BeginBuilds:") != 0 {
				t.Fatalf("no pass may run: %v", calls)
			}
			for v, p := range ps {
				if got := p.callsSnapshot(); len(got) != 0 {
					t.Fatalf("v%d's probe must not be called, got %+v", v, got)
				}
			}
		})
	}

	t.Run("selected version without a Probe", func(t *testing.T) {
		logs := captureDefaultLogs(t)
		st := &rebuildRecordingStore{uncovered: map[int][]ListedResource{2: uncoveredRows(nil, "X")}}
		v1 := probeReturning()
		if err := probePassRebuild(t.Context(), t, st, &captureBackend{}, v2Only, map[int]*fakeProbe{1: v1}, walkOf("1"), walkOf("1")); err != nil {
			t.Fatal(err)
		}
		if calls := st.callsSnapshot(); countPrefix(calls, "ListUncovered:") != 0 || len(v1.callsSnapshot()) != 0 {
			t.Fatalf("v2 has no Probe: no pass may run, nor another version's probe: %v", calls)
		}
		var skipped bool
		for _, rec := range logs.records(t) {
			if rec["level"] == "INFO" && strings.Contains(fmt.Sprint(rec["msg"]), "no Probe") && rec["version"] == float64(2) {
				skipped = true
			}
		}
		if !skipped {
			t.Fatalf("the skipped version must be logged at Info, got logs:\n%s", logs.buf.String())
		}
		if _, ok := passCounts(t, logs, 2); ok {
			t.Fatalf("a skipped version has no pass counts")
		}
	})
}
