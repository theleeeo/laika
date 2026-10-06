package core

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// The live build stores its edges per Schema Version at its Build Sequence
// (Store.ReplaceEdges), so each version's edges follow the Build Sequence
// like its document: the Store keeps the set of the highest-sequence build.

func (b *captureBackend) upsertsSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.upserts...)
}

// newEdgeBuildIndexer is a two-version product type whose v1 plan finds the
// children v1 and whose v2 plan finds the children v2.
func newEdgeBuildIndexer(st Store, es SearchBackend, v1, v2 []model.Resource) *Indexer {
	return mustNew(Config{
		Resources: twoVersionResources(),
		Plans: map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", v1...)}}},
			{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", v2...)}}},
		}},
		ES:    es,
		Store: st,
	})
}

// buildProduct1 marks product/1 stale and builds it directly (unowned).
func buildProduct1(t *testing.T, st *recordingStore, idx *Indexer) {
	t.Helper()
	if _, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, 0); err != nil {
		t.Fatal(err)
	}
	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}
}

// sortedSets is sets ordered by Schema Version, for comparison: the order of
// sets carries no meaning (Store.ReplaceEdges).
func sortedSets(sets []EdgeSet) []EdgeSet {
	out := slices.Clone(sets)
	slices.SortFunc(out, func(a, b EdgeSet) int { return a.SchemaVersion - b.SchemaVersion })
	return out
}

func equalSets(a, b []EdgeSet) bool {
	a, b = sortedSets(a), sortedSets(b)
	return slices.EqualFunc(a, b, func(x, y EdgeSet) bool {
		return x.SchemaVersion == y.SchemaVersion && slices.Equal(x.Children, y.Children)
	})
}

// After its Elasticsearch writes, the live build replaces each version's
// edge set — that version's own children, an empty set for a version whose
// plan found none — at its Build Sequence, declaring every configured
// version, and wipes nothing.
func TestBuild_ReplacesEachVersionsEdges_AtItsBuildSequence(t *testing.T) {
	st := &recordingStore{}
	es := &captureBackend{}
	idx := newEdgeBuildIndexer(st, es, nil, []model.Resource{product("c2"), product("c3")})
	var upsertsAtReplace []string
	st.onReplace = func(edgeReplace) { upsertsAtReplace = es.upsertsSnapshot() }

	buildProduct1(t, st, idx)

	if n := st.count("RemoveResource"); n != 0 {
		t.Fatalf("the live build must not wipe the resource's edges, RemoveResource called %d times: %v", n, st.callsSnapshot())
	}
	calls := st.replacedSnapshot()
	if len(calls) != 1 {
		t.Fatalf("the live build must replace its edges in exactly one call, got %d: %v", len(calls), st.callsSnapshot())
	}
	got := calls[0]
	if got.resource != product("1") || got.buildSeq != 1 {
		t.Fatalf("edges must be replaced for the root at the build's Build Sequence 1, got %v at %d", got.resource, got.buildSeq)
	}
	want := []EdgeSet{{SchemaVersion: 1}, {SchemaVersion: 2, Children: []model.Resource{product("c2"), product("c3")}}}
	if !equalSets(got.sets, want) {
		t.Fatalf("each version's set must hold that version's own children, got %+v want %+v", got.sets, want)
	}
	if d := slices.Sorted(slices.Values(got.declared)); !slices.Equal(d, []int{1, 2}) {
		t.Fatalf("the live build must declare every configured version, got %v", got.declared)
	}
	if len(upsertsAtReplace) != 2 {
		t.Fatalf("edges must be replaced after both Elasticsearch writes, saw %v at the replace", upsertsAtReplace)
	}
	if r, _ := st.row(product("1")); r.stale {
		t.Fatalf("the build must still clear its mark: %+v", r)
	}
}

// A version whose Elasticsearch write lost to a newer Build Sequence still
// has its set offered: the Store's sequence guard, not the build, decides
// whose edges stay.
func TestBuild_VersionConflict_StillReplacesThatVersionsEdges(t *testing.T) {
	st := &recordingStore{}
	es := &captureBackend{upsertConflict: map[string]bool{"product_search_v2/1": true}}
	idx := newEdgeBuildIndexer(st, es, []model.Resource{product("c1")}, []model.Resource{product("c2")})

	buildProduct1(t, st, idx)

	calls := st.replacedSnapshot()
	if len(calls) != 1 {
		t.Fatalf("an Elasticsearch version conflict must not skip the edge replace, got %d calls: %v", len(calls), st.callsSnapshot())
	}
	want := []EdgeSet{
		{SchemaVersion: 1, Children: []model.Resource{product("c1")}},
		{SchemaVersion: 2, Children: []model.Resource{product("c2")}},
	}
	if !equalSets(calls[0].sets, want) {
		t.Fatalf("the conflicted version's set must still be offered, got %+v want %+v", calls[0].sets, want)
	}
}

// A failed edge replace fails the build: it does not clear the mark, so the
// sweep rebuilds the resource.
func TestBuild_FailedEdgeReplace_FailsTheBuild(t *testing.T) {
	st := &recordingStore{replaceErr: errors.New("db down")}
	es := &captureBackend{}
	idx := newEdgeBuildIndexer(st, es, []model.Resource{product("c1")}, nil)

	buildProduct1(t, st, idx)

	if st.count("ClearStale") != 0 {
		t.Fatalf("a build whose edges were not stored must not clear its mark: %v", st.callsSnapshot())
	}
	if r, _ := st.row(product("1")); !r.stale {
		t.Fatalf("a failed build's mark must stay: %+v", r)
	}
}

// The live build passes the first non-empty report among its plans' documents
// (BuildDoc.ResourceMetadata), in plan order, to ReplaceEdges, which stores it
// on a row without metadata; an empty report is no report.
func TestBuild_SeveralPlans_TheFirstNonEmptyReportInPlanOrderIsStored(t *testing.T) {
	cases := map[string]struct {
		v1, v2, want map[string]string
	}{
		"both report: the first plan's": {
			v1: map[string]string{"a": "1"}, v2: map[string]string{"a": "2"}, want: map[string]string{"a": "1"},
		},
		"the first reports empty: the second's": {
			v1: map[string]string{}, v2: map[string]string{"a": "2"}, want: map[string]string{"a": "2"},
		},
		"the first reports none: the second's": {
			v2: map[string]string{"a": "2"}, want: map[string]string{"a": "2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st := &recordingStore{}
			idx := mustNew(Config{
				Resources: twoVersionResources(),
				Plans: map[string][]projection.Plan{"product": {
					{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), tc.v1)}}},
					{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{reporting(productDoc("1"), tc.v2)}}},
				}},
				ES:    &captureBackend{},
				Store: st,
			})

			buildProduct1(t, st, idx)

			calls := st.replacedSnapshot()
			if len(calls) != 1 {
				t.Fatalf("one ReplaceEdges per build, got %d: %v", len(calls), st.callsSnapshot())
			}
			if !maps.Equal(calls[0].reported, tc.want) {
				t.Fatalf("ReplaceEdges must pass the first non-empty report %v, passed %v", tc.want, calls[0].reported)
			}
			if r, _ := st.row(product("1")); !maps.Equal(r.metadata, tc.want) {
				t.Fatalf("the row must store the report %v, holds %v", tc.want, r.metadata)
			}
		})
	}
}

// An owned inline build of a row without metadata stores its plan's report,
// and its drift re-mark leaves it: FinishOwned re-claims the row for the
// follow-up, whose BeginBuild returns the report, and the follow-up build
// fetches as it.
func TestBuild_OwnedBuild_StoresTheReport_AndTheFollowUpFetchesAsIt(t *testing.T) {
	report := map[string]string{"actor": "a"}
	st := &recordingStore{}
	st.drift.Store(true) // the first build's drift check hits
	exec := &requestLog{exec: &staticExecuter{byID: map[string][]projection.BuildDoc{
		"1": {reporting(productDocWith("1", "child"), report)},
	}}}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  2,
		QueueSize: 4,
	})

	// No Metadata: the registration leaves the row without any.
	if err := idx.RegisterChange(t.Context(), Notification{ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if r, _ := st.row(product("1")); !maps.Equal(r.metadata, report) {
		t.Fatalf("the row must end with the plan's report %v, holds %v: %v", report, r.metadata, st.callsSnapshot())
	}
	if fus := st.followUpsOf(product("1")); len(fus) != 1 {
		t.Fatalf("the drift re-mark must hand FinishOwned one follow-up, got %+v: %v", fus, st.callsSnapshot())
	}
	builds := exec.requestsFor("1")
	if len(builds) != 2 {
		t.Fatalf("the build and its follow-up, got %d builds: %v", len(builds), st.callsSnapshot())
	}
	if builds[0].Metadata != nil {
		t.Fatalf("the first build begins on a row without metadata and fetches with none, fetched as %v", builds[0].Metadata)
	}
	if !maps.Equal(builds[1].Metadata, report) {
		t.Fatalf("the follow-up build must fetch as the stored report %v, fetched as %v", report, builds[1].Metadata)
	}
}

// A drift re-mark of a row left without metadata — none registered, none
// reported — leaves it without any: the direct build that owns nothing
// fetches with its caller's metadata, but the re-build the re-mark claims is
// owned and fetches with the row's, none.
func TestBuild_DriftRemark_RowWithoutMetadata_ReBuildsWithNone(t *testing.T) {
	buildMD := map[string]string{"b": "1"}
	st := &recordingStore{}
	st.drift.Store(true) // the first build's drift check hits
	exec := &requestLog{exec: &staticExecuter{docs: []projection.BuildDoc{productDocWith("1", "child")}}}
	idx := mustNew(Config{
		Resources: testResources(),
		Plans:     map[string][]projection.Plan{"product": {{Version: 1, Executer: exec}}},
		ES:        &fakeBackend{},
		Store:     st,
		PoolSize:  2,
		QueueSize: 4,
	})

	if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}, Metadata: buildMD}); err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if r, _ := st.row(product("1")); len(r.metadata) != 0 {
		t.Fatalf("the re-mark must leave the row without metadata, it holds %v: %v", r.metadata, st.callsSnapshot())
	}
	builds := exec.requestsFor("1")
	if len(builds) != 2 {
		t.Fatalf("the direct build and the claimed re-build, got %d builds: %v", len(builds), st.callsSnapshot())
	}
	if !maps.Equal(builds[0].Metadata, buildMD) {
		t.Fatalf("the direct build owns nothing and must fetch as its caller's %v, fetched as %v", buildMD, builds[0].Metadata)
	}
	if builds[1].Metadata != nil {
		t.Fatalf("the claimed re-build must fetch with the row's metadata, none, fetched as %v", builds[1].Metadata)
	}
}
