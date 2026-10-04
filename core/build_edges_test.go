package core

import (
	"errors"
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
	if _, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, nil, 0); err != nil {
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
