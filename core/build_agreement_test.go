package core

// Existence in multi-Schema-Version builds (ADR 0013, over ADR 0004's writes
// to every version): a build runs every version's plan, and a plan's nil is
// its own version's answer. A build whose plans disagree writes the documents
// of the versions whose plans returned one and, at the same Build Sequence,
// deletes the document and empties the edge set of each version whose plan
// returned nil, keeps the row and settles; one version's nil never deletes
// another version's document. Only when every plan returns nil is the
// resource gone: every version's document, the edge sets and the row go.
// Parent discovery (ADR 0006) unions over every plan with a document, not
// just the last.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// nilDoc is a plan result whose source returned no data: the root is known
// but the projected document is nil.
func nilDoc(id string) projection.BuildDoc {
	return projection.BuildDoc{Root: model.Resource{Type: "product", Id: id}}
}

// count reports how many recorded calls start with prefix.
func (s *rebuildRecordingStore) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// versionedBackend is a SearchBackend that keeps each document's external
// version as Elasticsearch does under external_gte: a write below the stored
// version loses with ErrVersionConflict, and a delete below it loses too,
// returning nil and leaving the document (SearchBackend.Delete). deleteErrs
// fails the delete of each "index/id" it names, deleting nothing. ops records
// every write and delete as "upsert|delete index/id@version", in order.
type versionedBackend struct {
	mu         sync.Mutex
	docs       map[string]int64
	deleteErrs map[string]error
	ops        []string
}

func (b *versionedBackend) Upsert(_ context.Context, index, docID string, _ any, version int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := index + "/" + docID
	b.ops = append(b.ops, fmt.Sprintf("upsert %s@%d", key, version))
	if stored, ok := b.docs[key]; ok && stored > version {
		return fmt.Errorf("superseded: %w", ErrVersionConflict)
	}
	if b.docs == nil {
		b.docs = make(map[string]int64)
	}
	b.docs[key] = version
	return nil
}

func (b *versionedBackend) Delete(_ context.Context, index, docID string, version int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := index + "/" + docID
	b.ops = append(b.ops, fmt.Sprintf("delete %s@%d", key, version))
	if err := b.deleteErrs[key]; err != nil {
		return err
	}
	if stored, ok := b.docs[key]; ok && stored > version {
		return nil // a newer write keeps its document: an OCC loss, not an error
	}
	delete(b.docs, key)
	return nil
}

func (b *versionedBackend) BulkUpsert(context.Context, []BulkItem) ([]BulkFailure, error) {
	return nil, errors.New("versionedBackend: a build writes documents one by one")
}

func (b *versionedBackend) Search(context.Context, SearchRequest, string, *resource.VersionConfig) (SearchResponse, error) {
	return SearchResponse{}, nil
}

func (b *versionedBackend) FederatedSearch(context.Context, FederatedSearchParams) (FederatedSearchResult, error) {
	return FederatedSearchResult{}, nil
}
func (b *versionedBackend) GetAliasTargets(context.Context, string) ([]string, error) {
	return nil, nil
}

func (b *versionedBackend) opsSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.ops)
}

// doc reports the stored version of index/id and whether it exists.
func (b *versionedBackend) doc(index, id string) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.docs[index+"/"+id]
	return v, ok
}

// newDisagreeIndexer serves "product" over two Schema Versions: v1's plan
// builds product/1's document with the child c1, and v2's plan returns nil
// for it.
func newDisagreeIndexer(st Store, be SearchBackend) *Indexer {
	return mustNew(Config{
		Resources: twoVersionResources(),
		Plans: map[string][]projection.Plan{"product": {
			{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", product("c1"))}}},
			{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
		}},
		ES:        be,
		Store:     st,
		PoolSize:  1,
		QueueSize: 8,
	})
}

// disagreeBuild is how a test builds product/1: owned, through a
// registration whose claim (stale_seq 1, token 1) submits the inline build,
// or unowned, as a direct Build of the row a lease-less mark left stale at
// stale_seq 1. finish is the call that settles it.
type disagreeBuild struct {
	name   string
	run    func(t *testing.T, st *recordingStore, idx *Indexer)
	finish string
}

var disagreeBuilds = []disagreeBuild{
	{
		name: "owned",
		run: func(t *testing.T, _ *recordingStore, idx *Indexer) {
			mustRegister(t, idx, notify("1", ChangeUpdated, nil))
			waitIdle(t, idx)
		},
		finish: "FinishOwned:product/1:1:1",
	},
	{
		name: "unowned",
		run: func(t *testing.T, st *recordingStore, idx *Indexer) {
			if _, err := st.MarkStale(t.Context(), []model.Resource{product("1")}, 0); err != nil {
				t.Fatal(err)
			}
			// Build reports a failure of an id that owns nothing through its
			// log, not its error: the contract is in the effects.
			if err := idx.Build(t.Context(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
				t.Fatal(err)
			}
		},
		finish: "ClearStale:product/1:1",
	},
}

// A build whose plans disagree — v1 returns a document, v2 nil — writes v1's
// document and deletes v2's, both at the build's Build Sequence and before
// it replaces the edges. Its one ReplaceEdges replaces v2's edge set with an
// empty one at that sequence: v2 is among the sets, with no children, not
// only declared, which would leave its old children. The row stays, and the
// build settles it, clearing the mark.
func TestBuild_PlansDisagreeOnExistence_DeletesOnlyTheNilVersionAndKeepsTheRow(t *testing.T) {
	for _, b := range disagreeBuilds {
		t.Run(b.name, func(t *testing.T) {
			st := &recordingStore{buildIdx: 41} // the build's BeginBuild bumps to 42
			be := &versionedBackend{}
			idx := newDisagreeIndexer(st, be)
			var opsAtReplace []string
			st.onReplace = func(edgeReplace) { opsAtReplace = be.opsSnapshot() }

			b.run(t, st, idx)

			want := []string{"delete product_search_v2/1@42", "upsert product_search_v1/1@42"}
			if got := slices.Sorted(slices.Values(be.opsSnapshot())); !slices.Equal(got, want) {
				t.Fatalf("the build must write v1's document and delete only v2's, each at its Build Sequence 42: got %v want %v", got, want)
			}
			if got := slices.Sorted(slices.Values(opsAtReplace)); !slices.Equal(got, want) {
				t.Fatalf("the write and the delete must both precede the edge replace: done by then %v", got)
			}

			calls := st.replacedSnapshot()
			if len(calls) != 1 {
				t.Fatalf("want one ReplaceEdges, got %d: %v", len(calls), st.callsSnapshot())
			}
			c := calls[0]
			wantSets := []EdgeSet{{SchemaVersion: 1, Children: []model.Resource{product("c1")}}, {SchemaVersion: 2}}
			if c.buildSeq != 42 || !equalSets(c.sets, wantSets) || len(c.sets) != 2 {
				t.Fatalf("ReplaceEdges must replace v1's set with its children and v2's with an empty one, at 42: got seq %d sets %+v", c.buildSeq, sortedSets(c.sets))
			}
			if !slices.Equal(c.declared, []int{1, 2}) {
				t.Fatalf("the build declares every configured version, got %v", c.declared)
			}

			for _, p := range []string{"RemoveResource:", "DeleteResourceIfSeq:", "ReleaseFailed"} {
				if n := st.count(p); n != 0 {
					t.Fatalf("a version with a document keeps the row and its edges: %s called: %v", p, st.callsSnapshot())
				}
			}
			if st.indexOf(b.finish) == -1 {
				t.Fatalf("the build must settle with %s: %v", b.finish, st.callsSnapshot())
			}
			if r, ok := st.row(product("1")); !ok || r.stale || r.owner != 0 {
				t.Fatalf("the row must stay, clean and unowned: %+v (exists %v)", r, ok)
			}
			if v, ok := be.doc("product_search_v1", "1"); !ok || v != 42 {
				t.Fatalf("v1's document must stay at 42, got %d (exists %v)", v, ok)
			}
		})
	}
}

// A failed delete of the nil version fails the build before it replaces the
// edges: the mark stays, an owned build is released through ReleaseFailed,
// and nothing finishes.
func TestBuild_PlansDisagreeOnExistence_FailedDeleteFailsTheBuild(t *testing.T) {
	for _, b := range disagreeBuilds {
		t.Run(b.name, func(t *testing.T) {
			st := &recordingStore{buildIdx: 41}
			be := &versionedBackend{deleteErrs: map[string]error{"product_search_v2/1": errors.New("es down")}}
			idx := newDisagreeIndexer(st, be)

			b.run(t, st, idx)

			if !slices.Contains(be.opsSnapshot(), "delete product_search_v2/1@42") {
				t.Fatalf("setup: the build must try v2's delete: %v", be.opsSnapshot())
			}
			for _, p := range []string{"ReplaceEdges:", "FinishOwned", "ClearStale", "DeleteResourceIfSeq:", "RemoveResource:"} {
				if n := st.count(p); n != 0 {
					t.Fatalf("a failed delete fails the build: %s called: %v", p, st.callsSnapshot())
				}
			}
			if b.name == "owned" && st.indexOf("ReleaseFailed:product/1:1") == -1 {
				t.Fatalf("a failed owned build is released through ReleaseFailed: %v", st.callsSnapshot())
			}
			if r, ok := st.row(product("1")); !ok || !r.stale || r.owner != 0 {
				t.Fatalf("the mark must stay, unowned: %+v (exists %v)", r, ok)
			}
		})
	}
}

// A newer build holds v2's document at Build Sequence 99. This build's
// delete of it at 42 loses, as a write below a newer one does: an OCC loss,
// not an error. The newer document stays and the build settles.
func TestBuild_PlansDisagreeOnExistence_DeleteLosingToANewerWrite_Settles(t *testing.T) {
	for _, b := range disagreeBuilds {
		t.Run(b.name, func(t *testing.T) {
			st := &recordingStore{buildIdx: 41}
			be := &versionedBackend{docs: map[string]int64{"product_search_v2/1": 99}}
			idx := newDisagreeIndexer(st, be)

			b.run(t, st, idx)

			if !slices.Contains(be.opsSnapshot(), "delete product_search_v2/1@42") {
				t.Fatalf("setup: the build must try v2's delete at 42: %v", be.opsSnapshot())
			}
			if v, ok := be.doc("product_search_v2", "1"); !ok || v != 99 {
				t.Fatalf("the newer build's v2 document must stay at 99, got %d (exists %v)", v, ok)
			}
			if st.count("ReplaceEdges:product/1:42") != 1 || st.indexOf(b.finish) == -1 || st.count("ReleaseFailed") != 0 {
				t.Fatalf("a delete lost to a newer write must not fail the build: %v", st.callsSnapshot())
			}
			if r, ok := st.row(product("1")); !ok || r.stale || r.owner != 0 {
				t.Fatalf("the row must stay, clean and unowned: %+v (exists %v)", r, ok)
			}
		})
	}
}

func TestBuild_AllPlansNil_DeletesEveryVersion(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	ds := es.deletesSnapshot()
	want := map[string]bool{"product_search_v1/1": false, "product_search_v2/1": false}
	for _, d := range ds {
		want[d] = true
	}
	for index, deleted := range want {
		if !deleted {
			t.Fatalf("unanimous nil means the resource is gone: %s must be deleted (got %v)", index, ds)
		}
	}
}

// assertDeletedAt checks that id was deleted exactly once from each of the
// two Schema Versions' indices, each delete at Build Sequence seq, and that
// its edges were removed once, at seq.
func assertDeletedAt(t *testing.T, st *rebuildRecordingStore, es *captureBackend, id string, seq int64) {
	t.Helper()
	var got []string
	for _, d := range es.deletesAt() {
		if strings.Contains(d, "/"+id+"@") {
			got = append(got, d)
		}
	}
	slices.Sort(got)
	want := []string{
		fmt.Sprintf("product_search_v1/%s@%d", id, seq),
		fmt.Sprintf("product_search_v2/%s@%d", id, seq),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("every version's delete of %s must carry the deleting path's Build Sequence %d: got %v want %v", id, seq, got, want)
	}
	var removes []string
	for _, c := range st.callsSnapshot() {
		if strings.HasPrefix(c, "RemoveResource:product/"+id+":") {
			removes = append(removes, c)
		}
	}
	if want := []string{fmt.Sprintf("RemoveResource:product/%s:%d", id, seq)}; !slices.Equal(removes, want) {
		t.Fatalf("the edge removal of %s must be bounded by Build Sequence %d: got %v want %v", id, seq, removes, want)
	}
}

// The live build's unanimous nil deletes at the build's own Build Sequence,
// the one its BeginBuild bumped: every version's document and the edge
// removal carry it.
func TestBuild_AllPlansNil_DeletesAtTheBuildsSequence(t *testing.T) {
	st := &rebuildRecordingStore{buildIdx: 41} // the build's BeginBuild bumps to 42
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	if n := st.count("BeginBuild:product/1"); n != 1 {
		t.Fatalf("setup: the build begins once: %v", st.callsSnapshot())
	}
	assertDeletedAt(t, st, es, "1", 42)
}

// A targeted rebuild's unanimous nil deletes the id at the Build Sequence
// its own BeginBuild bumped, not another id's.
func TestRebuildByIDs_AllPlansNil_DeletesAtThatIDsSequence(t *testing.T) {
	st := &rebuildRecordingStore{buildIdx: 41}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{byID: map[string][]projection.BuildDoc{"1": {productDoc("1")}, "2": {nilDoc("2")}}}},
		{Version: 2, Executer: &staticExecuter{byID: map[string][]projection.BuildDoc{"1": {productDoc("1")}, "2": {nilDoc("2")}}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1", "2"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ids begin in order: 1 at 42, 2 at 43.
	if got := st.count("BeginBuild:product/"); got != 2 {
		t.Fatalf("setup: each id begins once: %v", st.callsSnapshot())
	}
	assertDeletedAt(t, st, es, "2", 43)
	for _, d := range es.deletesSnapshot() {
		if strings.HasSuffix(d, "/1") {
			t.Fatalf("the existing id must not be deleted: %v", es.deletesSnapshot())
		}
	}
}

func TestBuild_ParentsCollectedFromEveryPlan(t *testing.T) {
	withParents := func(d projection.BuildDoc, parents ...model.Resource) projection.BuildDoc {
		d.Parents = parents
		return d
	}
	parentA := model.Resource{Type: "parent", Id: "a"}
	parentB := model.Resource{Type: "parent", Id: "b"}

	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		// parentA appears in both plans; parentB only in the first — the union
		// must keep both, once each.
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{withParents(productDoc("1"), parentB, parentA)}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{withParents(productDoc("1"), parentA)}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	if !st.has("MarkStale:parent/b") {
		t.Fatalf("a parent discovered by a non-final plan must still be scheduled (ADR 0006): %v", st.calls)
	}
	if got := st.count("MarkStale:parent/a"); got != 1 {
		t.Fatalf("a parent discovered by several plans must be scheduled once, got %d marks", got)
	}
}

// ADR 0013: a targeted rebuild applies each plan's outcome to its own
// version. v1 returns X's document and v2 nil: v1's document is written,
// v2's deleted and v2's edge set emptied at X's Build Sequence, and X
// settles with its row kept.
func TestRebuildByIDs_OneVersionNil_DeletesOnlyThatVersion_AndSettles(t *testing.T) {
	st := &rebuildRecordingStore{buildIdx: 41} // X begins at 42
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("X")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("X")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"X"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertRebuildDroppedVersion(t, st, es, "X", 42, 1, 2)
}
