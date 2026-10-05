package core

// Cross-version agreement in multi-Schema-Version builds (ADR 0004): all of a
// resource's plans must agree it exists before anything is written, one
// version's nil result must never delete the other versions' documents, and
// parent discovery (ADR 0006) must union over every plan — not just the last.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

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

func TestBuild_PlansDisagreeOnExistence_LeavesStaleInsteadOfDeleting(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	// Build reports per-id failures via logs, not its error; the contract is
	// in the effects below.
	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	if ds := es.deletesSnapshot(); len(ds) != 0 {
		t.Fatalf("plans disagreeing on existence must not delete any version's document, deleted %v", ds)
	}
	if len(es.upserts) != 0 {
		t.Fatalf("existence must be decided before any write, wrote %v", es.upserts)
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("the resource must stay stale so a retry can converge on the source's real state")
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

func TestRebuildByIDs_PlansDisagreeOnExistence_LeavesStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err == nil {
		t.Fatal("a rebuild that could not settle a resource must report failure")
	}

	if ds := es.deletesSnapshot(); len(ds) != 0 {
		t.Fatalf("plans disagreeing on existence must not delete any version's document, deleted %v", ds)
	}
	if items := es.allBulkItems(); len(items) != 0 {
		t.Fatalf("no version's document may be written for an unsettled resource, wrote %v", items)
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("the resource must stay stale for the sweep")
	}
	if !st.has("MarkStale:product/1") {
		t.Fatal("the failed resource must be durably re-marked stale")
	}
}
