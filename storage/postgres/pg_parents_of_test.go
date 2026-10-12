package postgres

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// parentsOf runs ParentsOf and fails the test on error.
func parentsOf(t *testing.T, st *Store, children ...model.Resource) []model.Resource {
	t.Helper()
	got, err := st.ParentsOf(context.Background(), children)
	if err != nil {
		t.Fatalf("ParentsOf: %v", err)
	}
	return got
}

// sortedResources is rs sorted by (type, id), for order-free comparison.
func sortedResources(rs []model.Resource) []model.Resource {
	out := slices.Clone(rs)
	slices.SortFunc(out, func(a, b model.Resource) int {
		return cmp.Or(strings.Compare(a.Type, b.Type), strings.Compare(a.Id, b.Id))
	})
	return out
}

// The race the second fanout closes. A Parent's build begins, so its start
// is below the registration's value. A registration of the Child starts and
// queues behind a gate on the Child's row, its statement's snapshot already
// taken. The Parent's build stores an edge naming the Child, which commits,
// and its drift check finds no change: the registration hasn't committed.
// Once the gate commits, the registration commits without the Parent — its
// fanout read the edges before the edge committed — and only a read after
// the commit, ParentsOf the Child, finds the Parent.
func TestParentsOf_FindsAParentWhoseEdgeCommittedWhileTheRegistrationRan(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	id := fmt.Sprint(deadlockRuns.Add(1)) // fresh rows on every -count run
	child := model.Resource{Type: "po-race-child", Id: id}
	parent := model.Resource{Type: "po-race-parent", Id: id}
	seed(t, child, 0, 0, false)

	begun, err := st.BeginBuild(ctx, parent, 0)
	if err != nil {
		t.Fatalf("BeginBuild of the Parent: %v", err)
	}

	g := lockRow(t, testPool, child)
	type result struct {
		reg core.Registered
		err error
	}
	done := make(chan result, 1)
	go func() {
		reg, err := st.RegisterChanges(ctx, []core.Registration{{Resource: child}}, time.Minute)
		done <- result{reg, err}
	}()
	g.waitForWaiters(t, 1)

	if err := st.ReplaceEdges(ctx, parent, begun.BuildIdx,
		[]core.EdgeSet{{SchemaVersion: 1, Children: []model.Resource{child}}}, nil, nil); err != nil {
		t.Fatalf("ReplaceEdges of the Parent: %v", err)
	}
	if changedSince(t, st, core.ChangeCheck{Resource: child, Start: begun.Start}) {
		t.Fatal("the drift check saw a registration that hasn't committed")
	}

	g.release()
	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RegisterChanges did not finish after the gate committed")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	item := r.reg.Items[0]
	if !item.Accepted || item.StaleSeq <= begun.Start {
		t.Fatalf("item %+v; want accepted at a value above the Parent's start %d", item, begun.Start)
	}
	if len(r.reg.Parents) != 0 {
		t.Fatalf("Parents %+v; the registration's fanout read the edges before the Parent's edge committed, so it must find none", r.reg.Parents)
	}

	if got := parentsOf(t, st, child); !slices.Equal(got, []model.Resource{parent}) {
		t.Fatalf("ParentsOf %v, want the Parent %v", got, parent)
	}
}

// ParentsOf returns every Parent the edges name, whatever its mark: one the
// registration's fanout marked, one marked after it, one marked before it,
// one never marked, and one with no row.
func TestParentsOf_ReturnsParentsWhateverTheirMarks(t *testing.T) {
	st := NewStore(testPool)
	id := fmt.Sprint(deadlockRuns.Add(1))
	r := func(name string) model.Resource { return model.Resource{Type: "po-marks", Id: name + "-" + id} }
	child := r("child")
	byFanout, markedAfter, markedBefore, clean, rowless := r("by-fanout"), r("marked-after"), r("marked-before"), r("clean"), r("rowless")

	relate(t, [2]model.Resource{byFanout, child})
	markUnclaimed(t, st, markedBefore)
	seed(t, clean, 0, 0, false)
	reg := register(t, st, core.Registration{Resource: child})
	if len(reg.Parents) != 1 || reg.Parents[0].Resource != byFanout {
		t.Fatalf("Parents %+v, want the fanout to mark %v", reg.Parents, byFanout)
	}
	relate(t,
		[2]model.Resource{markedAfter, child},
		[2]model.Resource{markedBefore, child},
		[2]model.Resource{clean, child},
		[2]model.Resource{rowless, child},
	)
	markUnclaimed(t, st, markedAfter)

	got := parentsOf(t, st, child)
	want := sortedResources([]model.Resource{byFanout, markedAfter, markedBefore, clean, rowless})
	if !slices.Equal(sortedResources(got), want) {
		t.Fatalf("ParentsOf %v, want %v", got, want)
	}
}

// A Parent two Children name, and one named in two Schema Versions, is
// returned once; a Child with no edges adds nothing.
func TestParentsOf_ReturnsEachParentOnce(t *testing.T) {
	st := NewStore(testPool)
	id := fmt.Sprint(deadlockRuns.Add(1))
	r := func(name string) model.Resource { return model.Resource{Type: "po-once", Id: name + "-" + id} }
	c1, c2, lone, parent := r("c1"), r("c2"), r("lone"), r("parent")

	relate(t, [2]model.Resource{parent, c1}, [2]model.Resource{parent, c2})
	if _, err := testPool.Exec(context.Background(),
		`INSERT INTO relations (resource, resource_id, schema_version, related_resource, related_resource_id) VALUES ($1, $2, 2, $3, $4)`,
		parent.Type, parent.Id, c1.Type, c1.Id); err != nil {
		t.Fatalf("relate version 2: %v", err)
	}

	if got := parentsOf(t, st, c1, c2, lone); !slices.Equal(got, []model.Resource{parent}) {
		t.Fatalf("ParentsOf %v, want %v once", got, parent)
	}
	if got := parentsOf(t, st, c1); !slices.Equal(got, []model.Resource{parent}) {
		t.Fatalf("ParentsOf one Child %v, want %v once across its two versions", got, parent)
	}
}

func TestParentsOf_NoChildrenIsNothingWithoutAQuery(t *testing.T) {
	// A nil pool panics on any query.
	got, err := NewStore(nil).ParentsOf(context.Background(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("no children: got %v, %v; want nothing, nil", got, err)
	}
}
