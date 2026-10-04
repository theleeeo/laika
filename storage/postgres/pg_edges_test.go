package postgres

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// stored is one Schema Version's edge set as the tables hold it: its stamp
// and its Children, sorted. A stamp of -1 means edges without an edge set.
type stored struct {
	seq      int64
	children []model.Resource
}

// edgeSet builds an EdgeSet.
func edgeSet(version int, children ...model.Resource) core.EdgeSet {
	return core.EdgeSet{SchemaVersion: version, Children: children}
}

// stamped builds the stored set a test expects; Children are sorted.
func stamped(seq int64, children ...model.Resource) stored {
	sortResources(children)
	return stored{seq: seq, children: children}
}

func sortResources(rs []model.Resource) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Type != rs[j].Type {
			return rs[i].Type < rs[j].Type
		}
		return rs[i].Id < rs[j].Id
	})
}

// storedSets reads every edge set of res directly from edge_sets and
// relations, independent of the Store under test.
func storedSets(t *testing.T, res model.Resource) map[int]stored {
	t.Helper()
	ctx := context.Background()
	out := map[int]stored{}
	rows, err := testPool.Query(ctx,
		`SELECT schema_version, build_seq FROM edge_sets WHERE type=$1 AND id=$2`, res.Type, res.Id)
	if err != nil {
		t.Fatalf("read edge_sets: %v", err)
	}
	for rows.Next() {
		var v int
		var seq int64
		if err := rows.Scan(&v, &seq); err != nil {
			t.Fatalf("scan edge_sets: %v", err)
		}
		out[v] = stored{seq: seq}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read edge_sets: %v", err)
	}
	rows, err = testPool.Query(ctx,
		`SELECT schema_version, related_resource, related_resource_id FROM relations WHERE resource=$1 AND resource_id=$2`,
		res.Type, res.Id)
	if err != nil {
		t.Fatalf("read relations: %v", err)
	}
	for rows.Next() {
		var v int
		var c model.Resource
		if err := rows.Scan(&v, &c.Type, &c.Id); err != nil {
			t.Fatalf("scan relations: %v", err)
		}
		s, ok := out[v]
		if !ok {
			s.seq = -1
		}
		s.children = append(s.children, c)
		out[v] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read relations: %v", err)
	}
	for v, s := range out {
		sortResources(s.children)
		out[v] = s
	}
	return out
}

func requireSets(t *testing.T, res model.Resource, want map[int]stored) {
	t.Helper()
	if got := storedSets(t, res); !reflect.DeepEqual(got, want) {
		t.Fatalf("edge sets of %s/%s:\n got  %+v\n want %+v", res.Type, res.Id, got, want)
	}
}

// replace runs ReplaceEdges and fails the test on error.
func replace(t *testing.T, st *Store, res model.Resource, buildSeq int64, declared []int, sets ...core.EdgeSet) {
	t.Helper()
	if err := st.ReplaceEdges(context.Background(), res, buildSeq, sets, declared); err != nil {
		t.Fatalf("ReplaceEdges %s/%s at %d: %v", res.Type, res.Id, buildSeq, err)
	}
}

func TestReplaceEdges_BelowTheStoredSequenceLeavesTheSetUnchanged(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re1c", Id: id} }
	p := model.Resource{Type: "re1", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b")))
	replace(t, st, p, 4, nil, edgeSet(1, c("x")))

	requireSets(t, p, map[int]stored{1: stamped(5, c("a"), c("b"))})
}

func TestReplaceEdges_EqualSequenceIsIdempotent(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re2c", Id: id} }
	p := model.Resource{Type: "re2", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b")))
	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b")))

	requireSets(t, p, map[int]stored{1: stamped(5, c("a"), c("b"))})
}

func TestReplaceEdges_EachVersionIsGuardedOnItsOwn(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re3c", Id: id} }
	p := model.Resource{Type: "re3", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a")), edgeSet(2, c("b")))
	replace(t, st, p, 7, nil, edgeSet(2, c("c")))
	requireSets(t, p, map[int]stored{1: stamped(5, c("a")), 2: stamped(7, c("c"))})

	// One call, two verdicts: version 1 (stamped 5) takes sequence 6,
	// version 2 (stamped 7) refuses it.
	replace(t, st, p, 6, nil, edgeSet(2, c("y")), edgeSet(1, c("x")))
	requireSets(t, p, map[int]stored{1: stamped(6, c("x")), 2: stamped(7, c("c"))})
}

func TestReplaceEdges_EmptyChildrenReplaceTheSetWithNoEdges(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re4c", Id: id} }
	p := model.Resource{Type: "re4", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b")))
	replace(t, st, p, 6, nil, edgeSet(1))

	requireSets(t, p, map[int]stored{1: stamped(6)})
}

func TestReplaceEdges_DeclaredDropsUndeclaredSetsAtOrBelowTheBuild(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re5c", Id: id} }
	p := model.Resource{Type: "re5", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a")), edgeSet(2, c("b")), edgeSet(4, c("d")))
	replace(t, st, p, 7, nil, edgeSet(5, c("e")))
	replace(t, st, p, 9, nil, edgeSet(3, c("c")))

	// Version 2 is declared but this build has no set for it: untouched.
	// Versions 4 (stamped 5) and 5 (stamped 7, equal) are undeclared at or
	// below 7: dropped with their edges. Version 3 is undeclared but stamped
	// 9, by a newer build: kept.
	replace(t, st, p, 7, []int{1, 2}, edgeSet(1, c("x")))

	requireSets(t, p, map[int]stored{
		1: stamped(7, c("x")),
		2: stamped(5, c("b")),
		3: stamped(9, c("c")),
	})
}

func TestReplaceEdges_RejectsAVersionNamedTwice(t *testing.T) {
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re6c", Id: id} }
	p := model.Resource{Type: "re6", Id: "p"}

	err := st.ReplaceEdges(context.Background(), p, 5, []core.EdgeSet{edgeSet(1, c("a")), edgeSet(2), edgeSet(1, c("b"))}, nil)
	if err == nil {
		t.Fatal("a sets slice naming version 1 twice must be rejected")
	}
	requireSets(t, p, map[int]stored{})
}

func TestRemoveResource_RemovesEveryVersionsEdgesAndSets(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re7c", Id: id} }
	p, q := model.Resource{Type: "re7", Id: "p"}, model.Resource{Type: "re7", Id: "q"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a")), edgeSet(2, c("a"), c("b")), edgeSet(3))
	replace(t, st, q, 5, nil, edgeSet(1, c("a")))

	if err := st.RemoveResource(ctx, p); err != nil {
		t.Fatal(err)
	}
	requireSets(t, p, map[int]stored{})
	requireSets(t, q, map[int]stored{1: stamped(5, c("a"))})
}

func TestGetParentAndChildResources_ReturnTheUnionAcrossVersions(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re8c", Id: id} }
	p, q := model.Resource{Type: "re8", Id: "p"}, model.Resource{Type: "re8", Id: "q"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b")), edgeSet(2, c("b"), c("c")))
	replace(t, st, q, 5, nil, edgeSet(1, c("b")), edgeSet(2, c("b")))

	children, err := st.GetChildResources(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	sortResources(children)
	if want := []model.Resource{c("a"), c("b"), c("c")}; !reflect.DeepEqual(children, want) {
		t.Errorf("children of p: got %v, want %v, each once", children, want)
	}

	parents, err := st.GetParentResources(ctx, c("b"))
	if err != nil {
		t.Fatal(err)
	}
	sortResources(parents)
	if want := []model.Resource{p, q}; !reflect.DeepEqual(parents, want) {
		t.Errorf("parents of b: got %v, want %v, each once", parents, want)
	}
}

func TestRegisterChanges_MarksAParentOnceWhenTwoVersionsHoldTheEdge(t *testing.T) {
	st := NewStore(testPool)
	p, child := model.Resource{Type: "re9", Id: "p"}, model.Resource{Type: "re9c", Id: "c"}
	seed(t, p, 0, 0, false)
	replace(t, st, p, 5, nil, edgeSet(1, child), edgeSet(2, child))

	got := register(t, st, core.Registration{Resource: child, Version: 1, Metadata: meta("c")})

	if _, _, seq, since, _ := row(t, p); seq != 1 || since == nil {
		t.Errorf("Parent must be marked exactly once: stale_seq=%d (want 1) since=%v", seq, since)
	}
	if len(got.Parents) != 1 || got.Parents[0].Resource != p {
		t.Errorf("Parents: got %+v, want p once", got.Parents)
	}
}

// lockEdgeSet opens a gate on res's stored edge set of version.
func lockEdgeSet(t *testing.T, res model.Resource, version int) *gate {
	t.Helper()
	return openGate(t, testPool,
		`SELECT pg_backend_pid() FROM edge_sets WHERE type=$1 AND id=$2 AND schema_version=$3 FOR UPDATE`,
		res.Type, res.Id, version)
}

// queueBehind runs the calls in order, each in its own goroutine, starting
// the next only once the previous one waits behind the gate (seen in
// pg_stat_activity, not assumed from timing); then it releases the gate
// and fails the test unless every call returns nil within 10s. Postgres
// grants a contended row to its waiters in the order they queued: the
// first waits on the gate's transaction holding the row's tuple lock, and
// the next waits for that tuple lock, so it gets the row only once the
// first has committed.
func queueBehind(t *testing.T, g *gate, calls ...func() error) {
	t.Helper()
	type result struct {
		i   int
		err error
	}
	results := make(chan result, len(calls))
	for i, call := range calls {
		go func() { results <- result{i, call()} }()
		g.waitForWaiters(t, i+1)
	}
	g.release()
	for range calls {
		select {
		case r := <-results:
			if r.err != nil {
				var pgErr *pgconn.PgError
				if errors.As(r.err, &pgErr) && pgErr.Code == "40P01" {
					t.Fatalf("call %d deadlocked: %v", r.i, r.err)
				}
				t.Fatalf("call %d: %v", r.i, r.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a queued call did not finish after the gate committed")
		}
	}
}

func TestReplaceEdges_ConcurrentReplacesEndWithTheHigherSequencesSet(t *testing.T) {
	c := func(id string) model.Resource { return model.Resource{Type: "re10c", Id: id} }
	for _, tc := range []struct {
		name          string
		first, second int64
	}{
		// The higher replace locks version 1's set first and commits; the
		// lower one's guard then re-checks against the stamp 20 and refuses.
		{"higher first", 20, 10},
		// The lower replace locks first and commits its edges; the higher
		// one's guard accepts, and its edge delete, a later statement, sees
		// the edges the lower one inserted and removes them.
		{"lower first", 10, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := NewStore(testPool)
			p := model.Resource{Type: "re10", Id: tc.name}
			replace(t, st, p, 1, nil, edgeSet(1, c("old")))
			children := map[int64]model.Resource{10: c("lo"), 20: c("hi")}
			call := func(seq int64) func() error {
				return func() error {
					return st.ReplaceEdges(ctx, p, seq, []core.EdgeSet{edgeSet(1, children[seq], c("shared"))}, nil)
				}
			}

			queueBehind(t, lockEdgeSet(t, p, 1), call(tc.first), call(tc.second))

			requireSets(t, p, map[int]stored{1: stamped(20, c("hi"), c("shared"))})
		})
	}
}

// A rolling deploy: X's config declares {2}, Y's declares {3}. X replaces
// version 2 and prunes 3 while Y prunes 2 and replaces 3. Taken in each
// call's own order — its sets, then its prunes — X would hold 2 and wait
// for 3 while Y holds 3 and waits for 2.
func TestReplaceEdges_DifferentDeclaredSetsDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re11c", Id: id} }
	p := model.Resource{Type: "re11", Id: "p"}
	replace(t, st, p, 1, nil, edgeSet(2, c("a")), edgeSet(3, c("b")))

	// X queues on version 2 first: it replaces 2 at 10 and drops 3
	// (stamped 1). Y then drops 2 (stamped 10, not above 11) and stores 3.
	x := func() error { return st.ReplaceEdges(ctx, p, 10, []core.EdgeSet{edgeSet(2, c("x"))}, []int{2}) }
	y := func() error { return st.ReplaceEdges(ctx, p, 11, []core.EdgeSet{edgeSet(3, c("y"))}, []int{3}) }
	queueBehind(t, lockEdgeSet(t, p, 2), x, y)

	requireSets(t, p, map[int]stored{3: stamped(11, c("y"))})
}

// A delete removes every version while a build replaces two of them: both
// lock the sets in ascending version order, so neither waits for a set the
// other holds while holding one it needs.
func TestRemoveResource_DoesNotDeadlockWithAReplace(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re12c", Id: id} }
	p := model.Resource{Type: "re12", Id: "p"}
	replace(t, st, p, 1, nil, edgeSet(1, c("a")), edgeSet(2, c("b")))

	// The replace queues on version 1 first, writes both versions and
	// commits; the remove then removes both.
	rep := func() error {
		return st.ReplaceEdges(ctx, p, 5, []core.EdgeSet{edgeSet(2, c("y")), edgeSet(1, c("x"))}, nil)
	}
	rm := func() error { return st.RemoveResource(ctx, p) }
	queueBehind(t, lockEdgeSet(t, p, 1), rep, rm)

	requireSets(t, p, map[int]stored{})
}
