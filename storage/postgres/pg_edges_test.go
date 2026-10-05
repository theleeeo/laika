package postgres

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

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

// replaceErr runs ReplaceEdges with no reported metadata and returns only its
// error.
func replaceErr(ctx context.Context, st *Store, res model.Resource, buildSeq int64, sets []core.EdgeSet, declared []int) error {
	_, err := st.ReplaceEdges(ctx, res, buildSeq, sets, declared, nil)
	return err
}

// replace runs ReplaceEdges and fails the test on error.
func replace(t *testing.T, st *Store, res model.Resource, buildSeq int64, declared []int, sets ...core.EdgeSet) {
	t.Helper()
	if _, err := st.ReplaceEdges(context.Background(), res, buildSeq, sets, declared, nil); err != nil {
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

	_, err := st.ReplaceEdges(context.Background(), p, 5, []core.EdgeSet{edgeSet(1, c("a")), edgeSet(2), edgeSet(1, c("b"))}, nil, nil)
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

	if err := st.RemoveResource(ctx, p, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	requireSets(t, p, map[int]stored{})
	requireSets(t, q, map[int]stored{1: stamped(5, c("a"))})
}

// A delete removes only the edge sets its Build Sequence covers: a set
// stamped above it was written by a newer build, of a recreate, and stays
// with its edges, as ReplaceEdges leaves a set stamped above its build.
func TestRemoveResource_RemovesOnlySetsStampedAtOrBelowItsSequence(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re16c", Id: id} }
	p := model.Resource{Type: "re16", Id: "p"}
	replace(t, st, p, 10, nil, edgeSet(1, c("a"), c("b")))
	replace(t, st, p, 30, nil, edgeSet(2, c("b"), c("c")))
	remove := func(buildSeq int64) {
		t.Helper()
		if err := st.RemoveResource(ctx, p, buildSeq); err != nil {
			t.Fatalf("RemoveResource at %d: %v", buildSeq, err)
		}
	}

	remove(5)
	requireSets(t, p, map[int]stored{1: stamped(10, c("a"), c("b")), 2: stamped(30, c("b"), c("c"))})

	remove(20)
	requireSets(t, p, map[int]stored{2: stamped(30, c("b"), c("c"))})
	children, err := st.GetChildResources(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	sortResources(children)
	if want := []model.Resource{c("b"), c("c")}; !reflect.DeepEqual(children, want) {
		t.Errorf("children of p: got %v, want %v: version 2's alone", children, want)
	}
	if parents, err := st.GetParentResources(ctx, c("a")); err != nil || len(parents) != 0 {
		t.Errorf("parents of a: got %v (%v), want none: version 1's edge is gone", parents, err)
	}
	if parents, err := st.GetParentResources(ctx, c("b")); err != nil || !reflect.DeepEqual(parents, []model.Resource{p}) {
		t.Errorf("parents of b: got %v (%v), want p through version 2", parents, err)
	}

	remove(30)
	requireSets(t, p, map[int]stored{})
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

	// Marking p once per edge would fail the statement: ON CONFLICT DO UPDATE
	// cannot affect a row twice. One entry in Parents, claimed under the
	// row's new stale_seq, is the one mark.
	_, _, seq, since, _ := row(t, p)
	if seq <= 0 || since == nil {
		t.Errorf("Parent must be marked: stale_seq=%d since=%v", seq, since)
	}
	if len(got.Parents) != 1 || got.Parents[0].Resource != p || got.Parents[0].Token != seq {
		t.Errorf("Parents: got %+v, want p once, claimed with its stale_seq %d", got.Parents, seq)
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
					return replaceErr(ctx, st, p, seq, []core.EdgeSet{edgeSet(1, children[seq], c("shared"))}, nil)
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
	x := func() error { return replaceErr(ctx, st, p, 10, []core.EdgeSet{edgeSet(2, c("x"))}, []int{2}) }
	y := func() error { return replaceErr(ctx, st, p, 11, []core.EdgeSet{edgeSet(3, c("y"))}, []int{3}) }
	queueBehind(t, lockEdgeSet(t, p, 2), x, y)

	requireSets(t, p, map[int]stored{3: stamped(11, c("y"))})
}

// A child repeated within one set is stored once for its version, and the
// same child in two versions once per version; GetChildResources reads it
// once. A replace of the set keeps it once.
func TestReplaceEdges_ARepeatedChildIsStoredOncePerVersion(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re13c", Id: id} }
	p := model.Resource{Type: "re13", Id: "p"}

	replace(t, st, p, 5, nil, edgeSet(1, c("a"), c("b"), c("a")), edgeSet(2, c("a"), c("a")))
	requireSets(t, p, map[int]stored{1: stamped(5, c("a"), c("b")), 2: stamped(5, c("a"))})

	children, err := st.GetChildResources(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	sortResources(children)
	if want := []model.Resource{c("a"), c("b")}; !reflect.DeepEqual(children, want) {
		t.Errorf("children of p: got %v, want %v, each once", children, want)
	}

	replace(t, st, p, 6, nil, edgeSet(1, c("a"), c("a")))
	requireSets(t, p, map[int]stored{1: stamped(6, c("a")), 2: stamped(5, c("a"))})
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
		return replaceErr(ctx, st, p, 5, []core.EdgeSet{edgeSet(2, c("y")), edgeSet(1, c("x"))}, nil)
	}
	rm := func() error { return st.RemoveResource(ctx, p, math.MaxInt64) }
	queueBehind(t, lockEdgeSet(t, p, 1), rep, rm)

	requireSets(t, p, map[int]stored{})
}

// The same race with the gate on version 2. The replace takes version 1 and
// queues on 2; the remove then queues on version 1 behind it. A replace that
// took its versions in another order — 2 before 1 — would get 2 once the gate
// commits and wait for 1, held by the remove, which waits for it on 2: a
// deadlock.
func TestRemoveResource_DoesNotDeadlockWithAReplaceHoldingTheLowerVersion(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	c := func(id string) model.Resource { return model.Resource{Type: "re14c", Id: id} }
	p := model.Resource{Type: "re14", Id: "p"}
	replace(t, st, p, 1, nil, edgeSet(1, c("a")), edgeSet(2, c("b")))

	rep := func() error {
		return replaceErr(ctx, st, p, 5, []core.EdgeSet{edgeSet(2, c("y")), edgeSet(1, c("x"))}, nil)
	}
	rm := func() error { return st.RemoveResource(ctx, p, math.MaxInt64) }
	queueBehind(t, lockEdgeSet(t, p, 2), rep, rm)

	requireSets(t, p, map[int]stored{})
}

// repeatableReadStore is a Store on a pool whose sessions default to
// REPEATABLE READ, as a server configured so would hand out.
func repeatableReadStore(t *testing.T) *Store {
	t.Helper()
	cfg := testPool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return NewStore(pool)
}

// ReplaceEdges and RemoveResource re-check a row a concurrent replace
// committed while they waited for it, which needs READ COMMITTED: they pin it,
// so a server defaulting to REPEATABLE READ does not turn the wait into a
// serialization failure.
func TestReplaceEdgesAndRemoveResource_WaitUnderARepeatableReadDefault(t *testing.T) {
	ctx := context.Background()
	st := repeatableReadStore(t)
	c := func(id string) model.Resource { return model.Resource{Type: "re15c", Id: id} }
	repAt := func(p model.Resource, seq int64, child string) func() error {
		return func() error { return replaceErr(ctx, st, p, seq, []core.EdgeSet{edgeSet(1, c(child))}, nil) }
	}

	t.Run("replace behind a replace", func(t *testing.T) {
		p := model.Resource{Type: "re15", Id: "replace"}
		replace(t, st, p, 1, nil, edgeSet(1, c("old")))
		queueBehind(t, lockEdgeSet(t, p, 1), repAt(p, 10, "lo"), repAt(p, 20, "hi"))
		requireSets(t, p, map[int]stored{1: stamped(20, c("hi"))})
	})
	t.Run("remove behind a replace", func(t *testing.T) {
		p := model.Resource{Type: "re15", Id: "remove"}
		replace(t, st, p, 1, nil, edgeSet(1, c("old")))
		queueBehind(t, lockEdgeSet(t, p, 1), repAt(p, 10, "lo"), func() error { return st.RemoveResource(ctx, p, math.MaxInt64) })
		requireSets(t, p, map[int]stored{})
	})
}
