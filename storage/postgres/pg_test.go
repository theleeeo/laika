package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := pgcontainer.Run(ctx, "postgres:17",
		pgcontainer.WithDatabase("indexer"),
		pgcontainer.WithUsername("user"),
		pgcontainer.WithPassword("pass"),
		pgcontainer.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		os.Exit(1)
	}
	endpoint, err := c.Endpoint(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres endpoint: %v\n", err)
		os.Exit(1)
	}
	cfg, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://user:pass@%s/indexer", endpoint))
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgxpool config: %v\n", err)
		os.Exit(1)
	}
	// A gate test holds gates and blocked statements while it polls on
	// another connection; the default max(4, NumCPU) can starve that poll.
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgxpool: %v\n", err)
		os.Exit(1)
	}
	if err := applySchema(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	_ = testcontainers.TerminateContainer(c)
	os.Exit(code)
}

// applySchema runs pg_schema.sql, as a deployment applies it, against the
// schema first on the pool's search_path.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	schema, err := os.ReadFile("pg_schema.sql")
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// row reads the full resources row for assertions.
func row(t *testing.T, res model.Resource) (version, buildIdx, staleSeq int64, staleSince *time.Time, deleted bool) {
	t.Helper()
	return rowIn(t, testPool, res)
}

// rowIn is row on the given pool, such as an isolatedStore's.
func rowIn(t *testing.T, pool *pgxpool.Pool, res model.Resource) (version, buildIdx, staleSeq int64, staleSince *time.Time, deleted bool) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT version, build_idx, stale_seq, stale_since, deleted FROM resources WHERE type=$1 AND id=$2`,
		res.Type, res.Id,
	).Scan(&version, &buildIdx, &staleSeq, &staleSince, &deleted)
	if err != nil {
		t.Fatalf("read row %s/%s: %v", res.Type, res.Id, err)
	}
	return
}

func TestMarkStale_InsertsAndBumps_PreservesOldestTimestamp(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "ms", Id: "1"}

	if _, err := st.MarkStale(ctx, []model.Resource{res}, 0); err != nil {
		t.Fatal(err)
	}
	_, _, seq1, since1, _ := row(t, res)
	if seq1 <= 0 || since1 == nil {
		t.Fatalf("after first mark: seq=%d since=%v", seq1, since1)
	}

	time.Sleep(10 * time.Millisecond)
	if _, err := st.MarkStale(ctx, []model.Resource{res}, 0); err != nil {
		t.Fatal(err)
	}
	_, _, seq2, since2, _ := row(t, res)
	if seq2 <= seq1 {
		t.Fatalf("after second mark: seq=%d, want above the first mark's %d", seq2, seq1)
	}
	if !since2.Equal(*since1) {
		t.Fatalf("stale_since must keep the oldest timestamp: was %v, now %v", since1, since2)
	}
}

func TestMarkStale_BatchWithDuplicates(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	dup := model.Resource{Type: "msd", Id: "dup"}
	other := model.Resource{Type: "msd", Id: "other"}

	// One batch where dup appears TWICE: must not error (ON CONFLICT DO UPDATE
	// cannot affect the same row twice in one statement without deduping), so
	// the statement marks each row once.
	before := start(t, st)
	if _, err := st.MarkStale(ctx, []model.Resource{dup, other, dup}, 0); err != nil {
		t.Fatalf("MarkStale with duplicate resources in one batch: %v", err)
	}
	after := start(t, st)

	_, _, dupSeq, dupSince, _ := row(t, dup)
	if dupSeq <= before || dupSeq >= after {
		t.Fatalf("duplicated resource must count as ONE logical mark: stale_seq=%d, want a mark between %d and %d, drawn around the call", dupSeq, before, after)
	}
	if dupSince == nil {
		t.Fatal("duplicated resource must have stale_since set")
	}

	_, _, otherSeq, otherSince, _ := row(t, other)
	if otherSeq <= before || otherSeq >= after || otherSeq == dupSeq || otherSince == nil {
		t.Fatalf("other resource wrong: seq=%d since=%v, want a mark of its own between %d and %d (dup's is %d)", otherSeq, otherSince, before, after, dupSeq)
	}
}

// A mark writes no metadata: a row's metadata is its own registrations'
// (or its plans' report), so a mark, with a lease or without, leaves what
// the row holds, and a row a mark creates has none.
func TestMarkStale_LeavesTheRowsMetadataAlone(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "msm", Id: id} }

	t.Run("lease 0", func(t *testing.T) {
		res := r("unclaimed")
		register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("reg")})
		_, _, before, _, _ := row(t, res)

		markUnclaimed(t, st, res)

		if _, _, seq, _, _ := row(t, res); seq <= before {
			t.Fatalf("the mark must move stale_seq above %d, got %d", before, seq)
		}
		requireMetadata(t, "metadata after a mark", metadataOf(t, res), meta("reg"))
	})
	t.Run("with a lease", func(t *testing.T) {
		res := r("claimed")
		register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("reg")})
		expireOwner(t, testPool, res) // so the mark claims

		owned, err := st.MarkStale(ctx, []model.Resource{res}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(owned) != 1 || owned[0].Resource != res {
			t.Fatalf("owned %+v, want the row claimed", owned)
		}
		requireMetadata(t, "metadata after a claiming mark", metadataOf(t, res), meta("reg"))
	})
	t.Run("a row the mark creates", func(t *testing.T) {
		for _, lease := range []time.Duration{0, time.Minute} {
			res := r(fmt.Sprint("new-", lease))
			if _, err := st.MarkStale(ctx, []model.Resource{res}, lease); err != nil {
				t.Fatal(err)
			}
			if m := rawMetadata(t, res); m != nil {
				t.Fatalf("lease %v: a row a mark creates stores metadata %q, want NULL", lease, *m)
			}
		}
	})
}

func TestBeginBuild_BumpsBuildIdx_ReturnsStaleSeq(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "bb", Id: "1"}

	if _, err := st.MarkStale(ctx, []model.Resource{res}, 0); err != nil {
		t.Fatal(err)
	}
	begun, err := st.BeginBuild(ctx, res, 0)
	if err != nil {
		t.Fatal(err)
	}
	buildIdx, staleSeq := begun.BuildIdx, begun.StaleSeq
	_, rowIdx, rowSeq, _, _ := row(t, res)
	if buildIdx <= 0 || buildIdx != rowIdx || staleSeq != rowSeq {
		t.Fatalf("got buildIdx=%d staleSeq=%d, want the row's build_idx %d (above 0) and stale_seq %d", buildIdx, staleSeq, rowIdx, rowSeq)
	}
	begun2, err := st.BeginBuild(ctx, res, 0)
	if err != nil {
		t.Fatal(err)
	}
	buildIdx2 := begun2.BuildIdx
	if _, rowIdx, _, _, _ := row(t, res); buildIdx2 <= buildIdx || buildIdx2 != rowIdx {
		t.Fatalf("build_idx must increment: got %d after %d, row holds %d", buildIdx2, buildIdx, rowIdx)
	}
}

func TestClearStale_GuardedBySeq(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "cs", Id: "1"}

	if _, err := st.MarkStale(ctx, []model.Resource{res}, 0); err != nil {
		t.Fatal(err)
	}
	_, _, seq, _, _ := row(t, res)
	if err := st.ClearStale(ctx, res, seq); err != nil {
		t.Fatal(err)
	}
	_, _, _, since, _ := row(t, res)
	if since != nil {
		t.Fatalf("matching seq must clear: stale_since=%v", since)
	}

	// Re-mark twice: clear with a stale seq must be a no-op.
	_, _ = st.MarkStale(ctx, []model.Resource{res}, 0)
	_, _, seq, _, _ = row(t, res)
	_, _ = st.MarkStale(ctx, []model.Resource{res}, 0)
	if err := st.ClearStale(ctx, res, seq); err != nil {
		t.Fatal(err)
	}
	_, _, _, since, _ = row(t, res)
	if since == nil {
		t.Fatal("clear with a moved seq must NOT clear the mark")
	}
}

func TestRegisterChanges_DeleteTombstonesAndResetsVersion(t *testing.T) {
	st := NewStore(testPool)
	res := model.Resource{Type: "md", Id: "1"}

	created := register(t, st, core.Registration{Resource: res, Version: 5}).Items[0].StaleSeq
	got := register(t, st, core.Registration{Resource: res, Deleted: true})
	seq := got.Items[0].StaleSeq
	version, _, staleSeq, since, deleted := row(t, res)
	if !got.Items[0].Accepted || !deleted || since == nil || staleSeq != seq || seq <= created {
		t.Fatalf("tombstone wrong: accepted=%v deleted=%v since=%v seq=%d ret=%d (want above the create's %d)", got.Items[0].Accepted, deleted, since, staleSeq, seq, created)
	}
	if version != 0 {
		t.Fatalf("a delete must reset version so a re-create is never 'stale': version=%d", version)
	}

	// Re-create with a low version must be accepted and clear the tombstone flag.
	got = register(t, st, core.Registration{Resource: res, Version: 1})
	if !got.Items[0].Accepted {
		t.Fatal("re-create after delete must be accepted")
	}
	_, _, _, _, deleted = row(t, res)
	if deleted {
		t.Fatal("an accepted upsert must clear the deleted flag")
	}
}

// DeleteResourceIfSeq removes the row at the matching stale_seq whether it is
// a tombstone (a notified delete) or a live row (a build path whose plans all
// returned nil: a marked row, or one a BeginBuild inserted and nothing
// marked), and leaves it when a newer mark moved stale_seq.
func TestDeleteResourceIfSeq_GuardedHardDelete(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	exists := func(res model.Resource) bool {
		t.Helper()
		var n int
		if err := testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	tombstone, live, inserted, remarked := model.Resource{Type: "dr", Id: "1"}, model.Resource{Type: "dr", Id: "2"},
		model.Resource{Type: "dr", Id: "3"}, model.Resource{Type: "dr", Id: "4"}
	tombSeq := register(t, st, core.Registration{Resource: tombstone, Deleted: true}).Items[0].StaleSeq
	liveSeq := register(t, st, core.Registration{Resource: live, Version: 7}).Items[0].StaleSeq
	begun, err := st.BeginBuild(ctx, inserted, 0)
	if err != nil {
		t.Fatal(err)
	}
	remarkedSeq := register(t, st, core.Registration{Resource: remarked, Version: 7}).Items[0].StaleSeq
	if _, err := st.MarkStale(ctx, []model.Resource{remarked}, 0); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string
		res  model.Resource
		seq  int64
		kept bool
	}{
		{"a tombstone at its stale_seq", tombstone, tombSeq, false},
		{"a live row at its stale_seq", live, liveSeq, false},
		{"a row BeginBuild inserted, never marked", inserted, begun.StaleSeq, false},
		{"a row a newer mark moved", remarked, remarkedSeq, true},
	} {
		if _, err := st.DeleteResourceIfSeq(ctx, c.res, c.seq, 0); err != nil {
			t.Fatal(err)
		}
		if got := exists(c.res); got != c.kept {
			t.Fatalf("%s: row kept %v, want %v", c.name, got, c.kept)
		}
	}
}

func TestListStale_CutoffOrderLimitAndDeletedFlag(t *testing.T) {
	ctx := context.Background()
	// ListStale claims across the table: an isolated one keeps it to these rows.
	st, pool := isolatedStore(t)

	oldRes := model.Resource{Type: "ls", Id: "old"}
	newRes := model.Resource{Type: "ls", Id: "new"}
	delRes := model.Resource{Type: "ls", Id: "del"}

	if _, err := st.MarkStale(ctx, []model.Resource{oldRes}, 0); err != nil {
		t.Fatal(err)
	}
	_, _, oldSeq, _, _ := rowIn(t, pool, oldRes)
	delSeq := register(t, st, core.Registration{Resource: delRes, Deleted: true}).Items[0].StaleSeq
	expireOwner(t, pool, delRes) // the registration claimed it; a live lease would hide it
	backdateIn(t, pool, oldRes, "10 minutes")
	backdateIn(t, pool, delRes, "5 minutes")
	if _, err := st.MarkStale(ctx, []model.Resource{newRes}, 0); err != nil { // fresh mark, excluded by the cutoff
		t.Fatal(err)
	}
	before := time.Now().Add(-time.Minute)

	// Limit 1 returns the oldest only.
	first, err := st.ListStale(ctx, before, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Resource != oldRes || first[0].Deleted || first[0].StaleSeq != oldSeq {
		t.Fatalf("limit 1: got %+v, want old alone (oldest first), not deleted, seq %d", first, oldSeq)
	}

	// The first call claimed old, so its live lease hides it now; new is
	// inside the cutoff.
	rest, err := st.ListStale(ctx, before, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].Resource != delRes || !rest[0].Deleted || rest[0].StaleSeq != delSeq {
		t.Fatalf("second call: got %+v, want del alone, Deleted, seq %d", rest, delSeq)
	}
}

func TestCountStale_FiltersByTypeAndCutoff_ReportsOldest(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)

	oldA := model.Resource{Type: "cs", Id: "old-a"}
	oldB := model.Resource{Type: "cs", Id: "old-b"}
	tomb := model.Resource{Type: "cs", Id: "del"}
	fresh := model.Resource{Type: "cs", Id: "fresh"}
	otherType := model.Resource{Type: "cs-other", Id: "old"}

	_, _ = st.MarkStale(ctx, []model.Resource{oldA, oldB, otherType}, 0)
	register(t, st, core.Registration{Resource: tomb, Deleted: true})
	backdate := func(r model.Resource, age string) {
		if _, err := testPool.Exec(ctx,
			`UPDATE resources SET stale_since = now() - $3::interval WHERE type=$1 AND id=$2`,
			r.Type, r.Id, age); err != nil {
			t.Fatal(err)
		}
	}
	backdate(oldA, "10 minutes")
	backdate(oldB, "5 minutes")
	backdate(tomb, "15 minutes") // tombstones are unfinished work: they count
	backdate(otherType, "10 minutes")
	_, _ = st.MarkStale(ctx, []model.Resource{fresh}, 0) // inside cutoff: excluded

	count, oldest, err := st.CountStale(ctx, "cs", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("count: got %d want 3 (old-a, old-b, del)", count)
	}
	wantOldest := time.Now().Add(-15 * time.Minute)
	if d := oldest.Sub(wantOldest); d < -30*time.Second || d > 30*time.Second {
		t.Fatalf("oldest: got %v, want about %v", oldest, wantOldest)
	}

	count, _, err = st.CountStale(ctx, "cs-other", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("other type must be counted separately: got %d want 1", count)
	}

	count, oldest, err = st.CountStale(ctx, "cs-none", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || !oldest.IsZero() {
		t.Fatalf("type with no stale rows: got count %d oldest %v, want 0 and zero time", count, oldest)
	}
}

// register runs RegisterChanges and fails the test on error.
func register(t *testing.T, st *Store, items ...core.Registration) core.Registered {
	t.Helper()
	got, err := st.RegisterChanges(context.Background(), items, time.Minute)
	if err != nil {
		t.Fatalf("RegisterChanges: %v", err)
	}
	return got
}

// seed writes a resources row directly, independent of the Store under test.
func seed(t *testing.T, res model.Resource, version, staleSeq int64, deleted bool) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`INSERT INTO resources (type, id, version, stale_seq, deleted) VALUES ($1, $2, $3, $4, $5)`,
		res.Type, res.Id, version, staleSeq, deleted); err != nil {
		t.Fatalf("seed %s/%s: %v", res.Type, res.Id, err)
	}
}

// relate writes parent -> child relations of Schema Version 1 directly,
// without an edge set: RegisterChanges reads relations only.
func relate(t *testing.T, pairs ...[2]model.Resource) {
	t.Helper()
	for _, p := range pairs {
		if _, err := testPool.Exec(context.Background(),
			`INSERT INTO relations (resource, resource_id, schema_version, related_resource, related_resource_id) VALUES ($1, $2, 1, $3, $4)`,
			p[0].Type, p[0].Id, p[1].Type, p[1].Id); err != nil {
			t.Fatalf("relate: %v", err)
		}
	}
}

// metadataOf reads the metadata stored on a row.
func metadataOf(t *testing.T, res model.Resource) map[string]string {
	t.Helper()
	var m map[string]string
	if err := testPool.QueryRow(context.Background(),
		`SELECT metadata FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&m); err != nil {
		t.Fatalf("read metadata %s/%s: %v", res.Type, res.Id, err)
	}
	return m
}

func meta(v string) map[string]string { return map[string]string{"k": v} }

// backdate sets an existing mark ten minutes in the past and returns it.
func backdate(t *testing.T, res model.Resource) time.Time {
	t.Helper()
	return backdateIn(t, testPool, res, "10 minutes")
}

// backdateIn sets an existing mark age (a Postgres interval) in the past on
// the given pool and returns it.
func backdateIn(t *testing.T, pool *pgxpool.Pool, res model.Resource, age string) time.Time {
	t.Helper()
	var since time.Time
	if err := pool.QueryRow(context.Background(),
		`UPDATE resources SET stale_since = now() - $3::interval WHERE type=$1 AND id=$2 RETURNING stale_since`,
		res.Type, res.Id, age).Scan(&since); err != nil {
		t.Fatalf("backdate %s/%s: %v", res.Type, res.Id, err)
	}
	return since
}

func TestRegisterChanges_CommitsAcceptedItemsAndMarksParentsOnce(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rc1", Id: id} }
	stale, fresh, inParent, child, staleParent := r("stale"), r("new"), r("q"), r("child"), r("rejected-parent")
	p1, p2 := r("p1"), r("p2")

	seed(t, stale, 5, 0, false)
	seed(t, staleParent, 9, 0, false)
	// p1 carries an earlier mark, drawn from the Change Sequence so no later
	// mark can collide with it.
	p1Seeded := start(t, st)
	seed(t, p1, 0, p1Seeded, false)
	seed(t, p2, 0, 0, false)
	p1Since := backdate(t, p1)
	relate(t,
		[2]model.Resource{p1, fresh},
		[2]model.Resource{p1, child},       // p1 is Parent of two accepted items
		[2]model.Resource{p2, stale},       // stale item: its Parent is not marked
		[2]model.Resource{inParent, child}, // accepted in-batch item as Parent
		[2]model.Resource{staleParent, fresh},
	)

	got, err := st.RegisterChanges(ctx, []core.Registration{
		{Resource: stale, Version: 5, Metadata: meta("stale")}, // equal version: stale
		{Resource: fresh, Version: 1, Metadata: meta("new")},
		{Resource: inParent, Version: 2, Metadata: meta("q")},
		{Resource: child, Version: 3, Metadata: meta("child")},
		{Resource: staleParent, Version: 4, Metadata: meta("rejected")}, // lower version: stale
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// The accepted items are new, unowned rows: each mark claims them, under
	// the row's new stale_seq.
	wantAccepted := []bool{false, true, true, true, false}
	if len(got.Items) != len(wantAccepted) {
		t.Fatalf("Items: got %d, want %d", len(got.Items), len(wantAccepted))
	}
	for i, accepted := range wantAccepted {
		it := got.Items[i]
		if !accepted {
			if it != (core.RegisteredItem{}) {
				t.Errorf("Items[%d]: got %+v, want rejected", i, it)
			}
			continue
		}
		if !it.Accepted || it.StaleSeq <= 0 || it.Token != it.StaleSeq {
			t.Errorf("Items[%d]: got %+v, want accepted, claimed with its StaleSeq as the token", i, it)
		}
	}

	for i, c := range []struct {
		res     model.Resource
		version int64
		meta    string
	}{{fresh, 1, "new"}, {inParent, 2, "q"}, {child, 3, "child"}} {
		version, _, seq, since, deleted := row(t, c.res)
		if version != c.version || seq != got.Items[i+1].StaleSeq || since == nil || deleted {
			t.Errorf("%s: version=%d seq=%d since=%v deleted=%v; want version %d, seq %d (its item's StaleSeq), marked", c.res.Id, version, seq, since, deleted, c.version, got.Items[i+1].StaleSeq)
		}
		if m := metadataOf(t, c.res); m["k"] != c.meta {
			t.Errorf("%s: metadata %v, want its own %q", c.res.Id, m, c.meta)
		}
	}

	if version, _, seq, since, _ := row(t, stale); version != 5 || seq != 0 || since != nil || metadataOf(t, stale) != nil {
		t.Errorf("stale item must write nothing: version=%d seq=%d since=%v", version, seq, since)
	}
	if _, _, seq, since, _ := row(t, p2); seq != 0 || since != nil {
		t.Errorf("Parent of the stale item must not be marked: seq=%d since=%v", seq, since)
	}
	// A Parent marked twice in one statement would fail it (ON CONFLICT DO
	// UPDATE cannot affect a row twice), so a single entry in Parents and a
	// moved seq show the one mark.
	_, _, p1Seq, since, _ := row(t, p1)
	if p1Seq <= p1Seeded || since == nil {
		t.Errorf("shared Parent must be marked: seq=%d (want above %d) since=%v", p1Seq, p1Seeded, since)
	} else if !since.Equal(p1Since) {
		t.Errorf("Parent mark must keep the oldest stale_since: was %v, now %v", p1Since, since)
	}
	if m := metadataOf(t, p1); m != nil {
		t.Errorf("shared Parent metadata %v, want none: a Parent mark leaves the row's metadata alone", m)
	}
	staleParentVersion, _, staleParentSeq, staleParentSince, _ := row(t, staleParent)
	if staleParentVersion != 9 || staleParentSeq <= 0 || staleParentSince == nil {
		t.Errorf("rejected in-batch item must still be marked as a Parent, version untouched: version=%d seq=%d since=%v", staleParentVersion, staleParentSeq, staleParentSince)
	}
	if m := metadataOf(t, staleParent); m != nil {
		t.Errorf("rejected in-batch Parent metadata %v, want none: neither its rejected registration nor its Parent mark writes any", m)
	}

	parents := map[model.Resource]core.MarkedParent{}
	for _, p := range got.Parents {
		if _, dup := parents[p.Resource]; dup {
			t.Errorf("Parent %v listed twice", p.Resource)
		}
		parents[p.Resource] = p
	}
	_, hasP1 := parents[p1]
	_, hasStaleParent := parents[staleParent]
	if len(parents) != 2 || !hasP1 || !hasStaleParent {
		t.Errorf("Parents: got %+v, want p1 and rejected-parent only", got.Parents)
	}
	// Both Parents were unowned: the mark claims them under their new stale_seq.
	if parents[p1].Token != p1Seq || parents[staleParent].Token != staleParentSeq {
		t.Errorf("Parent tokens: p1 %d (want %d), rejected-parent %d (want %d)", parents[p1].Token, p1Seq, parents[staleParent].Token, staleParentSeq)
	}
}

func TestRegisterChanges_Version0UntombstonesAndDeleteTombstones(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rc2", Id: id} }
	tomb, live, fresh := r("tomb"), r("live"), r("fresh")
	// Earlier marks, drawn from the Change Sequence so no later mark can
	// collide with them.
	tombSeeded, liveSeeded := start(t, st), start(t, st)
	seed(t, tomb, 4, tombSeeded, true)
	seed(t, live, 7, liveSeeded, false)
	tombSince := backdate(t, tomb)

	got := register(t, st,
		core.Registration{Resource: tomb, Version: 0, Metadata: meta("a")},
		core.Registration{Resource: live, Deleted: true, Version: 3, Metadata: meta("b")}, // Version ignored
		core.Registration{Resource: fresh, Version: 0},
	)

	// All three rows are unowned: each mark claims its row under its new
	// stale_seq, above the one it had.
	for i, earlier := range []int64{tombSeeded, liveSeeded, 0} {
		if it := got.Items[i]; !it.Accepted || it.StaleSeq <= earlier || it.Token != it.StaleSeq {
			t.Errorf("Items[%d]: got %+v, want accepted with a StaleSeq above %d, claimed with it as the token", i, it, earlier)
		}
	}
	if version, _, seq, since, deleted := row(t, tomb); deleted || version != 4 || seq != got.Items[0].StaleSeq || since == nil {
		t.Errorf("version-0 item must untombstone and mark: deleted=%v version=%d seq=%d since=%v", deleted, version, seq, since)
	} else if !since.Equal(tombSince) {
		t.Errorf("item mark must keep the oldest stale_since: was %v, now %v", tombSince, since)
	}
	if m := metadataOf(t, tomb); m["k"] != "a" {
		t.Errorf("untombstoned metadata %v", m)
	}
	if version, _, seq, since, deleted := row(t, live); !deleted || version != 0 || seq != got.Items[1].StaleSeq || since == nil {
		t.Errorf("delete must tombstone, reset version and mark: deleted=%v version=%d seq=%d since=%v", deleted, version, seq, since)
	}
	if m := metadataOf(t, live); m["k"] != "b" {
		t.Errorf("tombstone must store its own metadata: %v", m)
	}
	if version, _, seq, since, deleted := row(t, fresh); deleted || version != 0 || seq != got.Items[2].StaleSeq || since == nil {
		t.Errorf("new version-0 item: deleted=%v version=%d seq=%d since=%v", deleted, version, seq, since)
	}
	if m := metadataOf(t, fresh); m != nil {
		t.Errorf("nil metadata must store NULL, got %v", m)
	}
}

func TestRegisterChanges_StatementFailureCommitsNothing(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rc3", Id: id} }
	item, fresh, parent := r("item"), r("fresh"), r("parent")
	// The Parent's mark is drawn from the Change Sequence, so the statement's
	// own mark can't collide with it.
	parentSeq := start(t, st)
	seed(t, item, 1, 0, false)
	seed(t, parent, 0, parentSeq, false)
	relate(t, [2]model.Resource{parent, item})

	// Only the Parent's mark violates this, so the items' rows are written
	// earlier in the same statement before it fails.
	if _, err := testPool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE resources ADD CONSTRAINT rc3_fail CHECK (type <> 'rc3' OR id <> 'parent' OR stale_seq = %d)`, parentSeq)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `ALTER TABLE resources DROP CONSTRAINT rc3_fail`)
	})

	_, err := st.RegisterChanges(ctx, []core.Registration{
		{Resource: item, Version: 2, Metadata: meta("x")},
		{Resource: fresh, Version: 1},
	}, time.Minute)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.ConstraintName != "rc3_fail" {
		t.Fatalf("want the injected constraint violation to fail the statement, got %v", err)
	}

	if version, _, seq, since, _ := row(t, item); version != 1 || seq != 0 || since != nil {
		t.Errorf("no version may be stored without its mark: version=%d seq=%d since=%v", version, seq, since)
	}
	var n int
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type='rc3' AND id='fresh'`).Scan(&n)
	if n != 0 {
		t.Error("new item's row must not be committed")
	}
	if _, _, seq, _, _ := row(t, parent); seq != parentSeq {
		t.Errorf("Parent seq changed: %d, want %d", seq, parentSeq)
	}
}

func TestRegisterChanges_DeleteMarksItsParents(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rc4", Id: id} }
	parent, child := r("parent"), r("child")
	seed(t, parent, 0, 0, false)
	relate(t, [2]model.Resource{parent, child})
	register(t, st, core.Registration{Resource: child, Version: 1, Metadata: meta("create")})
	_, _, parentSeq, _, _ := row(t, parent)

	got := register(t, st, core.Registration{Resource: child, Deleted: true, Metadata: meta("M")})

	// The create claimed child and parent a moment ago; their leases are
	// live, so the delete's marks claim neither.
	if _, _, childSeq, _, deleted := row(t, child); !deleted || got.Items[0] != (core.RegisteredItem{Accepted: true, StaleSeq: childSeq}) {
		t.Errorf("delete item: got %+v, want accepted with its row's seq %d and no token (deleted=%v)", got.Items[0], childSeq, deleted)
	}
	if _, _, seq, since, _ := row(t, parent); seq <= parentSeq || since == nil {
		t.Errorf("Parent of the deleted item must be marked: seq=%d (want above %d) since=%v", seq, parentSeq, since)
	}
	if m := metadataOf(t, parent); m != nil {
		t.Errorf("Parent metadata %v, want none: the delete's mark of its Parent leaves the Parent's metadata alone", m)
	}
	if len(got.Parents) != 1 || got.Parents[0] != (core.MarkedParent{Resource: parent}) {
		t.Errorf("Parents: got %+v, want the Parent once and no token", got.Parents)
	}
}

// changeSeq reads a row's change_seq.
func changeSeq(t *testing.T, pool *pgxpool.Pool, res model.Resource) int64 {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(context.Background(),
		`SELECT change_seq FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&seq); err != nil {
		t.Fatalf("read change_seq %s/%s: %v", res.Type, res.Id, err)
	}
	return seq
}

// start takes a start from the Change Sequence through the Store.
func start(t *testing.T, st *Store) int64 {
	t.Helper()
	s, err := st.NextChangeSeq(context.Background())
	if err != nil {
		t.Fatalf("NextChangeSeq: %v", err)
	}
	return s
}

// changedSince runs AnyChangedSince and fails the test on error.
func changedSince(t *testing.T, st *Store, checks ...core.ChangeCheck) bool {
	t.Helper()
	got, err := st.AnyChangedSince(context.Background(), checks)
	if err != nil {
		t.Fatalf("AnyChangedSince: %v", err)
	}
	return got
}

var isolatedSchemas atomic.Int64

// isolatedStore applies pg_schema.sql to a fresh Postgres schema of its own,
// so a test can reset or inspect the Change Sequence without touching the
// one the other tests share. The returned pool resolves every unqualified
// name — resources, change_sequence — to that schema.
func isolatedStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("iso_%d", isolatedSchemas.Add(1))
	if _, err := testPool.Exec(ctx, `CREATE SCHEMA `+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	cfg := testPool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool for %s: %v", name, err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = testPool.Exec(context.Background(), `DROP SCHEMA `+name+` CASCADE`)
	})
	if err := applySchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return NewStore(pool), pool
}

// sequenceState reads the Change Sequence without advancing it.
func sequenceState(t *testing.T, pool *pgxpool.Pool) (lastValue int64, isCalled bool) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT last_value, is_called FROM change_sequence`).Scan(&lastValue, &isCalled); err != nil {
		t.Fatalf("read change_sequence: %v", err)
	}
	return
}

func TestRegisterChanges_StampsAcceptedRowsAboveAnEarlierStart(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "cq1", Id: id} }
	upserted, unversioned, deletedRes, inserted := r("upserted"), r("unversioned"), r("deleted"), r("inserted")
	seed(t, upserted, 1, 0, false)
	seed(t, unversioned, 3, 0, false)
	seed(t, deletedRes, 2, 0, false)

	fromNext := start(t, st)
	begun, err := st.BeginBuild(ctx, r("builder"), 0)
	if err != nil {
		t.Fatal(err)
	}
	got := register(t, st,
		core.Registration{Resource: upserted, Version: 2},
		core.Registration{Resource: unversioned, Version: 0},
		core.Registration{Resource: deletedRes, Deleted: true},
		core.Registration{Resource: inserted, Version: 7},
	)

	for i, res := range []model.Resource{upserted, unversioned, deletedRes, inserted} {
		if !got.Items[i].Accepted {
			t.Fatalf("%s must be accepted", res.Id)
		}
		seq := changeSeq(t, testPool, res)
		if seq <= fromNext || seq <= begun.Start {
			t.Errorf("%s: change_seq %d must exceed the earlier starts (NextChangeSeq %d, BeginBuild %d)", res.Id, seq, fromNext, begun.Start)
		}
	}

	// A second accepted change of the same row is stamped above the first.
	first := changeSeq(t, testPool, upserted)
	register(t, st, core.Registration{Resource: upserted, Version: 3})
	if again := changeSeq(t, testPool, upserted); again <= first {
		t.Errorf("re-registered row: change_seq %d, want above its previous %d", again, first)
	}
}

func TestChangeSeq_UntouchedByStaleRejectionMarksAndBeginBuild(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "cq2", Id: id} }
	item, parent, newParent, marked, built := r("item"), r("parent"), r("new-parent"), r("marked"), r("built")

	register(t, st,
		core.Registration{Resource: item, Version: 5},
		core.Registration{Resource: parent, Version: 1},
		core.Registration{Resource: marked, Version: 1},
		core.Registration{Resource: built, Version: 1},
	)
	relate(t, [2]model.Resource{parent, item}, [2]model.Resource{newParent, item})
	itemSeq := changeSeq(t, testPool, item)
	parentSeq := changeSeq(t, testPool, parent)
	markedSeq := changeSeq(t, testPool, marked)
	builtSeq := changeSeq(t, testPool, built)

	// A stale notification is rejected and stamps nothing.
	if got := register(t, st, core.Registration{Resource: item, Version: 4}); got.Items[0].Accepted {
		t.Fatal("a lower version must be rejected as stale")
	}
	if seq := changeSeq(t, testPool, item); seq != itemSeq {
		t.Errorf("stale rejection moved change_seq: %d -> %d", itemSeq, seq)
	}

	// A Parent marked by fanout keeps its change_seq; one without a row
	// gets a row that reads as never changed.
	if got := register(t, st, core.Registration{Resource: item, Version: 6}); len(got.Parents) != 2 {
		t.Fatalf("both Parents must be marked, got %+v", got.Parents)
	}
	if seq := changeSeq(t, testPool, parent); seq != parentSeq {
		t.Errorf("parent mark moved change_seq: %d -> %d", parentSeq, seq)
	}
	if seq := changeSeq(t, testPool, newParent); seq != 0 {
		t.Errorf("a row created by a parent mark must read as never changed, change_seq %d", seq)
	}

	// MarkStale, on an existing row and on a new one.
	fresh := r("fresh-mark")
	if _, err := st.MarkStale(ctx, []model.Resource{marked, fresh}, 0); err != nil {
		t.Fatal(err)
	}
	if seq := changeSeq(t, testPool, marked); seq != markedSeq {
		t.Errorf("MarkStale moved change_seq: %d -> %d", markedSeq, seq)
	}
	if seq := changeSeq(t, testPool, fresh); seq != 0 {
		t.Errorf("a row created by MarkStale must read as never changed, change_seq %d", seq)
	}

	// BeginBuild, on an existing row and on a new one.
	freshBuild := r("fresh-build")
	for _, res := range []model.Resource{built, freshBuild} {
		if _, err := st.BeginBuild(ctx, res, 0); err != nil {
			t.Fatal(err)
		}
	}
	if seq := changeSeq(t, testPool, built); seq != builtSeq {
		t.Errorf("BeginBuild moved change_seq: %d -> %d", builtSeq, seq)
	}
	if seq := changeSeq(t, testPool, freshBuild); seq != 0 {
		t.Errorf("a row created by BeginBuild must read as never changed, change_seq %d", seq)
	}
}

func TestAnyChangedSince_ComparesEachCheckWithItsOwnStart(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "cq3", Id: id} }
	before, after, missing := r("before"), r("after"), r("missing")
	check := func(res model.Resource, s int64) core.ChangeCheck { return core.ChangeCheck{Resource: res, Start: s} }

	s0 := start(t, st)
	register(t, st, core.Registration{Resource: before, Version: 1})
	s1 := start(t, st)
	register(t, st, core.Registration{Resource: after, Version: 1})
	s2 := start(t, st)

	if changedSince(t, st, check(before, s1)) {
		t.Error("a change accepted before the start must not count")
	}
	if !changedSince(t, st, check(after, s1)) {
		t.Error("a change accepted after the start must count")
	}
	if changedSince(t, st, check(missing, s0)) {
		t.Error("a resource without a row has never changed")
	}

	// One batched call: each check against its own start.
	if changedSince(t, st, check(before, s1), check(after, s2), check(missing, s0)) {
		t.Error("each resource changed before its own start: no drift")
	}
	if !changedSince(t, st, check(before, s1), check(after, s1)) {
		t.Error("after changed after its check's start: drift")
	}
	if !changedSince(t, st, check(before, s0), check(after, s2)) {
		t.Error("before changed after its check's start: drift")
	}
}

func TestAnyChangedSince_EmptyInputIsFalseWithoutAQuery(t *testing.T) {
	// A nil pool panics on any query.
	got, err := NewStore(nil).AnyChangedSince(context.Background(), nil)
	if err != nil || got {
		t.Fatalf("empty checks: got %v, %v; want false, nil", got, err)
	}
}

func TestBeginBuild_TakesItsStartFromTheChangeSequence(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res, between := model.Resource{Type: "cq4", Id: "built"}, model.Resource{Type: "cq4", Id: "between"}

	first, err := st.BeginBuild(ctx, res, 0)
	if err != nil {
		t.Fatal(err)
	}
	register(t, st, core.Registration{Resource: between, Version: 1})
	second, err := st.BeginBuild(ctx, res, 0)
	if err != nil {
		t.Fatal(err)
	}

	stamp := changeSeq(t, testPool, between)
	if first.Start >= stamp || stamp >= second.Start {
		t.Fatalf("starts and the registration between them must be ordered: %d < %d < %d", first.Start, stamp, second.Start)
	}
	if next := start(t, st); next <= second.Start {
		t.Fatalf("NextChangeSeq %d must come after BeginBuild's start %d", next, second.Start)
	}
}

// The Build Sequence is drawn from the Change Sequence: one value per call
// (the package's tests run one at a time, so nothing else draws between the
// test's own draws), returned as both BuildIdx and Start, on a new row as on
// an existing one.
func TestBeginBuild_DrawsItsBuildIdxFromTheChangeSequence(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "cq7", Id: "built"}

	for _, why := range []string{"a new row", "an existing row"} {
		before := start(t, st)
		begun, err := st.BeginBuild(ctx, res, 0)
		if err != nil {
			t.Fatal(err)
		}
		after := start(t, st)
		if begun.BuildIdx != before+1 || after != before+2 || begun.Start != begun.BuildIdx {
			t.Fatalf("%s: BuildIdx %d Start %d, want the one value drawn between %d and %d, as both", why, begun.BuildIdx, begun.Start, before, after)
		}
		if _, idx, _, _, _ := row(t, res); idx != begun.BuildIdx {
			t.Fatalf("%s: build_idx %d, want the returned BuildIdx %d", why, idx, begun.BuildIdx)
		}
	}
}

// A writer that waits for an existing row's lock draws its number after the
// wait, so of two writers of one row the one that locks it later carries
// the higher number: a later build pairs the later stale_seq with the
// version that wins in Elasticsearch. A gate holds the row while the call
// queues behind it, the test draws a value, and the call's number must be
// above that value.
func TestDraws_AnExistingRowsNumberIsDrawnAfterItsLock(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "dl9", Id: id} }
	staleSeqOf := func(res model.Resource) int64 { _, _, seq, _, _ := row(t, res); return seq }

	for _, c := range []struct {
		name string
		res  model.Resource // the row the gate holds
		call func() (int64, error)
	}{
		{"BeginBuild", r("build"), func() (int64, error) {
			b, err := st.BeginBuild(ctx, r("build"), 0)
			return b.BuildIdx, err
		}},
		{"MarkStale lease 0", r("mark0"), func() (int64, error) {
			_, err := st.MarkStale(ctx, []model.Resource{r("mark0")}, 0)
			return staleSeqOf(r("mark0")), err
		}},
		{"MarkStale with a lease", r("mark"), func() (int64, error) {
			_, err := st.MarkStale(ctx, []model.Resource{r("mark")}, time.Minute)
			return staleSeqOf(r("mark")), err
		}},
		{"RegisterChanges item", r("item"), func() (int64, error) {
			got, err := st.RegisterChanges(ctx, []core.Registration{{Resource: r("item")}}, time.Minute)
			if err != nil {
				return 0, err
			}
			return got.Items[0].StaleSeq, nil
		}},
		{"RegisterChanges Parent", r("parent"), func() (int64, error) {
			_, err := st.RegisterChanges(ctx, []core.Registration{{Resource: r("child")}}, time.Minute)
			return staleSeqOf(r("parent")), err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			seed(t, c.res, 0, 0, false)
			if c.res == r("parent") {
				relate(t, [2]model.Resource{c.res, r("child")})
			}
			g := lockRow(t, testPool, c.res)
			type result struct {
				n   int64
				err error
			}
			done := make(chan result, 1)
			go func() {
				n, err := c.call()
				done <- result{n, err}
			}()
			g.waitForWaiters(t, 1)
			drawn := start(t, st)
			g.release()
			select {
			case got := <-done:
				if got.err != nil {
					t.Fatal(got.err)
				}
				if got.n <= drawn {
					t.Fatalf("number %d, want above %d drawn while the call waited for the row lock", got.n, drawn)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the call did not finish after the gate committed")
			}
		})
	}
}

// A BeginBuild that queues behind a concurrent creation of its row — a row
// its snapshot could not see, so no lock was taken before the draw — still
// ends above every number the creator left: it draws again under the lock
// when the value it drew is not above them.
func TestBeginBuild_ARowCreatedWhileItWaitsGetsAHigherBuildIdx(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dl10", Id: "created"}
	g := openGate(t, testPool,
		`INSERT INTO resources (type, id) VALUES ($1, $2) RETURNING pg_backend_pid()`, res.Type, res.Id)
	type result struct {
		b   core.BuildBegun
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := st.BeginBuild(ctx, res, 0)
		done <- result{b, err}
	}()
	g.waitForWaiters(t, 1)
	var creator int64
	if err := g.tx.QueryRow(ctx,
		`UPDATE resources SET build_idx = nextval('change_sequence') WHERE type=$1 AND id=$2 RETURNING build_idx`,
		res.Type, res.Id).Scan(&creator); err != nil {
		t.Fatalf("write in the gate: %v", err)
	}
	g.release()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.b.BuildIdx <= creator || got.b.Start != got.b.BuildIdx {
			t.Fatalf("BuildIdx %d Start %d, want above the creator's build_idx %d, as the Start too", got.b.BuildIdx, got.b.Start, creator)
		}
		if _, idx, _, _, _ := row(t, res); idx != got.b.BuildIdx {
			t.Fatalf("build_idx %d, want the returned BuildIdx %d", idx, got.b.BuildIdx)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BeginBuild did not finish after the gate committed")
	}
}

// BeginDelete's UPDATE that waited for a concurrent write of its tombstone
// re-evaluates against the committed row, drawing its value then: it lands
// above the number that write left. The gate locks the tombstone first;
// BeginDelete starts and queues behind it; only then does the gate draw a
// number into build_idx, keeping the row a tombstone, and commit. A
// BeginDelete that drew its value before it waited would hold a number
// below the gate's.
func TestBeginDelete_DrawsAboveAWriteItWaitedFor(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "bd4", Id: "1"}
	tombstone(t, st, res)
	g := lockRow(t, testPool, res)
	type result struct {
		d   core.DeleteBegun
		err error
	}
	done := make(chan result, 1)
	go func() {
		d, err := st.BeginDelete(ctx, res, 0)
		done <- result{d, err}
	}()
	g.waitForWaiters(t, 1)
	var written int64
	if err := g.tx.QueryRow(ctx,
		`UPDATE resources SET build_idx = nextval('change_sequence') WHERE type=$1 AND id=$2 AND deleted RETURNING build_idx`,
		res.Type, res.Id).Scan(&written); err != nil {
		t.Fatalf("write in the gate: %v", err)
	}
	g.release()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.d.Superseded || got.d.BuildIdx <= written {
			t.Fatalf("got %+v, want not superseded, a BuildIdx above %d the waited-for write left", got.d, written)
		}
		if _, idx, _, _, deleted := row(t, res); idx != got.d.BuildIdx || !deleted {
			t.Fatalf("build_idx %d deleted %v, want the returned BuildIdx %d on a tombstone", idx, deleted, got.d.BuildIdx)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BeginDelete did not finish after the gate committed")
	}
}

func TestSchema_RaisesAChangeSequenceLeftBehindTheStamps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart func(stamp int64) string
	}{
		// Back to the sequence's first value.
		{"restart", func(int64) string { return `ALTER SEQUENCE change_sequence RESTART` }},
		// Next value exactly the highest stamp: last_value equals it, but
		// is_called is false, so a start would not exceed it.
		{"restart at the stamp", func(stamp int64) string {
			return fmt.Sprintf(`ALTER SEQUENCE change_sequence RESTART WITH %d`, stamp)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, pool := isolatedStore(t)
			res := model.Resource{Type: "cq5", Id: "1"}
			// Starts first, so the stamp is above the sequence's first value
			// and the two restarts differ.
			start(t, st)
			start(t, st)
			register(t, st, core.Registration{Resource: res, Version: 1})
			stamp := changeSeq(t, pool, res)

			if _, err := pool.Exec(ctx, tc.restart(stamp)); err != nil {
				t.Fatal(err)
			}
			if err := applySchema(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if next := start(t, st); next <= stamp {
				t.Fatalf("after re-applying the schema the next start %d must exceed the stamp %d", next, stamp)
			}
		})
	}
}

// build_idx and stale_seq are drawn from the Change Sequence too, so the
// guard keeps it ahead of them as well: a sequence behind either would hand
// a recreated row numbers below those its old row carried. An edge set's
// stamp is a Build Sequence that can outlive its row — a late build writes
// it after a hard delete — so the guard keeps the sequence ahead of it too:
// behind it, a recreate's ReplaceEdges would be rejected for that version.
func TestSchema_RaisesAChangeSequenceLeftBehindABuildIdxStaleSeqOrEdgeSetStamp(t *testing.T) {
	ahead := map[string]string{
		"build_idx": `UPDATE resources SET build_idx = change_seq + 100 WHERE type=$1 AND id=$2 RETURNING build_idx`,
		"stale_seq": `UPDATE resources SET stale_seq = change_seq + 100 WHERE type=$1 AND id=$2 RETURNING stale_seq`,
		// Stamped above every number of the row, which is then hard-deleted.
		"edge_sets.build_seq": `WITH gone AS (DELETE FROM resources WHERE type=$1 AND id=$2 RETURNING change_seq)
		     INSERT INTO edge_sets (type, id, schema_version, build_seq)
		     SELECT $1, $2, 1, change_seq + 100 FROM gone RETURNING build_seq`,
	}
	for _, column := range []string{"build_idx", "stale_seq", "edge_sets.build_seq"} {
		for _, tc := range []struct {
			name    string
			restart bool
		}{
			// The number is ahead of the sequence, which is ahead of change_seq.
			{"behind", false},
			// Next value exactly the number: is_called is false, so a draw
			// would not exceed it.
			{"restart at the number", true},
		} {
			t.Run(column+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				st, pool := isolatedStore(t)
				res := model.Resource{Type: "cq8", Id: "1"}
				register(t, st, core.Registration{Resource: res, Version: 1})
				var number int64
				if err := pool.QueryRow(ctx, ahead[column], res.Type, res.Id).Scan(&number); err != nil {
					t.Fatal(err)
				}
				if tc.restart {
					if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER SEQUENCE change_sequence RESTART WITH %d`, number)); err != nil {
						t.Fatal(err)
					}
				}
				if err := applySchema(ctx, pool); err != nil {
					t.Fatal(err)
				}
				if next := start(t, st); next <= number {
					t.Fatalf("after re-applying the schema the next value %d must exceed the %s %d", next, column, number)
				}
			})
		}
	}
}

func TestSchema_LeavesTheChangeSequenceAloneWhenEmptyOrAhead(t *testing.T) {
	ctx := context.Background()
	st, pool := isolatedStore(t)
	reapply := func(why string) {
		t.Helper()
		lastBefore, calledBefore := sequenceState(t, pool)
		if err := applySchema(ctx, pool); err != nil {
			t.Fatal(err)
		}
		if last, called := sequenceState(t, pool); last != lastBefore || called != calledBefore {
			t.Errorf("%s: sequence moved from (%d, %v) to (%d, %v)", why, lastBefore, calledBefore, last, called)
		}
	}

	reapply("fresh sequence, empty table")

	// Rows that never changed: change_seq 0 is below anything handed out,
	// and a mark's stale_seq, when drawn from the sequence, is behind it.
	if _, err := st.MarkStale(ctx, []model.Resource{{Type: "cq6", Id: "marked"}}, 0); err != nil {
		t.Fatal(err)
	}
	reapply("only unchanged rows")

	// Starts taken with no stamped row: the sequence is ahead.
	start(t, st)
	start(t, st)
	reapply("sequence ahead, no stamps")

	// A stamped row with the sequence ahead of it.
	register(t, st, core.Registration{Resource: model.Resource{Type: "cq6", Id: "stamped"}, Version: 1})
	reapply("sequence last handed out the stamp")
	start(t, st)
	reapply("sequence ahead of the stamp")
}

// ---- Build ownership (L1.4) ----

// owner is a row's owner columns; seq 0 and a zero since mean no owner.
type owner struct {
	seq   int64
	since time.Time
}

func (o owner) is(p owner) bool { return o.seq == p.seq && o.since.Equal(p.since) }

func (o owner) String() string {
	if o.seq == 0 && o.since.IsZero() {
		return "no owner"
	}
	return fmt.Sprintf("owner_seq %d since %s", o.seq, o.since.Format(time.RFC3339Nano))
}

// ownerOf reads a row's owner columns.
func ownerOf(t *testing.T, pool *pgxpool.Pool, res model.Resource) owner {
	t.Helper()
	var seq *int64
	var since *time.Time
	if err := pool.QueryRow(context.Background(),
		`SELECT owner_seq, owner_since FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&seq, &since); err != nil {
		t.Fatalf("read owner %s/%s: %v", res.Type, res.Id, err)
	}
	var o owner
	if seq != nil {
		o.seq = *seq
	}
	if since != nil {
		o.since = *since
	}
	return o
}

// own makes the row's current stale_seq its owner token, claimed now, in SQL
// independent of the Store's claims, and returns the token.
func own(t *testing.T, pool *pgxpool.Pool, res model.Resource) int64 {
	t.Helper()
	var token int64
	if err := pool.QueryRow(context.Background(),
		`UPDATE resources SET owner_seq = stale_seq, owner_since = now() WHERE type=$1 AND id=$2 RETURNING owner_seq`,
		res.Type, res.Id).Scan(&token); err != nil {
		t.Fatalf("own %s/%s: %v", res.Type, res.Id, err)
	}
	return token
}

// expireOwner moves owner_since ten minutes into the past, beyond every lease
// the tests use, and returns it; owner_seq stays.
func expireOwner(t *testing.T, pool *pgxpool.Pool, res model.Resource) time.Time {
	t.Helper()
	return backdateOwner(t, pool, res, "10 minutes")
}

// backdateOwner moves owner_since age (a Postgres interval) into the past
// and returns it; owner_seq stays.
func backdateOwner(t *testing.T, pool *pgxpool.Pool, res model.Resource, age string) time.Time {
	t.Helper()
	var since time.Time
	if err := pool.QueryRow(context.Background(),
		`UPDATE resources SET owner_since = now() - $3::interval WHERE type=$1 AND id=$2 RETURNING owner_since`,
		res.Type, res.Id, age).Scan(&since); err != nil {
		t.Fatalf("expire owner %s/%s: %v", res.Type, res.Id, err)
	}
	return since
}

// requireFreshOwner fails unless token owns res with an owner_since set in
// the last few seconds by the database clock: a claim or renewal, later than
// any value expireOwner left.
func requireFreshOwner(t *testing.T, pool *pgxpool.Pool, res model.Resource, token int64) {
	t.Helper()
	var seq *int64
	var fresh *bool
	if err := pool.QueryRow(context.Background(),
		`SELECT owner_seq, owner_since > now() - interval '5 seconds' FROM resources WHERE type=$1 AND id=$2`,
		res.Type, res.Id).Scan(&seq, &fresh); err != nil {
		t.Fatalf("read owner %s/%s: %v", res.Type, res.Id, err)
	}
	if seq == nil || *seq != token || fresh == nil || !*fresh {
		o := ownerOf(t, pool, res)
		t.Fatalf("%s/%s: owner_seq %d since %v, want token %d claimed or renewed just now", res.Type, res.Id, o.seq, o.since, token)
	}
}

// requireOwner fails unless the row's owner columns are want.
func requireOwner(t *testing.T, pool *pgxpool.Pool, res model.Resource, want owner, why string) {
	t.Helper()
	if got := ownerOf(t, pool, res); !got.is(want) {
		t.Fatalf("%s/%s: %s: %v, want %v", res.Type, res.Id, why, got, want)
	}
}

// requireFollowUp compares follow-ups.
func requireFollowUp(t *testing.T, got, want core.FollowUp) {
	t.Helper()
	if got != want {
		t.Fatalf("follow-up: got %+v, want %+v", got, want)
	}
}

// markUnclaimed marks resources without claiming them (lease 0).
func markUnclaimed(t *testing.T, st *Store, res ...model.Resource) {
	t.Helper()
	if _, err := st.MarkStale(context.Background(), res, 0); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}
}

// gate is a transaction holding a row lock on a connection of its own, so
// any statement writing the row queues behind it until release commits. A
// test may write the row in tx; the write commits with release.
type gate struct {
	tx      pgx.Tx
	pid     int32 // the gate's backend
	release func()
}

// lockRow opens a gate on res's row. The test's cleanup releases it too.
func lockRow(t *testing.T, pool *pgxpool.Pool, res model.Resource) *gate {
	t.Helper()
	return openGate(t, pool, `SELECT pg_backend_pid() FROM resources WHERE type=$1 AND id=$2 FOR UPDATE`, res.Type, res.Id)
}

// openGate opens a gate whose lock query selects pg_backend_pid() from the
// one row it locks FOR UPDATE. The test's cleanup releases it too.
func openGate(t *testing.T, pool *pgxpool.Pool, lock string, args ...any) *gate {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin gate: %v", err)
	}
	g := &gate{tx: tx}
	if err := tx.QueryRow(ctx, lock, args...).Scan(&g.pid); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("gate lock %v: %v", args, err)
	}
	var once sync.Once
	g.release = func() {
		once.Do(func() {
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("commit gate: %v", err)
			}
		})
	}
	t.Cleanup(g.release)
	return g
}

// waitForWaiters polls until n backends wait behind the gate: blocked by
// it, or by another backend that waits behind it, as the second of two
// statements queued on one row waits for the first one's tuple lock. A
// backend waiting on anything else does not count.
func (g *gate) waitForWaiters(t *testing.T, n int) {
	t.Helper()
	waitBehindGates(t, n, g)
}

// waitBehindGates is waitForWaiters for several gates: it polls until n
// backends wait behind any of them, each counted once.
func waitBehindGates(t *testing.T, n int, gates ...*gate) {
	t.Helper()
	pids := make([]int32, len(gates))
	for i, g := range gates {
		pids[i] = g.pid
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for {
		var got int
		// The deadline bounds the wait for a pool connection too.
		if err := testPool.QueryRow(ctx,
			`WITH RECURSIVE w(pid) AS (
			     SELECT pid FROM pg_stat_activity WHERE pg_blocking_pids(pid) && $1::int[]
			     UNION
			     SELECT a.pid FROM pg_stat_activity a JOIN w ON w.pid = ANY(pg_blocking_pids(a.pid))
			 )
			 SELECT count(*) FROM w`, pids).Scan(&got); err != nil {
			t.Fatalf("read the gates' waiters: %v", err)
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends waiting behind the gates after 10s, want %d", got, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// raceOnRow forces two calls of mark to contend for res's row. The
// interleaving: a gate transaction locks the row; both calls start and
// queue behind it (seen in pg_stat_activity, not assumed from timing);
// the gate commits; Postgres grants the lock to one call, which updates the
// row and commits, and the other then updates the row the first committed.
// It returns the token each call (0 or 1) reports.
func raceOnRow(t *testing.T, res model.Resource, mark func(i int) (int64, error)) [2]int64 {
	t.Helper()
	g := lockRow(t, testPool, res)
	type result struct {
		i     int
		token int64
		err   error
	}
	results := make(chan result, 2)
	for i := range 2 {
		go func() {
			token, err := mark(i)
			results <- result{i, token, err}
		}()
	}
	g.waitForWaiters(t, 2)
	g.release()
	var tokens [2]int64
	for range 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("mark %d: %v", r.i, r.err)
			}
			tokens[r.i] = r.token
		case <-time.After(10 * time.Second):
			t.Fatal("a racing mark did not finish after the gate committed")
		}
	}
	return tokens
}

// requireOneClaim fails unless exactly one token is non-zero, it is the
// row's owner token, and the row was marked by both calls: the claim is the
// first to lock the row, and a mark draws its value after it locks an
// existing row, so the other call's mark leaves a stale_seq above the
// winner's token.
func requireOneClaim(t *testing.T, res model.Resource, tokens [2]int64) {
	t.Helper()
	winner, claims := int64(0), 0
	for _, tok := range tokens {
		if tok != 0 {
			winner, claims = tok, claims+1
		}
	}
	if claims != 1 {
		t.Fatalf("tokens %v: exactly one of two concurrent marks must claim", tokens)
	}
	if o := ownerOf(t, testPool, res); o.seq != winner {
		t.Fatalf("owner_seq %d, want the winner's token %d", o.seq, winner)
	}
	if _, _, seq, _, _ := row(t, res); seq <= winner {
		t.Fatalf("stale_seq %d, want above the winner's token %d: both marks must move it, the later one higher", seq, winner)
	}
}

func TestClaim_ExactlyOneOfTwoConcurrentMarks(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)

	t.Run("MarkStale", func(t *testing.T) {
		res := model.Resource{Type: "cc-ms", Id: "1"}
		seed(t, res, 0, 0, false)
		tokens := raceOnRow(t, res, func(int) (int64, error) {
			owned, err := st.MarkStale(ctx, []model.Resource{res}, time.Minute)
			if err != nil || len(owned) == 0 {
				return 0, err
			}
			if len(owned) != 1 || owned[0].Resource != res {
				return 0, fmt.Errorf("owned %+v, want at most %v", owned, res)
			}
			return owned[0].Token, nil
		})
		requireOneClaim(t, res, tokens)
	})

	t.Run("RegisterChanges item", func(t *testing.T) {
		res := model.Resource{Type: "cc-ri", Id: "1"}
		seed(t, res, 0, 0, false)
		tokens := raceOnRow(t, res, func(int) (int64, error) {
			got, err := st.RegisterChanges(ctx, []core.Registration{{Resource: res, Version: 0}}, time.Minute)
			if err != nil {
				return 0, err
			}
			if it := got.Items[0]; !it.Accepted || (it.Token != 0 && it.Token != it.StaleSeq) {
				return 0, fmt.Errorf("item %+v: want accepted, with Token 0 or its StaleSeq", it)
			}
			return got.Items[0].Token, nil
		})
		requireOneClaim(t, res, tokens)
	})

	t.Run("RegisterChanges Parent", func(t *testing.T) {
		parent := model.Resource{Type: "cc-rp", Id: "parent"}
		children := [2]model.Resource{{Type: "cc-rp", Id: "c1"}, {Type: "cc-rp", Id: "c2"}}
		seed(t, parent, 0, 0, false)
		relate(t, [2]model.Resource{parent, children[0]}, [2]model.Resource{parent, children[1]})
		// The children's rows are their own; only the Parent's row is contended.
		tokens := raceOnRow(t, parent, func(i int) (int64, error) {
			got, err := st.RegisterChanges(ctx, []core.Registration{{Resource: children[i], Version: 1}}, time.Minute)
			if err != nil {
				return 0, err
			}
			if len(got.Parents) != 1 || got.Parents[0].Resource != parent {
				return 0, fmt.Errorf("Parents %+v, want the Parent alone", got.Parents)
			}
			return got.Parents[0].Token, nil
		})
		requireOneClaim(t, parent, tokens)
	})
}

var deadlockRuns atomic.Int64

// Two single-item registrations of a and b, each the other's Parent, take
// the same two rows: an item and then its Parent. Gates hold both rows until
// both registrations queue behind them, so both reach the rows at once. A
// registration that locked its item before its Parent would then hold one
// row and wait for the other, and Postgres would abort one with 40P01; in
// (type, id) order both queue on a's row and run one after the other.
func TestRegisterChanges_ItemsThatAreEachOthersParentsDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	id := fmt.Sprint(deadlockRuns.Add(1)) // fresh rows on every -count run
	a, b := model.Resource{Type: "dl-a", Id: id}, model.Resource{Type: "dl-b", Id: id}
	seed(t, a, 0, 0, false)
	seed(t, b, 0, 0, false)
	relate(t, [2]model.Resource{a, b}, [2]model.Resource{b, a})

	gates := []*gate{lockRow(t, testPool, a), lockRow(t, testPool, b)}
	type result struct {
		item model.Resource
		got  core.Registered
		err  error
	}
	results := make(chan result, 2)
	for _, item := range []model.Resource{a, b} {
		go func() {
			got, err := st.RegisterChanges(ctx, []core.Registration{{Resource: item, Version: 1}}, time.Minute)
			results <- result{item, got, err}
		}()
	}
	waitBehindGates(t, 2, gates...)
	for _, g := range gates {
		g.release()
	}

	itemSeq := map[model.Resource]int64{}     // each row's mark as an item
	itemToken := map[model.Resource]int64{}   // each row's claim as an item, 0 if none
	parentToken := map[model.Resource]int64{} // each row's claim as a Parent, 0 if none
	for range 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("register %v: %v", r.item, r.err)
			}
			parent := a
			if r.item == a {
				parent = b
			}
			if !r.got.Items[0].Accepted {
				t.Fatalf("register %v: item %+v, want it accepted", r.item, r.got.Items[0])
			}
			if len(r.got.Parents) != 1 || r.got.Parents[0].Resource != parent {
				t.Fatalf("register %v: Parents %+v, want %v alone", r.item, r.got.Parents, parent)
			}
			itemSeq[r.item] = r.got.Items[0].StaleSeq
			itemToken[r.item] = r.got.Items[0].Token
			parentToken[parent] = r.got.Parents[0].Token
		case <-time.After(10 * time.Second):
			t.Fatal("a registration did not finish after the gates committed")
		}
	}
	// Each row is marked twice, as item and as Parent. The registrations run
	// one after the other: the first claims both its rows, the second, under
	// those live leases, claims neither and writes both rows last. So the
	// first one's item row holds the second one's Parent mark, not its item
	// mark; the second one's item row holds that item mark, not the first
	// one's Parent claim.
	for _, res := range []model.Resource{a, b} {
		version, _, seq, since, _ := row(t, res)
		markedTwice := seq > itemSeq[res] // the first registration's item, marked again as Parent
		if itemToken[res] == 0 {          // the second registration's item, claimed before as Parent
			markedTwice = seq == itemSeq[res] && parentToken[res] != 0 && parentToken[res] < seq
		}
		if version != 1 || since == nil || !markedTwice {
			t.Fatalf("%v: version %d stale_seq %d stale_since %v (item mark %d, item claim %d, Parent claim %d), want version 1, marked twice (as item and as Parent)", res, version, seq, since, itemSeq[res], itemToken[res], parentToken[res])
		}
	}
}

// A row no statement saw at its start can't be locked in order, so two
// registrations can still deadlock over new rows: x and y, each the other's
// Parent, without resources rows. A gate holds an uncommitted insert of z.
// The registration of y and z inserts y and queues behind the gate on z; the
// registration of x inserts x and queues behind y's insert to mark y. Once
// the gate commits, the first marks x, which the second holds, and Postgres
// aborts one of them: that one returns core.ErrRegistrationAborted.
func TestRegisterChanges_ADeadlockOverNewRowsReturnsErrRegistrationAborted(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	id := fmt.Sprint(deadlockRuns.Add(1))
	x, y, z := model.Resource{Type: "dn-x", Id: id}, model.Resource{Type: "dn-y", Id: id}, model.Resource{Type: "dn-z", Id: id}
	relate(t, [2]model.Resource{x, y}, [2]model.Resource{y, x})

	g := openGate(t, testPool, `INSERT INTO resources (type, id) VALUES ($1, $2) RETURNING pg_backend_pid()`, z.Type, z.Id)
	errs := make(chan error, 2)
	register := func(items ...model.Resource) {
		regs := make([]core.Registration, len(items))
		for i, res := range items {
			regs[i] = core.Registration{Resource: res, Version: 1}
		}
		go func() {
			_, err := st.RegisterChanges(ctx, regs, time.Minute)
			errs <- err
		}()
	}
	register(y, z)
	g.waitForWaiters(t, 1)
	register(x)
	g.waitForWaiters(t, 2)
	g.release()

	var aborted []error
	for range 2 {
		select {
		case err := <-errs:
			if err != nil {
				aborted = append(aborted, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a registration did not finish after the gate committed")
		}
	}
	if len(aborted) != 1 {
		t.Fatalf("errors %v: want exactly one registration aborted", aborted)
	}
	if !errors.Is(aborted[0], core.ErrRegistrationAborted) {
		t.Fatalf("error %v: want it to wrap core.ErrRegistrationAborted", aborted[0])
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](aborted[0]); !ok || pgErr.Code != "40P01" {
		t.Fatalf("error %v: want the Postgres deadlock error kept", aborted[0])
	}
}

func TestClaim_LiveLeaseBlocksAndExpiredLeaseYields(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)

	t.Run("MarkStale", func(t *testing.T) {
		res, other := model.Resource{Type: "cl-ms", Id: "1"}, model.Resource{Type: "cl-ms", Id: "other"}
		owned, err := st.MarkStale(ctx, []model.Resource{res}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, _, first, _, _ := row(t, res)
		if len(owned) != 1 || owned[0] != (core.Owned{Resource: res, Token: first}) || first <= 0 {
			t.Fatalf("first mark of an unowned row: owned %+v, want it claimed with its stale_seq %d as the token", owned, first)
		}
		claimed := ownerOf(t, testPool, res)

		// A batch with the live-owned row and an unowned one claims only the latter.
		owned, err = st.MarkStale(ctx, []model.Resource{res, other}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, otherSeq, _, _ := row(t, other); len(owned) != 1 || owned[0] != (core.Owned{Resource: other, Token: otherSeq}) {
			t.Fatalf("owned %+v, want only the unowned row, token its stale_seq %d", owned, otherSeq)
		}
		requireOwner(t, testPool, res, claimed, "a mark under a live lease must leave the owner")
		_, _, second, since, _ := row(t, res)
		if second <= first || since == nil {
			t.Fatalf("a mark under a live lease still marks: seq %d (want above %d) since %v", second, first, since)
		}

		expireOwner(t, testPool, res)
		owned, err = st.MarkStale(ctx, []model.Resource{res}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, _, third, _, _ := row(t, res)
		if len(owned) != 1 || owned[0] != (core.Owned{Resource: res, Token: third}) || third <= second {
			t.Fatalf("mark after the lease expired: owned %+v, want it claimed with the new stale_seq %d (above %d)", owned, third, second)
		}
		requireFreshOwner(t, testPool, res, third)
	})

	t.Run("RegisterChanges item", func(t *testing.T) {
		res := model.Resource{Type: "cl-ri", Id: "1"}
		first := register(t, st, core.Registration{Resource: res, Metadata: meta("1")}).Items[0]
		if !first.Accepted || first.StaleSeq <= 0 || first.Token != first.StaleSeq {
			t.Fatalf("first registration: %+v, want claimed with its StaleSeq as the token", first)
		}
		claimed := ownerOf(t, testPool, res)

		second := register(t, st, core.Registration{Resource: res, Metadata: meta("2")}).Items[0]
		if !second.Accepted || second.StaleSeq <= first.StaleSeq || second.Token != 0 {
			t.Fatalf("registration under a live lease: %+v, want accepted, seq above %d, no token", second, first.StaleSeq)
		}
		requireOwner(t, testPool, res, claimed, "a mark under a live lease must leave the owner")
		if m := metadataOf(t, res); m["k"] != "2" {
			t.Fatalf("a mark under a live lease still stores its metadata: %v", m)
		}

		expireOwner(t, testPool, res)
		third := register(t, st, core.Registration{Resource: res}).Items[0]
		if !third.Accepted || third.StaleSeq <= second.StaleSeq || third.Token != third.StaleSeq {
			t.Fatalf("registration after the lease expired: %+v, want claimed with its StaleSeq (above %d) as the token", third, second.StaleSeq)
		}
		requireFreshOwner(t, testPool, res, third.Token)
	})

	t.Run("RegisterChanges Parent", func(t *testing.T) {
		parent, child := model.Resource{Type: "cl-rp", Id: "parent"}, model.Resource{Type: "cl-rp", Id: "child"}
		seed(t, parent, 0, 0, false)
		relate(t, [2]model.Resource{parent, child})
		parentOf := func(got core.Registered) core.MarkedParent {
			t.Helper()
			if len(got.Parents) != 1 || got.Parents[0].Resource != parent {
				t.Fatalf("Parents %+v, want the Parent alone", got.Parents)
			}
			return got.Parents[0]
		}

		first := parentOf(register(t, st, core.Registration{Resource: child, Metadata: meta("1")})).Token
		if _, _, seq, _, _ := row(t, parent); first <= 0 || first != seq {
			t.Fatalf("first mark of the unowned Parent: token %d, want its new stale_seq %d", first, seq)
		}
		claimed := ownerOf(t, testPool, parent)

		if p := parentOf(register(t, st, core.Registration{Resource: child, Metadata: meta("2")})); p.Token != 0 {
			t.Fatalf("Parent under a live lease: token %d, want 0", p.Token)
		}
		requireOwner(t, testPool, parent, claimed, "a mark under a live lease must leave the owner")
		_, _, second, _, _ := row(t, parent)
		if second <= first {
			t.Fatalf("a Parent mark under a live lease still marks: seq %d (want above %d)", second, first)
		}

		expireOwner(t, testPool, parent)
		third := parentOf(register(t, st, core.Registration{Resource: child})).Token
		if _, _, seq, _, _ := row(t, parent); third <= second || third != seq {
			t.Fatalf("Parent after the lease expired: token %d, want the new stale_seq %d (above %d)", third, seq, second)
		}
		requireFreshOwner(t, testPool, parent, third)
	})
}

func TestMarkStale_LeaseZeroClaimsNothing(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	unowned, owned := model.Resource{Type: "mz", Id: "unowned"}, model.Resource{Type: "mz", Id: "owned"}
	markUnclaimed(t, st, owned)
	own(t, testPool, owned)
	before := ownerOf(t, testPool, owned)

	got, err := st.MarkStale(ctx, []model.Resource{unowned, owned}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("lease 0 must claim nothing, got %+v", got)
	}
	requireOwner(t, testPool, unowned, owner{}, "lease 0 must not claim an unowned row")
	requireOwner(t, testPool, owned, before, "lease 0 must leave an owner alone")
	for _, res := range []model.Resource{unowned, owned} {
		if _, _, _, since, _ := row(t, res); since == nil {
			t.Fatalf("%s: lease 0 still marks: since %v", res.Id, since)
		}
	}
}

func TestFinishOwned_UnchangedSeqClearsMarkAndOwnership(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	mine, theirs := model.Resource{Type: "fo1", Id: "mine"}, model.Resource{Type: "fo1", Id: "theirs"}
	markUnclaimed(t, st, mine, theirs)
	tok := own(t, testPool, mine)
	theirTok := own(t, testPool, theirs)

	// An unchanged stale_seq clears whatever the token: the mark is served.
	for _, c := range []struct {
		res   model.Resource
		token int64
	}{{mine, tok}, {theirs, theirTok + 1}} {
		_, _, marked, _, _ := row(t, c.res)
		f, err := st.FinishOwned(ctx, c.res, marked, c.token)
		if err != nil {
			t.Fatal(err)
		}
		requireFollowUp(t, f, core.FollowUp{})
		if _, _, seq, since, _ := row(t, c.res); seq != marked || since != nil {
			t.Fatalf("%s: unchanged seq must clear the mark: seq %d (was %d) since %v", c.res.Id, seq, marked, since)
		}
		requireOwner(t, testPool, c.res, owner{}, "an unchanged seq must clear the ownership")
	}
}

func TestFinishOwned_MovedSeqReclaimsForAFollowUp(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)

	t.Run("build", func(t *testing.T) {
		res := model.Resource{Type: "fo2", Id: "built"}
		markUnclaimed(t, st, res)
		tok := own(t, testPool, res)
		// Two changes during the build; the owner holds, so neither claims.
		markUnclaimed(t, st, res)
		markUnclaimed(t, st, res)
		// The lease ran out meanwhile: matching ignores lease liveness.
		expireOwner(t, testPool, res)

		_, _, moved, _, _ := row(t, res)
		if moved <= tok {
			t.Fatalf("the changes must move stale_seq above the token %d, got %d", tok, moved)
		}

		f, err := st.FinishOwned(ctx, res, tok, tok)
		if err != nil {
			t.Fatal(err)
		}
		requireFollowUp(t, f, core.FollowUp{Token: moved})
		requireFreshOwner(t, testPool, res, moved)
		if _, _, seq, since, _ := row(t, res); seq != moved || since == nil {
			t.Fatalf("a re-claim keeps the mark: seq %d (want %d) since %v", seq, moved, since)
		}
	})

	t.Run("tombstone", func(t *testing.T) {
		res := model.Resource{Type: "fo2", Id: "tomb"}
		seq := register(t, st, core.Registration{Resource: res, Deleted: true, Metadata: meta("del")}).Items[0].StaleSeq
		tok := own(t, testPool, res)
		markUnclaimed(t, st, res) // a mark that keeps the tombstone
		_, _, moved, _, _ := row(t, res)
		if moved <= seq {
			t.Fatalf("the mark must move stale_seq above %d, got %d", seq, moved)
		}

		f, err := st.FinishOwned(ctx, res, seq, tok)
		if err != nil {
			t.Fatal(err)
		}
		requireFollowUp(t, f, core.FollowUp{Token: moved, Deleted: true})
		requireFreshOwner(t, testPool, res, moved)
	})
}

func TestFinishOwned_LostTokenChangesNothing(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "fo3", Id: "1"}
	markUnclaimed(t, st, res)
	tok := own(t, testPool, res)
	markUnclaimed(t, st, res) // seq moved
	want := ownerOf(t, testPool, res)
	_, _, moved, _, _ := row(t, res)

	for _, token := range []int64{tok + 1, 0} {
		f, err := st.FinishOwned(ctx, res, tok, token)
		if err != nil {
			t.Fatal(err)
		}
		requireFollowUp(t, f, core.FollowUp{})
		requireOwner(t, testPool, res, want, fmt.Sprintf("token %d does not match", token))
		if _, _, seq, since, _ := row(t, res); seq != moved || since == nil {
			t.Fatalf("token %d does not match: the mark must stay, seq %d (want %d) since %v", token, seq, moved, since)
		}
	}
}

func TestDeleteResourceIfSeq_RecreateDuringDeleteIsReclaimed(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dr2", Id: "1"}
	seq := register(t, st, core.Registration{Resource: res, Deleted: true, Metadata: meta("del")}).Items[0].StaleSeq
	tok := own(t, testPool, res)

	// The recreate lands while the delete is in flight: the owner is live,
	// so it claims nothing and rides on the delete's follow-up.
	re := register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("re")}).Items[0]
	if !re.Accepted || re.StaleSeq <= seq || re.Token != 0 {
		t.Fatalf("recreate under the delete's live lease: %+v, want accepted, seq above %d, no token", re, seq)
	}

	f, err := st.DeleteResourceIfSeq(ctx, res, seq, tok)
	if err != nil {
		t.Fatal(err)
	}
	requireFollowUp(t, f, core.FollowUp{Token: re.StaleSeq})
	if _, _, staleSeq, since, deleted := row(t, res); deleted || staleSeq != re.StaleSeq || since == nil {
		t.Fatalf("the recreated row must stay marked: deleted %v seq %d (want %d) since %v", deleted, staleSeq, re.StaleSeq, since)
	}
	// The follow-up's build reads the recreate's metadata at BeginBuild.
	requireMetadata(t, "the recreated row's metadata", metadataOf(t, res), meta("re"))
	requireFreshOwner(t, testPool, res, re.StaleSeq)
}

func TestDeleteResourceIfSeq_LostTokenChangesNothing(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dr3", Id: "1"}
	seq := register(t, st, core.Registration{Resource: res, Deleted: true}).Items[0].StaleSeq
	tok := own(t, testPool, res)
	recreated := register(t, st, core.Registration{Resource: res, Version: 1, Metadata: meta("re")}).Items[0].StaleSeq
	want := ownerOf(t, testPool, res)

	f, err := st.DeleteResourceIfSeq(ctx, res, seq, tok+1)
	if err != nil {
		t.Fatal(err)
	}
	requireFollowUp(t, f, core.FollowUp{})
	requireOwner(t, testPool, res, want, "a token that does not match")
	if _, _, staleSeq, since, deleted := row(t, res); deleted || staleSeq != recreated || since == nil {
		t.Fatalf("the recreated row must stay as it was: deleted %v seq %d (want %d) since %v", deleted, staleSeq, recreated, since)
	}
}

func TestBeginBuild_RenewsOnlyAMatchingToken(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "bb2", Id: "1"}
	markUnclaimed(t, st, res)
	tok := own(t, testPool, res)
	expired := owner{seq: tok, since: expireOwner(t, testPool, res)}

	var prevIdx int64
	for _, token := range []int64{tok + 1, 0, tok} {
		begun, err := st.BeginBuild(ctx, res, token)
		if err != nil {
			t.Fatal(err)
		}
		if begun.BuildIdx <= prevIdx || begun.StaleSeq != tok {
			t.Fatalf("token %d: BuildIdx %d StaleSeq %d, want a BuildIdx above %d and StaleSeq %d", token, begun.BuildIdx, begun.StaleSeq, prevIdx, tok)
		}
		prevIdx = begun.BuildIdx
		if token != tok {
			requireOwner(t, testPool, res, expired, fmt.Sprintf("token %d does not match", token))
		}
	}
	requireFreshOwner(t, testPool, res, tok)
}

func TestRenewOwners_RenewsOnlyMatchingTokens(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "ro", Id: id} }
	match, mismatch, omitted, unowned, zero := r("match"), r("mismatch"), r("omitted"), r("unowned"), r("zero")
	markUnclaimed(t, st, match, mismatch, omitted, unowned, zero)
	tokens := map[model.Resource]int64{}
	expired := map[model.Resource]owner{}
	for _, res := range []model.Resource{match, mismatch, omitted, zero} {
		tokens[res] = own(t, testPool, res)
		expired[res] = owner{seq: tokens[res], since: expireOwner(t, testPool, res)}
	}

	held, err := st.RenewOwners(ctx, []core.Owned{
		{Resource: match, Token: tokens[match]},
		{Resource: mismatch, Token: tokens[mismatch] + 1},
		{Resource: unowned, Token: 1},
		{Resource: r("absent"), Token: 1},
		{Resource: zero, Token: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []core.Owned{{Resource: match, Token: tokens[match]}}; !reflect.DeepEqual(held, want) {
		t.Fatalf("RenewOwners must return exactly the ownerships still held: got %v want %v", held, want)
	}
	requireFreshOwner(t, testPool, match, tokens[match])
	requireOwner(t, testPool, mismatch, expired[mismatch], "a token that does not match")
	requireOwner(t, testPool, omitted, expired[omitted], "a row not given")
	requireOwner(t, testPool, zero, expired[zero], "a token of 0")
	requireOwner(t, testPool, unowned, owner{}, "no token matches an unowned row")

	if held, err := st.RenewOwners(ctx, nil); err != nil || len(held) != 0 {
		t.Fatalf("empty input must be a no-op: %v %v", held, err)
	}
}

// Several ownerships still held are all returned, each renewed; a renewal
// after another owner claimed the row, or after a token-less ClearStale
// dropped it, holds nothing.
func TestRenewOwners_ReturnsEveryHeldOwnership_AndNoneOnceLost(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rh", Id: id} }
	a, b, taken, cleared := r("a"), r("b"), r("taken"), r("cleared")
	markUnclaimed(t, st, a, b, taken, cleared)
	tokens := map[model.Resource]int64{}
	for _, res := range []model.Resource{a, b, taken, cleared} {
		tokens[res] = own(t, testPool, res)
		expireOwner(t, testPool, res)
	}
	// taken: another owner claims the row under a newer mark.
	markUnclaimed(t, st, taken)
	newTok := own(t, testPool, taken)
	if newTok == tokens[taken] {
		t.Fatalf("the new claim must have a new token, got %d twice", newTok)
	}
	// cleared: a build that owns nothing clears the mark and its ownership.
	var seq int64
	if err := testPool.QueryRow(ctx, `SELECT stale_seq FROM resources WHERE type=$1 AND id=$2`, cleared.Type, cleared.Id).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if err := st.ClearStale(ctx, cleared, seq); err != nil {
		t.Fatal(err)
	}

	held, err := st.RenewOwners(ctx, []core.Owned{
		{Resource: a, Token: tokens[a]},
		{Resource: taken, Token: tokens[taken]},
		{Resource: b, Token: tokens[b]},
		{Resource: cleared, Token: tokens[cleared]},
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(held, func(i, j int) bool { return held[i].Id < held[j].Id })
	if want := []core.Owned{{Resource: a, Token: tokens[a]}, {Resource: b, Token: tokens[b]}}; !reflect.DeepEqual(held, want) {
		t.Fatalf("held: got %v want %v", held, want)
	}
	requireFreshOwner(t, testPool, a, tokens[a])
	requireFreshOwner(t, testPool, b, tokens[b])
}

func TestReleaseOwners_DropsOnlyMatchingTokensAndKeepsTheMark(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	match, mismatch := model.Resource{Type: "rl", Id: "match"}, model.Resource{Type: "rl", Id: "mismatch"}
	register(t, st,
		core.Registration{Resource: match, Version: 1, Metadata: meta("m")},
		core.Registration{Resource: mismatch, Version: 1, Metadata: meta("m")})
	matchTok := own(t, testPool, match)
	own(t, testPool, mismatch)
	mismatchOwner := ownerOf(t, testPool, mismatch)
	_, _, matchSeq, since, _ := row(t, match)

	if err := st.ReleaseOwners(ctx, []core.Owned{
		{Resource: match, Token: matchTok},
		{Resource: mismatch, Token: mismatchOwner.seq + 1},
	}); err != nil {
		t.Fatal(err)
	}
	requireOwner(t, testPool, match, owner{}, "a matching token is released")
	if _, _, seq, after, _ := row(t, match); seq != matchSeq || after == nil || !after.Equal(*since) || metadataOf(t, match)["k"] != "m" {
		t.Fatalf("release must keep the mark: seq %d (was %d) since %v (was %v) metadata %v", seq, matchSeq, after, since, metadataOf(t, match))
	}
	requireOwner(t, testPool, mismatch, mismatchOwner, "a token that does not match")

	if err := st.ReleaseOwners(ctx, nil); err != nil {
		t.Fatalf("empty input must be a no-op: %v", err)
	}
}

func TestClearStale_ClearsOwnershipOnlyWithTheMark(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	cleared, moved := model.Resource{Type: "cs2", Id: "cleared"}, model.Resource{Type: "cs2", Id: "moved"}
	markUnclaimed(t, st, cleared, moved)
	marked := map[model.Resource]int64{cleared: own(t, testPool, cleared), moved: own(t, testPool, moved)}
	markUnclaimed(t, st, moved) // seq moves
	movedOwner := ownerOf(t, testPool, moved)

	// Each finishes with the stale_seq of the first mark.
	for _, res := range []model.Resource{cleared, moved} {
		if err := st.ClearStale(ctx, res, marked[res]); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, since, _ := row(t, cleared); since != nil {
		t.Fatalf("matching seq must clear the mark: since %v", since)
	}
	requireOwner(t, testPool, cleared, owner{}, "matching seq must clear the ownership with the mark")
	if _, _, _, since, _ := row(t, moved); since == nil {
		t.Fatal("moved seq must keep the mark")
	}
	requireOwner(t, testPool, moved, movedOwner, "moved seq must keep the ownership")
}

// seedStale writes a stale-marked row directly: stale_seq staleSeq, marked
// age (a Postgres interval) ago, no owner.
func seedStale(t *testing.T, pool *pgxpool.Pool, res model.Resource, staleSeq int64, age string, deleted bool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO resources (type, id, stale_seq, stale_since, deleted) VALUES ($1, $2, $3, now() - $4::interval, $5)`,
		res.Type, res.Id, staleSeq, age, deleted); err != nil {
		t.Fatalf("seed stale %s/%s: %v", res.Type, res.Id, err)
	}
}

// listStale runs ListStale with a one-minute lease and a cutoff a minute
// ago, bounded by a timeout so a call blocking on a row lock fails the test
// instead of hanging it.
func listStale(t *testing.T, st *Store, limit int) []core.StaleResource {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := st.ListStale(ctx, time.Now().Add(-time.Minute), limit, time.Minute)
	if err != nil {
		t.Fatalf("ListStale: %v", err)
	}
	return got
}

func staleIDs(entries []core.StaleResource) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Id
	}
	return out
}

func TestListStale_SkipsLiveOwnersAndClaimsWhatItReturns(t *testing.T) {
	st, pool := isolatedStore(t)
	r := func(id string) model.Resource { return model.Resource{Type: "lo", Id: id} }
	unowned, expired, live, tomb, fresh := r("unowned"), r("expired"), r("live"), r("tomb"), r("fresh")
	seedStale(t, pool, unowned, 3, "10 minutes", false)
	seedStale(t, pool, live, 2, "9 minutes", false)
	seedStale(t, pool, expired, 5, "8 minutes", false)
	seedStale(t, pool, tomb, 4, "7 minutes", true)
	seedStale(t, pool, fresh, 1, "0 seconds", false) // inside the cutoff
	own(t, pool, live)
	own(t, pool, expired)
	expireOwner(t, pool, expired)
	liveOwner := ownerOf(t, pool, live)

	got := listStale(t, st, 10)
	want := []core.StaleResource{
		{Resource: unowned, StaleSeq: 3, Token: 3},
		{Resource: expired, StaleSeq: 5, Token: 5},
		{Resource: tomb, StaleSeq: 4, Token: 4, Deleted: true},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v: oldest first, the live owner and the fresh mark left out", staleIDs(got), staleIDs(want))
	}
	for i, w := range want {
		g := got[i]
		if g != w {
			t.Fatalf("entry %d: got %+v, want %+v", i, g, w)
		}
		// The claim takes the row's stale_seq as the token without bumping it.
		requireFreshOwner(t, pool, w.Resource, w.StaleSeq)
		if _, _, seq, _, _ := rowIn(t, pool, w.Resource); seq != w.StaleSeq {
			t.Fatalf("%s: stale_seq %d, want it unchanged at %d", w.Id, seq, w.StaleSeq)
		}
	}
	requireOwner(t, pool, live, liveOwner, "a row with a live owner is not listed or claimed")

	if again := listStale(t, st, 10); len(again) != 0 {
		t.Fatalf("a second call right after: got %v, want none (all claimed under live leases)", staleIDs(again))
	}
}

func TestListStale_SkipsRowsLockedByAnotherTransaction(t *testing.T) {
	st, pool := isolatedStore(t)
	r := func(id string) model.Resource { return model.Resource{Type: "lk", Id: id} }
	a, b, c := r("a"), r("b"), r("c")
	seedStale(t, pool, a, 1, "10 minutes", false)
	seedStale(t, pool, b, 1, "9 minutes", false)
	seedStale(t, pool, c, 1, "8 minutes", false)

	// The interleaving: another transaction holds b's row lock (a concurrent
	// claim, mark or finish of b) before ListStale runs; ListStale skips b
	// instead of waiting, claims a and c, and commits while the lock is held.
	// Once it is released, the next call finds b.
	g := lockRow(t, pool, b)
	if got := staleIDs(listStale(t, st, 10)); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("with b locked: got %v, want [a c]", got)
	}
	g.release()
	if got := staleIDs(listStale(t, st, 10)); len(got) != 1 || got[0] != "b" {
		t.Fatalf("after the lock is released: got %v, want [b]", got)
	}
}

func TestListStale_ConcurrentCallsNeverClaimARowTwice(t *testing.T) {
	st, pool := isolatedStore(t)
	const rows, callers, limit = 20, 4, 8
	for i := range rows {
		seedStale(t, pool, model.Resource{Type: "lc", Id: fmt.Sprint(i)}, int64(i+1), fmt.Sprintf("%d minutes", 10+i), false)
	}

	// No interleaving is forced: whichever order the callers' statements
	// lock, skip and commit in, no row may be returned to two of them.
	type result struct {
		got []core.StaleResource
		err error
	}
	results := make(chan result, callers)
	startGate := make(chan struct{})
	for range callers {
		go func() {
			<-startGate
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := st.ListStale(ctx, time.Now().Add(-time.Minute), limit, time.Minute)
			results <- result{got, err}
		}()
	}
	close(startGate)
	seen := map[string]core.StaleResource{}
	for range callers {
		res := <-results
		if res.err != nil {
			t.Fatalf("ListStale: %v", res.err)
		}
		for _, e := range res.got {
			if _, dup := seen[e.Id]; dup {
				t.Fatalf("row %s returned to two concurrent calls", e.Id)
			}
			seen[e.Id] = e
		}
	}
	for _, e := range seen {
		if o := ownerOf(t, pool, e.Resource); o.seq != e.Token || e.Token != e.StaleSeq {
			t.Fatalf("row %s: token %d, stale_seq %d, owner_seq %d; want all equal", e.Id, e.Token, e.StaleSeq, o.seq)
		}
	}
}

// commitWhileWaiting forces a write to res's row to commit while finish
// waits for the row lock: the gate transaction locks the row and runs write
// in it, finish starts and queues on the lock (seen in pg_stat_activity), and
// only then does the gate commit, so finish runs against the written row
// although its snapshot, taken before the commit, predates the write. It
// returns finish's follow-up.
func commitWhileWaiting(t *testing.T, res model.Resource, write string, finish func() (core.FollowUp, error)) core.FollowUp {
	t.Helper()
	g := lockRow(t, testPool, res)
	if _, err := g.tx.Exec(context.Background(), write, res.Type, res.Id); err != nil {
		t.Fatalf("write in the gate: %v", err)
	}
	type result struct {
		f   core.FollowUp
		err error
	}
	done := make(chan result, 1)
	go func() {
		f, err := finish()
		done <- result{f, err}
	}()
	g.waitForWaiters(t, 1)
	g.release()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("finish: %v", r.err)
		}
		return r.f
	case <-time.After(10 * time.Second):
		t.Fatal("the finishing statement did not complete after the gate committed")
	}
	return core.FollowUp{}
}

func TestFinishOwned_MarkCommittedWhileWaitingIsReclaimed(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "fw", Id: "1"}
	markUnclaimed(t, st, res)
	tok := own(t, testPool, res)

	// The interleaving: the owned build finishes with FinishOwned(tok, tok)
	// while a change of res is mid-commit. The change's mark holds the row
	// lock (a mark under the live lease: stale_seq bumped, metadata and
	// owner columns untouched); FinishOwned queues on it; the mark commits.
	// FinishOwned must see the moved stale_seq and re-claim instead of
	// clearing the mark the change just made.
	f := commitWhileWaiting(t, res,
		`UPDATE resources SET stale_seq = nextval('change_sequence'), stale_since = COALESCE(stale_since, now())
		 WHERE type=$1 AND id=$2`,
		func() (core.FollowUp, error) { return st.FinishOwned(ctx, res, tok, tok) })

	_, _, seq, since, _ := row(t, res)
	if seq <= tok || since == nil {
		t.Fatalf("the mark committed during the finish must stay: seq %d (was %d) since %v", seq, tok, since)
	}
	requireFollowUp(t, f, core.FollowUp{Token: seq})
	requireFreshOwner(t, testPool, res, seq)
}

func TestDeleteResourceIfSeq_RecreateCommittedWhileWaitingIsReclaimed(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dw", Id: "1"}
	register(t, st, core.Registration{Resource: res, Deleted: true, Metadata: meta("del")})
	tok := own(t, testPool, res)

	// The interleaving: the owned delete finishes with
	// DeleteResourceIfSeq(tok, tok) while a recreate of res is mid-commit.
	// The recreate holds the row lock (stale_seq bumped, tombstone cleared,
	// owner columns untouched under the live lease); the delete queues on it;
	// the recreate commits. The delete must see the moved stale_seq, keep the
	// row and re-claim it for the recreate's build.
	f := commitWhileWaiting(t, res,
		`UPDATE resources SET stale_seq = nextval('change_sequence'), deleted = false, version = 1, metadata = '{"k": "m"}'
		 WHERE type=$1 AND id=$2`,
		func() (core.FollowUp, error) { return st.DeleteResourceIfSeq(ctx, res, tok, tok) })

	var n int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the recreated row must survive the delete: %d rows (%v)", n, err)
	}
	version, _, seq, since, deleted := row(t, res)
	if deleted || version != 1 || seq <= tok || since == nil {
		t.Fatalf("the recreated row must stay marked: deleted %v version %d seq %d (was %d) since %v", deleted, version, seq, tok, since)
	}
	requireFollowUp(t, f, core.FollowUp{Token: seq})
	requireFreshOwner(t, testPool, res, seq)
	// The follow-up's build reads the recreate's metadata at BeginBuild.
	requireMetadata(t, "the recreated row's metadata", metadataOf(t, res), meta("m"))
}

func TestClaim_LeaseIsMeasuredFromOwnerSince(t *testing.T) {
	ctx := context.Background()
	// Owners claimed 30 seconds and 2 minutes ago, under a one-minute lease:
	// the first is live, the second expired.
	t.Run("MarkStale", func(t *testing.T) {
		st := NewStore(testPool)
		live, expired := model.Resource{Type: "lm", Id: "live"}, model.Resource{Type: "lm", Id: "expired"}
		markUnclaimed(t, st, live, expired)
		liveOwner := owner{seq: own(t, testPool, live)}
		expiredTok := own(t, testPool, expired)
		liveOwner.since = backdateOwner(t, testPool, live, "30 seconds")
		backdateOwner(t, testPool, expired, "2 minutes")

		owned, err := st.MarkStale(ctx, []model.Resource{live, expired}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, _, newSeq, _, _ := row(t, expired)
		if len(owned) != 1 || owned[0] != (core.Owned{Resource: expired, Token: newSeq}) || newSeq <= expiredTok {
			t.Fatalf("owned %+v, want only the 2-minute-old owner's row, claimed at its new stale_seq %d (above %d)", owned, newSeq, expiredTok)
		}
		requireOwner(t, testPool, live, liveOwner, "a 30-second-old owner is live under a one-minute lease")
		requireFreshOwner(t, testPool, expired, newSeq)
	})

	t.Run("ListStale", func(t *testing.T) {
		st, pool := isolatedStore(t)
		live, expired := model.Resource{Type: "lm", Id: "live"}, model.Resource{Type: "lm", Id: "expired"}
		seedStale(t, pool, live, 1, "10 minutes", false)
		seedStale(t, pool, expired, 1, "10 minutes", false)
		own(t, pool, live)
		own(t, pool, expired)
		liveOwner := owner{seq: 1, since: backdateOwner(t, pool, live, "30 seconds")}
		backdateOwner(t, pool, expired, "2 minutes")

		if got := staleIDs(listStale(t, st, 10)); len(got) != 1 || got[0] != "expired" {
			t.Fatalf("got %v, want [expired]: the 30-second-old owner is live under a one-minute lease", got)
		}
		requireOwner(t, pool, live, liveOwner, "a live owner is not claimed")
		requireFreshOwner(t, pool, expired, 1)
	})
}

// ---- Numbers drawn from the Change Sequence ----

// exists reports whether res has a row.
func exists(t *testing.T, res model.Resource) bool {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&n); err != nil {
		t.Fatalf("count %s/%s: %v", res.Type, res.Id, err)
	}
	return n > 0
}

// rowJSON reads res's row whole, every column, to compare it before and
// after a call; nil when it has no row.
func rowJSON(t *testing.T, res model.Resource) map[string]any {
	t.Helper()
	var m map[string]any
	err := testPool.QueryRow(context.Background(),
		`SELECT to_jsonb(r) FROM resources r WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&m)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatalf("read row %s/%s: %v", res.Type, res.Id, err)
	}
	return m
}

// highest reads the greatest number res's row carries: its build_idx,
// stale_seq, owner_seq and change_seq.
func highest(t *testing.T, res model.Resource) int64 {
	t.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(),
		`SELECT GREATEST(build_idx, stale_seq, COALESCE(owner_seq, 0), change_seq) FROM resources WHERE type=$1 AND id=$2`,
		res.Type, res.Id).Scan(&n); err != nil {
		t.Fatalf("read numbers %s/%s: %v", res.Type, res.Id, err)
	}
	return n
}

// retire gives res a history — registrations with and without a claim,
// builds, a MarkStale — then tombstones it, bumps its Build Sequence with
// BeginDelete and hard-deletes it, as an owned delete runs. The bump is the
// row's last and highest number. It returns the greatest number the row
// carried or a call returned for it.
func retire(t *testing.T, st *Store, res model.Resource) int64 {
	t.Helper()
	ctx := context.Background()
	var high int64
	see := func(ns ...int64) {
		for _, n := range ns {
			high = max(high, n)
		}
	}

	first := register(t, st, core.Registration{Resource: res, Version: 1}).Items[0]
	if first.Token == 0 {
		t.Fatalf("retire %v: the first registration must claim the row: %+v", res, first)
	}
	see(first.StaleSeq, first.Token)
	see(register(t, st, core.Registration{Resource: res, Version: 2}).Items[0].StaleSeq) // under the live lease
	for range 2 {
		b, err := st.BeginBuild(ctx, res, first.Token)
		if err != nil {
			t.Fatal(err)
		}
		see(b.BuildIdx, b.StaleSeq, b.Start)
	}
	markUnclaimed(t, st, res)
	expireOwner(t, testPool, res)
	del := register(t, st, core.Registration{Resource: res, Deleted: true}).Items[0]
	if del.Token == 0 {
		t.Fatalf("retire %v: the delete must claim the row after the lease expired: %+v", res, del)
	}
	see(del.StaleSeq, del.Token, highest(t, res))

	bump := beginDelete(t, st, res, del.Token)
	if bump.Superseded || bump.BuildIdx <= high {
		t.Fatalf("retire %v: BeginDelete %+v, want not superseded, a BuildIdx above every earlier number %d", res, bump, high)
	}
	see(bump.BuildIdx, highest(t, res))

	f, err := st.DeleteResourceIfSeq(ctx, res, del.StaleSeq, del.Token)
	if err != nil {
		t.Fatal(err)
	}
	requireFollowUp(t, f, core.FollowUp{})
	if exists(t, res) {
		t.Fatalf("retire %v: the delete must hard-delete the row", res)
	}
	return high
}

// A hard delete must not reset the numbers: every writer's first number on
// a recreated row is above every number its old row carried, so a build,
// delete or owner of the old row can never match or outrank the new one.
func TestRecreate_FirstNumbersAreAboveEveryNumberOfTheDeletedRow(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rn", Id: id} }

	t.Run("RegisterChanges", func(t *testing.T) {
		res := r("registered")
		high := retire(t, st, res)

		it := register(t, st, core.Registration{Resource: res, Version: 1}).Items[0]
		if !it.Accepted || it.StaleSeq <= high || it.Token != it.StaleSeq {
			t.Fatalf("recreate: %+v, want accepted and claimed with a StaleSeq above the old row's %d", it, high)
		}
		begun, err := st.BeginBuild(ctx, res, it.Token)
		if err != nil {
			t.Fatal(err)
		}
		if begun.BuildIdx <= high || begun.Start != begun.BuildIdx {
			t.Fatalf("first build: BuildIdx %d Start %d, want a BuildIdx above the old row's %d, as the Start too", begun.BuildIdx, begun.Start, high)
		}
	})

	t.Run("BeginBuild", func(t *testing.T) {
		// A rebuild walk reaching the gone id inserts the row.
		res := r("built")
		high := retire(t, st, res)

		begun, err := st.BeginBuild(ctx, res, 0)
		if err != nil {
			t.Fatal(err)
		}
		if begun.BuildIdx <= high || begun.Start != begun.BuildIdx {
			t.Fatalf("first build: BuildIdx %d Start %d, want a BuildIdx above the old row's %d, as the Start too", begun.BuildIdx, begun.Start, high)
		}
		markUnclaimed(t, st, res)
		if _, _, seq, _, _ := row(t, res); seq <= high {
			t.Fatalf("first mark: stale_seq %d, want above the old row's %d", seq, high)
		}
	})

	t.Run("MarkStale lease 0", func(t *testing.T) {
		res := r("marked")
		high := retire(t, st, res)

		markUnclaimed(t, st, res)
		if _, _, seq, _, _ := row(t, res); seq <= high {
			t.Fatalf("first mark: stale_seq %d, want above the old row's %d", seq, high)
		}
	})

	t.Run("MarkStale with a lease", func(t *testing.T) {
		claimed := r("claimed")
		high := retire(t, st, claimed)

		owned, err := st.MarkStale(ctx, []model.Resource{claimed}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(owned) != 1 || owned[0].Resource != claimed || owned[0].Token <= high {
			t.Fatalf("first mark: owned %+v, want the row claimed with a token above the old row's %d", owned, high)
		}
		if _, _, seq, _, _ := row(t, claimed); seq != owned[0].Token {
			t.Fatalf("first mark: stale_seq %d, want the token %d", seq, owned[0].Token)
		}
	})

	t.Run("Parent", func(t *testing.T) {
		parent, child := r("parent"), model.Resource{Type: "rn-c", Id: "child"}
		replace(t, st, parent, 1, nil, edgeSet(1, child))
		high := retire(t, st, parent)
		// The old row's delete removed its edges; a build of the recreated
		// parent writes them again before its row is.
		if err := st.RemoveResource(ctx, parent, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		replace(t, st, parent, high+1, nil, edgeSet(1, child))

		got := register(t, st, core.Registration{Resource: child, Version: 1})
		if len(got.Parents) != 1 || got.Parents[0].Resource != parent || got.Parents[0].Token <= high {
			t.Fatalf("Parents %+v, want the parent claimed with a token above the old row's %d", got.Parents, high)
		}
		if _, _, seq, _, _ := row(t, parent); seq != got.Parents[0].Token {
			t.Fatalf("parent's stale_seq %d, want the token %d", seq, got.Parents[0].Token)
		}
	})
}

// beginDelete runs BeginDelete and fails the test on error.
func beginDelete(t *testing.T, st *Store, res model.Resource, token int64) core.DeleteBegun {
	t.Helper()
	got, err := st.BeginDelete(context.Background(), res, token)
	if err != nil {
		t.Fatalf("BeginDelete %s/%s: %v", res.Type, res.Id, err)
	}
	return got
}

// tombstone creates res, builds it, and tombstones it with a delete
// registration that claims the row, as a notified delete starts. It returns
// the delete's mark, which is also its owner token.
func tombstone(t *testing.T, st *Store, res model.Resource) int64 {
	t.Helper()
	register(t, st, core.Registration{Resource: res, Version: 1})
	if _, err := st.BeginBuild(context.Background(), res, 0); err != nil {
		t.Fatal(err)
	}
	expireOwner(t, testPool, res)
	del := register(t, st, core.Registration{Resource: res, Deleted: true}).Items[0]
	if !del.Accepted || del.Token == 0 || del.Token != del.StaleSeq {
		t.Fatalf("tombstone %v: %+v, want the delete accepted and claimed", res, del)
	}
	return del.StaleSeq
}

func TestBeginDelete_BumpsTheBuildSequenceOfATombstone(t *testing.T) {
	st := NewStore(testPool)
	res := model.Resource{Type: "bd1", Id: "1"}
	tombstone(t, st, res)
	expireOwner(t, testPool, res) // an unrenewed owner_since stays this one
	earlier := highest(t, res)
	before := rowJSON(t, res)
	drawn := start(t, st)

	got := beginDelete(t, st, res, 0)
	if got.Superseded || got.BuildIdx <= earlier || got.BuildIdx <= drawn {
		t.Fatalf("got %+v, want not superseded, a BuildIdx above every number of the row (%d) and above %d drawn before the call", got, earlier, drawn)
	}
	if _, idx, _, _, _ := row(t, res); idx != got.BuildIdx {
		t.Fatalf("build_idx %d, want the returned BuildIdx %d", idx, got.BuildIdx)
	}
	after := rowJSON(t, res)
	delete(before, "build_idx")
	delete(after, "build_idx")
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("only build_idx may change:\n before %v\n after  %v", before, after)
	}
}

func TestBeginDelete_RenewsOnlyAMatchingToken(t *testing.T) {
	st := NewStore(testPool)
	res := model.Resource{Type: "bd2", Id: "1"}
	tok := tombstone(t, st, res)
	expired := owner{seq: tok, since: expireOwner(t, testPool, res)}
	_, prev, _, _, _ := row(t, res)

	for _, token := range []int64{tok + 1, 0, tok} {
		got := beginDelete(t, st, res, token)
		if got.Superseded || got.BuildIdx <= prev {
			t.Fatalf("token %d: got %+v, want not superseded, a BuildIdx above %d", token, got, prev)
		}
		prev = got.BuildIdx
		if token != tok {
			requireOwner(t, testPool, res, expired, fmt.Sprintf("token %d does not match", token))
		}
	}
	requireFreshOwner(t, testPool, res, tok)
}

// A mark that moves a tombstone's stale_seq and keeps it deleted — a Parent
// mark from a child's registration, a MarkStale — does not supersede its
// delete: the delete still bumps the Build Sequence above every number the
// row carries and renews a matching token, and its finish
// (DeleteResourceIfSeq) sees the moved mark and hands on the follow-up.
func TestBeginDelete_AMovedMarkOnATombstoneDoesNotSupersede(t *testing.T) {
	st := NewStore(testPool)
	for _, tc := range []struct {
		name, id string
		move     func(t *testing.T, res model.Resource)
	}{
		{"Parent mark", "parent", func(t *testing.T, res model.Resource) {
			child := model.Resource{Type: "bd5-c", Id: res.Id}
			got := register(t, st, core.Registration{Resource: child, Version: 1})
			if len(got.Parents) != 1 || got.Parents[0].Resource != res {
				t.Fatalf("the child's registration must mark the deleted parent: %+v", got.Parents)
			}
		}},
		{"MarkStale", "marked", func(t *testing.T, res model.Resource) {
			markUnclaimed(t, st, res)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := model.Resource{Type: "bd5", Id: tc.id}
			// The parent's edge set, written by an earlier build, is what a
			// child's registration reaches it through.
			replace(t, st, res, 1, nil, edgeSet(1, model.Resource{Type: "bd5-c", Id: res.Id}))
			tok := tombstone(t, st, res)
			tc.move(t, res)
			if _, _, seq, _, deleted := row(t, res); seq == tok || !deleted {
				t.Fatalf("setup: stale_seq %d deleted %v, want a stale_seq moved off %d on a row still deleted", seq, deleted, tok)
			}
			expireOwner(t, testPool, res) // a renewal would show
			high := highest(t, res)
			_, _, moved, _, _ := row(t, res)

			got := beginDelete(t, st, res, tok)
			if got.Superseded || got.BuildIdx <= high {
				t.Fatalf("got %+v, want not superseded, a BuildIdx above every number of the row (%d)", got, high)
			}
			if _, idx, seq, _, deleted := row(t, res); idx != got.BuildIdx || seq != moved || !deleted {
				t.Fatalf("build_idx %d stale_seq %d deleted %v, want the returned BuildIdx %d, the moved mark %d kept, still deleted", idx, seq, deleted, got.BuildIdx, moved)
			}
			requireFreshOwner(t, testPool, res, tok)
		})
	}
}

// A delete is superseded only when its row is no longer a tombstone or is
// gone, and a superseded delete changes nothing; a mark that keeps the
// tombstone does not supersede it (TestBeginDelete_AMovedMarkOnATombstoneDoesNotSupersede).
func TestBeginDelete_SupersededChangesNothing(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "bd3", Id: id} }
	superseded := core.DeleteBegun{Superseded: true}

	t.Run("recreated", func(t *testing.T) {
		res := r("recreated")
		tok := tombstone(t, st, res)
		re := register(t, st, core.Registration{Resource: res, Version: 2}).Items[0]
		if !re.Accepted {
			t.Fatalf("recreate: %+v, want accepted", re)
		}
		expireOwner(t, testPool, res) // a renewal would show
		before := rowJSON(t, res)

		// Even under the token that still owns the row: it is not a tombstone.
		if got := beginDelete(t, st, res, tok); got != superseded {
			t.Fatalf("got %+v, want %+v", got, superseded)
		}
		if after := rowJSON(t, res); !reflect.DeepEqual(after, before) {
			t.Fatalf("a superseded delete must change nothing:\n before %v\n after  %v", before, after)
		}
	})

	t.Run("hard-deleted", func(t *testing.T) {
		res := r("gone")
		tok := tombstone(t, st, res)
		if _, err := st.DeleteResourceIfSeq(ctx, res, tok, tok); err != nil {
			t.Fatal(err)
		}
		if exists(t, res) {
			t.Fatal("the finished delete must hard-delete the row")
		}

		if got := beginDelete(t, st, res, tok); got != superseded {
			t.Fatalf("got %+v, want %+v", got, superseded)
		}
		if exists(t, res) {
			t.Fatal("a superseded delete must not create a row")
		}
	})

	t.Run("never existed", func(t *testing.T) {
		res := r("never")
		if got := beginDelete(t, st, res, 0); got != superseded {
			t.Fatalf("got %+v, want %+v", got, superseded)
		}
		if exists(t, res) {
			t.Fatal("a superseded delete must not create a row")
		}
	})
}
