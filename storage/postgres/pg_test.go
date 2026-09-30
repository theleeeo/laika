package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://user:pass@%s/indexer", endpoint))
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgxpool: %v\n", err)
		os.Exit(1)
	}
	schema, err := os.ReadFile("pg_schema.sql")
	if err != nil {
		fmt.Fprintf(os.Stderr, "read schema: %v\n", err)
		os.Exit(1)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		fmt.Fprintf(os.Stderr, "apply schema: %v\n", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	_ = testcontainers.TerminateContainer(c)
	os.Exit(code)
}

// row reads the full resources row for assertions.
func row(t *testing.T, res model.Resource) (version, buildIdx, staleSeq int64, staleSince *time.Time, deleted bool) {
	t.Helper()
	err := testPool.QueryRow(context.Background(),
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

	if err := st.MarkStale(ctx, []model.Resource{res}, nil); err != nil {
		t.Fatal(err)
	}
	_, _, seq1, since1, _ := row(t, res)
	if seq1 != 1 || since1 == nil {
		t.Fatalf("after first mark: seq=%d since=%v", seq1, since1)
	}

	time.Sleep(10 * time.Millisecond)
	if err := st.MarkStale(ctx, []model.Resource{res}, nil); err != nil {
		t.Fatal(err)
	}
	_, _, seq2, since2, _ := row(t, res)
	if seq2 != 2 {
		t.Fatalf("after second mark: seq=%d", seq2)
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
	// cannot affect the same row twice in one statement without deduping).
	if err := st.MarkStale(ctx, []model.Resource{dup, other, dup}, nil); err != nil {
		t.Fatalf("MarkStale with duplicate resources in one batch: %v", err)
	}

	_, _, dupSeq, dupSince, _ := row(t, dup)
	if dupSeq != 1 {
		t.Fatalf("duplicated resource must count as ONE logical mark: stale_seq=%d", dupSeq)
	}
	if dupSince == nil {
		t.Fatal("duplicated resource must have stale_since set")
	}

	_, _, otherSeq, otherSince, _ := row(t, other)
	if otherSeq != 1 || otherSince == nil {
		t.Fatalf("other resource wrong: seq=%d since=%v", otherSeq, otherSince)
	}
}

func TestMarkStale_MetadataLastMarkWins(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "msm", Id: "1"}

	if err := st.MarkStale(ctx, []model.Resource{res}, map[string]string{"fiber_operator_id": "op-1"}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkStale(ctx, []model.Resource{res}, map[string]string{"fiber_operator_id": "op-2"}); err != nil {
		t.Fatal(err)
	}

	entries, err := st.ListStale(ctx, time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Type == "msm" && e.Id == "1" {
			if e.Metadata["fiber_operator_id"] != "op-2" {
				t.Fatalf("metadata must be the LAST mark's: %v", e.Metadata)
			}
			return
		}
	}
	t.Fatal("marked resource missing from stale listing")
}

func TestBeginBuild_BumpsBuildIdx_ReturnsStaleSeq(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "bb", Id: "1"}

	if err := st.MarkStale(ctx, []model.Resource{res}, nil); err != nil {
		t.Fatal(err)
	}
	buildIdx, staleSeq, err := st.BeginBuild(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	if buildIdx != 1 || staleSeq != 1 {
		t.Fatalf("got buildIdx=%d staleSeq=%d, want 1, 1", buildIdx, staleSeq)
	}
	buildIdx2, _, err := st.BeginBuild(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	if buildIdx2 != 2 {
		t.Fatalf("build_idx must increment: got %d", buildIdx2)
	}
}

func TestClearStale_GuardedBySeq(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "cs", Id: "1"}

	if err := st.MarkStale(ctx, []model.Resource{res}, nil); err != nil { // seq=1
		t.Fatal(err)
	}
	if err := st.ClearStale(ctx, res, 1); err != nil {
		t.Fatal(err)
	}
	_, _, _, since, _ := row(t, res)
	if since != nil {
		t.Fatalf("matching seq must clear: stale_since=%v", since)
	}

	// Re-mark twice: clear with a stale seq must be a no-op.
	_ = st.MarkStale(ctx, []model.Resource{res}, nil) // seq=2
	_ = st.MarkStale(ctx, []model.Resource{res}, nil) // seq=3
	if err := st.ClearStale(ctx, res, 2); err != nil {
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

	register(t, st, core.Registration{Resource: res, Version: 5}) // seq=1
	got := register(t, st, core.Registration{Resource: res, Deleted: true})
	seq := got.Items[0].StaleSeq
	version, _, staleSeq, since, deleted := row(t, res)
	if !got.Items[0].Accepted || !deleted || since == nil || staleSeq != seq || seq != 2 {
		t.Fatalf("tombstone wrong: accepted=%v deleted=%v since=%v seq=%d ret=%d", got.Items[0].Accepted, deleted, since, staleSeq, seq)
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

func TestDeleteResourceIfSeq_GuardedHardDelete(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dr", Id: "1"}

	seq := register(t, st, core.Registration{Resource: res, Deleted: true}).Items[0].StaleSeq

	// Wrong seq: row must survive.
	if err := st.DeleteResourceIfSeq(ctx, res, seq+1); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&n)
	if n != 1 {
		t.Fatal("mismatched seq must not delete the row")
	}

	// Matching seq: row goes away.
	if err := st.DeleteResourceIfSeq(ctx, res, seq); err != nil {
		t.Fatal(err)
	}
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res.Type, res.Id).Scan(&n)
	if n != 0 {
		t.Fatal("matching seq must delete the row")
	}

	// Not-deleted rows are never hard-deleted even with matching seq.
	res2 := model.Resource{Type: "dr", Id: "2"}
	_ = st.MarkStale(ctx, []model.Resource{res2}, nil) // seq=1, deleted=false
	if err := st.DeleteResourceIfSeq(ctx, res2, 1); err != nil {
		t.Fatal(err)
	}
	_ = testPool.QueryRow(ctx, `SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, res2.Type, res2.Id).Scan(&n)
	if n != 1 {
		t.Fatal("non-tombstoned rows must never be hard-deleted")
	}
}

func TestListStale_CutoffOrderLimitAndDeletedFlag(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)

	oldRes := model.Resource{Type: "ls", Id: "old"}
	newRes := model.Resource{Type: "ls", Id: "new"}
	delRes := model.Resource{Type: "ls", Id: "del"}

	_ = st.MarkStale(ctx, []model.Resource{oldRes}, map[string]string{"fiber_operator_id": "op-1"})
	register(t, st, core.Registration{Resource: delRes, Deleted: true})
	// Backdate the "old" and "del" marks.
	for _, r := range []model.Resource{oldRes, delRes} {
		if _, err := testPool.Exec(ctx,
			`UPDATE resources SET stale_since = now() - interval '10 minutes' WHERE type=$1 AND id=$2`,
			r.Type, r.Id); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.MarkStale(ctx, []model.Resource{newRes}, nil) // fresh mark, must be excluded by cutoff

	entries, err := st.ListStale(ctx, time.Now().Add(-time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	var gotOld, gotDel, gotNew bool
	for _, e := range entries {
		if e.Type != "ls" {
			continue
		}
		switch e.Id {
		case "old":
			gotOld = true
			if e.Deleted {
				t.Fatal("old must not be flagged deleted")
			}
			if e.Metadata["fiber_operator_id"] != "op-1" {
				t.Fatalf("old must carry the metadata stored at MarkStale: %v", e.Metadata)
			}
		case "del":
			gotDel = true
			if !e.Deleted {
				t.Fatal("del must carry Deleted=true")
			}
			if e.StaleSeq != 1 {
				t.Fatalf("del StaleSeq: got %d want 1", e.StaleSeq)
			}
		case "new":
			gotNew = true
		}
	}
	if !gotOld || !gotDel || gotNew {
		t.Fatalf("cutoff filter wrong: old=%v del=%v new=%v", gotOld, gotDel, gotNew)
	}

	// Limit applies.
	limited, err := st.ListStale(ctx, time.Now().Add(-time.Minute), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 {
		t.Fatalf("limit ignored: got %d entries", len(limited))
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

	_ = st.MarkStale(ctx, []model.Resource{oldA, oldB, otherType}, nil)
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
	_ = st.MarkStale(ctx, []model.Resource{fresh}, nil) // inside cutoff: excluded

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
	got, err := st.RegisterChanges(context.Background(), items)
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

// relate writes parent -> child relations directly.
func relate(t *testing.T, pairs ...[2]model.Resource) {
	t.Helper()
	for _, p := range pairs {
		if _, err := testPool.Exec(context.Background(),
			`INSERT INTO relations (resource, resource_id, related_resource, related_resource_id) VALUES ($1, $2, $3, $4)`,
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
	var since time.Time
	if err := testPool.QueryRow(context.Background(),
		`UPDATE resources SET stale_since = now() - interval '10 minutes' WHERE type=$1 AND id=$2 RETURNING stale_since`,
		res.Type, res.Id).Scan(&since); err != nil {
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
	seed(t, p1, 0, 3, false)
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
	})
	if err != nil {
		t.Fatal(err)
	}

	wantItems := []core.RegisteredItem{{Accepted: false, StaleSeq: 0}, {Accepted: true, StaleSeq: 1}, {Accepted: true, StaleSeq: 1}, {Accepted: true, StaleSeq: 1}, {Accepted: false, StaleSeq: 0}}
	if len(got.Items) != len(wantItems) {
		t.Fatalf("Items: got %d, want %d", len(got.Items), len(wantItems))
	}
	for i, w := range wantItems {
		if got.Items[i] != w {
			t.Errorf("Items[%d]: got %+v, want %+v", i, got.Items[i], w)
		}
	}

	for _, c := range []struct {
		res     model.Resource
		version int64
		meta    string
	}{{fresh, 1, "new"}, {inParent, 2, "q"}, {child, 3, "child"}} {
		version, _, seq, since, deleted := row(t, c.res)
		if version != c.version || seq != 1 || since == nil || deleted {
			t.Errorf("%s: version=%d seq=%d since=%v deleted=%v; want version %d, seq 1 (one mark), marked", c.res.Id, version, seq, since, deleted, c.version)
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
	if _, _, seq, since, _ := row(t, p1); seq != 4 || since == nil {
		t.Errorf("shared Parent must be marked exactly once: seq=%d (want 4) since=%v", seq, since)
	} else if !since.Equal(p1Since) {
		t.Errorf("Parent mark must keep the oldest stale_since: was %v, now %v", p1Since, since)
	}
	if m := metadataOf(t, p1); m["k"] != "child" {
		t.Errorf("shared Parent metadata %v, want the last accepted child's (child)", m)
	}
	if version, _, seq, since, _ := row(t, staleParent); version != 9 || seq != 1 || since == nil {
		t.Errorf("rejected in-batch item must still be marked as a Parent, version untouched: version=%d seq=%d since=%v", version, seq, since)
	}
	if m := metadataOf(t, staleParent); m["k"] != "new" {
		t.Errorf("rejected in-batch Parent metadata %v, want its child's (new)", m)
	}

	parents := map[model.Resource]map[string]string{}
	for _, p := range got.Parents {
		if _, dup := parents[p.Resource]; dup {
			t.Errorf("Parent %v listed twice", p.Resource)
		}
		parents[p.Resource] = p.Metadata
	}
	if len(parents) != 2 || parents[p1]["k"] != "child" || parents[staleParent]["k"] != "new" {
		t.Errorf("Parents: got %+v, want p1 (child) and rejected-parent (new) only", got.Parents)
	}
}

func TestRegisterChanges_Version0UntombstonesAndDeleteTombstones(t *testing.T) {
	st := NewStore(testPool)
	r := func(id string) model.Resource { return model.Resource{Type: "rc2", Id: id} }
	tomb, live, fresh := r("tomb"), r("live"), r("fresh")
	seed(t, tomb, 4, 1, true)
	seed(t, live, 7, 2, false)
	tombSince := backdate(t, tomb)

	got := register(t, st,
		core.Registration{Resource: tomb, Version: 0, Metadata: meta("a")},
		core.Registration{Resource: live, Deleted: true, Version: 3, Metadata: meta("b")}, // Version ignored
		core.Registration{Resource: fresh, Version: 0},
	)

	want := []core.RegisteredItem{{Accepted: true, StaleSeq: 2}, {Accepted: true, StaleSeq: 3}, {Accepted: true, StaleSeq: 1}}
	for i, w := range want {
		if got.Items[i] != w {
			t.Errorf("Items[%d]: got %+v, want %+v", i, got.Items[i], w)
		}
	}
	if version, _, seq, since, deleted := row(t, tomb); deleted || version != 4 || seq != 2 || since == nil {
		t.Errorf("version-0 item must untombstone and mark: deleted=%v version=%d seq=%d since=%v", deleted, version, seq, since)
	} else if !since.Equal(tombSince) {
		t.Errorf("item mark must keep the oldest stale_since: was %v, now %v", tombSince, since)
	}
	if m := metadataOf(t, tomb); m["k"] != "a" {
		t.Errorf("untombstoned metadata %v", m)
	}
	if version, _, seq, since, deleted := row(t, live); !deleted || version != 0 || seq != 3 || since == nil {
		t.Errorf("delete must tombstone, reset version and mark: deleted=%v version=%d seq=%d since=%v", deleted, version, seq, since)
	}
	if m := metadataOf(t, live); m["k"] != "b" {
		t.Errorf("tombstone must store its own metadata: %v", m)
	}
	if version, _, seq, since, deleted := row(t, fresh); deleted || version != 0 || seq != 1 || since == nil {
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
	seed(t, item, 1, 0, false)
	seed(t, parent, 0, 1, false)
	relate(t, [2]model.Resource{parent, item})

	// Only the Parent's mark violates this, so the items' rows are written
	// earlier in the same statement before it fails.
	if _, err := testPool.Exec(ctx,
		`ALTER TABLE resources ADD CONSTRAINT rc3_fail CHECK (type <> 'rc3' OR id <> 'parent' OR stale_seq < 2)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `ALTER TABLE resources DROP CONSTRAINT rc3_fail`)
	})

	_, err := st.RegisterChanges(ctx, []core.Registration{
		{Resource: item, Version: 2, Metadata: meta("x")},
		{Resource: fresh, Version: 1},
	})
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
	if _, _, seq, _, _ := row(t, parent); seq != 1 {
		t.Errorf("Parent seq changed: %d", seq)
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

	if _, _, childSeq, _, deleted := row(t, child); !deleted || got.Items[0] != (core.RegisteredItem{Accepted: true, StaleSeq: childSeq}) {
		t.Errorf("delete item: got %+v, want accepted with its row's seq %d (deleted=%v)", got.Items[0], childSeq, deleted)
	}
	if _, _, seq, since, _ := row(t, parent); seq != parentSeq+1 || since == nil {
		t.Errorf("Parent of the deleted item must be marked once: seq=%d (want %d) since=%v", seq, parentSeq+1, since)
	}
	if m := metadataOf(t, parent); m["k"] != "M" {
		t.Errorf("Parent metadata %v, want the delete's (M)", m)
	}
	if len(got.Parents) != 1 || got.Parents[0].Resource != parent || got.Parents[0].Metadata["k"] != "M" {
		t.Errorf("Parents: got %+v, want the Parent once with the delete's metadata", got.Parents)
	}
}
