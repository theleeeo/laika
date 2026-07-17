package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

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

func TestMarkDeleted_TombstonesAndResetsVersion(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "md", Id: "1"}

	if err := st.UpsertResource(ctx, res, 5); err != nil {
		t.Fatal(err)
	}
	seq, err := st.MarkDeleted(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	version, _, staleSeq, since, deleted := row(t, res)
	if !deleted || since == nil || staleSeq != seq || seq != 1 {
		t.Fatalf("tombstone wrong: deleted=%v since=%v seq=%d ret=%d", deleted, since, staleSeq, seq)
	}
	if version != 0 {
		t.Fatalf("MarkDeleted must reset version so a re-create is never 'stale': version=%d", version)
	}

	// Re-create with a low version must be accepted and clear the tombstone flag.
	if err := st.UpsertResource(ctx, res, 1); err != nil {
		t.Fatalf("re-create after delete must be accepted: %v", err)
	}
	_, _, _, _, deleted = row(t, res)
	if deleted {
		t.Fatal("upsert must clear the deleted flag")
	}
}

func TestUpsertResource_Version0_ClearsTombstone(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "uv0", Id: "1"}

	if _, err := st.MarkDeleted(ctx, res); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertResource(ctx, res, 0); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, deleted := row(t, res)
	if deleted {
		t.Fatal("version-0 upsert must clear the deleted flag")
	}
}

func TestDeleteResourceIfSeq_GuardedHardDelete(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "dr", Id: "1"}

	seq, err := st.MarkDeleted(ctx, res)
	if err != nil {
		t.Fatal(err)
	}

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
	if _, err := st.MarkDeleted(ctx, delRes); err != nil {
		t.Fatal(err)
	}
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
