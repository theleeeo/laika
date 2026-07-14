# Inline Builds + Stale-Mark Durability + Temporal Slow Lane — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the River job hop from the notification hot path: notifications durably mark resources stale in Postgres, build immediately on an in-process bounded pool, and a Temporal-scheduled sweep recovers anything whose stale mark survives; explicit rebuilds become Temporal workflows.

**Architecture:** "Stale mark = durable build intent" — every build-triggering path marks stale *first* (Postgres), then opportunistically builds inline. The pool is an accelerator; the sweep is the guarantee. River is removed entirely. `storage/postgres` and `backend/elasticsearch` fold into the root module; Temporal becomes a core dependency.

**Tech Stack:** Go 1.26.1 (`GOEXPERIMENT=jsonv2`), pgx/v5, go-elasticsearch/v8, `go.temporal.io/sdk`, testcontainers-go, connectrpc.

**Spec:** `docs/superpowers/specs/2026-07-14-inline-builds-stale-sweep-design.md` — read it before starting any task.

## Global Constraints

- Every Go command needs `GOEXPERIMENT=jsonv2` (Go 1.26.1).
- Run module-local commands from that module's directory; the repo uses `go.work`.
- The schema file `storage/postgres/pg_schema.sql` is **rewritten in place** — no ALTER migrations (nothing outside testing uses it yet).
- Breaking the core public API is allowed (`harness` and `vx-provider` live in sibling repos and are updated separately — out of scope here).
- Every task must end **green**: the whole workspace compiles and that task's tests pass. Commit at the end of every task (and at intermediate steps where marked).
- Unit tests in `core/` must not need Docker. `storage/postgres` tests use testcontainers (Docker). `app/tests` uses testcontainers.
- Commit messages: conventional (`feat:`, `refactor:`, `test:`, `docs:`), each ending with the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer.

## Parallel Execution Waves

Tasks within a wave touch disjoint files and can be dispatched to parallel agents. Waves are sequential.

| Wave | Tasks | Parallel? |
|---|---|---|
| 0 | Task 1 (module merge) | sequential |
| 1 | Task 2 (schema+store) ∥ Task 3 (build pool) ∥ Task 4 (proto) ∥ Task 5 (docs) | 4-way parallel |
| 2 | Task 6 (hot-path rewire + suite migration) | sequential |
| 3 | Task 7 (Temporal slow lane) | sequential |
| 4 | Task 8 (app wiring) ∥ Task 9 (new integration tests) | 2-way parallel |
| 5 | Task 10 (dependency cleanup + full verification) | sequential |

Wave-1 tasks all live in different files (`storage/postgres/*` + `core/store.go` + `core/build_cancel_test.go` / `core/pool.go` + `core/pool_test.go` / `proto/` + `app/gen/` / `docs/`). None of them edit any `go.mod`. If parallel agents share one working tree, they must each commit only their own files.

---

### Task 1: Merge implementation modules into the root module

**Files:**
- Delete: `storage/postgres/go.mod`, `storage/postgres/go.sum`, `backend/elasticsearch/go.mod`, `backend/elasticsearch/go.sum`
- Modify: `go.work`, `go.mod` (root), `app/go.mod`

Import paths do **not** change — `github.com/theleeeo/laika/storage/postgres` and `github.com/theleeeo/laika/backend/elasticsearch` become plain packages of the root module `github.com/theleeeo/laika`, and their directory paths already match.

- [ ] **Step 1: Delete the two module files**

```bash
cd /Users/leo/priv/laika-dev/laika
rm storage/postgres/go.mod storage/postgres/go.sum
rm backend/elasticsearch/go.mod backend/elasticsearch/go.sum
```

- [ ] **Step 2: Remove them from go.work**

Edit `go.work` to:

```
go 1.26.1

use (
	. // TODO: Should be removed once the root module is gone.
	./aggregation
	./app
)
```

- [ ] **Step 3: Add the ES client dependency to the root go.mod**

`backend/elasticsearch` imports `github.com/elastic/go-elasticsearch/v8`. Add to the root `go.mod` require block (same version the deleted module used):

```
github.com/elastic/go-elasticsearch/v8 v8.15.0
```

Then fetch: `GOEXPERIMENT=jsonv2 go mod download github.com/elastic/go-elasticsearch/v8`. If the root `go.sum` lacks entries, run `GOEXPERIMENT=jsonv2 go mod tidy` from the repo root — if tidy fails trying to resolve `github.com/theleeeo/laika/...` self-references remotely, instead copy the needed `go.sum` lines from the deleted `backend/elasticsearch/go.sum` and verify with the build in Step 5.

- [ ] **Step 4: Drop the merged-module requires from app/go.mod**

In `app/go.mod`, delete the `require` lines for `github.com/theleeeo/laika/backend/elasticsearch` and `github.com/theleeeo/laika/storage/postgres` (and any `replace` lines for them, if present). The existing `github.com/theleeeo/laika` require now covers those packages via the workspace. Do **not** run `go mod tidy` in `app/` (it resolves against the published root module, which doesn't have the merged packages yet).

- [ ] **Step 5: Verify the whole workspace builds and tests pass**

```bash
GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go vet ./...
GOEXPERIMENT=jsonv2 go test ./core/... ./backend/... ./app/server/... ./app/dsl/...
```

Expected: PASS (no behavior changed).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: fold storage/postgres and backend/elasticsearch into the root module"
```

---

### Task 2: Schema rewrite + Store stale primitives

**Files:**
- Modify: `storage/postgres/pg_schema.sql` (rewrite)
- Modify: `core/store.go` (interface additions — do NOT remove existing methods yet)
- Modify: `storage/postgres/pg.go` (new methods)
- Modify: `core/build_cancel_test.go` (stub the new methods on `cancellingStore`)
- Create: `storage/postgres/pg_test.go`

**Interfaces:**
- Consumes: nothing new (Task 1 layout).
- Produces (later tasks call these exact signatures on `core.Store`):

```go
MarkStale(ctx context.Context, resources []model.Resource) error
MarkDeleted(ctx context.Context, resource model.Resource) (staleSeq int64, err error)
BeginBuild(ctx context.Context, resource model.Resource) (buildIdx, staleSeq int64, err error)
ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error
DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq int64) error
ListStale(ctx context.Context, before time.Time, limit int) ([]StaleResource, error)
```

and the type:

```go
// StaleResource is one entry of the stale backlog.
type StaleResource struct {
	model.Resource
	StaleSeq int64
	Deleted  bool
}
```

`NextRebuildCounter` and `DeleteResource` **stay in the interface for now** (Task 6 removes them) so the existing hot path keeps compiling.

- [ ] **Step 1: Rewrite the schema**

Replace the `resources` table block in `storage/postgres/pg_schema.sql` (keep the `relations` table and its indexes unchanged):

```sql
CREATE TABLE IF NOT EXISTS resources (
    type VARCHAR NOT NULL,
    id VARCHAR NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    build_idx BIGINT NOT NULL DEFAULT 0,
    stale_seq BIGINT NOT NULL DEFAULT 0,
    stale_since TIMESTAMPTZ,                -- NULL = clean
    deleted BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (type, id)
);

CREATE INDEX IF NOT EXISTS idx_resources_stale ON resources (stale_since)
    WHERE stale_since IS NOT NULL;
```

- [ ] **Step 2: Add the interface methods and StaleResource to core/store.go**

Append to the `Store` interface in `core/store.go` (keeping every existing method) the six signatures from the Interfaces block above, and add the `StaleResource` type below the interface. Add `"time"` to imports.

- [ ] **Step 3: Stub the new methods on cancellingStore so core compiles**

In `core/build_cancel_test.go` add (with `"time"` import):

```go
func (s *cancellingStore) MarkStale(context.Context, []model.Resource) error { return nil }
func (s *cancellingStore) MarkDeleted(context.Context, model.Resource) (int64, error) {
	return 0, nil
}
func (s *cancellingStore) BeginBuild(context.Context, model.Resource) (int64, int64, error) {
	return 1, 0, nil
}
func (s *cancellingStore) ClearStale(context.Context, model.Resource, int64) error { return nil }
func (s *cancellingStore) DeleteResourceIfSeq(context.Context, model.Resource, int64) error {
	return nil
}
func (s *cancellingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
}
```

Run: `GOEXPERIMENT=jsonv2 go test ./core/...` — expected: PASS. Note: `app/cmd/indexer` passes `*postgres.Store` as `core.Store`, so the app module won't compile again until the methods land in Step 6 — that's fine mid-task; the whole-workspace build check comes in Step 8.

- [ ] **Step 4: Write failing store tests (testcontainers)**

Create `storage/postgres/pg_test.go`:

```go
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

	if err := st.MarkStale(ctx, []model.Resource{res}); err != nil {
		t.Fatal(err)
	}
	_, _, seq1, since1, _ := row(t, res)
	if seq1 != 1 || since1 == nil {
		t.Fatalf("after first mark: seq=%d since=%v", seq1, since1)
	}

	time.Sleep(10 * time.Millisecond)
	if err := st.MarkStale(ctx, []model.Resource{res}); err != nil {
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

func TestBeginBuild_BumpsBuildIdx_ReturnsStaleSeq(t *testing.T) {
	ctx := context.Background()
	st := NewStore(testPool)
	res := model.Resource{Type: "bb", Id: "1"}

	if err := st.MarkStale(ctx, []model.Resource{res}); err != nil {
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

	if err := st.MarkStale(ctx, []model.Resource{res}); err != nil { // seq=1
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
	_ = st.MarkStale(ctx, []model.Resource{res}) // seq=2
	_ = st.MarkStale(ctx, []model.Resource{res}) // seq=3
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
	_ = st.MarkStale(ctx, []model.Resource{res2}) // seq=1, deleted=false
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

	_ = st.MarkStale(ctx, []model.Resource{oldRes})
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
	_ = st.MarkStale(ctx, []model.Resource{newRes}) // fresh mark, must be excluded by cutoff

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
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./storage/postgres/ -run 'TestMarkStale|TestBeginBuild|TestClearStale|TestMarkDeleted|TestUpsert|TestDeleteResourceIfSeq|TestListStale' -v`
Expected: compile FAIL — `st.MarkStale undefined` etc.

- [ ] **Step 6: Implement the store methods**

Append to `storage/postgres/pg.go` (add `"time"` import) and **modify UpsertResource**:

```go
// MarkStale durably records build intent for the given resources: bump
// stale_seq and set stale_since — keeping the OLDEST timestamp, so
// "stale for too long" measures the oldest unserved change.
func (s *Store) MarkStale(ctx context.Context, resources []model.Resource) error {
	if len(resources) == 0 {
		return nil
	}
	types := make([]string, len(resources))
	ids := make([]string, len(resources))
	for i, r := range resources {
		types[i] = r.Type
		ids[i] = r.Id
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO resources (type, id, stale_seq, stale_since)
		 SELECT t, i, 1, now() FROM unnest($1::text[], $2::text[]) AS x(t, i)
		 ON CONFLICT (type, id) DO UPDATE
		 SET stale_seq = resources.stale_seq + 1,
		     stale_since = COALESCE(resources.stale_since, now())`,
		types, ids,
	)
	return err
}

// MarkDeleted tombstones the resource: deleted=true plus a stale mark, so the
// sweep retries an inline delete that failed or was shed. version resets to 0
// so a later re-create is never rejected as stale against the old lifecycle.
func (s *Store) MarkDeleted(ctx context.Context, resource model.Resource) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, deleted, stale_seq, stale_since)
		 VALUES ($1, $2, true, 1, now())
		 ON CONFLICT (type, id) DO UPDATE
		 SET deleted = true,
		     version = 0,
		     stale_seq = resources.stale_seq + 1,
		     stale_since = COALESCE(resources.stale_since, now())
		 RETURNING stale_seq`,
		resource.Type, resource.Id,
	).Scan(&seq)
	return seq, err
}

// BeginBuild atomically bumps the Build Sequence (ES external_gte OCC version)
// and captures the current stale_seq for the race-safe ClearStale at the end
// of the build.
func (s *Store) BeginBuild(ctx context.Context, resource model.Resource) (int64, int64, error) {
	var buildIdx, staleSeq int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO resources (type, id, build_idx)
		 VALUES ($1, $2, 1)
		 ON CONFLICT (type, id) DO UPDATE
		 SET build_idx = resources.build_idx + 1
		 RETURNING build_idx, stale_seq`,
		resource.Type, resource.Id,
	).Scan(&buildIdx, &staleSeq)
	if err != nil {
		return 0, 0, err
	}
	return buildIdx, staleSeq, nil
}

// ClearStale clears the stale mark only if no newer change arrived since the
// build captured staleSeq. A moved seq makes this a no-op, leaving the row
// stale for the newer change's own build or the sweep.
func (s *Store) ClearStale(ctx context.Context, resource model.Resource, staleSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE resources SET stale_since = NULL
		 WHERE type=$1 AND id=$2 AND stale_seq=$3`,
		resource.Type, resource.Id, staleSeq,
	)
	return err
}

// DeleteResourceIfSeq hard-deletes a tombstoned row, guarded by stale_seq so a
// concurrent re-create (which bumps the seq) wins over the in-flight delete.
func (s *Store) DeleteResourceIfSeq(ctx context.Context, resource model.Resource, staleSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM resources WHERE type=$1 AND id=$2 AND stale_seq=$3 AND deleted`,
		resource.Type, resource.Id, staleSeq,
	)
	return err
}

// ListStale returns up to limit resources whose stale mark is older than
// before, oldest first, including delete tombstones.
func (s *Store) ListStale(ctx context.Context, before time.Time, limit int) ([]core.StaleResource, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT type, id, stale_seq, deleted FROM resources
		 WHERE stale_since IS NOT NULL AND stale_since < $1
		 ORDER BY stale_since
		 LIMIT $2`,
		before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []core.StaleResource
	for rows.Next() {
		var e core.StaleResource
		if err := rows.Scan(&e.Type, &e.Id, &e.StaleSeq, &e.Deleted); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
```

And change `UpsertResource` so re-creates clear tombstones:

```go
func (s *Store) UpsertResource(ctx context.Context, resource model.Resource, version int64) error {
	if version == 0 {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO resources (type, id) VALUES ($1, $2)
			 ON CONFLICT (type, id) DO UPDATE SET deleted = false`,
			resource.Type, resource.Id,
		)
		return err
	}

	tag, err := s.pool.Exec(ctx,
		`INSERT INTO resources (type, id, version) VALUES ($1, $2, $3)
		 ON CONFLICT (type, id) DO UPDATE SET version = EXCLUDED.version, deleted = false
		 WHERE resources.version < EXCLUDED.version`,
		resource.Type, resource.Id, version,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return core.ErrStaleVersion
	}
	return nil
}
```

- [ ] **Step 7: Add testcontainers to the root go.mod**

`GOEXPERIMENT=jsonv2 go get github.com/testcontainers/testcontainers-go github.com/testcontainers/testcontainers-go/modules/postgres` (match the versions `app/go.mod` already uses, to keep the workspace consistent).

- [ ] **Step 8: Run tests to verify they pass (Docker required)**

Run: `GOEXPERIMENT=jsonv2 go test ./storage/postgres/ -v`
Expected: all PASS.
Also: `GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test ./core/...` — PASS.

- [ ] **Step 9: Commit**

```bash
git add storage/postgres/ core/store.go core/build_cancel_test.go go.mod go.sum
git commit -m "feat(store): stale-mark primitives — mark/clear/tombstone/list with seq-guarded clearing"
```

---

### Task 3: In-process build pool

**Files:**
- Create: `core/pool.go`
- Create: `core/pool_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (Task 6 uses these exact unexported names inside package `core`):

```go
newBuildPool(size int) *buildPool
(p *buildPool) trySubmit(ctx context.Context, wait time.Duration, task func(context.Context)) bool
(p *buildPool) waitIdle(ctx context.Context) error
(p *buildPool) shutdown(ctx context.Context) error
```

- [ ] **Step 1: Write failing tests**

Create `core/pool_test.go`:

```go
package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_RunsSubmittedTask(t *testing.T) {
	p := newBuildPool(2)
	var ran atomic.Bool
	ok := p.trySubmit(context.Background(), time.Second, func(context.Context) {
		ran.Store(true)
	})
	if !ok {
		t.Fatal("submit must succeed on an empty pool")
	}
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("task did not run")
	}
}

func TestPool_ShedsWhenSaturated(t *testing.T) {
	p := newBuildPool(1)
	block := make(chan struct{})
	if !p.trySubmit(context.Background(), time.Second, func(context.Context) { <-block }) {
		t.Fatal("first submit must succeed")
	}
	ok := p.trySubmit(context.Background(), 20*time.Millisecond, func(context.Context) {
		t.Error("shed task must never run")
	})
	if ok {
		t.Fatal("submit on a saturated pool must shed after the wait budget")
	}
	close(block)
	_ = p.waitIdle(t.Context())
}

func TestPool_WaitIdle_CoversCascadedSubmits(t *testing.T) {
	p := newBuildPool(2)
	var childRan atomic.Bool
	p.trySubmit(context.Background(), time.Second, func(ctx context.Context) {
		// A task submits follow-up work (parent cascade) before finishing.
		p.trySubmit(context.Background(), time.Second, func(context.Context) {
			time.Sleep(20 * time.Millisecond)
			childRan.Store(true)
		})
	})
	if err := p.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !childRan.Load() {
		t.Fatal("waitIdle returned before the cascaded task finished")
	}
}

func TestPool_TaskRunsOnPoolContextNotCallerContext(t *testing.T) {
	p := newBuildPool(1)
	callerCtx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	p.trySubmit(callerCtx, time.Second, func(taskCtx context.Context) {
		cancel() // caller's ctx dies (RPC returned) while the task runs
		got <- taskCtx.Err()
	})
	if err := <-got; err != nil {
		t.Fatalf("task ctx must outlive the caller's ctx, got %v", err)
	}
	_ = p.waitIdle(t.Context())
}

func TestPool_ShutdownDrainsAndRejects(t *testing.T) {
	p := newBuildPool(1)
	var ran atomic.Bool
	p.trySubmit(context.Background(), time.Second, func(context.Context) {
		time.Sleep(30 * time.Millisecond)
		ran.Store(true)
	})
	if err := p.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !ran.Load() {
		t.Fatal("shutdown must wait for in-flight tasks")
	}
	if p.trySubmit(context.Background(), time.Second, func(context.Context) {}) {
		t.Fatal("submit after shutdown must be rejected")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestPool -v`
Expected: compile FAIL — `newBuildPool undefined`.

- [ ] **Step 3: Implement the pool**

Create `core/pool.go`:

```go
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// buildPool is the bounded in-process executor for inline builds and deletes.
// It is an accelerator, not a guarantee: callers must durably mark work
// (stale mark / tombstone) BEFORE submitting, so anything the pool sheds or
// loses to a crash is recovered by the stale sweep. See ADR 0008.
type buildPool struct {
	sem     chan struct{}
	wg      sync.WaitGroup
	pending atomic.Int64
	closed  atomic.Bool

	// baseCtx is the context tasks run under. It is independent of any
	// caller's context — inline work must outlive the RPC that triggered it —
	// and is cancelled only when shutdown stops waiting.
	baseCtx context.Context
	cancel  context.CancelFunc
}

func newBuildPool(size int) *buildPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &buildPool{
		sem:     make(chan struct{}, size),
		baseCtx: ctx,
		cancel:  cancel,
	}
}

// trySubmit runs task on the pool if a slot frees up within wait. It returns
// false — without running the task — when the pool stays saturated, the
// caller's ctx ends, or the pool is shut down. Tasks may call trySubmit
// themselves (cascades); because this never blocks past wait, a full pool
// sheds instead of deadlocking.
func (p *buildPool) trySubmit(ctx context.Context, wait time.Duration, task func(context.Context)) bool {
	if p.closed.Load() {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case p.sem <- struct{}{}:
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}

	if p.closed.Load() {
		<-p.sem
		return false
	}

	p.pending.Add(1)
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.pending.Add(-1)
			p.wg.Done()
		}()
		task(p.baseCtx)
	}()
	return true
}

// waitIdle blocks until no tasks are queued or running. Tasks submit their
// follow-up work (parent cascades, drift re-builds) before finishing, so
// pending only reaches zero once a whole cascade has settled.
func (p *buildPool) waitIdle(ctx context.Context) error {
	for {
		if p.pending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// shutdown stops accepting work and waits for in-flight tasks until ctx ends,
// then cancels any stragglers. Unfinished work stays stale and is swept.
func (p *buildPool) shutdown(ctx context.Context) error {
	p.closed.Store(true)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestPool -v -race`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add core/pool.go core/pool_test.go
git commit -m "feat(core): bounded in-process build pool with shed-on-saturation and idle tracking"
```

---

### Task 4: Proto — RebuildResponse carries workflow IDs

**Files:**
- Modify: `proto/index/v1/index.proto:72`
- Regenerate: `app/gen/` (via `buf generate`)

- [ ] **Step 1: Add the field**

Change line 72 of `proto/index/v1/index.proto` from `message RebuildResponse {}` to:

```proto
message RebuildResponse {
  // IDs of the Temporal RebuildWalk workflows started, one per selector.
  // Operators can inspect/retry/cancel them in the Temporal UI.
  repeated string workflow_ids = 1;
}
```

- [ ] **Step 2: Regenerate bindings and verify**

```bash
buf generate
GOEXPERIMENT=jsonv2 go build ./... && cd app && GOEXPERIMENT=jsonv2 go build ./... && cd ..
```

Expected: builds pass (new field is additive; nothing reads it yet).

- [ ] **Step 3: Commit**

```bash
git add proto/ app/gen/
git commit -m "feat(proto): RebuildResponse returns started workflow IDs"
```

---

### Task 5: Docs — ADR 0008, amendments, CONTEXT.md, CLAUDE.md

**Files:**
- Create: `docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md`
- Modify: `docs/adr/0001-core-is-a-library-the-app-is-one-assembly.md` (status note)
- Modify: `docs/adr/0002-distributed-safety-via-occ-and-drift-check-not-locks.md` (status note)
- Modify: `CONTEXT.md` (new vocabulary)
- Modify: `CLAUDE.md` (commands + architecture sections)

These document the **target** state; they land in Wave 1 so later tasks can reference ADR 0008 in code comments. Content must follow the structure/tone of the existing ADRs (read 0002 and 0006 first).

- [ ] **Step 1: Write ADR 0008**

Create `docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md` covering, in the repo's existing ADR format (Status/Context/Decision/Consequences):

- Decision: notifications mark resources stale in Postgres (durable intent: `stale_seq` bump + `stale_since` COALESCE), then build immediately on an in-process bounded pool; every internal re-build signal (fanout, drift re-build, ADR 0006 parent cascade) uses the same mark-first primitive; a Temporal-scheduled StaleSweep rebuilds anything stale past a threshold; explicit rebuilds run as Temporal RebuildWalk workflows; River is removed.
- The race-safe clear: builds capture `stale_seq` at `BeginBuild` and clear only if unchanged.
- Delete tombstones: `deleted=true` + stale mark; hard-delete guarded by seq; `MarkDeleted` resets `version` to 0 so re-creates are never version-stale.
- Failure isolation: Temporal down degrades recovery latency only, never hot-path throughput or correctness.
- Consequences: core now depends on Postgres/ES/Temporal directly (partial reversal of ADR 0001's module split); `storage/postgres` + `backend/elasticsearch` are root-module packages; at-least-once now spans mark→sweep instead of River retries (amends ADR 0002's re-enqueue leg); known limitation — a resource whose type is removed from config stays stale forever (logged by the sweep).

- [ ] **Step 2: Add status notes to ADR 0001 and 0002**

At the top of each (below the title/status line), add a one-line amendment, e.g. for 0001: `Amended by [ADR 0008](0008-stale-mark-inline-builds-and-temporal-slow-lane.md): the storage and search-backend implementations were folded into the core module, and Temporal became a core dependency; the core-as-library principle stands.` For 0002: `Amended by ADR 0008: the at-least-once re-enqueue leg is now the stale mark + sweep instead of River retries; Build Sequence OCC and the drift check are unchanged.`

- [ ] **Step 3: Add CONTEXT.md vocabulary**

Add entries (matching CONTEXT.md's existing style) for: **Stale mark** (durable record of build intent; `stale_seq`/`stale_since`), **Sweep** (Temporal-scheduled recovery pass rebuilding resources stale past a threshold), **Tombstone** (deleted-flagged resource row awaiting ES cleanup), **Inline build** (pool-executed immediate build; the accelerator). Update the existing **Build vs Rebuild** / at-least-once wording where it references River jobs.

- [ ] **Step 4: Update CLAUDE.md**

- Commands: note `go test ./storage/...` and `./backend/...` need Docker (testcontainers); remove River references.
- Module table: root module now includes `storage/postgres` and `backend/elasticsearch` packages; only `aggregation` and `app` remain separate modules.
- Core Data Flow: replace "enqueues River jobs / River workers" with mark-stale → inline pool build → Temporal sweep/rebuild lane.
- Critical Invariants: add the mark-first rule and seq-guarded clear.

- [ ] **Step 5: Commit**

```bash
git add docs/ CONTEXT.md CLAUDE.md
git commit -m "docs: ADR 0008 stale-mark inline builds + Temporal slow lane; amend 0001/0002; vocabulary"
```

---

### Task 6: Hot-path rewire — mark-first inline builds, de-River core and suite

**Files:**
- Modify: `core/indexer.go` (drop River, add pool config/fields, Shutdown/WaitForIdle)
- Modify: `core/register.go` (mark-first + inline submit)
- Create: `core/schedule.go` (`scheduleBuild`)
- Modify: `core/build.go` (BeginBuild/ClearStale, scheduleBuild for drift + parents, move BuildArgs/RebuildArgs here)
- Modify: `core/handle.go` (add `deleteOne`)
- Modify: `core/rebuild.go` (`Rebuild` → synchronous `RebuildNow`; Task 7 adds the workflow-based `Rebuild`)
- Delete: `core/worker.go`
- Modify: `core/store.go` + `storage/postgres/pg.go` + `core/build_cancel_test.go` (remove `NextRebuildCounter`, `DeleteResource`)
- Modify: `core/rebuild_test.go` (rename calls to `RebuildNow`)
- Create: `core/register_test.go` (new unit tests)
- Modify: `app/server/index.go` (call `RebuildNow` — temporary until Task 7)
- Modify: `app/tests/suite_test.go`, `app/tests/drainer_test.go` (de-River the suite), plus `Rebuild(` → `RebuildNow(` call sites in `app/tests/index_test.go`
- Modify: root `go.mod` (drop river requires after code stops importing it)

**Interfaces:**
- Consumes: Task 2's Store methods, Task 3's `buildPool`.
- Produces:
  - `Config{PoolSize int, SubmitWait time.Duration}` (defaults 10 / 250ms; `RiverClient` and `SetRiverClient` are gone)
  - `(idx *Indexer) Shutdown(ctx) error`, `(idx *Indexer) WaitForIdle(ctx) error`
  - `(idx *Indexer) RebuildNow(ctx, []ResourceSelector) error` (synchronous; Task 7's activity calls it)
  - `(idx *Indexer) scheduleBuild(ctx, roots []model.Resource, metadata map[string]string) error`
  - `(idx *Indexer) deleteOne(ctx, res model.Resource, staleSeq int64)` (logs failures, never returns error)
  - `BuildArgs`/`RebuildArgs` now live in `core/build.go`; `DeleteArgs`, all River workers, and `RegisterWorkers` are deleted.

- [ ] **Step 1: Write failing unit tests for the new hot path**

Create `core/register_test.go`:

```go
package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// recordingStore records the order of Store calls and serves canned data.
type recordingStore struct {
	mu      sync.Mutex
	calls   []string
	parents []model.Resource
	drift   atomic.Bool // one-shot: report drift on the first AnyResourceVersionDrifted
}

func (s *recordingStore) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *recordingStore) callsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *recordingStore) indexOf(prefix string) int {
	for i, c := range s.callsSnapshot() {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			return i
		}
	}
	return -1
}

func (s *recordingStore) MarkStale(_ context.Context, rs []model.Resource) error {
	s.record("MarkStale:%d", len(rs))
	return nil
}
func (s *recordingStore) MarkDeleted(_ context.Context, r model.Resource) (int64, error) {
	s.record("MarkDeleted:%s/%s", r.Type, r.Id)
	return 7, nil
}
func (s *recordingStore) BeginBuild(_ context.Context, r model.Resource) (int64, int64, error) {
	s.record("BeginBuild:%s/%s", r.Type, r.Id)
	return 1, 3, nil
}
func (s *recordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.record("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}
func (s *recordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, seq int64) error {
	s.record("DeleteResourceIfSeq:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}
func (s *recordingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
}
func (s *recordingStore) AddChildResources(context.Context, model.Resource, []model.Resource) error {
	return nil
}
func (s *recordingStore) AddRelations(context.Context, []Relation) error { return nil }
func (s *recordingStore) AnyResourceVersionDrifted(context.Context, []model.VersionedResource) (bool, error) {
	return s.drift.Swap(false), nil
}
func (s *recordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}
func (s *recordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return s.parents, nil
}
func (s *recordingStore) RemoveResource(_ context.Context, r model.Resource) error {
	s.record("RemoveResource:%s/%s", r.Type, r.Id)
	return nil
}
func (s *recordingStore) UpsertResource(_ context.Context, r model.Resource, v int64) error {
	s.record("UpsertResource:%s/%s:%d", r.Type, r.Id, v)
	return nil
}

func newHotPathIndexer(st Store, poolSize int, submitWait time.Duration) *Indexer {
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
	}
	return New(Config{
		Resources: testResources(),
		Plans: map[string][]projection.Plan{
			"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
		},
		ES:         &fakeBackend{},
		Store:      st,
		PoolSize:   poolSize,
		SubmitWait: submitWait,
	})
}

func TestRegisterChange_MarksStaleBeforeBuilding_ThenClears(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, time.Second)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	mark, begin, clear := st.indexOf("MarkStale"), st.indexOf("BeginBuild"), st.indexOf("ClearStale:product/1:3")
	if mark == -1 || begin == -1 || clear == -1 {
		t.Fatalf("missing calls: %v", st.callsSnapshot())
	}
	if mark > begin {
		t.Fatalf("MarkStale must land before the build starts: %v", st.callsSnapshot())
	}
	if clear < begin {
		t.Fatalf("ClearStale must follow the build: %v", st.callsSnapshot())
	}
}

func TestRegisterChange_PoolSaturated_ShedsButReturnsSuccess(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 1, 20*time.Millisecond)

	// Occupy the only slot.
	block := make(chan struct{})
	if !idx.pool.trySubmit(context.Background(), time.Second, func(context.Context) { <-block }) {
		t.Fatal("failed to occupy pool")
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatalf("shed must not surface as an RPC error: %v", err)
	}
	if st.indexOf("MarkStale") == -1 {
		t.Fatal("the stale mark must land even when the build is shed")
	}
	if st.indexOf("BeginBuild") != -1 {
		t.Fatal("no build may start on a saturated pool")
	}
	close(block)
	_ = idx.WaitForIdle(t.Context())
}

func TestRegisterChange_Delete_TombstonesAndRunsInlineDelete(t *testing.T) {
	st := &recordingStore{}
	idx := newHotPathIndexer(st, 2, time.Second)

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeDeleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if st.indexOf("MarkDeleted:product/1") == -1 {
		t.Fatalf("delete must tombstone first: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/1:7") == -1 {
		t.Fatalf("inline delete must finish the tombstone with the captured seq: %v", st.callsSnapshot())
	}
}

func TestBuildOne_Drift_RemarksStale_SoGuardedClearIsNoop(t *testing.T) {
	st := &recordingStore{}
	st.drift.Store(true) // first drift check reports drift
	idx := newHotPathIndexer(st, 2, time.Second)

	// Plan must emit relations for the drift check to run.
	doc := projection.BuildDoc{
		Root: model.Resource{Type: "product", Id: "1"},
		Doc:  map[string]any{"fields": map[string]any{"title": "t"}},
		Relations: []model.VersionedResource{
			{Resource: model.Resource{Type: "product", Id: "child"}, Version: 1},
		},
	}
	idx.plans = map[string][]projection.Plan{
		"product": {{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{doc}}}},
	}

	err := idx.RegisterChange(context.Background(), Notification{
		ResourceType: "product", ResourceID: "1", Kind: ChangeUpdated, Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Drift must re-mark (MarkStale at least twice: once for the notification,
	// once for the drift re-schedule) and trigger a second build.
	calls := st.callsSnapshot()
	var marks, begins int
	for _, c := range calls {
		if len(c) >= 9 && c[:9] == "MarkStale" {
			marks++
		}
		if len(c) >= 10 && c[:10] == "BeginBuild" {
			begins++
		}
	}
	if marks < 2 || begins < 2 {
		t.Fatalf("drift must re-mark and re-build (marks=%d begins=%d): %v", marks, begins, calls)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestRegisterChange|TestBuildOne_Drift' -v`
Expected: compile FAIL (`unknown field PoolSize`, `idx.pool undefined`, `WaitForIdle undefined`).

- [ ] **Step 3: Rewire core/indexer.go**

- Remove imports `github.com/jackc/pgx/v5` and `github.com/riverqueue/river`; add `"time"`.
- In `Config`: delete the `RiverClient` field; add:

```go
	// PoolSize bounds the number of concurrent inline builds. Default 10.
	PoolSize int

	// SubmitWait is how long a caller waits for a free build slot before
	// shedding the inline build and leaving the resource stale for the
	// sweep. Default 250ms.
	SubmitWait time.Duration
```

- In `Indexer`: replace `river *river.Client[pgx.Tx]` with:

```go
	pool       *buildPool
	submitWait time.Duration
```

- In `New()`: replace `river: cfg.RiverClient,` with pool construction:

```go
const (
	defaultPoolSize   = 10
	defaultSubmitWait = 250 * time.Millisecond
)
```

```go
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultPoolSize
	}
	submitWait := cfg.SubmitWait
	if submitWait <= 0 {
		submitWait = defaultSubmitWait
	}
	idx.pool = newBuildPool(poolSize)
	idx.submitWait = submitWait
```

- Delete `SetRiverClient` entirely, and add:

```go
// Shutdown stops accepting inline work and waits for in-flight builds until
// ctx ends. Unfinished work stays stale and is recovered by the sweep.
func (idx *Indexer) Shutdown(ctx context.Context) error {
	return idx.pool.shutdown(ctx)
}

// WaitForIdle blocks until the inline pool has fully settled, including
// cascaded parent builds and drift re-builds. Intended for tests and
// embedders that need a quiescence point.
func (idx *Indexer) WaitForIdle(ctx context.Context) error {
	return idx.pool.waitIdle(ctx)
}
```

(`"context"` import needed.)

- [ ] **Step 4: Create core/schedule.go**

```go
package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
)

// scheduleBuild durably marks roots stale, then opportunistically builds them
// inline on the pool. The mark always lands before the build attempt, so a
// shed submission, failed build, or crash is recovered by the stale sweep.
// See ADR 0008.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource, metadata map[string]string) error {
	if len(roots) == 0 {
		return nil
	}

	if err := idx.st.MarkStale(ctx, roots); err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}

	for resourceType, ids := range groupResourceIDsByType(roots) {
		args := BuildArgs{ResourceType: resourceType, ResourceIds: ids, Metadata: metadata}
		submitted := idx.pool.trySubmit(ctx, idx.submitWait, func(taskCtx context.Context) {
			if err := idx.Build(taskCtx, args); err != nil {
				slog.Warn("inline build failed; resources remain stale for sweep",
					slog.String("type", args.ResourceType),
					slog.String("error", err.Error()),
				)
			}
		})
		if !submitted {
			slog.Info("build pool saturated; resources left stale for sweep",
				slog.String("type", resourceType),
				slog.Int("count", len(ids)),
			)
		}
	}

	return nil
}
```

- [ ] **Step 5: Rewrite core/register.go**

Replace the body after the `slog.Info("registering change", ...)` block (keep validation, the upsert/markDeleted split, parent lookup, and logging):

```go
func (idx *Indexer) RegisterChange(ctx context.Context, n Notification) error {
	if err := idx.verifyResourceConfig(n); err != nil {
		return err
	}

	res := model.Resource{Type: n.ResourceType, Id: n.ResourceID}

	var deleteSeq int64
	if n.Kind == ChangeDeleted {
		seq, err := idx.st.MarkDeleted(ctx, res)
		if err != nil {
			return fmt.Errorf("mark deleted %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
		deleteSeq = seq
	} else {
		if err := idx.st.UpsertResource(ctx, res, n.Version); err != nil {
			return fmt.Errorf("upsert resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	}

	parents, err := idx.st.GetParentResources(ctx, res)
	if err != nil {
		return fmt.Errorf("getting parents: %w", err)
	}

	slog.Info("registering change",
		"resource_type", n.ResourceType,
		"resource_id", n.ResourceID,
		"kind", n.Kind.String(),
		"affected_parents", len(parents),
	)

	roots := make([]model.Resource, 0, len(parents)+1)
	roots = append(roots, parents...)

	if n.Kind == ChangeDeleted {
		if !idx.pool.trySubmit(ctx, idx.submitWait, func(taskCtx context.Context) {
			idx.deleteOne(taskCtx, res, deleteSeq)
		}) {
			slog.Info("pool saturated; tombstone left for sweep",
				slog.String("type", res.Type), slog.String("id", res.Id))
		}
	} else {
		roots = append(roots, res)
	}

	return idx.scheduleBuild(ctx, roots, n.Metadata)
}
```

(`groupResourceIDsByType` stays in this file unchanged.)

- [ ] **Step 6: Add deleteOne to core/handle.go**

```go
// deleteOne removes the resource's documents and edges, then hard-deletes the
// tombstoned row if no newer change arrived. Failures are logged, not
// returned: the tombstone stays stale and the sweep retries it.
func (idx *Indexer) deleteOne(ctx context.Context, res model.Resource, staleSeq int64) {
	if err := idx.handleDelete(ctx, RebuildPayload{ResourceType: res.Type, ResourceID: res.Id}); err != nil {
		slog.Warn("inline delete failed; tombstone remains for sweep",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
		return
	}
	if err := idx.st.DeleteResourceIfSeq(ctx, res, staleSeq); err != nil {
		slog.Warn("tombstone cleanup failed; sweep will retry",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
	}
}
```

- [ ] **Step 7: Rework core/build.go**

1. Move `BuildArgs` and `RebuildArgs` structs (unchanged, with their json tags) from `core/worker.go` into the top of `core/build.go`; then `rm core/worker.go` (`DeleteArgs`, all workers, `RegisterWorkers` die with it).
2. In `Build`, per-ID loop becomes:

```go
	for _, id := range params.ResourceIds {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		res := model.Resource{Type: params.ResourceType, Id: id}
		occVersion, staleSeq, err := idx.st.BeginBuild(ctx, res)
		if err != nil {
			logger.Warn("failed to begin build", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}

		// TODO: Build multiple documents in a batch.
		if err := idx.buildOne(ctx, plans, params.ResourceType, id, params.Metadata, occVersion); err != nil {
			logger.Warn("build failed", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}

		// Race-safe: a no-op if a newer change bumped stale_seq mid-build —
		// including buildOne's own drift re-mark, which must survive this clear.
		if err := idx.st.ClearStale(ctx, res, staleSeq); err != nil {
			logger.Warn("clear stale failed; sweep may rebuild redundantly",
				slog.String("id", id), slog.String("error", err.Error()))
		}
	}
```

3. In `buildOne`, replace the `enqueueParents` call and comment block with:

```go
	// Reverse-relation discovery (ADR 0006): mark-first schedule of the
	// Parents the Plan derived from the Child's own data.
	if err := idx.scheduleBuild(ctx, builtDoc.Parents, metadata); err != nil {
		return err
	}
```

and delete the `enqueueParents` function. Replace the drift re-enqueue (the `idx.river.Insert(...)` inside the drift block) with:

```go
			if err := idx.scheduleBuild(ctx, []model.Resource{{Type: resourceType, Id: resourceID}}, metadata); err != nil {
				return fmt.Errorf("re-schedule after drift for %s/%s: %w", resourceType, resourceID, err)
			}
```

4. In `rebuildByIDs`: replace the `NextRebuildCounter` call with `BeginBuild` (track both values), and clear marks for fully-succeeded IDs:

```go
		occVersion, staleSeq, err := idx.st.BeginBuild(ctx, root)
		if err != nil {
			logger.Warn("failed to begin build", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}
		staleSeqs[id] = staleSeq
```

(add `staleSeqs := make(map[string]int64)` beside `resourceRelations`; in the final relations-persist loop, after a successful `AddChildResources`, add:)

```go
		if err := idx.st.ClearStale(ctx, model.Resource{Type: params.ResourceType, Id: id}, staleSeqs[id]); err != nil {
			logger.Warn("clear stale failed", slog.String("id", id), slog.String("error", err.Error()))
		}
```

5. In `rebuildAll`: same substitution — `occVersion, staleSeq, err := idx.st.BeginBuild(ctx, doc.Root)`, store `staleSeqs[id] = staleSeq` next to `occVersions[id] = occVersion`, and clear in the final relations loop exactly as in rebuildByIDs.

- [ ] **Step 8: Make Rebuild synchronous (interim) in core/rebuild.go**

Rename `Rebuild` to `RebuildNow` and replace the enqueue loop with direct execution (Task 7 adds the workflow-starting `Rebuild`):

```go
// RebuildNow synchronously rebuilds the selected resources in-process. It is
// the body of the RebuildWalk activity, and is exported for embedders and
// tests that want rebuild semantics without Temporal.
func (idx *Indexer) RebuildNow(ctx context.Context, selectors []ResourceSelector) error {
	if err := idx.validateSelectors(selectors); err != nil {
		return err
	}
	for _, sel := range selectors {
		if err := idx.rebuild(ctx, RebuildArgs{
			ResourceType: sel.ResourceType,
			Versions:     sel.Versions,
			ResourceIDs:  sel.ResourceIDs,
		}); err != nil {
			return fmt.Errorf("rebuild %s: %w", sel.ResourceType, err)
		}
	}
	return nil
}

func (idx *Indexer) validateSelectors(selectors []ResourceSelector) error {
	if len(selectors) == 0 {
		return &InvalidArgumentError{Msg: "at least one selector is required"}
	}
	for _, sel := range selectors {
		cfg := idx.resources.Get(sel.ResourceType)
		if cfg == nil {
			return fmt.Errorf("resource type %q: %w", sel.ResourceType, ErrUnknownResource)
		}
		for _, v := range sel.Versions {
			if cfg.GetVersion(v) == nil {
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q has no version %d", sel.ResourceType, v)}
			}
		}
	}
	return nil
}
```

Update `core/rebuild_test.go`: change every `idx.Rebuild(` to `idx.RebuildNow(`.

- [ ] **Step 9: Drop the superseded Store methods**

Remove `NextRebuildCounter` and `DeleteResource` from `core/store.go`, `storage/postgres/pg.go`, and their stubs in `core/build_cancel_test.go`. Grep to confirm no remaining callers:

```bash
grep -rn "NextRebuildCounter\|DeleteResource\b" --include='*.go' . | grep -v DeleteResourceIfSeq
```

Expected: no hits outside comments/tests you just fixed (note: `app/tests/index_test.go` has a comment mentioning NextRebuildCounter — update the comment to say BeginBuild).

- [ ] **Step 10: Update app/server/index.go (temporary)**

Change `s.idx.Rebuild(ctx, selectors)` to `s.idx.RebuildNow(ctx, selectors)` in the `Rebuild` handler (Task 7 replaces this with the workflow-starting call).

- [ ] **Step 11: De-River the integration suite**

`app/tests/drainer_test.go` — full replacement:

```go
package tests

import (
	"context"
	"fmt"

	"github.com/theleeeo/laika/core"
)

// drainer waits until the indexer's inline build pool has fully settled —
// including cascaded parent builds and drift re-builds.
type drainer struct {
	idx *core.Indexer
}

func (d *drainer) Drain(ctx context.Context) {
	if err := d.idx.WaitForIdle(ctx); err != nil {
		panic(fmt.Errorf("wait for idle: %w", err))
	}
}
```

`app/tests/suite_test.go`:
- Remove imports: `river`, `riverpgxv5`, `rivermigrate`.
- Remove the "Apply River migrations" block, the `workers := river.NewWorkers()` / `RegisterWorkers` / `river.NewClient` / `SetRiverClient` / `riverClient.Start` block, and the `cancelWorker` field + its TearDown usage; remove the river stop in `TearDownSuite`.
- Change the suite field `worker *riverDrainer` to `worker *drainer`.
- In `core.New(core.Config{...})` add `PoolSize: 10, SubmitWait: 30 * time.Second,` — the generous wait makes shedding impossible so `Drain` is deterministic in tests.
- After constructing `t.idx`: `t.worker = &drainer{idx: t.idx}`.

`app/tests/index_test.go`: change `t.idx.Rebuild(` → `t.idx.RebuildNow(` everywhere (7 call sites).

- [ ] **Step 12: Remove river from the root go.mod**

Delete the `github.com/riverqueue/river` require (and its `riverdriver`/`rivershared`/`rivertype` indirects) from the root `go.mod`; leave `app/go.mod` alone (the app module no longer imports river either after Step 11, but its go.mod cleanup happens in Task 10).

- [ ] **Step 13: Run all tests**

```bash
GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test ./core/... ./storage/... ./backend/... -race
cd app && GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test ./server/... ./dsl/... && GOEXPERIMENT=jsonv2 go test ./tests/... && cd ..
```

Expected: all PASS (integration tests need Docker). Timing-sensitive suite tests that relied on queue ordering may need their `Drain` calls kept exactly where they are — investigate any failure individually before touching assertions.

- [ ] **Step 14: Commit**

```bash
git add -A
git commit -m "feat(core)!: inline mark-first builds on in-process pool; remove River from the hot path"
```

---

### Task 7: Temporal slow lane — sweep + rebuild workflows

**Files:**
- Create: `core/sweep.go`, `core/sweep_test.go`
- Create: `core/temporal.go`, `core/temporal_test.go`
- Modify: `core/indexer.go` (Config.Temporal, Config.TaskQueue, fields)
- Modify: `core/rebuild.go` (add workflow-starting `Rebuild`)
- Modify: `core/rebuild_test.go` (add Rebuild workflow test)
- Modify: `app/server/index.go` (Rebuild handler returns workflow IDs)
- Modify: root `go.mod` (add temporal SDK)

**Interfaces:**
- Consumes: `SweepStale` prerequisites from Task 2 (`ListStale`), `deleteOne`/`Build`/`RebuildNow` from Task 6, proto field from Task 4.
- Produces:

```go
// Config additions (both used by app wiring in Task 8):
Temporal  client.Client // REQUIRED for Rebuild / NewWorker / EnsureSweepSchedule
TaskQueue string        // default core.DefaultTaskQueue = "laika-indexer"

(idx *Indexer) SweepStale(ctx context.Context, threshold time.Duration, limit int) (int, error)
(idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) ([]string, error) // now returns workflow IDs
(idx *Indexer) NewWorker() worker.Worker
(idx *Indexer) EnsureSweepSchedule(ctx context.Context, interval time.Duration, p SweepParams) error
type SweepParams struct{ Threshold time.Duration; BatchSize int }
```

Workflow names: `"StaleSweep"`, `"RebuildWalk"`. Activity names: `"SweepStale"`, `"RunRebuild"`. Schedule ID: `"laika-stale-sweep"`.

- [ ] **Step 1: Add the Temporal SDK**

```bash
GOEXPERIMENT=jsonv2 go get go.temporal.io/sdk@latest
```

- [ ] **Step 2: Write failing SweepStale unit test**

Create `core/sweep_test.go`:

```go
package core

import (
	"context"
	"testing"
	"time"

	"github.com/theleeeo/laika/model"
)

// staleListingStore returns a canned stale backlog on top of recordingStore.
type staleListingStore struct {
	recordingStore
	entries []StaleResource
}

func (s *staleListingStore) ListStale(_ context.Context, before time.Time, limit int) ([]StaleResource, error) {
	s.record("ListStale")
	if len(s.entries) > limit {
		return s.entries[:limit], nil
	}
	return s.entries, nil
}

func TestSweepStale_RebuildsMarksAndFinishesTombstones(t *testing.T) {
	st := &staleListingStore{
		entries: []StaleResource{
			{Resource: model.Resource{Type: "product", Id: "1"}, StaleSeq: 4},
			{Resource: model.Resource{Type: "product", Id: "gone"}, StaleSeq: 9, Deleted: true},
		},
	}
	idx := newHotPathIndexer(st, 2, time.Second)

	n, err := idx.SweepStale(context.Background(), 5*time.Minute, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("swept count: got %d want 2", n)
	}
	if st.indexOf("BeginBuild:product/1") == -1 {
		t.Fatalf("live stale entry must be rebuilt: %v", st.callsSnapshot())
	}
	if st.indexOf("DeleteResourceIfSeq:product/gone:9") == -1 {
		t.Fatalf("tombstone must be finished with its listed seq: %v", st.callsSnapshot())
	}
}

func TestSweepStale_EmptyBacklog(t *testing.T) {
	st := &staleListingStore{}
	idx := newHotPathIndexer(st, 2, time.Second)
	n, err := idx.SweepStale(context.Background(), time.Minute, 100)
	if err != nil || n != 0 {
		t.Fatalf("got n=%d err=%v", n, err)
	}
}
```

Note: `newHotPathIndexer` takes a `Store`; it was defined in Task 6's `core/register_test.go`.

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestSweepStale -v` — expected: compile FAIL (`SweepStale undefined`).

- [ ] **Step 3: Implement core/sweep.go**

```go
package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SweepStale rebuilds (or finishes deleting) up to limit resources whose
// stale mark is older than threshold, synchronously. It returns the number of
// stale entries it attempted. Per-resource failures are logged, not returned:
// the mark or tombstone survives and the next sweep retries it.
//
// This is the safety net behind the inline build pool (ADR 0008). It runs as
// the body of the StaleSweep Temporal activity; embedders without Temporal
// can drive it from a ticker.
func (idx *Indexer) SweepStale(ctx context.Context, threshold time.Duration, limit int) (int, error) {
	entries, err := idx.st.ListStale(ctx, time.Now().Add(-threshold), limit)
	if err != nil {
		return 0, fmt.Errorf("list stale: %w", err)
	}
	if len(entries) == 0 {
		return 0, nil
	}

	byType := make(map[string][]string)
	for _, e := range entries {
		if e.Deleted {
			idx.deleteOne(ctx, e.Resource, e.StaleSeq)
			continue
		}
		byType[e.Type] = append(byType[e.Type], e.Id)
	}

	for resourceType, ids := range byType {
		if err := idx.Build(ctx, BuildArgs{ResourceType: resourceType, ResourceIds: ids}); err != nil {
			slog.Warn("sweep build failed; resources remain stale",
				slog.String("type", resourceType), slog.String("error", err.Error()))
		}
	}

	slog.Info("stale sweep pass complete", slog.Int("swept", len(entries)))
	return len(entries), nil
}
```

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestSweepStale -v` — expected: PASS. Commit:

```bash
git add core/sweep.go core/sweep_test.go
git commit -m "feat(core): SweepStale — synchronous recovery pass over the stale backlog"
```

- [ ] **Step 4: Write failing workflow tests**

Create `core/temporal_test.go`:

```go
package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestStaleSweepWorkflow_LoopsUntilBacklogDrained(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	counts := []int{100, 100, 3} // two full batches, then a partial one
	i := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, p SweepParams) (int, error) {
		n := counts[i]
		i++
		return n, nil
	}, activity.RegisterOptions{Name: sweepActivityName})

	env.ExecuteWorkflow(StaleSweepWorkflow, SweepParams{Threshold: 5 * time.Minute, BatchSize: 100})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 3, i, "workflow must loop until a pass returns less than a full batch")
}

func TestRebuildWalkWorkflow_RunsSelectorActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var got ResourceSelector
	env.RegisterActivityWithOptions(func(ctx context.Context, sel ResourceSelector) error {
		got = sel
		return nil
	}, activity.RegisterOptions{Name: rebuildActivityName})

	sel := ResourceSelector{ResourceType: "product", ResourceIDs: []string{"1", "2"}}
	env.ExecuteWorkflow(RebuildWalkWorkflow, sel)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, sel, got)
}

// fakeScheduleCreator captures EnsureSweepSchedule's create-if-absent behavior.
type fakeScheduleCreator struct {
	opts []client.ScheduleOptions
	err  error
}

func (f *fakeScheduleCreator) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	f.opts = append(f.opts, options)
	return nil, f.err
}

func TestEnsureSweepSchedule_CreatesWithSkipOverlap(t *testing.T) {
	f := &fakeScheduleCreator{}
	err := ensureSweepSchedule(context.Background(), f, "laika-indexer", time.Minute, SweepParams{Threshold: 5 * time.Minute, BatchSize: 500})
	require.NoError(t, err)
	require.Len(t, f.opts, 1)
	require.Equal(t, sweepScheduleID, f.opts[0].ID)
	require.Equal(t, time.Minute, f.opts[0].Spec.Intervals[0].Every)
}

func TestEnsureSweepSchedule_ToleratesExisting(t *testing.T) {
	f := &fakeScheduleCreator{err: temporalErrScheduleAlreadyRunning()}
	err := ensureSweepSchedule(context.Background(), f, "laika-indexer", time.Minute, SweepParams{})
	require.NoError(t, err, "an already-existing schedule is success")
}
```

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestStaleSweep|TestRebuildWalk|TestEnsureSweep' -v` — expected: compile FAIL.

- [ ] **Step 5: Implement core/temporal.go**

```go
package core

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// DefaultTaskQueue is the Temporal task queue the Indexer's worker polls
	// and its workflows run on.
	DefaultTaskQueue = "laika-indexer"

	sweepScheduleID = "laika-stale-sweep"

	staleSweepWorkflowName  = "StaleSweep"
	rebuildWalkWorkflowName = "RebuildWalk"
	sweepActivityName       = "SweepStale"
	rebuildActivityName     = "RunRebuild"
)

// SweepParams configures one stale-sweep pass.
type SweepParams struct {
	// Threshold: only resources stale for longer than this are swept.
	Threshold time.Duration
	// BatchSize is the maximum number of resources per sweep activity.
	BatchSize int
}

// temporalActivities hosts the Indexer-backed activity implementations.
type temporalActivities struct {
	idx *Indexer
}

func (a *temporalActivities) SweepStale(ctx context.Context, p SweepParams) (int, error) {
	return a.idx.SweepStale(ctx, p.Threshold, p.BatchSize)
}

// RunRebuild executes one rebuild selector synchronously, heartbeating so a
// dead worker is detected and the activity retried on another instance.
// Restart-from-scratch is correct: rebuilds are idempotent.
func (a *temporalActivities) RunRebuild(ctx context.Context, sel ResourceSelector) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return a.idx.RebuildNow(ctx, []ResourceSelector{sel})
}

// StaleSweepWorkflow drains the stale backlog in batches until a pass returns
// fewer than a full batch. Bounded per run; the next scheduled run (overlap
// policy: skip) picks up any remainder.
func StaleSweepWorkflow(ctx workflow.Context, p SweepParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})
	for range 100 {
		var n int
		if err := workflow.ExecuteActivity(ctx, sweepActivityName, p).Get(ctx, &n); err != nil {
			return err
		}
		if n < p.BatchSize {
			return nil
		}
	}
	return nil
}

// RebuildWalkWorkflow runs one rebuild selector as a single long-running,
// heartbeating activity.
func RebuildWalkWorkflow(ctx workflow.Context, sel ResourceSelector) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	return workflow.ExecuteActivity(ctx, rebuildActivityName, sel).Get(ctx, nil)
}

// NewWorker creates a Temporal worker hosting the Indexer's workflows and
// activities. The caller starts and stops it. Every embedder runs the same
// worker, so the sweep safety net has exactly one implementation.
func (idx *Indexer) NewWorker() worker.Worker {
	w := worker.New(idx.temporal, idx.taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(StaleSweepWorkflow, workflow.RegisterOptions{Name: staleSweepWorkflowName})
	w.RegisterWorkflowWithOptions(RebuildWalkWorkflow, workflow.RegisterOptions{Name: rebuildWalkWorkflowName})
	a := &temporalActivities{idx: idx}
	w.RegisterActivityWithOptions(a.SweepStale, activity.RegisterOptions{Name: sweepActivityName})
	w.RegisterActivityWithOptions(a.RunRebuild, activity.RegisterOptions{Name: rebuildActivityName})
	return w
}

// scheduleCreator is the slice of client.ScheduleClient EnsureSweepSchedule
// needs; a seam for testing create-if-absent behavior.
type scheduleCreator interface {
	Create(ctx context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error)
}

// EnsureSweepSchedule idempotently creates the StaleSweep schedule. An
// existing schedule is left untouched — changing interval or params requires
// deleting the schedule in Temporal first.
func (idx *Indexer) EnsureSweepSchedule(ctx context.Context, interval time.Duration, p SweepParams) error {
	return ensureSweepSchedule(ctx, idx.temporal.ScheduleClient(), idx.taskQueue, interval, p)
}

func ensureSweepSchedule(ctx context.Context, sc scheduleCreator, taskQueue string, interval time.Duration, p SweepParams) error {
	_, err := sc.Create(ctx, client.ScheduleOptions{
		ID: sweepScheduleID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: interval}},
		},
		Action: &client.ScheduleWorkflowAction{
			Workflow:  staleSweepWorkflowName,
			Args:      []any{p},
			TaskQueue: taskQueue,
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return nil
	}
	return err
}

// temporalErrScheduleAlreadyRunning exists so tests can produce the sentinel
// without importing the temporal package themselves.
func temporalErrScheduleAlreadyRunning() error { return temporal.ErrScheduleAlreadyRunning }
```

**Note:** if `temporal.ErrScheduleAlreadyRunning` doesn't exist under that name in the SDK version installed, check `client.ScheduleClient().Create`'s documented already-exists error (`go doc go.temporal.io/sdk/temporal | grep -i schedule` and `go doc go.temporal.io/sdk/client ScheduleClient`) and use that sentinel; also tolerate `*serviceerror.AlreadyExists` via `errors.AsType`.

- [ ] **Step 6: Add Temporal to the Indexer**

In `core/indexer.go` — Config additions:

```go
	// Temporal is the Temporal client used for the durable slow lane:
	// RebuildWalk workflows and the StaleSweep schedule. Required for any
	// deployment; construction does not nil-check so search-only tests can
	// omit it, but Rebuild/NewWorker/EnsureSweepSchedule will panic without it.
	Temporal client.Client

	// TaskQueue is the Temporal task queue for the Indexer's workflows.
	// Default DefaultTaskQueue.
	TaskQueue string
```

Indexer fields + New():

```go
	temporal  client.Client
	taskQueue string
```

```go
	taskQueue := cfg.TaskQueue
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	idx.temporal = cfg.Temporal
	idx.taskQueue = taskQueue
```

(import `"go.temporal.io/sdk/client"`.)

- [ ] **Step 7: Workflow-starting Rebuild in core/rebuild.go**

```go
// Rebuild validates the selectors and starts one durable RebuildWalk workflow
// per selector, returning the workflow IDs (inspect/retry/cancel in the
// Temporal UI).
func (idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) ([]string, error) {
	if err := idx.validateSelectors(selectors); err != nil {
		return nil, err
	}
	workflowIDs := make([]string, 0, len(selectors))
	for _, sel := range selectors {
		run, err := idx.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: idx.taskQueue,
		}, rebuildWalkWorkflowName, sel)
		if err != nil {
			return workflowIDs, fmt.Errorf("start rebuild workflow for %s: %w", sel.ResourceType, err)
		}
		workflowIDs = append(workflowIDs, run.GetID())
	}
	return workflowIDs, nil
}
```

(import `"go.temporal.io/sdk/client"`.)

Add to `core/rebuild_test.go` (imports: `"github.com/stretchr/testify/mock"`, `"github.com/stretchr/testify/require"`, `"go.temporal.io/sdk/mocks"`):

```go
func TestRebuild_StartsOneWorkflowPerSelector(t *testing.T) {
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("wf-1")
	mockClient := &mocks.Client{}
	mockClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, "RebuildWalk", mock.Anything).
		Return(mockRun, nil).Once()

	idx := New(Config{Resources: testResources(), Temporal: mockClient})

	ids, err := idx.Rebuild(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	require.NoError(t, err)
	require.Equal(t, []string{"wf-1"}, ids)
	mockClient.AssertExpectations(t)
}
```

Existing validation tests in that file keep calling `RebuildNow` (renamed in Task 6); validation is shared via `validateSelectors`. Add one test proving `Rebuild` validates before starting any workflow:

```go
func TestRebuild_ValidationFailure_StartsNoWorkflow(t *testing.T) {
	mockClient := &mocks.Client{} // no expectations: any ExecuteWorkflow call fails the test
	idx := New(Config{Resources: testResources(), Temporal: mockClient})

	_, err := idx.Rebuild(context.Background(), nil)
	invalidArg, ok := errors.AsType[*InvalidArgumentError](err)
	require.True(t, ok, "expected InvalidArgumentError, got %v", err)
	require.Equal(t, "at least one selector is required", invalidArg.Msg)
	mockClient.AssertExpectations(t)
}
```

- [ ] **Step 8: Update the Rebuild RPC handler**

In `app/server/index.go`:

```go
	workflowIDs, err := s.idx.Rebuild(ctx, selectors)
	if err != nil {
		return nil, mapAppError(err)
	}

	return connect.NewResponse(&index.RebuildResponse{WorkflowIds: workflowIDs}), nil
}
```

- [ ] **Step 9: Run all tests**

```bash
GOEXPERIMENT=jsonv2 go test ./core/... -race
GOEXPERIMENT=jsonv2 go build ./... && cd app && GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test ./server/... && cd ..
```

Note: `app/tests` still passes because it uses `RebuildNow` (Task 6). Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(core): Temporal slow lane — StaleSweep schedule + RebuildWalk workflows; Rebuild returns workflow IDs"
```

---

### Task 8: App wiring — Temporal client, worker, schedule, config

**Files:**
- Modify: `app/cmd/indexer/main.go`
- Modify: `app/cmd/indexer/app_config.go`
- Modify: `app/cmd/indexer/app_config_test.go`
- Modify: `example.indexer.yml`

**Interfaces:**
- Consumes: `core.Config{Temporal, TaskQueue, PoolSize, SubmitWait}`, `idx.NewWorker()`, `idx.EnsureSweepSchedule(...)`, `idx.Shutdown(ctx)` from Tasks 6–7.

- [ ] **Step 1: Extend app config with failing test first**

Add to `app_config_test.go`'s defaults test (mirror its existing assertion style):

```go
	if cfg.Temporal.HostPort != "localhost:7233" {
		t.Errorf("temporal.host_port default: %q", cfg.Temporal.HostPort)
	}
	if cfg.Temporal.Namespace != "default" {
		t.Errorf("temporal.namespace default: %q", cfg.Temporal.Namespace)
	}
	if cfg.Temporal.TaskQueue != "laika-indexer" {
		t.Errorf("temporal.task_queue default: %q", cfg.Temporal.TaskQueue)
	}
	if cfg.Sweep.Interval != time.Minute {
		t.Errorf("sweep.interval default: %v", cfg.Sweep.Interval)
	}
	if cfg.Sweep.Threshold != 5*time.Minute {
		t.Errorf("sweep.threshold default: %v", cfg.Sweep.Threshold)
	}
	if cfg.Sweep.BatchSize != 500 {
		t.Errorf("sweep.batch_size default: %d", cfg.Sweep.BatchSize)
	}
	if cfg.Pool.Size != 10 {
		t.Errorf("pool.size default: %d", cfg.Pool.Size)
	}
	if cfg.Pool.SubmitWait != 250*time.Millisecond {
		t.Errorf("pool.submit_wait default: %v", cfg.Pool.SubmitWait)
	}
```

Run: `cd app && GOEXPERIMENT=jsonv2 go test ./cmd/indexer/ -run TestLoadAppConfig -v` — expected: compile FAIL.

- [ ] **Step 2: Implement the config**

In `app_config.go` add to `appConfig`:

```go
	Temporal temporalConfig `mapstructure:"temporal"`
	Sweep    sweepConfig    `mapstructure:"sweep"`
	Pool     poolConfig     `mapstructure:"pool"`
```

```go
type temporalConfig struct {
	HostPort  string `mapstructure:"host_port"`
	Namespace string `mapstructure:"namespace"`
	TaskQueue string `mapstructure:"task_queue"`
}

type sweepConfig struct {
	// Interval between StaleSweep schedule firings.
	Interval time.Duration `mapstructure:"interval"`
	// Threshold: only resources stale longer than this are swept.
	Threshold time.Duration `mapstructure:"threshold"`
	BatchSize int           `mapstructure:"batch_size"`
}

type poolConfig struct {
	// Size bounds concurrent inline builds.
	Size int `mapstructure:"size"`
	// SubmitWait before shedding an inline build to the sweep.
	SubmitWait time.Duration `mapstructure:"submit_wait"`
}
```

Defaults in `loadAppConfig`:

```go
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "laika-indexer")
	v.SetDefault("sweep.interval", "1m")
	v.SetDefault("sweep.threshold", "5m")
	v.SetDefault("sweep.batch_size", 500)
	v.SetDefault("pool.size", 10)
	v.SetDefault("pool.submit_wait", "250ms")
```

(add `"time"` import; viper's default decode hooks parse duration strings). Update the env-var doc comment on `appConfig` with the new keys (`temporal.host_port → TEMPORAL_HOST_PORT`, etc.).

Run the config test — expected: PASS.

- [ ] **Step 3: Rewire main.go**

- Remove imports: `river`, `riverpgxv5`, `rivermigrate`; add `"go.temporal.io/sdk/client"` (and drop the now-unused river blocks).
- Delete the "Apply River migrations" block and the entire workers/riverClient/SetRiverClient block plus the river goroutine in the run group and the `riverClient.Stop` call in shutdown.
- After the store is built, dial Temporal and construct the Indexer:

```go
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Fatalf("temporal dial: %v", err)
	}
	defer temporalClient.Close()

	idx := core.New(core.Config{
		Plans:      plans,
		Resources:  resources,
		ES:         esClientImpl,
		Store:      st,
		Temporal:   temporalClient,
		TaskQueue:  cfg.Temporal.TaskQueue,
		PoolSize:   cfg.Pool.Size,
		SubmitWait: cfg.Pool.SubmitWait,
	})

	w := idx.NewWorker()
	if err := w.Start(); err != nil {
		log.Fatalf("temporal worker start: %v", err)
	}

	if err := idx.EnsureSweepSchedule(context.Background(), cfg.Sweep.Interval, core.SweepParams{
		Threshold: cfg.Sweep.Threshold,
		BatchSize: cfg.Sweep.BatchSize,
	}); err != nil {
		log.Fatalf("ensure sweep schedule: %v", err)
	}
```

- Shutdown sequence (replacing the river stop, after the HTTP servers shut down):

```go
	// Drain in-flight inline builds; anything unfinished stays stale and is
	// recovered by the sweep.
	if err := idx.Shutdown(stopCtx); err != nil {
		log.Printf("indexer drain: %v", err)
	}
	w.Stop()
```

- Fix any compile fallout in `app/cmd/indexer/main_test.go` / `serve_test.go` (they may reference removed river wiring).

- [ ] **Step 4: Extend example.indexer.yml**

Append:

```yaml
# Temporal drives the durable slow lane: the StaleSweep schedule and
# RebuildWalk workflows. The hot path (inline builds) works without it,
# but recovery and explicit rebuilds require the cluster.
temporal:
  host_port: "localhost:7233"
  namespace: "default"
  task_queue: "laika-indexer"

# The sweep rebuilds resources whose stale mark is older than threshold.
sweep:
  interval: "1m"
  threshold: "5m"
  batch_size: 500

# Bounded in-process pool for inline builds.
pool:
  size: 10
  submit_wait: "250ms"
```

Also update the env-var comment block at the top of the file with the new keys.

- [ ] **Step 5: Build and test**

```bash
cd app && GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test ./cmd/... ./server/... && cd ..
```

Expected: PASS. (`main.go` requires a running Temporal at runtime, not at test time.)

- [ ] **Step 6: Commit**

```bash
git add app/cmd/indexer/ example.indexer.yml
git commit -m "feat(app): wire Temporal client/worker/schedule; drop River from the assembly"
```

---

### Task 9: New integration coverage — stale lifecycle end to end

**Files:**
- Create: `app/tests/stale_test.go`
- Modify: `app/tests/suite_test.go` (FakeProvider error injection)

**Interfaces:**
- Consumes: `idx.SweepStale`, `idx.WaitForIdle` (via `t.worker.Drain`), `st.MarkDeleted` from earlier tasks; the suite's existing helpers (`t.pool`, `t.esClient`, `t.fakeProvider`, `t.worker.Drain`, ES doc helpers — reuse whatever `index_test.go` uses to fetch documents).

- [ ] **Step 1: Add error injection to FakeProvider**

In `app/tests/suite_test.go`, add to `FakeProvider`: an `errs map[string]error` field, and:

```go
// SetError makes FetchResource fail for the given resource until cleared.
func (f *FakeProvider) SetError(resourceType, id string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errs == nil {
		f.errs = map[string]error{}
	}
	key := resourceType + "|" + id
	if err == nil {
		delete(f.errs, key)
		return
	}
	f.errs[key] = err
}
```

In `FetchResource`, before serving from `f.resources`, return the injected error if the key matches (mirror the exact key construction the method already uses for its `resources` map lookup). Ensure `Clear()` also resets `errs`.

- [ ] **Step 2: Write the lifecycle tests**

Create `app/tests/stale_test.go` (adapt data-seeding and ES-assertion helpers to the exact ones `index_test.go` uses — e.g. its notify helper and document getter; the `staleSince` helper below is new):

```go
package tests

import (
	"errors"
	"time"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/model"
)

// staleSince reads the resource's stale mark; nil means clean.
func (t *TestSuite) staleSince(resourceType, id string) *time.Time {
	var ts *time.Time
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT stale_since FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&ts)
	t.Require().NoError(err)
	return ts
}

func (t *TestSuite) resourceRowCount(resourceType, id string) int {
	var n int
	err := t.pool.QueryRow(t.T().Context(),
		`SELECT count(*) FROM resources WHERE type=$1 AND id=$2`, resourceType, id).Scan(&n)
	t.Require().NoError(err)
	return n
}

// Inline happy path: the build clears the stale mark.
func (t *TestSuite) Test_InlineBuild_ClearsStaleMark() {
	// Seed provider data and notify exactly like Test_ChildUpdate_Rebuilds_Parent does.
	// ... seed a/1 ...
	// notify created a/1 version 1
	t.worker.Drain(t.T().Context())

	t.Require().Nil(t.staleSince("a", "1"), "successful inline build must clear the mark")
	// assert document present in ES via the suite's existing doc helper
}

// Failure path: build fails, mark survives, sweep recovers after the fix.
func (t *TestSuite) Test_FailedBuild_StaysStale_SweepRecovers() {
	// ... seed a/1 ...
	t.fakeProvider.SetError("a", "1", errors.New("provider down"))

	// notify created a/1 version 1
	t.worker.Drain(t.T().Context())

	t.Require().NotNil(t.staleSince("a", "1"), "failed build must leave the stale mark")

	t.fakeProvider.SetError("a", "1", nil)

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)

	t.Require().Nil(t.staleSince("a", "1"), "sweep must rebuild and clear the mark")
	// assert document present in ES
}

// Delete happy path: inline delete removes doc, edges, and the row.
func (t *TestSuite) Test_DeleteNotification_RemovesDocAndRow() {
	// ... seed and build a/1, Drain, assert doc present ...

	// notify deleted a/1
	t.worker.Drain(t.T().Context())

	t.Require().Equal(0, t.resourceRowCount("a", "1"), "finished tombstone must hard-delete the row")
	// assert document absent in ES (404)
}

// Sweep finishes a tombstone whose inline delete never ran (e.g. shed/crash).
func (t *TestSuite) Test_SweepStale_FinishesTombstone() {
	// ... seed and build a/1, Drain, assert doc present ...

	// Simulate a shed inline delete: tombstone directly, no pool work.
	_, err := t.st.MarkDeleted(t.T().Context(), model.Resource{Type: "a", Id: "1"})
	t.Require().NoError(err)

	n, err := t.idx.SweepStale(t.T().Context(), 0, 100)
	t.Require().NoError(err)
	t.Require().GreaterOrEqual(n, 1)

	t.Require().Equal(0, t.resourceRowCount("a", "1"))
	// assert document absent in ES (404)
}

var _ = core.SweepParams{} // keep the core import if only used in commented sections
```

Fill the `...` seeding/notify/assert lines with the suite's real helpers (copy the patterns from `Test_ChildUpdate_Rebuilds_Parent` and the delete tests in `index_test.go`); remove the `var _ =` line if `core` ends up genuinely imported. These tests must be complete and self-contained when written — the `...` here only defers to existing suite helpers, not to unwritten logic.

- [ ] **Step 3: Run the suite**

```bash
cd app && GOEXPERIMENT=jsonv2 go test ./tests/ -run 'TestSuite' -v && cd ..
```

Expected: all PASS, including the four new tests.

- [ ] **Step 4: Commit**

```bash
git add app/tests/
git commit -m "test(app): stale-mark lifecycle — inline clear, sweep recovery, tombstone finishing"
```

---

### Task 10: Dependency cleanup + full verification

**Files:**
- Modify: root `go.mod`/`go.sum`, `app/go.mod`/`app/go.sum`, `go.work.sum`

- [ ] **Step 1: Verify no river references remain in code**

```bash
grep -rn "riverqueue\|river\." --include='*.go' . | grep -v '_test.go: *//' | grep -vi 'driver\b'
```

Expected: no hits in non-comment Go code. Fix any stragglers.

- [ ] **Step 2: Remove river from app/go.mod**

Delete the `github.com/riverqueue/river*` require lines from `app/go.mod`. Try `GOEXPERIMENT=jsonv2 go mod tidy` in `app/`; if it fails resolving the unpublished root module, revert to manual removal and verify with builds only.

- [ ] **Step 3: Full workspace verification**

```bash
GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go vet ./...
GOEXPERIMENT=jsonv2 go test ./... -race
cd aggregation && GOEXPERIMENT=jsonv2 go test ./... && cd ..
cd app && GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go vet ./... && GOEXPERIMENT=jsonv2 go test ./... && cd ..
go run ./app/cmd/gen-mapping -config example.resources.yml >/dev/null && echo GEN-MAPPING-OK
```

Expected: everything green (Docker required for `storage/postgres` and `app/tests`).

- [ ] **Step 4: Throughput sanity check (spec requirement)**

Run the suite's notify→document-visible flow once with timing, as a smoke-level check that the inline path is faster than queue dispatch used to be — e.g. add a temporary `t.T().Logf` around a notify+Drain in `Test_InlineBuild_ClearsStaleMark` and eyeball single-digit-milliseconds-to-low-tens locally. No committed benchmark (YAGNI) — record the observed number in the final PR/commit message.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: drop River dependency; workspace-wide verification"
```

---

## Post-plan notes

- **Out of scope, do next:** update `harness/` (sibling repo) — it embeds core, calls `SetRiverClient`, and will not compile against the new API until adapted (it can drive `SweepStale` from a ticker instead of running Temporal).
- **Known limitations accepted by the spec/ADR:** sweep can't fix resources whose type left the config (logged); `EnsureSweepSchedule` doesn't update an existing schedule's interval; all-of-type RebuildWalk restarts from scratch on worker death (idempotent, resumable cursors are a future refinement).
