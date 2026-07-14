# Inline Builds with Stale-Mark Durability and a Temporal Slow Lane

**Date:** 2026-07-14
**Status:** Approved design, pending implementation plan

## Goal

Improve hot-path throughput by removing the River job hop between a Notification
and its Build. A Notification should result in an immediate in-process Build.
Durability moves from the job queue to a **stale mark** on the resource row,
recovered by a periodic sweep. Rare durable work (full Rebuild walks, the sweep
schedule, future maintenance jobs) moves to **Temporal**, which already runs in
the vxfiber infrastructure.

## The spine: stale mark = durable build intent

Every path that today enqueues a River job instead does two things, in order:

1. **Mark the resource stale** in Postgres — the durable guarantee.
2. **Try to build inline** on an in-process bounded worker pool — the fast
   path, allowed to fail, shed, or die.

A Temporal-scheduled sweep rebuilds anything whose stale mark survives longer
than a threshold. No work is ever lost because the mark always lands before the
build attempt. The pool is an accelerator; the sweep is the guarantee.

## Schema changes (`storage/postgres/pg_schema.sql`)

The schema file is rewritten in place — no ALTER migration. Nothing outside
testing uses the schema yet, so a breaking schema file is fine.

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

- **Marking stale:** `stale_seq = stale_seq + 1, stale_since =
  COALESCE(stale_since, now())`. Keeping the oldest timestamp means
  "stale for too long" measures the oldest unserved change, not the latest.
- **Clearing (race-safe):** a Build captures `stale_seq` when it starts
  (returned by the same statement that bumps `build_idx`). On success it runs
  `UPDATE resources SET stale_since = NULL WHERE type=$1 AND id=$2 AND
  stale_seq = $3`. If a newer Notification arrived mid-Build, `stale_seq`
  moved, the clear is a no-op, and the row stays stale for the newer change's
  own inline build or the sweep.
- **Delete tombstones:** delete-Notifications no longer remove the row.
  `RegisterChange` sets `deleted = true` and marks stale. The inline delete
  removes the ES documents (all Schema Versions) and the resource's relation
  edges, then hard-deletes the row guarded by `stale_seq` — a concurrent
  re-create (which sets `deleted = false` with a new version) wins. The sweep
  retries lingering tombstones. This fixes a real gap in the inline model: a
  failed ES delete would otherwise leave the document in the index forever.

## Core changes

### Worker pool replaces the River client

`core.Indexer` drops `*river.Client` and gains a bounded in-process pool
(semaphore + goroutines). Configuration:

- **Pool size** — default 10 (matches today's River `MaxWorkers`).
- **Submit wait budget** — how long a caller waits for a free slot before
  shedding, default ~250ms, always capped by the caller's context.

New internal primitive, used by every build-triggering path:

```
scheduleBuild(ctx, roots []model.Resource, metadata map[string]string)
  = for each root: mark stale (durable)
    then: submit build task to pool, waiting up to the budget
    on timeout/shutdown: return success — the row stays stale for the sweep
```

### RegisterChange

Unchanged through validation, version upsert (stale-Version rejection), and
parent discovery via the Relation graph. Then `scheduleBuild` for all roots —
the changed resource plus affected Parents — instead of `river.Insert`.

Fanout now marks Parents stale too. This strictly improves on today: a Parent
build lost after River retry exhaustion is currently unrecoverable; now it is
swept.

### Build / buildOne

The OCC machinery is unchanged: bump `build_idx` (`NextRebuildCounter`), wipe
edges, execute Plans, ES upsert with `external_gte`, persist child edges. Two
internal re-enqueue sites become `scheduleBuild`:

- the **drift-check re-build** (concurrent child update during the edge-less
  window), and
- the **ADR 0006 parent cascade** (reverse-relation bootstrap for brand-new
  Parents).

Both gain the mark-first guarantee — today a failed re-enqueue silently loses
the convergence signal.

On successful build, clear `stale_since` guarded by the captured `stale_seq`.

### Shutdown and crash

On SIGTERM the pool stops accepting and drains in-flight builds within a
deadline. Anything unfinished — including work lost to a hard crash — is still
marked stale and gets swept. This is the entire crash-recovery design.

## Temporal (a required core dependency)

The Temporal client is a required constructor dependency of `core.Indexer`.
Workflows and activities live in core; every embedder (the indexer app,
`harness`) runs the same worker. One implementation of the safety net.

### StaleSweep workflow

- Driven by a **Temporal Schedule** (default every 1m, overlap policy *skip*).
  Core exposes an idempotent schedule-upsert taking interval and staleness
  threshold; the app calls it at startup with values from `indexer.yml`,
  embedders pass their own.
- One activity: `SweepStale(threshold, batchLimit)` — select resources where
  `stale_since < now() - threshold` (including `deleted` tombstones), run each
  through the existing build/delete path, return the count. The workflow loops
  until a sweep returns 0.
- Multiple indexer instances poll the same task queue; Temporal places each
  activity on one instance. No advisory locks: `build_idx` OCC makes even
  accidental concurrent builds of the same Document safe.

### RebuildWalk workflow

Replaces the River `rebuild` job.

- Explicit-ID rebuilds: a single activity.
- All-of-type walks (`ResourceID == ""` → `ListResources` pagination): a
  heartbeating activity. If an instance dies, Temporal retries the activity on
  another instance from scratch — correct because Rebuilds are idempotent.
  Resumable cursors via continue-as-new are a future refinement if walks grow
  large enough to make restart-from-scratch painful.
- The `Rebuild` RPC starts a RebuildWalk workflow and returns the workflow ID.
  Operators get Temporal's UI for status, retry, and cancel.

### Failure isolation

Temporal being down never affects correctness or hot-path throughput: inline
builds keep flowing, stale marks accumulate harmlessly, and recovery latency
degrades until the cluster returns.

## Consolidation and removal

- **River removed entirely:** `core/worker.go`, the client wiring in
  `app/cmd/indexer/main.go`, River tables/migrations, and the dependency.
- **Module collapse (scoped):** `storage/postgres` and `backend/elasticsearch`
  fold into the root module as plain packages; their `go.mod`s and the
  corresponding `go.work` entries go away. `aggregation` and `app` remain
  separate modules. Only one Store and one SearchBackend will ever exist
  (YAGNI), but the `core.Store` / `core.SearchBackend` **interfaces stay** as
  test seams — unit tests mock them to avoid Docker.
- **Docs:** new ADR for the stale-mark / inline-build / Temporal architecture,
  superseding notes on ADR 0001 (core now carries Postgres/ES/Temporal
  dependencies) and amending ADR 0002 (the at-least-once re-enqueue leg is now
  mark-and-sweep). CONTEXT.md gains *Stale mark* and *Sweep* vocabulary.

## Error handling matrix

| Failure | Outcome |
|---|---|
| Postgres upsert / stale-mark fails | RPC returns error; caller retries (as today) |
| Pool saturated past wait budget | RPC succeeds; row stays stale; sweep rebuilds |
| Inline build fails (Provider/ES down) | Logged; stale mark survives; sweep retries with activity backoff |
| Process crashes mid-build | Stale mark survives; sweep rebuilds |
| ES delete fails | Tombstone survives; sweep retries the delete |
| Temporal cluster down | Hot path unaffected; recovery latency degrades until it returns |

## RPC semantics (unchanged surface, clarified contract)

`NotifyChange` / `NotifyChangeBatch` still return after state is durably
recorded — success now means "your change is durably marked and will be
indexed", usually within milliseconds via the inline build, worst-case within
sweep threshold + interval. Error mapping (`ErrStaleVersion` →
`FAILED_PRECONDITION`, etc.) is unchanged.

## Testing

- **Unit (no Docker):** mark-before-submit ordering in `RegisterChange`;
  race-safe clear (moved `stale_seq` → no-op); shedding leaves the mark;
  tombstone delete flow including concurrent re-create; drift re-build and
  parent cascade go through mark-first; pool drain on shutdown.
- **Integration (`app/tests`, testcontainers):** notify → inline build → ES
  document present and `stale_since` cleared; failing Provider → sweep
  recovers; delete with ES down → tombstone swept. Temporal workflows tested
  with the SDK `testsuite` in-memory environment; one full-stack flow against
  the Temporal dev server container covering Schedule bootstrap.
- **Throughput sanity check:** before/after benchmark of notify →
  document-visible latency, since throughput is the stated goal.

## Decisions log

| Decision | Choice |
|---|---|
| RPC sync model | Async: RPC returns after mark; build runs on in-process pool |
| River's fate | Removed entirely |
| Backpressure | Brief bounded wait for a pool slot, then shed to sweep |
| Durable/scheduled work | Temporal (already in vxfiber infra) |
| Temporal placement | In core, as a **required** Indexer dependency |
| Module collapse | `storage/postgres` + `backend/elasticsearch` → root; `aggregation`, `app` stay separate |
| Interfaces | `Store` / `SearchBackend` interfaces retained as test seams |
