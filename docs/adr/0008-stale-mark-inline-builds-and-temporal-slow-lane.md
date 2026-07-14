# Stale-mark durability, inline builds, and a Temporal slow lane

A Notification used to result in a River job that a worker later picked up and
turned into a Build. The job queue was the durability boundary: the RPC returned
once the job was enqueued, and River's retries were the only guarantee the Build
would eventually happen. That hop is pure latency on the hot path, and River is a
second piece of durable infrastructure alongside the Postgres relation graph we
already run.

We remove the hop. Durability moves onto the resource row itself as a **stale
mark**, and the Build runs immediately, in-process. Every path that used to
enqueue a job now does two things, strictly in that order:

1. **Mark the resource stale in Postgres** — the durable record of build intent.
   `MarkStale` bumps `stale_seq` and sets `stale_since = COALESCE(stale_since,
   now())`. Keeping the oldest timestamp means "stale for too long" measures the
   oldest unserved change, not the most recent one.
2. **Try to build inline** on an in-process bounded worker pool — the
   accelerator, allowed to shed, fail, or die.

Because the mark always lands before the build attempt, no work is ever lost: a
Temporal-scheduled **sweep** rebuilds anything whose mark survives longer than a
threshold. The pool makes the common case fast; the sweep is the guarantee.

## Mark-first is the one primitive

Every build-triggering path goes through the same internal step — mark stale,
then submit to the pool, waiting up to a bounded budget for a slot and treating a
timeout or shutdown as success (the row stays stale for the sweep). This covers:

- **`RegisterChange`** — after validation, the version upsert (stale-Version
  rejection is unchanged), and Parent discovery via the Relation graph, the
  changed Resource and every affected Parent are marked and submitted. Fanout now
  marks Parents stale too, which strictly improves on the old model: a Parent
  build lost to River retry exhaustion was unrecoverable; now it is swept.
- **The drift-check re-build** — the [ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md)
  convergence signal for a concurrent Child update during the edge-less window.
- **The [ADR 0006](./0006-reverse-relation-discovery-bootstraps-parent-edges.md)
  parent cascade** — the reverse-relation bootstrap for brand-new Parents.

The last two previously re-enqueued River jobs and silently lost the signal if
the enqueue failed. Routing them through the mark-first primitive gives them the
same durability as an ingest Notification.

## The race-safe clear

A stale mark must be cleared only when the change that set it has actually been
served, never a later one. A Build captures the current `stale_seq` at its start
via `BeginBuild` (the same statement that bumps `build_idx`, the Build Sequence).
On success it clears with `ClearStale`, which nulls `stale_since` only if
`stale_seq` still equals the captured value. If a newer Notification arrived
mid-Build, `stale_seq` has moved, the clear is a no-op, and the row stays stale
for the newer change's own inline build — or, failing that, the sweep. This is
the same optimistic-concurrency shape as the Build Sequence OCC on ES writes,
applied to the mark.

## Delete tombstones

Delete-Notifications no longer remove the row. `MarkDeleted` sets `deleted =
true`, marks the row stale, and resets `version` to `0` so that a later re-create
is never rejected as version-stale. The inline delete removes the ES documents
(all Schema Versions) and the Resource's outgoing Relation edges, then
hard-deletes the row via `DeleteResourceIfSeq`, guarded by the captured
`stale_seq` — a concurrent re-create (which flips `deleted` back to false with a
fresh version) wins the race and the row survives. The sweep retries any
lingering tombstone. This closes a gap the inline model would otherwise open: a
failed ES delete used to leave the document in the index with nothing to retry
it.

## The Temporal slow lane

Rare durable and scheduled work moves to **Temporal**, which already runs in the
vxfiber infrastructure. The Temporal client is a **required** constructor
dependency of `core.Indexer`; the workflows and activities live in core, and
every embedder (the standalone indexer app, `harness`) runs the same worker on
the `laika-indexer` task queue. There is one implementation of the safety net.

- **`StaleSweep`** — driven by a Temporal Schedule (id `laika-stale-sweep`,
  default interval 1m, overlap policy *skip*). Its single activity, `SweepStale`,
  calls `ListStale` for resources where `stale_since < now() - threshold`
  (tombstones included), runs each through the existing build/delete path in
  batches, and returns the count; the workflow loops until a sweep returns 0.
  Core exposes an idempotent schedule-upsert that the app calls at startup with
  the `sweep.*` values from `indexer.yml`. Multiple instances poll the same task
  queue and Temporal places each activity on one of them; no advisory lock is
  needed because `build_idx` OCC already makes concurrent builds of the same
  Document safe.
- **`RebuildWalk`** — replaces the River `rebuild` job and is what the `Rebuild`
  RPC and `Indexer.Rebuild` now start, returning a workflow ID so operators get
  Temporal's UI for status, retry, and cancel. An explicit-ID rebuild is a single
  activity (`RebuildNow`); an all-of-type walk (`ResourceID == ""` →
  `ListResources` pagination) is a heartbeating activity that Temporal retries
  from scratch on another instance if one dies — correct because Rebuilds are
  idempotent. Resumable cursors via continue-as-new are a future refinement.

### Failure isolation

Temporal being down never affects correctness or hot-path throughput. Inline
builds keep flowing, stale marks accumulate harmlessly on their rows, and only
recovery latency degrades until the cluster returns. The hot path depends on
Postgres and ES; it does not depend on Temporal.

### Shutdown and crash

`Shutdown` stops the pool accepting new work and drains in-flight builds within a
deadline; `WaitForIdle` lets callers (and tests) block until the pool is quiet.
Anything unfinished at shutdown — or lost to a hard crash — is still marked stale
and gets swept. That is the whole of the crash-recovery design.

## Consequences

- **Core now depends on Postgres, ES, and Temporal directly.** This partially
  reverses the module split of [ADR 0001](./0001-core-is-a-library-the-app-is-one-assembly.md):
  `storage/postgres` and `backend/elasticsearch` fold into the root module as
  plain packages (their `go.mod`s and `go.work` entries go away), and Temporal
  joins them as a core dependency. The `core.Store` and `core.SearchBackend`
  interfaces stay, now as test seams — unit tests mock them to avoid Docker — not
  as swap points for alternative implementations, of which there will only ever
  be one (YAGNI). The core-as-library principle itself stands: an embedder still
  supplies its own Plans and drives the same orchestrator.
- **At-least-once now spans mark → sweep, not River retries.** This amends the
  re-enqueue leg of [ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md):
  the other two legs — Build Sequence OCC ordering and the wipe-and-replace edge
  rebuild — and the drift check itself are unchanged. "A Build runs at least
  once for a given change" is now guaranteed by the durable mark and the sweep
  rather than by a job queue's retry count.
- **River is removed entirely** — the worker client wiring, the River tables and
  migrations, and the dependency.
- **Known limitation.** A resource whose Type has been removed from the config
  can never be rebuilt, so its stale mark lives forever. The sweep logs each such
  resource it cannot dispatch rather than clearing it; cleanup is an operator
  action, not something the sweep does silently.

**Implication for contributors:** never submit a build without marking the row
stale first — the mark is the durability, the pool is only the accelerator, and
reversing the order reintroduces silent loss on shed or crash. Always clear via
the captured-`stale_seq` guard; never null `stale_since` unconditionally. Do not
add a hot-path dependency on Temporal: it is the slow lane, and the ingest path
must stay correct and fast while it is down.
