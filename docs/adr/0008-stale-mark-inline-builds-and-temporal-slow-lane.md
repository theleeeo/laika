# Stale-mark durability, inline builds, and a Temporal slow lane

> **Note (2026-10-04, runbook step L1.4):** inline builds are owned per
> resource. The `resources` row records a **Build owner** next to the stale
> mark: `owner_seq`, the owner token, and `owner_since`, when the owner last
> claimed or renewed, with a lease (`Config.OwnerLease`, default 30s; the
> app's `pool.owner_lease`). Every instance sees it, so a resource whose owner
> is live has one inline build queued or running, plus at most one follow-up.
>
> - **A mark claims.** A statement that marks a row for a submit —
>   `Store.RegisterChanges` for its items and Parents, `scheduleBuild`'s
>   `MarkStale` — claims the row in the same update when it has no owner or
>   the owner's lease has expired, setting `owner_seq` to the row's new
>   `stale_seq` and `owner_since` to `now()`. Only claimed rows are submitted;
>   a row with a live owner is marked and nothing else. `MarkStale` with a
>   lease of 0 marks without claiming: the rebuild flusher's `fail` and
>   `salvage` hand their roots to the sweep.
> - **The lease is renewed** when a pool task is dequeued and by `BeginBuild`
>   when the token matches, so it covers queue wait as well as the run.
> - **The owner's finish re-claims for at most one follow-up.** An owned build
>   finishes with `FinishOwned` instead of `ClearStale`: with `stale_seq`
>   unchanged it clears the mark and the ownership; with `stale_seq` moved and
>   the token still the owner's, it re-claims the row and the finishing task
>   submits one follow-up. The follow-up runs with the row's metadata — the
>   last mark's, in commit order — and is a delete when the row is a
>   tombstone. An owned delete finishes the same way through
>   `DeleteResourceIfSeq`, so a recreate registered during it is built as its
>   follow-up. A failed, shed or cancelled owned build or delete releases its
>   ownership (`ReleaseOwners`) and keeps the mark.
> - **The sweep claims what it lists.** `ListStale` skips rows with a live
>   owner and claims the rest in the same statement; a sweep build finishes
>   like any owner. Rebuild walks and a direct `idx.Build` own nothing and
>   finish with `ClearStale`, which clears any ownership with the mark.
>
> Ownership is not a lock: nothing waits on it, and correctness does not rest
> on it. A wrong answer is a duplicate build, which the Build Sequence OCC
> orders, or a delayed one, which the mark and the sweep recover — with one
> exception until runbook step L2.1 makes the Elasticsearch delete versioned. A
> delete whose lease lapses lets a recreate claim and build; the delete's
> unversioned Elasticsearch delete can land after that build's upsert, and the
> build's finish clears the mark, so the recreated document is lost. Any inline
> delete racing an inline build had this before ownership; ownership narrows
> it to an expired lease (seam S9 in laika-dev's `docs/open-questions.md`).
>
> *The race-safe clear* below changes accordingly: a newer change that moved
> `stale_seq` mid-build is served by the owner's follow-up, not by "the newer
> change's own inline build", which is not submitted while the owner is live —
> or, failing the follow-up, by the sweep. A build that owns nothing still
> clears with `ClearStale` as described there.
>
> Rejected: a per-instance in-memory registry of the builds in flight. It
> can't see builds on other instances, and it keeps the follow-up's metadata
> in submit order rather than in the commit order the row records.

> **Decision (2026-09-30, after runbook step L1.2):** push producers are never
> throttled. The RPCs (`NotifyChange`, `NotifyChangeBatch`, in the app and the
> harness) register without `WaitForSlot`, so a load spike sheds to the sweep
> rather than slowing the producer. A shed coalesces: several changes to one
> resource bump one row's `stale_seq`, and the sweep's single build of it
> settles them all, whereas throttling would run a build per change. The cost
> is latency: shed work shows up in search after the stale threshold and the
> sweep interval. The heavy lifting is the builds, not ingestion, so it is the
> indexer that absorbs the spike. `WaitForSlot` remains for in-process pull
> producers, like the harness poller (SB1.3), which read current state and
> advance a checkpoint only after their registrations return.

> **Correction (2026-09-30, runbook step L1.2):** registration is now one
> atomic statement. `Indexer.RegisterChanges` (`RegisterChange` is a batch of
> one) calls `Store.RegisterChanges`, which writes each accepted item's version
> or tombstone, its stale mark and metadata, and the marks of its Parents in
> one statement, or nothing. The crash gap between the version upsert and the
> mark described under *Mark-first is the one primitive* is closed, and
> `MarkDeleted` below is gone: the tombstone is set in the same statement.
> After the commit, each accepted item and each marked Parent is submitted as
> its own build with its own metadata, and each delete with the `stale_seq`
> the statement returned. `WaitForSlot` applies to all of these submits.

> **Correction (2026-09-30, runbook step L1.1):** *Mark-first is the one
> primitive* below says a submit waits "up to a bounded budget for a slot".
> The pool never did that: `trySubmit` sheds as soon as the queue is full. There
> are now two submit paths, both after the mark. **`trySubmit`** never blocks,
> and every cascade uses it. ADR 0006 parents and drift re-builds can run
> inside pool tasks, where a blocking submit could deadlock the pool. The
> rebuild flusher runs in a rebuild walk, which must not stall behind producer
> backpressure. **`submitWait`** is used only for a `RegisterChange` called with
> `WaitForSlot()`. It covers that call's own builds and delete, and waits while
> the queue is at or above `Config.QueueHighWater` (default 80% of
> `QueueSize`). A wait ended by the caller's context or by shutdown counts as
> success, because the row stays stale for the sweep. `RegisterChange` lands
> all its marks, a delete's Parents included, before its first submit, so no
> mark waits behind a submit. This gives pull-based
> producers backpressure without shedding. The mark is still what makes the
> write durable. `buildPool.pressured()` reports whether the queue is at or
> above the high-water mark.

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
  build lost to River retry exhaustion was unrecoverable; now it is swept. The
  version upsert and the stale mark are separate statements, though, not one
  transaction: a crash between them leaves the version durably bumped but the
  resource unmarked, so that change is not swept — it recovers only when the
  next Notification for the same resource arrives and marks it.
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
  (tombstones included), runs up to `BatchSize` of them through the existing
  build/delete path per pass, and returns the count; the workflow keeps
  executing passes until one returns fewer than `BatchSize`, capped at 100
  passes per run so a pathological backlog can't run the workflow forever.
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
  idempotent. Resumable cursors landed in
  [ADR 0011](./0011-resumable-rebuild-walks-via-heartbeat-cursors.md) — via
  activity heartbeat details rather than continue-as-new.

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
  action, not something the sweep does silently. Because `ListStale` serves
  oldest-first with a fixed batch limit, once these permanently-un-buildable
  rows number at least one full batch they fill every sweep pass and newer
  stale resources behind them are never reached — removing a Type while its
  rows are still stale can stall sweep recovery for everything else. The
  intended follow-up is for the sweep to skip past rows it cannot dispatch
  instead of re-fetching them at the head of every pass.

**Implication for contributors:** never submit a build without marking the row
stale first — the mark is the durability, the pool is only the accelerator, and
reversing the order reintroduces silent loss on shed or crash. Always clear via
the captured-`stale_seq` guard; never null `stale_since` unconditionally. Do not
add a hot-path dependency on Temporal: it is the slow lane, and the ingest path
must stay correct and fast while it is down.
