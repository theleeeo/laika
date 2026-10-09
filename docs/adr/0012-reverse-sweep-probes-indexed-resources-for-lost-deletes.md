# A reverse sweep probes every indexed resource at its source and deletes what is gone

_Accepted, 2026-10-06._

> **Note (2026-10-09, runbook step L4.1):** `SearchBackend` now has a
> point-in-time read, the scan ([ADR
> 0015](./0015-a-scan-reads-one-snapshot-under-the-version-it-pinned.md)), so
> the first alternative below no longer rests on its absence. The choice
> stands: a scan reads only the index the read alias serves, through the search
> chain, and returns ids without the actor a probe needs, which the
> `resources` table holds per row.

> **Note (2026-10-07, runbook step L2.5):** the other direction is built, the
> **forward walk**. The reverse sweep finds what the source no longer has; the
> forward walk finds what Laika never heard of, a resource created or changed
> at its source without a notification, by walking a configured type from its
> source on a schedule. It is Rebuild walks and adds no build path.
>
> - **Configured per type.** `core.Config.ForwardWalks` maps a resource type
>   to its `ForwardWalkConfig`: `Interval` (default 24h), `PageSize` (default
>   100), `PageInterval` (default 1s), and `Metadata`, a function called at
>   each run that returns the metadata maps the run walks with, so an embedder
>   can derive them. A nil function is one walk with no metadata; one that
>   returns no maps walks nothing that run, with a warning. `core.New` rejects
>   an entry for a type the resource configs don't have, negative values, and
>   a page size above the provider contract's `int32`.
> - **Scheduled and run.** `EnsureForwardWalkSchedules` creates one schedule
>   per entry, `laika-forward-walk-<type>`, every `Interval`, overlap _skip_,
>   and, like `EnsureReverseSweepSchedules`, only creates. Each run is a
>   `ForwardWalk` workflow: its `ForwardWalkSelectors` activity calls the
>   `Metadata` function, and the workflow starts one `RebuildWalk` child per
>   map, each walking the whole type with the map as its metadata, one after
>   another, so the type's rate budget holds per type. A type the worker
>   can't walk, unknown or without an entry, fails the run without retrying.
>   `ForwardWalkNow` is the run's body in-process, as `RebuildNow` is
>   `RebuildWalk`'s.
> - **A map is the walk's actor and nothing else.** An actor lists only what
>   it can see, so an embedder whose actors partition the source returns one
>   map per actor. A row's metadata stays its own, its registrations' or else
>   its plans' report (ADR 0008's L2.3 note), never the walk's map. A resource several actors see is built once
>   per walk that lists it, and the Build Sequence orders the writes.
> - **Paced as the sweep is.** A walk carries its type's pacing
>   (`ResourceSelector.Pacing`; an explicit rebuild has none). Its plans are
>   asked for pages of `PageSize` (`projection.BuildRequest.PageSize`). Before
>   taking each page, a later plan's first page included, it flushes what it
>   has begun, so no begun resource waits unwritten (seams S16), then waits
>   out the rest of the previous page's `PageInterval`, measured from that
>   page's start, and then while the build pool is pressured. Pacing delays
>   when the walk takes a page, not the plan's fetch of it, and a plan may
>   ignore the page size (seams S29 in the multirepo's records).
> - **A walk whose failures are all marked is done.** Its failed resources are
>   durably marked stale for the sweep, so `RunRebuild` fails it with the
>   non-retryable `RebuildMarkedFailures` error ([ADR
>   0011](./0011-resumable-rebuild-walks-via-heartbeat-cursors.md)'s L2.5
>   note); the run counts its resources in `ForwardWalkResult.FailedResources`
>   and logs them. Any other failure of a walk fails the run, once every walk
>   has run.
> - **The app** configures it in `indexer.yml`'s `forward_walks` list, with
>   static metadata maps, and ensures the schedules at startup.
>
> The consequence "It heals nothing" below names the forward walk as what
> heals; it now does so for a type configured for it, at its next run that
> lists the resource.

Laika's durability has had two legs.
[ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md): concurrent builds
and deletes of a resource land in Build Sequence order, and a build that raced a change
re-schedules itself through the drift check.
[ADR 0008](./0008-stale-mark-inline-builds-and-temporal-slow-lane.md): every build intent is a
stale mark before it is a build, inline builds are the accelerator, and `StaleSweep` builds
whatever mark outlives them. Both legs start from something Laika heard of — a notification, a
Parent cascade, a drift re-mark, a rebuild. A resource deleted at its source without a
notification reaching Laika starts nothing: no row is marked, no build runs, and its documents
stay searchable and its row live until something else happens to build it — a change to a child
its edge sets name, a by-ids rebuild. A Rebuild walk doesn't find it either: it lists what the
source has, and the source no longer has it.

The decision: **a third leg, the reverse sweep, checks every indexed resource of a configured
type against its source on a schedule, as the actor in its own row, and hands each one the source
no longer has to the build path, which deletes it.** This changes delete semantics: a resource of
a swept type deleted at its source without Laika hearing of it is now found and deleted — its documents and
its row — by the next sweep run of its type, where until now it stayed indexed. The sweep only
nominates. The delete is an owned build's, decided as every build's is: each version's document goes when
that version's plan finds the resource gone, and the row when every plan does
([ADR 0013](./0013-each-schema-version-decides-its-documents-existence.md)).

## How it works

- **It enumerates the `resources` table, per type, by keyset.** `Store.ListResources(type,
  after, limit)` returns up to `limit` live rows of the type whose id sorts after `after`, in id
  order, each with its metadata — a keyset page on the existing `UNIQUE (type, id)`. Tombstones
  are not listed: they are `StaleSweep`'s. It claims and locks nothing. Every Elasticsearch
  write is preceded by a `BeginBuild`, which inserts the row, so the table holds every indexed
  resource but one whose late build wrote after its row was hard-deleted (see the consequences).
  A run ends after a page shorter than the page size.
- **A plan's `Probe` answers existence.** `projection.Plan.Probe(ctx, ids, metadata) (present,
  err)` is optional. Given several ids and one actor's metadata, it returns exactly the ids the
  plan's root fetch would find for that actor, and fails where that fetch would fail — as a
  fetch with no actor does in a source that needs one. The embedder writes it beside the plan, on
  the clients its root fetch uses: a by-ids call where the source has one, a call per id otherwise.
  The sweep uses the first of the type's plans, in the order the embedder gave them, that has a
  Probe. That plan answers for its own Schema Version, which decides its own document's existence
  (ADR 0013): an id its version excludes is a suspect at every run, a needless build each time, and
  an id only another version's plan stops returning, without a notification, is never a suspect,
  so that version's document stays until something else builds the id. A type with none is
  skipped at every run with a warning — nothing is listed, probed or marked — and there is no
  fallback.
- **Each page is probed once per actor.** A page's ids are grouped by their row's metadata, NULL
  and empty metadata being one group, and each group is probed in one call with that metadata —
  none for the group without. So each resource is asked about as the actor in its own row
  (ADR 0008's L1.8 note: a resource's metadata is its own). Ids a probe returns that it wasn't
  asked about are ignored.
- **A failed probe skips its group, and the next run asks again.** An error fails the whole
  call: nothing of it is used, its ids are neither marked nor built, the failure is logged with
  the type and the group's size, and the run's result counts it (`FailedProbes`, and its ids in
  `Unprobed`). The rest of the page goes on.
- **What the probe doesn't return is a suspect, and a suspect is marked before it is built.** A
  page's suspects go through `scheduleBuild` in one call, as the ADR 0006 cascade and the drift
  re-mark do: one `MarkStale` marks them and claims each that has no live Build owner, and each
  claimed one is submitted as an owned inline build. That build runs every plan with the
  metadata its `BeginBuild` reads from the row (ADR 0008's L1.8 note). When every plan finds the
  resource gone, it takes `Build`'s all-plans-nil path: each version's document is deleted at
  the build's Build Sequence, so a recreate built above it keeps its document (ADR 0002's L2.1
  note), and the row by `DeleteResourceIfSeq`, guarded by the `stale_seq` the build captured, so
  a recreate registered meanwhile keeps its row (ADR 0008's L2.2 note).
  - A suspect that does exist — a registration changed its row's metadata after the page was
    listed, say — is rebuilt, which is harmless.
  - A suspect some plans find and others don't keeps its row: the build writes the documents of
    the versions whose plans return one, deletes those of the others, and clears the mark
    (ADR 0013).
  - A suspect whose row has a live owner is only marked; that owner's follow-up serves it.
  - A shed or failed build releases its claim and leaves the mark for `StaleSweep`.

  Only suspects are marked, never the whole type. A `scheduleBuild` that fails ends the run with
  its error and the result so far, and its page is not checkpointed.
- **A heartbeat cursor on the keyset id.** Once a page's suspects are marked — failed probes
  don't hold it back — the run reports the page's last listed id as its checkpoint. Every id up
  to it was found present, is durably marked, or was skipped by a failed probe, which the next
  run asks about again. The `RunReverseSweep` activity carries the cursor in its heartbeat
  details, and a retried attempt resumes after it and skips no id: ADR 0011's pattern, including
  the liveness beat that re-records the newest cursor, the inherited one included. ADR 0011's
  rule that only single-active-plan walks checkpoint does not apply. That rule exists because a
  multi-plan walk holds resources begun but unsettled across its plan walks; the sweep runs no
  plan walk, and each page is settled — by its marks — before its cursor is reported. Unlike a
  provider's page token, the cursor is an id in Laika's own table, so a delete at the source
  can't shift it.
- **Paced by a rate budget and the pool's pressure.** Before each page, the first included, the
  sweep waits while the build pool reports pressure (`buildPool.pressured`, its queue at or
  above `Config.QueueHighWater`), checking again after a short back-off, so its builds don't
  crowd out the live path's. After a page it waits out the rest of the page's `PageInterval`,
  measured from the page's start, so it probes at most `PageSize` ids per `PageInterval`. There
  is no wait after the last page.
- **Configured and scheduled per type.** `core.Config.ReverseSweeps` maps a resource type to its
  `ReverseSweepConfig`: `Interval` (default 24h), `PageSize` (default 200) and `PageInterval`
  (default 1s). An entry enables its type; `core.New` rejects an entry for a type the resource
  configs don't have, and negative values. `EnsureReverseSweepSchedules` creates one Temporal
  schedule per entry, in type order, with ID `laika-reverse-sweep-<type>`, every `Interval`,
  overlap _skip_, and tolerates one that exists, as `EnsureSweepSchedule` does for
  `laika-stale-sweep`. Each run is a `ReverseSweep` workflow running one `RunReverseSweep`
  activity under `RebuildWalk`'s activity options (24h start-to-close, one-minute heartbeat
  timeout, five attempts). Its body is callable in-process — `ReverseSweepNow`, and
  `ReverseSweepResumable` with a starting id and a checkpoint callback — as `RebuildNow` is
  `RebuildWalk`'s; calling it for a type without an entry, or one the resource configs don't
  have, is an error. The activity fails such a run with a non-retryable error, since no retry
  could fix the worker's config: one failed attempt per run, not five. A failed attempt's result
  is dropped by Temporal, so the activity logs the counts it got, and a retried run's result
  counts only its last attempt's work.
- **The app sweeps nothing.** The app's DSL plans have no Probe, so the app configures no
  reverse sweep and creates no schedule. An embedder that writes probes configures its types and
  calls `EnsureReverseSweepSchedules` at startup, beside `EnsureSweepSchedule`.

## Why not the alternatives

- **Enumerating Elasticsearch instead of the `resources` table.** `SearchBackend` has no scroll
  or point-in-time read to page through an index with (`core/backend.go`), and the table already
  holds each row's metadata, the actor its probe needs.
- **A sweep that rebuilds every row.** It would find deletes through the same nil path, but it
  repeats the forward walk's builds, a full plan execution per resource per run, where the
  existence question needs only the probe and a build per suspect.
- **A root-only flag on the plan's regular fetch** instead of a separate Probe. The typed stages
  can't skip: a `Sub` stage needs what it fetched to build its output (`aggregation/plan.go`).
  In the harness, such a flag would still run the member expansion its root fetch includes.
- **Accepting equal Versions in the `RegisterChanges` upsert** (its `accepted` CTE,
  `storage/postgres/pg.go`), so that a second change to a resource within one tick of its source's
  `updated_at`, notified at the Version already stored, is built again. Kept strict-greater:
  accepting equal Versions would help only a second write that keeps
  a row's `updated_at`, which the harness walk cursor's consumed-boundary-id check
  (`harness/resolvers/pagetoken.go`) already drops, and strict-greater dedupes a row listed once
  per member operator.

## Consequences

- **What it can't see.** A document whose row is gone. A build that writes more than
  `index.gc_deletes` after a delete overtook it brings the document back with no row (seams S16
  in laika-dev's `docs/seams.md`), and documents can lose their rows otherwise, after a Postgres
  restore, say (deferral D2 in laika-dev's `docs/deferrals.md`: no Elasticsearch ↔ Postgres
  audit). The sweep lists rows, so it reaches neither; such a document stays until a
  notification for its id.
- **What it does reach that nothing else did.** A row a Rebuild walk recreated for a resource
  deleted after its page was fetched (seams S17) is a live row whose resource is gone: the sweep
  lists it, its probe doesn't return it, and its build deletes it.
- **A probe's errors cost differently.** One that returns an id its root fetch would not find
  misses that delete, silently and at every run. One that misses an id its root fetch would find
  costs a needless build of it at every run. Since a build deletes a version's document only when that
  version's plan finds the resource gone, and the row only when every plan does, a probe can't
  delete a live document by itself.
- **A row with no metadata is probed with none, and built with none** (seams S20). Where the
  source needs an actor, the probe fails as the root fetch would, so that group is a failed probe
  at every run and nothing of it is marked until the row gets its owner. Where the source answers
  NotFound to a fetch without an actor, the probe returns nothing, and each such row's build
  deletes it — live or not.
- **It heals nothing.** A resource the probe returns is not rebuilt, however stale its document.
  The forward walk heals what the sweep doesn't.
- **A lost delete is late, not lost.** It is removed within one `Interval` plus the run that
  reaches it; a run takes about the type's row count over `PageSize`, times `PageInterval`, and
  longer while the pool is pressured. A row inserted behind the cursor during a run is checked by
  the next.
- **A suspect deleted mid-page comes back as a stale row** (seams S22). A suspect hard-deleted
  between its page's listing and the page's mark is recreated by `MarkStale`'s upsert as a stale
  row with no metadata. Where the source needs an actor, its build fails and the row stays stale,
  visible in `StaleSweep`'s logs; nothing wrong is served or deleted. Accepted while nothing is
  live.
- **A workflow's death starts the type over.** As in ADR 0011, the cursor belongs to the
  activity's attempt chain, so a fresh run starts at the type's first id. Overlap _skip_ keeps a
  long run from stacking another behind it.

**Implication for contributors:** the sweep nominates and the build decides. Never delete from a
probe's answer: hand suspects to `scheduleBuild`, so they are marked before they are built and
deleted only by an owned build's all-plans-nil path. Mark only suspects, never the whole type,
and hold no lock across a probe. A Probe must answer as its plan's root fetch would, for the same
actor.
