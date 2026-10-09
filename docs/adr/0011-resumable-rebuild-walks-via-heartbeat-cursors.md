# Rebuild walks resume from heartbeat cursors, and only single-active-plan walks checkpoint

_Accepted, 2026-09-04._

> **Note (2026-10-09, runbook step L3.9):** the page-token bullet's backstop
> changed. `cutover-check` no longer has a doc-count parity gate
> ([ADR 0010](./0010-cutover-readiness-is-a-pre-deploy-gate.md)'s Q24 note).
> A resumed walk that skipped rows is caught by the coverage gate for the
> resources Laika has a row for: each lacks an edge set of the backfilled
> version, and no flag passes coverage. A resource Laika has no row for shows
> only in the doc-gap, which the operator may accept.

> **Note (2026-10-07, runbook step L2.5):** two rules join the ones below,
> and a scheduled walk runs under them.
>
> - **A walk checkpoints nothing more once one of its marks has failed.** A
>   cursor promises that every resource behind it is settled or durably
>   marked stale; a resource whose mark failed is neither. The rebuild
>   flusher records a failed mark (`rebuildFlusher.markFailed`; a mark
>   `fail` retries counts only when the retry fails too), and from then on
>   the after-flush hook reports no checkpoint, so a retried attempt resumes
>   before that resource.
> - **A rebuild whose failures are all durably marked is not retried.** A
>   retry would add nothing the sweep doesn't, so such a rebuild, walk or
>   targeted, returns a `RebuildMarkedFailuresError` with the count, which
>   `RunRebuild` returns as a non-retryable application error of type
>   `RebuildMarkedFailuresErrorType` with the count as its details, for an
>   explicit rebuild and a scheduled one alike. A rebuild that aborted (a
>   page or write error, cancellation) or one of whose marks failed stays
>   retryable, and a retried walk resumes by the rules below: a single-plan
>   walk from its cursor, a multi-plan walk from the start.
> - **The scheduled forward walk is `RebuildWalk`s** ([ADR
>   0012](./0012-reverse-sweep-probes-indexed-resources-for-lost-deletes.md)'s
>   L2.5 note), one per metadata map, so it checkpoints by the same rule: a
>   walk of a type with one active plan resumes, and a walk of a type with
>   several restarts. Its walks are paced (`ResourceSelector.Pacing`); pacing
>   changes when a walk takes a page, never where it checkpoints.

> **Note (2026-10-06, runbook step L2.4):** the reverse sweep
> ([ADR 0012](./0012-reverse-sweep-probes-indexed-resources-for-lost-deletes.md))
> resumes by this pattern — its `RunReverseSweep` activity heartbeats its
> cursor and re-records it on liveness beats — but the rule that only
> single-active-plan walks checkpoint does not cover it. The rule is about
> plan walks, whose multi-plan form holds resources begun but unsettled
> across plans; the sweep walks no plan. Its cursor is the last id of a page
> of `resources` rows, reported once that page's suspects are durably
> marked, so every position it reports is settled whatever the number of
> plans. Nor does it carry the page-token precondition below: the id is
> Laika's own, and a delete at the source can't shift it.

> **Note (2026-10-04, runbook step L1.5):** no rebuild wipes or merges edges.
> Each flush replaces the edge set of each (resource, Schema Version) document
> it landed, at the resource's Build Sequence
> ([ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md)'s
> L1.5 note), so a version-targeted rebuild replaces exactly its versions'
> sets and leaves the others alone. Two passages below change with it. In the
> multi-plan bullet, a resource a resumed attempt skips keeps its remaining
> versions' older edges, not wiped ones, beside their un-refreshed documents
> and with no stale mark: still silent unmarked divergence, so only
> single-active-plan walks checkpoint, as before. And resumption doesn't touch
> the edge replace: the pages a resume skips hold only settled resources,
> whose landed versions' sets were replaced when they flushed.

An all-of-type rebuild walk — `RebuildWalk` → the `RunRebuild` activity — could die mid-walk and recover only by starting over. [ADR 0008](./0008-stale-mark-inline-builds-and-temporal-slow-lane.md) called resumable cursors "a future refinement" and sketched continue-as-new. At production scale a restarted walk repeats hours of provider and Elasticsearch work, and the [ADR 0010](./0010-cutover-readiness-is-a-pre-deploy-gate.md) backfill is precisely the walk that runs long enough to be killed. The provider wire already paginated (`ListResourcesRequest.page_token`); nothing carried the token up to anywhere durable.

The decision: **a walk checkpoints a `RebuildCursor` into its activity's heartbeat details, and only a walk with exactly one active plan is allowed to.**

- **The cursor marks a settled position, not progress.** A `RebuildCursor` is `{plan_version, page_token}`: every resource listed by the pages before that token is fully settled — document flushed, edges persisted, stale mark cleared — or durably marked stale for the sweep. The rebuild flusher's after-flush hook reports it, stamped with the last *fully consumed* page's token, so a checkpoint only ever trails a successful flush; a flush landing mid-page re-reports the previous boundary, which is conservative, never ahead of what was written.
- **Only single-active-plan walks checkpoint and resume** — a single-version type's full rebuild, or an ADR 0010 version-targeted backfill, which are the long-running production walks. With one plan every flushed document is the resource's last (`expected == 1`), so a resource settles the moment it flushes and a page boundary is never reached over unsettled work. A multi-plan walk never checkpoints and ignores any cursor it inherits (warn, then restart from scratch): its resources stay begun-but-unsettled across plan walks, and an attempt resuming past one that a remaining plan's listing no longer emits — hand-rolled per-version listings exist, e.g. in the harness — would leave it with wiped edges _(older edges since L1.5: see the note above)_, an un-refreshed document, and no stale mark. That is silent unmarked divergence, exactly what ADR 0008's mark-first invariant exists to prevent.
- **Heartbeat details, not continue-as-new.** `RecordHeartbeat(ctx, cursor)` on the way out, `GetHeartbeatDetails` on the way in. This supersedes ADR 0008's continue-as-new sketch: the workflow shape does not change (it still runs one activity), history does not grow, and the retry Temporal already schedules on a heartbeat timeout *is* the resume point.
- **The liveness ticker must re-record the latest cursor.** Heartbeat details replace each other wholesale, so a bare liveness beat after a checkpoint would erase it — the cursor inherited from a previous attempt included, costing the next attempt the whole walk. The ticker beats bare only until a cursor exists, and re-records the newest one from then on.
- **Discarding beats guessing.** A cursor naming a version this walk does not run (the config or selector changed between attempts, or the walk is multi-plan) is dropped and the walk restarts from scratch: restarting is always safe, resuming from a wrong position is not. Page tokens that are not strings — a hand-rolled executer's — are never checkpointed, so such plans restart on retry.
- **Resumption does not touch wipe-and-replace.** _(Superseded by the L1.5 note above: there is no wipe or merge.)_ A full rebuild still wipes a resource's edges at its first sighting and a version-targeted one still merges ([ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md)), resumed or not: the pages a resume skips hold only settled resources, whose edges were re-added after their own wipe.
- **Targeted (by-ID) rebuilds neither checkpoint nor resume.** Their input is bounded and restarting is cheap, so an inherited cursor is warned about and ignored. They still heartbeat — a dead worker is still detected and retried.
- **The plumbing, top to bottom**: `aggregation.ExecutionResult.NextPageToken` (each page carries the token that fetches the next; stages map pages 1:1 and forward it unchanged), `projection.BuildRequest.PageToken` (seeds an all-of-type walk mid-listing), and `core.RebuildNowResumable(sel, start, checkpoint)` — the embedder-visible seam, usable without Temporal.
- **The page token must name a stable position — that is a precondition on the provider, not something Laika can enforce.** A cursor is redeemed minutes to hours after it was recorded (a heartbeat timeout plus retry backoff), so the token has to mean the same place then as it did when it was written. Keyset or last-ID tokens do; *offset* tokens do not — and offsets are what `vx-provider`'s resolvers and the harness's `nextPageToken` emit today. Upstream deletions between the two attempts shift every later row forward, so the resumed walk starts past resources it never walked: they are never rebuilt, and nothing marks them, so the backfill quietly finishes short. Insertions are harmless in the other direction — some resources are simply walked twice, which a Rebuild is idempotent under. The backstop is the [ADR 0010](./0010-cutover-readiness-is-a-pre-deploy-gate.md) `cutover-check` doc-count parity gate: an incomplete backfill fails it, before a `readVersion` bump can flip the alias onto the short index. _(Since L3.9 the coverage gate, and for resources Laika has no row for only the doc-gap: see the note above.)_
- **An Executer that ignores `PageToken` is safe but not resumable.** `projection.BuildRequest.PageToken` is where a walk re-enters the listing, so honouring it is the contract a plan must meet to be resumable. One that does not — the harness's hand-rolled domain plans today — restarts from the head of the listing on every resume: correct, because a full re-walk settles everything it touches, but the feature is silently forfeited rather than loudly broken. An embedder that wants resumable backfills has to thread the field through its root fetch.

Consequences:

- A retried `RunRebuild` attempt of a single-plan walk repeats at most the work since the last flushed checkpoint — typically one chunk — rather than the whole walk. Multi-plan full rebuilds keep restart-from-scratch recovery: no worse than before this ADR, and they are the rarer, migration-window operation.
- Heartbeat details are throttled by the SDK (~80% of the one-minute `HeartbeatTimeout`), so a recovered cursor can trail the true position by seconds. The resumed walk redoes that window, idempotently.
- **A cancelled walk now fails instead of returning nil.** The page channel closes on cancellation just as it does on exhaustion, and a cancelled producer abandons its terminal error when no receiver is parked on the channel — which is where the walk sits while it flushes. The walk therefore re-checks the context at the page-loop boundary and returns its error. Without it, `RunRebuild` would report a half-walked backfill as a completed one — and a successful activity is never retried, so nothing would ever resume it.
- Surviving a *workflow* death, not just an activity retry, is out of scope: heartbeat details belong to the activity's attempt chain, so a fresh workflow starts a fresh walk. That is the pre-ADR behavior — never worse — and covering it would need the continue-as-new machinery this ADR set aside.
