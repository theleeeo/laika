# Distributed safety via OCC and drift-check, not locks

> **Note (2026-10-06, runbook step L2.7):** Decided 2026-10-06 (Q23): a
> rebuild that selects versions (`RebuildArgs.Versions`) and whose selected
> plans are not all of its type's plans never deletes on them alone. An id
> every selected plan finds gone, in a walk or by id, is marked stale with
> one `MarkStale`, and nothing of it is deleted, its row included. The
> sweep's build then runs every plan and deletes only if they all agree. The
> rebuild doesn't count such an id as failed. A mark that fails does fail it,
> so a checkpoint never steps over an id that is neither deleted nor marked
> (ADR 0011). A rebuild that runs every plan with an `Executer` still deletes
> as the L2.6 note says. Rejected: deleting from every version on the
> selected plans' word, which removed documents that plans never asked still
> served; and deleting only the selected versions' documents while keeping
> the row, which left versions disagreeing on whether the resource exists.

> **Note (2026-10-06, runbook step L2.6):** a multi-plan Rebuild walk deletes
> a root only when every plan of the walk listed it without data, as a build
> deletes only when all its plans return nil; a root whose plans disagree is
> marked stale for the sweep, whose build runs every plan. The L2.1 note's
> check of a root the walk deleted measures from the start of the plan walk
> that first listed the root, which precedes every fetch of it, not from the
> start of the walk that listed it last.

> **Note (2026-10-06, runbook step L2.4):** a third durability leg stands
> beside the ones below and ADR 0008's mark and sweep: the **reverse sweep**
> ([ADR 0012](./0012-reverse-sweep-probes-indexed-resources-for-lost-deletes.md)).
> These legs order and converge the changes Laika hears of; none reaches a
> resource deleted at its source without a notification, whose documents
> stayed indexed. The reverse sweep lists a configured type's live rows
> (`Store.ListResources`), asks the source through a plan's `Probe` which
> still exist, and marks the rest for owned builds, which delete through the
> all-plans-nil path. It adds nothing to the legs here: its deletes are
> builds', versioned at their Build Sequence (the L2.1 note), so a recreate
> built above one keeps its document, and it takes no lock across a probe or
> a fetch — `ListResources` locks nothing, and its marks are `MarkStale`'s
> one statement.

> **Note (2026-10-05, runbook step L2.1):** leg 1 orders **deletes** by the
> Build Sequence like writes, and the Build Sequence is drawn from the
> **Change Sequence**.
>
> - **Deletes are versioned.** Every Elasticsearch delete is sent as
>   `external_gte` at the deleting path's Build Sequence: the live build's or
>   a rebuild's own `BeginBuild` when every plan found the resource gone, the
>   one a Rebuild walk began the id at (or a fresh `BeginBuild` for an id it
>   hadn't begun), and for a notified delete the bump `Store.BeginDelete`
>   takes before it deletes anything. A document a newer build wrote rejects
>   a late delete (409), which is success, like a write that lost OCC; a
>   missing document (404) is success too. The edge removal
>   (`RemoveResource`) takes the same sequence and removes only the edge
>   sets stamped at or below it, so the L1.5 note's unguarded delete is
>   gone: a set a newer build stored stays.
> - **The numbers don't restart.** `build_idx` and `stale_seq` no longer
>   count per row from 1. `BeginBuild` draws one value from `change_sequence`
>   and returns it as both the Build Sequence and the build's start (leg 3),
>   and every stale mark sets `stale_seq` to a fresh value. Neither restarts
>   across a hard delete and a recreate: `build_idx` keeps rising, so a
>   recreated resource is built above every version Elasticsearch remembers
>   for its deleted document, and no `stale_seq` value is reused, so nothing
>   of the old row matches the new row's numbers. The schema
>   guard that keeps the sequence ahead of `change_seq` keeps it ahead of
>   `build_idx`, `stale_seq` and `edge_sets.build_seq` too.
> - **A walk checks a root it deleted.** A Rebuild walk fetches a page before
>   its roots' `BeginBuild`, so a root listed without data can be recreated
>   and built before the walk's delete runs at a higher sequence. After such
>   a delete the walk checks the root against its walk start, as it checks a
>   root it settles, and re-builds it on a hit.
>
> Why: an unversioned delete from a build that found a resource gone could
> remove the document, and its edge removal the edges, that a later build of
> the recreated resource wrote. And a row recreated after a hard delete
> restarted at 1, while Elasticsearch remembers a deleted document's version
> for `index.gc_deletes` (60s by default): the recreate's write lost as an OCC
> conflict, its finish cleared the mark, and it was not indexed.

> **Note (2026-10-04, runbook step L1.5):** leg 2 is a **per-version replace
> guarded by the Build Sequence**, with no wipe. Edges are stored per Schema
> Version: each `relations` row carries the `schema_version` whose plan found
> it, and an `edge_sets` row (`type, id, schema_version, build_seq`) stamps
> each version's set with the Build Sequence of the build that wrote it.
> `Store.ReplaceEdges` replaces, in one transaction, each given version's set
> when the build's sequence is not below its stamp, and leaves a set stamped
> higher unchanged. So each version's edges follow the Build Sequence like its
> document does (leg 1): they come from the build whose document
> Elasticsearch keeps. Fanout — the `RegisterChanges` Parents read,
> `GetParentResources`, `GetChildResources` — reads the union across
> versions, each resource once.
>
> - **No build path wipes.** The live build (`buildOne`) replaces each
>   version's set at its Build Sequence after its Elasticsearch writes, also
>   for a version whose write lost OCC: the stamp orders the two builds' sets
>   as Elasticsearch ordered their documents. A rebuild flush replaces the set
>   of each (resource, version) document it landed, so a version-targeted
>   rebuild replaces exactly its versions' sets and leaves the others alone.
>   A delete (`RemoveResource`) removes every set it locks, not yet guarded by
>   the Build Sequence (runbook step L2.1).
> - **Only the live build declares.** It runs every configured plan, so it
>   passes the configured versions as `declared`, and the same transaction
>   drops the undeclared sets stamped at or below its sequence; a rebuild
>   passes none. An instance on an older config during a rolling deploy
>   therefore can't drop a version a newer build wrote: that set is stamped
>   above it. The guard covers reordering, not mixed configs: an old-config
>   build at a *higher* sequence than the new-config build that wrote a new
>   version's set drops that set while the version's index keeps the newer
>   build's document — as can a build that captured the plans before a
>   `SetPlans` added the version. The drop therefore relies on
>   [ADR 0010](0010-cutover-readiness-is-a-pre-deploy-gate.md)'s order: the
>   rolling deploy completes before the new version's backfill, which
>   rewrites every document of that version and its set (the old-config
>   build left that document stale anyway). The undeclared versions are read
>   without a lock before the guarded statements, so a set stored after that
>   read isn't pruned — extra fanout only, until the next live build.
> - **One lock order.** Every writer of a resource's edges takes the
>   resource's `edge_sets` rows in ascending `schema_version` order and holds
>   them to commit (`ReplaceEdges`, `RemoveResource`), so two writers never
>   wait on each other in a cycle. A new writer takes them the same way. The
>   row locks last one short transaction, never a fetch: the rejection below
>   of a lock held across Plan execution stands.
>
> This supersedes two claims below. Leg 2's "a rebuild starts by removing the
> Parent's outgoing Relation rows" and the closing "do not 'optimise' away the
> edge wipe at the start of Build": there is no wipe to keep, and what a
> change to Build must preserve is that edges are written only through the
> guarded replace. And leg 3's "edge-less window": no build leaves a resource
> without edges while it builds, so a change to a Child in both the old and
> the new set reaches the Parent by fanout. The drift check still covers a
> Child new to the Parent's edges, whose registration can read the edges
> before the build commits them (seam S6 in laika-dev's
> `docs/open-questions.md` (at e1dc955)).
>
> Why: the wipe and the re-add were separate commits, so a slower build that
> wiped and re-added after a newer one left its own edges beside the newer
> build's document, and a change to a Child only the newer build found
> reached no Parent; the drift check, which compares only each build's own
> Children, didn't catch it. One stamp per resource couldn't order a
> version-targeted rebuild, which runs only some versions' plans, against a
> live build; one per version can.

> **Note (2026-10-02, runbook step L1.3):** leg 3 compares the **Change
> Sequence**, not upstream versions. Every change `RegisterChanges` accepts —
> upserts, version-0 notifications and deletes — is stamped with the next value
> of a global Postgres sequence Laika owns, in `resources.change_seq`. Each
> build takes a start from the same sequence before its fetches
> (`BuildBegun.Start` from `BeginBuild`), and the drift check
> (`Store.AnyChangedSince`) asks whether any resource the build fetched has a
> `change_seq` above that start. On a hit the build re-schedules itself as
> before.
>
> Why: the old check compared each Child's stored `Notification.Version` with
> the version the Plan observed, and two different producers filled the two
> sides — the notifier and the provider's `FetchRelated`. A unit mismatch, an
> in-memory stamp against a coarser read-back, or a source version that went
> backwards looped forever. A looping build re-marked before its own
> `ClearStale`, so `stale_since` never reset, and the sweep, which serves the
> oldest first, picked that resource on every pass and started another chain.
>
> Why it is safe: a change numbered below the build's start took its `nextval`
> before the build's (the sequence is `CACHE 1`, so `nextval` order is
> real-time order across sessions), and its notification came after its write
> was visible at source, so the build's fetch sees it. This is the assumption of
> seam S5 in laika-dev's `docs/open-questions.md` (at e1dc955) — the source
> serves a resource at least as new as its Notification — now for the Children
> as well as the root. No bound is needed: each hit is a change accepted after
> the build started. What the check still misses — a registration numbered
> above the start that is uncommitted when the drift query runs, and a deleted
> Child whose row is hard-deleted before it — is seam S6 there; its design pass
> is open point Q14.
>
> A Rebuild walk takes one start per plan walk (`Store.NextChangeSeq`) before
> the plan executes: the aggregation pipeline fetches pages ahead of the roots'
> `BeginBuild`, so that start would come too late. Each root carries its
> walk's start into the flusher, which checks the root itself as well as its
> Children: a root changed after its page was fetched is re-scheduled instead
> of keeping the page's older data under a newer Build Sequence. The live path
> and `rebuildByIDs` take their start from `BeginBuild`, before fetching, and
> check the Children only.
>
> Behaviour change: a version-0 notification or a delete of a Child now counts
> as drift. A producer whose versions are in the wrong unit no longer loops;
> its changes are rejected as stale instead.

> **Note (2026-10-04, runbook step L1.6):** the observed versions the old
> check compared are gone: a Plan's `BuildDoc.Relations` and the provider's
> `RelatedResource` carry identities only (`RelatedResource` field 2 is
> reserved). Nothing in the build path holds a Child's version.

> **Note (2026-10-05, runbook step L1.7):** the row locks a Store statement
> takes on several `resources` rows are taken in **(type, id) order**.
> `RegisterChanges`, `MarkStale`, `RenewOwners` and `ReleaseOwners`
> (`storage/postgres/pg.go`) each open with a `locked` CTE that takes `FOR
> UPDATE` on the existing rows they may write — for a registration, its items
> and the Parents of every item, read from `relations` — `ORDER BY type, id`,
> and their write depends on it, so every lock is held before the first row is
> written; their inserts run in the same order. Two registrations whose
> resources are each other's Parents no longer deadlock. A row the
> statement's snapshot doesn't see can't be locked ahead, so rows created or
> removed concurrently can still close a cycle; Postgres then aborts one
> registration whole, and the Store returns `core.ErrRegistrationAborted`,
> which `NotifyChange` and `NotifyChangeBatch` answer as `Aborted` for the
> producer to retry. A new statement or
> Store transaction that locks several `resources` rows follows the same
> order. The locks still last one statement and are never held across a
> fetch.

> _Amended by [ADR 0008](0008-stale-mark-inline-builds-and-temporal-slow-lane.md): the at-least-once re-enqueue leg is now the stale mark + sweep instead of River retries; Build Sequence OCC and the drift check are unchanged._

Multiple indexer instances run concurrently and there is no per-resource lock. Safety against racing rebuilds of the same Parent comes from three independent mechanisms:

1. **Build Sequence as ES OCC ordering.** Every rebuild bumps a Postgres counter for the Resource and uses that value as Elasticsearch's `external_gte` version. Racing writes are commutative — ES keeps the highest sequence and rejects the rest.
2. **Wipe-and-replace edges per rebuild.** A rebuild starts by removing the Parent's outgoing Relation rows; the executed Plan then re-adds the Relations it discovers. The Plan, not stored history, is the source of truth for the Parent's current children.
3. **Drift check at the end of each rebuild.** The Build observes a Version for each Child as it fetches them. After writing the document, it compares those observed Versions against the Versions currently stored in the resources table (written by `RegisterChange`). If any Child's stored Version is higher than what was observed, a concurrent upstream change landed during the rebuild's edge-less window and the Parent fanout could not reach this rebuild. The Build re-enqueues itself to converge.

We chose this over per-Parent row locking because the system targets thousands of changes per second and must stay consistent with upstream within seconds. A `SELECT … FOR UPDATE` held across a Plan execution (which makes N upstream calls and can take tens to hundreds of milliseconds) would serialise rebuilds of any hot Parent across the whole indexer fleet — that is the hot-path bottleneck we cannot afford. The drift-check re-enqueue is the eventual-consistency price we pay for keeping the build path lock-free.

**Implication for contributors:** anything that changes how Build interacts with the Store must preserve all three legs. In particular: do not "optimise" away the edge wipe at the start of Build, do not skip the drift check, and never assume a Build runs at most once for a given (Resource, change) — at-least-once is the contract.
