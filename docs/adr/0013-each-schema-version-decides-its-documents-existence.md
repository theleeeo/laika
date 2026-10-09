# Each Schema Version decides its own document's existence

_Accepted, 2026-10-06. Supersedes the cross-version existence agreement in
[ADR 0004](0004-multi-schema-version-writes-for-graceful-migrations.md) and the existence rule of
ADR 0002's L2.6 and L2.7 notes. Built by runbook step L2.9._

> **Note (2026-10-09, runbook step L3.10):** the backfill's part of the Q24
> note below is built, as the probe pass (`core/rebuild_probe_pass.go`). It
> runs at the end of a whole-type rebuild that runs fewer than all of the
> type's plans with an `Executer`, once the walk has finished, for each
> selected version whose plan has a `Probe`; a version without one is
> skipped, logged at Info, and its left-out rows stay uncovered. It also
> leaves out tombstones. It lists its rows with `Store.ListUncovered`, by the
> predicate the cutover check's coverage counts with, and takes each page's
> Build Sequences in one statement with `Store.BeginBuilds`, which begins
> only rows that still exist and aren't tombstones: a row gone since the
> listing is neither probed nor written. An excluded id's empty edge set is
> written by a `ReplaceEdges` that declares nothing, so no other version's
> set is touched. A failed probe, delete, edge write or mark leaves its ids
> uncovered for the next backfill and doesn't fail the walk; a failed listing
> or begin fails the rebuild, which `RunRebuild` retries. The pass keeps no
> cursor, so a resumed attempt runs it whole again.

> **Note (2026-10-08, open point Q24):** decided, and runbook step L3.10 builds
> the backfill's part.
>
> - **A version excludes a resource only through its per-id answers.** Its
>   plan's root fetch returns nil for the id, and its `Probe` leaves the id
>   out. A filter only in the plan's listing excludes nothing. Every live
>   build fetches one resource by id, so a version whose fetch ignores the
>   filter indexes an excluded resource the first time it changes, and keeps
>   one that stops passing the filter. A plan that filters its listing applies
>   the same filter to its fetch and its probe.
> - **A backfill asks its version about the rows its listing left out.** A
>   rebuild of the whole type that selects versions, and so runs fewer than
>   all of the type's plans, asks each selected version's `Probe` once its
>   walk has run its listing to the end. It asks a page at a time about the
>   type's rows that have no stale mark, no edge set of that version, and the
>   walk's own metadata. For an id the probe leaves out, that version's
>   document is deleted and its edge set emptied, at a Build Sequence taken
>   before the probe. The row isn't marked: marking would cost a full build
>   per id, the other versions keep their documents, and a resource gone from
>   every version is the reverse sweep's to find (ADR 0012). An id the probe
>   returns is marked for the sweep's build. The cutover check's coverage gate
>   relies on this (ADR 0010's Q24 note).
>
> Rejected:
>
> - Building each such id: one full build, every plan's fetches, per id. That
>   costs too much at scale, where a source's by-ids call is the unit of work.
> - Treating a completed listing's omissions as exclusions. Rows the listing
>   skipped would leave the version silently: through offset page tokens
>   (ADR 0011), or a listing scoped to one actor.
>
> A version-selected rebuild's nil for an id its listing *does* return still
> marks the id for the sweep (seams S40 in laika-dev's `docs/seams.md`).

A build runs every Schema Version's plan for a resource (ADR 0004). Until now, existence was
decided only when they all agreed. If every plan returned nil, the build deleted every version's
document and the resource's row. If the plans disagreed, some returning a document and some nil,
the build failed and wrote nothing. That left the stale mark for the sweep, whose build ran the
same plans and failed again.

When two versions' plans disagree for good, that loops forever. That happens with a new version
whose plan reads a different endpoint or filters out some resources, or with a broken new plan
that returns nil for everything. Every sweep pass retries the resource, and the old version's
document stays frozen at its last good build while the resource keeps changing at its source.

**Decision.** A plan's nil is its own version's answer.

- **A build** writes the document of every version whose plan returned one. For each version
  whose plan returned nil, it deletes that version's document at the build's Build Sequence
  (`external_gte`, ADR 0002) and replaces that version's edge set with an empty one at the same
  sequence. Then it settles as a successful build does, clearing the mark.
- **The row** stays while any version has a document. One row per resource remains:
  - documents live per version index, and edge sets per version;
  - the row holds what the versions share: the Build Sequence, the mark, the owner and the
    metadata.
- **When every plan returns nil**, the build deletes every version's document, the edge sets and
  the row, as before.
- A rebuild walk applies the same rule to each plan's outcome for an id.
- A version-selected rebuild deletes its selected versions' documents when their plans return nil,
  and keeps the row. The row can be removed only by a build that runs every plan. So when all of
  its selected plans return nil, the rebuild also marks the resource stale, and the sweep's full
  build decides the row.

**Why not keep agreement.** It treats a permanent difference between versions as a transient
error. The transient case, a resource deleted between two plans' fetches, converges without it:
- a notified delete is a tombstone, whose delete removes every version's document and the row
  without running a plan (`deleteOne`);
- a lost delete is the reverse sweep's to find (ADR 0012), as it is today for one that lands after
  both fetches.

The other alternative was to back off a disagreeing resource without changing the rule. It would
stop the loop, which the stale sweep's backoff does anyway for every build that keeps failing, but
the old version's document would stay frozen.

**Consequences.**

- Versions may hold different sets of documents. No read spans two versions of one type, because
  every search reads through the type's alias (ADR 0009), so a search shows what the read
  version's plan says exists.
- The cutover check's document-count parity (ADR 0010) assumed every version holds the same set.
  A version that legitimately holds fewer fails it unless the operator allows the gap.
- The reverse sweep (ADR 0012) still probes through one plan, which answers for its own version.
  An id its version excludes is a suspect at every run: a needless but harmless build. An id only
  another version's plan stops returning, without a notification, is never a suspect, so that
  version's document stays until something else builds the id.
