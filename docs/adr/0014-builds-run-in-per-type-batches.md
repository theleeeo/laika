# Builds run in per-type batches

_Accepted, 2026-10-08. Built by runbook steps L5.1–L5.7._

Every build path builds one resource at a time. `projection.BuildRequest` carries one
`ResourceID`, `Indexer.Build` begins, executes and finishes one id after another, a registration
submits one pool task per id, and the stale sweep calls `Build` once per entry. Each version's
plan therefore reads its source once per resource, and its related reads once per resource
again. A poller page of 100 changes costs 100 reads per plan. A backfill that marks thousands of
ids for the sweep costs thousands (seams S40). At scale that load is the cost of keeping the
index current. The source services are getting reads of many ids in one call
(`vx-service-changes.md` items 28–30).

**Decision.** A build runs a batch of one type's resources, and each plan reads the batch at
once.

- **A plan answers a list of ids.** `BuildRequest` asks about a list of ids (none: the
  all-of-type walk) with one `Metadata`. The plan answers every asked id exactly once, with a
  document or an explicit nil, its version's answer (ADR 0013).
  - An asked id the plan doesn't answer fails. It is never read as a nil, as a walk's id that one
    plan's listing omits isn't. A plan whose source leaves an id out of a by-ids read answers nil
    for it itself, so only a broken plan leaves one unanswered, and it fails safe rather than
    deleting.
  - A plan may report an error for one id, which fails that id alone.
- **A batch is one type and one metadata.** A source read is scoped to its caller's actor, so
  ids are grouped by the metadata their rows hold (an owned build) or the caller's (a build that
  owns nothing). A batch holds at most `Config.BuildBatchSize` ids, 100 by default, the size the
  services' batch reads accept. An owned batch begins its rows in one Store statement and groups
  them by the metadata it returns.
- **Each id keeps its own build.** Its Build Sequence, drift start, write, edge set, nil deletes
  and finish are its own, as a single build's are (ADR 0002). What is shared is the begin, each
  plan's reads, one bulk write, one mark of the batch's Parents (ADR 0006) and one drift check.
- **A batch that fails as a whole fails every id in it.** Each is released with backoff
  (`Store.ReleaseFailed`) and the sweep retries it. The failure is logged once, at Error, with
  the type, the metadata, the count, the first ids and the error, so it can be monitored.
  Splitting a failed batch to find the id that fails it is deferred (laika-dev `docs/deferrals.md`
  D13).
- **An owned build keeps its ownership while it builds, and writes only what it holds.** It
  renews its rows' leases while it runs, so a batch of any length holds them. A delete that
  arrives meanwhile only marks the row, and the owner's follow-up deletes after the write, at a
  higher Build Sequence. Right before writing an id, the build checks that its last renewal still
  held it, less than a lease ago; an id it lost isn't written, and its mark stays for the new
  owner or the sweep. A slow owned batch therefore doesn't restore a deleted document past
  `index.gc_deletes` (seams S16), short of a stall between that check and the write. A build
  that owns nothing, a walk or a by-ids rebuild, stays exposed as before (open point Q17).
- **The build pool counts tasks.** A task holds one batch. With batched reads a task's source
  cost is about one read per plan whatever its size, so tasks are the unit of load the pool's
  queue limits, its pressure threshold and `WaitForSlot` measure.
- **Submissions group by type.** What one registration accepts, one cascade marks, one batch's
  finishes hand on and one sweep pass claims is grouped per type into batches. A single
  notification still builds alone; collecting those over a window is deferred (D14).

**Rejected.**

- Confirming each id a batch read leaves out with a single read. It brings back a per-id read for
  every resource a version excludes. The services' batch-read contract is trusted instead, as
  the reverse sweep's probes trust it (ADR 0012), and it must fail rather than silently cap.
- Splitting a failed batch now. A clear log line per failed batch comes first (D13).
- Counting ids in the pool. A task's cost no longer grows with its ids.
- Bounding a batch's duration instead of renewing its leases. Nothing bounds a source's latency.
- Coalescing concurrent single reads in the harness. It batches one embedder's reads, not
  Laika's builds, and leaves the per-id begin, write and finish.

**Open.** How a source plugs into the aggregator's batched reads, whether it reads many ids or
one, and many parents' related resources or one's, is open point Q25 in laika-dev's
`docs/open-points.md`.
