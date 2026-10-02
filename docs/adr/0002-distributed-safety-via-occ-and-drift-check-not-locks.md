# Distributed safety via OCC and drift-check, not locks

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
> seam S5 in laika-dev's `docs/open-questions.md` — the source serves a resource
> at least as new as its Notification — now for the Children as well as the
> root. No bound is needed: each hit is a change accepted after the build
> started. What the check still misses — a registration numbered above the
> start that is uncommitted when the drift query runs, and a deleted Child
> whose row is hard-deleted before it — is seam S6 there; its design pass is
> open point Q14.
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

> _Amended by [ADR 0008](0008-stale-mark-inline-builds-and-temporal-slow-lane.md): the at-least-once re-enqueue leg is now the stale mark + sweep instead of River retries; Build Sequence OCC and the drift check are unchanged._

Multiple indexer instances run concurrently and there is no per-resource lock. Safety against racing rebuilds of the same Parent comes from three independent mechanisms:

1. **Build Sequence as ES OCC ordering.** Every rebuild bumps a Postgres counter for the Resource and uses that value as Elasticsearch's `external_gte` version. Racing writes are commutative — ES keeps the highest sequence and rejects the rest.
2. **Wipe-and-replace edges per rebuild.** A rebuild starts by removing the Parent's outgoing Relation rows; the executed Plan then re-adds the Relations it discovers. The Plan, not stored history, is the source of truth for the Parent's current children.
3. **Drift check at the end of each rebuild.** The Build observes a Version for each Child as it fetches them. After writing the document, it compares those observed Versions against the Versions currently stored in the resources table (written by `RegisterChange`). If any Child's stored Version is higher than what was observed, a concurrent upstream change landed during the rebuild's edge-less window and the Parent fanout could not reach this rebuild. The Build re-enqueues itself to converge.

We chose this over per-Parent row locking because the system targets thousands of changes per second and must stay consistent with upstream within seconds. A `SELECT … FOR UPDATE` held across a Plan execution (which makes N upstream calls and can take tens to hundreds of milliseconds) would serialise rebuilds of any hot Parent across the whole indexer fleet — that is the hot-path bottleneck we cannot afford. The drift-check re-enqueue is the eventual-consistency price we pay for keeping the build path lock-free.

**Implication for contributors:** anything that changes how Build interacts with the Store must preserve all three legs. In particular: do not "optimise" away the edge wipe at the start of Build, do not skip the drift check, and never assume a Build runs at most once for a given (Resource, change) — at-least-once is the contract.
