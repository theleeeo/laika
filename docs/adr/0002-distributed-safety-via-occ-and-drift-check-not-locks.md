# Distributed safety via OCC and drift-check, not locks

> _Amended by [ADR 0008](0008-stale-mark-inline-builds-and-temporal-slow-lane.md): the at-least-once re-enqueue leg is now the stale mark + sweep instead of River retries; Build Sequence OCC and the drift check are unchanged._

Multiple indexer instances run concurrently and there is no per-resource lock. Safety against racing rebuilds of the same Parent comes from three independent mechanisms:

1. **Build Sequence as ES OCC ordering.** Every rebuild bumps a Postgres counter for the Resource and uses that value as Elasticsearch's `external_gte` version. Racing writes are commutative — ES keeps the highest sequence and rejects the rest.
2. **Wipe-and-replace edges per rebuild.** A rebuild starts by removing the Parent's outgoing Relation rows; the executed Plan then re-adds the Relations it discovers. The Plan, not stored history, is the source of truth for the Parent's current children.
3. **Drift check at the end of each rebuild.** The Build observes a Version for each Child as it fetches them. After writing the document, it compares those observed Versions against the Versions currently stored in the resources table (written by `RegisterChange`). If any Child's stored Version is higher than what was observed, a concurrent upstream change landed during the rebuild's edge-less window and the Parent fanout could not reach this rebuild. The Build re-enqueues itself to converge.

We chose this over per-Parent row locking because the system targets thousands of changes per second and must stay consistent with upstream within seconds. A `SELECT … FOR UPDATE` held across a Plan execution (which makes N upstream calls and can take tens to hundreds of milliseconds) would serialise rebuilds of any hot Parent across the whole indexer fleet — that is the hot-path bottleneck we cannot afford. The drift-check re-enqueue is the eventual-consistency price we pay for keeping the build path lock-free.

**Implication for contributors:** anything that changes how Build interacts with the Store must preserve all three legs. In particular: do not "optimise" away the edge wipe at the start of Build, do not skip the drift check, and never assume a Build runs at most once for a given (Resource, change) — at-least-once is the contract.
