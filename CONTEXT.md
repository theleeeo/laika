# Indexer

A distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via change notifications. The system listens for "something changed upstream", figures out which search documents are affected, fetches fresh data from the source of truth, and writes the result to Elasticsearch.

## Language

### Versioning

**Version**:
The upstream service's notion of how current a resource is, carried on a [[notification]]. Monotonic per resource, and compared only with that resource's stored Version, for stale rejection: a notification whose Version is not above the stored one is dropped. `0` means the upstream service does not track versions for this resource. Versions of different resources, or from different producers, are never compared: the drift check reads the [[change sequence]], not Versions.

**Schema Version**:
The versioned shape of an indexed document — which fields are present, which relations are pulled, which index name it lives under (e.g. `a_search_v2`). A single resource type can have multiple Schema Versions in flight simultaneously, used for zero-downtime migrations between document shapes.

**Build Sequence**:
The indexer's own per-resource monotonic counter, bumped on every build of a given resource. Used as the value for Elasticsearch's `external_gte` versioning so that concurrent rebuilds of the same document land in the correct order. Has nothing to do with the upstream — it exists purely to serialise writes to a single ES document across distributed indexer instances.

**Change Sequence**:
A global Postgres sequence the indexer owns (`change_sequence`). Every change `RegisterChanges` accepts — upserts, version-`0` notifications and deletes alike — is stamped with its next value in `resources.change_seq`; a stale-rejected notification, a [[stale mark]] and `BeginBuild` leave the stamp alone. Each build takes a start from the same sequence before its fetches — from `BeginBuild`, or, in a [[rebuild]] walk, one per plan walk before its first page — and the drift check asks whether any resource the build fetched has a `change_seq` above that start; if so, the build re-schedules itself. Like the Build Sequence it has nothing to do with upstream Versions, so no upstream clock, unit or precision can make the check loop.

### The graph

**Resource**:
A single item from an upstream service, identified by Type and ID. The atomic unit the indexer reasons about.

**Relation**:
A directed link from one Resource to another: a Parent contains a Child in its indexed document. The word is used consistently — in the YAML config (`relations:`), in the Store's persisted edges, and in prose.

**Parent / Child**:
The two endpoints of a Relation. The Parent is the Resource whose document includes data from the Child. A single Resource is a Parent in some Relations and a Child in others — these are roles, not types.

**Strategy**:
How a Relation's Child reaches the Parent's searchable surface. `denormalize`
(default) copies the Child's selected fields into the Parent's Document — a
change to the Child rebuilds every Parent that contains it. `reference` copies
nothing: the Parent keeps only the join key and the Child's fields are resolved
at search time via a two-phase join. Used when a low-count Child would otherwise
be denormalized into a high-count Parent, where the rebuild fanout is ruinous.
Distinct from Cardinality (`one`/`many`), which is join multiplicity.

### Operations

**Notification**:
A message from an upstream service that a single Resource changed — carries Type, ID, change Kind, upstream Version, and optional metadata. Deliberately carries no field data; the indexer always re-fetches from upstream when it builds the document.

**Plan**:
An executable that, given a Resource Type and ID (or no ID, for the listing case), produces the documents the indexer will write. Plans encapsulate all data fetching; the orchestrator only executes them. Each Schema Version of a resource has its own Plan.

**Build**:
The normal update path: produce and write the document for one or a few specific Resource IDs, respecting the Build Sequence OCC ordering. Triggered by a Notification, by the drift-check re-build, or by another Build's downstream effect. Every trigger first records a [[stale mark]] (durable intent), then attempts an [[inline build]] on the in-process pool; the guarantee is the mark, not the run.

**Inline build**:
A Build executed immediately on `core.Indexer`'s in-process bounded worker pool, right after the triggering change is marked stale. The accelerator, not the guarantee: it is allowed to shed (a submit that finds the pool's queue full is dropped; a `RegisterChanges` called with `WaitForSlot` instead waits while the queue is at or above `QueueHighWater`, and if its context or shutdown ends the wait it leaves the build to the sweep too), fail, or die, because the [[stale mark]] survives and the [[sweep]] will rebuild. Replaces the old job-queue hop between a Notification and its Build. Only a mark that claimed its row for a [[build owner]] submits one: a resource whose owner is live gets no second inline build on any instance, and the changes marked meanwhile are served by the owner's single follow-up.

**Build owner**:
The one inline build, delete or [[sweep]] entry that holds a resource, recorded on its `resources` row (`owner_seq`, the owner token; `owner_since`) with a lease (`Config.OwnerLease`, default 30s) renewed when its pool task is dequeued, when the sweep reaches its entry, and at a build's `BeginBuild`. A [[stale mark]] made for a submit claims the row when it has no owner or the lease has expired, and a sweep claims what it lists (`ListStale`). The owner finishes with `FinishOwned` (a delete with `DeleteResourceIfSeq`): if `stale_seq` is unchanged it clears the mark and the ownership, and if a change moved it the owner re-claims the row for at most one follow-up, run with the row's metadata (the last mark's, in commit order) — a delete if the row is a [[tombstone]]. A failed or shed owner releases the row and keeps the mark. [[Rebuild]] walks and a direct `idx.Build` own nothing. Not a lock: nothing waits on it, and a lapsed lease costs a duplicate build that the Build Sequence orders — except for a delete, until the delete is versioned (ADR 0008).

**Stale mark**:
The durable record of build intent on a resource row: a `stale_seq` counter bumped on every marking and a `stale_since` timestamp set to the oldest unserved change (`COALESCE(stale_since, now())`). Set before any [[inline build]] attempt, so no change is ever lost. Cleared only by a Build that captured the same `stale_seq` it started with — a newer change that moved the counter leaves the row stale for the [[build owner]]'s follow-up or the [[sweep]]. Carries at-least-once: "a Build runs at least once for this change" is guaranteed by the mark plus the sweep, not by any retry queue.

**Sweep**:
A Temporal-scheduled recovery pass (`StaleSweep` workflow, schedule `laika-stale-sweep`) that rebuilds every resource whose [[stale mark]] has survived past a staleness threshold — including [[tombstone]]s awaiting cleanup — skipping any with a live [[build owner]] and claiming the rest. The safety net behind the [[inline build]]: crashes, shed builds, and failed builds all converge here. Runs on the shared `laika-indexer` task queue and relies on Build Sequence OCC for safety rather than locks.

**Tombstone**:
A resource row flagged `deleted = true` (with `version` reset to `0`) whose Elasticsearch documents and Relation edges are still being cleaned up. A delete-Notification marks the row rather than removing it, so a failed ES delete has something durable to retry; the hard delete of the row happens only after ES cleanup succeeds, guarded by the captured `stale_seq` so a concurrent re-create wins. The [[sweep]] retries lingering tombstones.

**Rebuild**:
The reset path: produce and write documents for one, many, or all Resources of a Type without honouring prior state. Used to populate a newly-added Schema Version, to recover from corruption, or to reset documents to a new shape. Always wins over any concurrent Build because it stamps a fresh Build Sequence. A walk fetches each page before its roots' builds begin, so it re-schedules a root that changed after its page was fetched — or whose child did — rather than leave the page's older data in place.

**Rebuild cursor**:
The durable position of an all-of-type [[rebuild]] walk: a `{plan version, page token}` pair meaning every Resource listed by the pages before that token has settled — its [[document]]s written and its [[stale mark]] cleared — or is durably marked stale for the [[sweep]]. Carried in the `RunRebuild` activity's Temporal heartbeat details, so a retried attempt resumes there instead of walking again from the head. Only a walk with exactly one active [[plan]] has such a position — a single-version Type, or a version-targeted backfill — and it records one only at a page boundary it has fully consumed and flushed; a multi-plan walk has none and restarts from scratch. The page token must mean the same place when it is redeemed as when it was written, which is a property of the upstream listing, not something the indexer can enforce (ADR 0011).

**Document**:
The denormalised search artifact written to Elasticsearch — the result of a Build or a Rebuild. One Document per (Resource, Schema Version) pair.

### Search

**Search** (single-resource):
A query against the indexed Documents of one Resource Type. The caller names the Type; matching, filtering, and reference-relation joins are all scoped to it. The existing search path.

**Federated Search**:
A single query over a caller-supplied set of Resource Types, returning one relevance-ranked list whose hits span Types — "the most relevant one" is simply the top hit. Executed, by default, as a single multi-index Elasticsearch query against standardized searchable text the indexer populates at Build time; the Elasticsearch backend also offers experimental execution modes behind a toggle (the same query with index-local term statistics, and a per-Type fan-out merged client-side) for ranking/latency comparison. Per-Type document visibility is enforced by per-Type filters that a federated search middleware supplies on the request, combined as per-index filter groups; the federated middleware chain is independent of the single-resource one. Fields reachable only through a `reference` Relation do not contribute to Federated Search ranking (they remain searchable via single-resource Search).

**Searchable Tier**:
Which standardized searchable surface a field feeds, declared per field (`search: primary | secondary | none`, omitted = `none`). `primary` is a Document's own high-signal text (e.g. its name); it is matchable whenever the searcher may see the Document at all, and ranks above `secondary`. `secondary` holds lower-signal or denormalized-child text. A match on a Document's own name outranks a match that only landed via a related Resource's name.

**Scoped Searchable Text**:
The `secondary` tier, stored as an array of nested entries each carrying searchable text and an optional scope attribution. The text is populated from `search: secondary` DSL fields; the scope attribution is left empty by the standalone app (so its secondary matches are unscoped) and populated only by a library consumer, whose federated middleware also supplies the caller's scope value on the request. This lets a consumer prevent a Parent shared across tenants from leaking a tenant-specific Child's text to a tenant lacking access, while keeping all tenant policy out of the standalone app. The structure and the Federated Search query are identical in both modes — only whether the scope attribution and filter are present differs. Primary fields need no attribution: a Document's own fields are never narrower in visibility than the Document itself.
