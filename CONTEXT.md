# Indexer

A distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via change notifications. The system listens for "something changed upstream", figures out which search documents are affected, fetches fresh data from the source of truth, and writes the result to Elasticsearch.

## Language

### Versioning

**Version**:
The upstream service's notion of how current a resource is, carried on a [[notification]]. Monotonic per resource, and compared only with that resource's stored Version, for stale rejection: a notification whose Version is not above the stored one is dropped. `0` means the upstream service does not track versions for this resource. Versions of different resources, or from different producers, are never compared: the drift check reads the [[change sequence]], not Versions.

**Schema Version**:
The versioned shape of an indexed document — which fields are present, which relations are pulled, which index name it lives under (e.g. `a_search_v2`). A single resource type can have multiple Schema Versions in flight simultaneously, used for zero-downtime migrations between document shapes.

**Build Sequence**:
The indexer's own number for a build or delete of a resource (`resources.build_idx`): each `BeginBuild`, and each notified delete's `BeginDelete`, draws a fresh value from the [[change sequence]], so a resource's Build Sequences rise with every build and keep rising across a hard delete and a recreate of its row. Used as the value for Elasticsearch's `external_gte` versioning — of a delete as well as a write — so that concurrent builds and deletes of the same document land in the correct order, and a delete never removes a document a newer build wrote. Has nothing to do with the upstream — it exists purely to serialise writes to a single ES document, and to the resource's [[edge set]]s, across distributed indexer instances.

**Change Sequence**:
A global Postgres sequence the indexer owns (`change_sequence`). Every change `RegisterChanges` accepts — upserts, version-`0` notifications and deletes alike — is stamped with its next value in `resources.change_seq`; a stale-rejected notification, a [[stale mark]], `BeginBuild` and `BeginDelete` leave the stamp alone. The [[build sequence]] and the stale mark's `stale_seq` are drawn from the same sequence. Each build takes a start from it before its fetches — from `BeginBuild`, whose one value is both its start and its Build Sequence, or, in a [[rebuild]] walk, one per plan walk before its first page — and the drift check asks whether any resource the build fetched has a `change_seq` above that start; if so, the build re-schedules itself. Like the Build Sequence it has nothing to do with upstream Versions, so no upstream clock, unit or precision can make the check loop.

### The graph

**Resource**:
A single item from an upstream service, identified by Type and ID. The atomic unit the indexer reasons about.

**Relation**:
A directed link from one Resource to another: a Parent contains a Child in its indexed document. The word is used consistently — in the YAML config (`relations:`), in the Store's persisted edges, and in prose. Each [[schema version]]'s Plan finds its own Children, so the Store keeps a Parent's edges per Schema Version, one [[edge set]] each; fanout reads their union, each Parent once.

**Edge set**:
One Resource's edges of one [[schema version]] — the Children that version's Plan found — stamped with the [[build sequence]] of the build that wrote it (`edge_sets.build_seq`). Replaced only by a build at or above that stamp (`Store.ReplaceEdges`), so a version's edges come from the build whose [[document]] Elasticsearch keeps for that version; no build removes them first. A version whose Plan returns nil for the Resource while another returns a [[document]] gets an empty set at the build's Build Sequence, beside the delete of its Document, so its old Children go (ADR 0013). A [[build]], which runs every configured Plan, also drops the sets of Schema Versions the config no longer declares, when stamped at or below it; a [[rebuild]] replaces only the sets of the versions whose documents it wrote or whose Plans returned nil. A delete removes those stamped at or below its Build Sequence, and leaves a set a newer build wrote (`RemoveResource`).

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

**Resource metadata**:
A resource's own metadata, on its `resources` row (`metadata`): the actor a [[build owner]] — an [[inline build]], a follow-up, a [[sweep]] entry — fetches as, read from the row when its build begins (`BeginBuild`). Written only by the resource's own registrations, each replacing it, and, while the row has none (NULL or empty), by its [[plan]]'s report: `projection.BuildDoc.ResourceMetadata`, the resource's own metadata as the plan knows it, which a build stores with its [[edge set]]s and never over metadata the row holds — with several plans, the first that reports one wins. No other [[stale mark]] writes it: a Parent's mark (a child's registration's or its build's ADR 0006 cascade), a drift re-mark and a [[rebuild]]'s marks leave it alone, so a change to one resource never gives another its metadata. A row with none builds with none. Not `BuildDoc.Metadata`, the fetch context flowing down a plan (the request's), and not a [[rebuild]] walk's metadata, the walk's actor, which a report never is and which a build that owns nothing fetches with.

**Plan**:
An executable that, given a Resource Type and ID (or no ID, for the listing case), produces the documents the indexer will write. Plans encapsulate all data fetching; the orchestrator only executes them. Each Schema Version of a resource has its own Plan.

**Probe**:
A [[plan]]'s optional existence check (`projection.Plan.Probe`): given several Resource IDs and one actor's metadata, it returns exactly the IDs the plan's root fetch would find for that actor, without building anything. The embedder writes it beside the plan, on the clients its root fetch uses. The [[reverse sweep]] calls it, with the first of a type's plans that has one. It answers for that plan's [[schema version]] only, since each version's plan decides its own [[document]]'s existence (ADR 0013): an ID that version excludes is a suspect at every run, a needless build, and an ID only another version's plan stops returning is never a suspect. One that returns an ID its root fetch would not find misses a delete; one that misses an ID its root fetch would find costs a needless build. The app's DSL plans have none.

**Build**:
The normal update path: produce and write the document for one or a few specific Resource IDs, respecting the Build Sequence OCC ordering. Triggered by a Notification, by the drift-check re-build, or by another Build's downstream effect. Every trigger first records a [[stale mark]] (durable intent), then attempts an [[inline build]] on the in-process pool; the guarantee is the mark, not the run.

**Inline build**:
A Build executed immediately on `core.Indexer`'s in-process bounded worker pool, right after the triggering change is marked stale. The accelerator, not the guarantee: it is allowed to shed (a submit that finds the pool's queue full is dropped; a `RegisterChanges` called with `WaitForSlot` instead waits while the queue is at or above `QueueHighWater`, and if its context or shutdown ends the wait it leaves the build to the sweep too), fail, or die, because the [[stale mark]] survives and the [[sweep]] will rebuild. Replaces the old job-queue hop between a Notification and its Build. Only a mark that claimed its row for a [[build owner]] submits one: a resource whose owner is live gets no second inline build on any instance, and the changes marked meanwhile are served by the owner's single follow-up.

**Build owner**:
The one inline build, delete or [[sweep]] entry that holds a resource, recorded on its `resources` row (`owner_seq`, the owner token; `owner_since`) with a lease (`Config.OwnerLease`, default 30s) renewed when its pool task is dequeued, when the sweep reaches its entry, and at a build's `BeginBuild` or a delete's `BeginDelete`; a renewal at dequeue or at a sweep entry that finds the ownership lost (another owner claimed the row, or a clear or hard delete dropped it) skips the work, and a failed one skips it and releases the row. The owner's build runs with the row's [[resource metadata]] as its `BeginBuild` reads it, so a build that waited in the pool queue or the sweep runs with metadata registered meanwhile. A [[stale mark]] made for a submit claims the row when it has no owner or the lease has expired, and a sweep claims what it lists (`ListStale`). The owner finishes with `FinishOwned` (a delete, or a build whose plans all returned nil, with `DeleteResourceIfSeq`, which removes the row): if `stale_seq` is unchanged it clears the mark and the ownership, and if a change moved it the owner re-claims the row for at most one follow-up — a delete if the row is a [[tombstone]] — which runs with the row's metadata, so metadata registered while the owner ran is built by it. A failed owner releases the row and, unless a registration of the resource itself was accepted since its claim, backs it off for the [[sweep]] (`ReleaseFailed`); a shed one, or one whose renewal failed or whose build a cancellation cut short, only releases it (`ReleaseOwners`). Either keeps the mark. [[Rebuild]] walks and a direct `idx.Build` own nothing. Not a lock: nothing waits on it, and a lapsed lease costs a duplicate build or delete that the Build Sequence orders, unless a build overtaken by a delete writes after Elasticsearch has forgotten the delete (`index.gc_deletes`) (ADR 0008).

**Stale mark**:
The durable record of build intent on a resource row: a `stale_seq` set to a fresh [[change sequence]] value on every marking — so no value is reused, across a hard delete and a recreate of the row too, and no number of the old row matches the new one — and a `stale_since` timestamp set to the oldest unserved change (`COALESCE(stale_since, now())`). Set before any [[inline build]] attempt, so no change is ever lost. Cleared only by a Build that captured the same `stale_seq` it started with — a newer change that moved it leaves the row stale for the [[build owner]]'s follow-up or the [[sweep]]. Carries at-least-once: "a Build runs at least once for this change" is guaranteed by the mark plus the sweep, not by any retry queue.

**Sweep**:
A Temporal-scheduled recovery pass (`StaleSweep` workflow, schedule `laika-stale-sweep`) that rebuilds every resource whose [[stale mark]] has survived past a staleness threshold — including [[tombstone]]s awaiting cleanup — skipping any with a live [[build owner]] or a backoff still running, and claiming the rest, earliest turn first. The safety net behind the [[inline build]]: crashes, shed builds, and failed builds all converge here. A resource whose owned build or delete fails is backed off: its next turn is set `Config.SweepBackoff` after the failure, doubling with each failure in a row up to `Config.SweepBackoffMax`, until a successful build or delete resets it (except an owned build whose ownership a moved mark took meanwhile, which changes nothing), or a registration of the resource itself does. So one that never builds is retried at the cap's interval and never holds back the rest, and its mark stays. Runs on the shared `laika-indexer` task queue and relies on Build Sequence OCC for safety rather than locks.

**Reverse sweep**:
A Temporal-scheduled existence check per configured Resource Type (`ReverseSweep` workflow, schedule `laika-reverse-sweep-<type>`, `Config.ReverseSweeps`) that finds Resources deleted at their source without a [[notification]]. It lists the Type's live `resources` rows by ID in pages (`Store.ListResources`), asks the source which still exist through a [[probe]] — once per page and actor, each row as the actor in its own [[resource metadata]] — and puts a [[stale mark]] on each ID the probe doesn't return, claiming it for a [[build owner]] unless one is live; the owner's [[build]], running every plan, deletes the [[document]] of each version whose plan finds the Resource gone, and its row too when every plan does, and rebuilds the rest. Its [[probe]] answers for one version's plan. It deletes nothing itself and heals nothing: a Resource the probe finds is left alone. It cannot see a document whose row is gone. A Type whose plans have no probe is skipped with a warning. Its cursor, the last ID of a page whose suspects are marked, rides in its activity's heartbeat details like a [[rebuild cursor]] (ADR 0012). Not the [[sweep]], which serves stale marks.

**Forward walk**:
A Temporal-scheduled [[rebuild]] walk per configured Resource Type (`ForwardWalk` workflow, schedule `laika-forward-walk-<type>`, `Config.ForwardWalks`) that finds Resources created or changed at their source without a [[notification]]. Each run walks the whole Type from its source once per metadata map its configuration returns at that run, one `RebuildWalk` after another. A map is the walk's actor and nothing else: an actor lists only what it can see, and a row's [[resource metadata]] stays its own, never the map. A walk is paced: it asks its plans for pages of a configured size, waits before each page while the build pool is pressured, and gives each page a minimum time. It builds what the source lists, so it cannot find what the source no longer has; that is the [[reverse sweep]]'s. A walk whose failed Resources are all marked stale is done, and the [[sweep]] serves them (ADR 0011, ADR 0012). Not the [[sweep]], which serves stale marks.

**Tombstone**:
A resource row flagged `deleted = true` (with `version` reset to `0`) whose Elasticsearch documents and Relation edges are still being cleaned up. A delete-Notification marks the row rather than removing it, so a failed ES delete has something durable to retry; the delete first takes a [[build sequence]] (`BeginDelete`) that its ES deletes and edge removal carry, so a re-create built above it keeps its document and edges, and the hard delete of the row happens only after ES cleanup succeeds, guarded by the captured `stale_seq` so a concurrent re-create wins. A re-create that reaches the row before `BeginDelete` makes the delete delete nothing. The [[sweep]] retries lingering tombstones. A build that finds a resource gone at source — every plan nil — deletes it the same way without a tombstone, and removes its row, tombstone or not, by the same guarded hard delete.

**Rebuild**:
The reset path: produce and write documents for one, many, or all Resources of a Type without honouring prior state. Used to populate a newly-added Schema Version, to recover from corruption, or to reset documents to a new shape. Always wins over any concurrent Build because it stamps a fresh Build Sequence. A walk fetches each page before its roots' builds begin, so it re-schedules a root that changed after its page was fetched — or whose child did — rather than leave the page's older data in place. Run explicitly, or for a configured Type on a schedule as the [[forward walk]].

**Rebuild cursor**:
The durable position of an all-of-type [[rebuild]] walk: a `{plan version, page token}` pair meaning every Resource listed by the pages before that token has settled — its [[document]]s written and its [[stale mark]] cleared — or is durably marked stale for the [[sweep]]. Carried in the `RunRebuild` activity's Temporal heartbeat details, so a retried attempt resumes there instead of walking again from the head. Only a walk with exactly one active [[plan]] has such a position — a single-version Type, or a version-targeted backfill — and it records one only at a page boundary it has fully consumed and flushed, and none once one of its stale marks has failed; a multi-plan walk has none and restarts from scratch. The page token must mean the same place when it is redeemed as when it was written, which is a property of the upstream listing, not something the indexer can enforce (ADR 0011).

**Document**:
The denormalised search artifact written to Elasticsearch — the result of a Build or a Rebuild. At most one Document per (Resource, Schema Version) pair: each version's [[plan]] decides whether its Document exists, so a Resource can have a Document in one version's index and none in another's (ADR 0013). A build deletes the Document of each version whose plan returns nil; the Resource's row stays while any version has one.

### Search

**Search** (single-resource):
A query against the indexed Documents of one Resource Type. The caller names the Type; matching, filtering, and reference-relation joins are all scoped to it. The existing search path.

**Federated Search**:
A single query over a caller-supplied set of Resource Types, returning one relevance-ranked list whose hits span Types — "the most relevant one" is simply the top hit. Executed, by default, as a single multi-index Elasticsearch query against standardized searchable text the indexer populates at Build time; the Elasticsearch backend also offers experimental execution modes behind a toggle (the same query with index-local term statistics, and a per-Type fan-out merged client-side) for ranking/latency comparison. Per-Type document visibility is enforced by per-Type filters that a federated search middleware supplies on the request, combined as per-index filter groups; the federated middleware chain is independent of the single-resource one. Fields reachable only through a `reference` Relation do not contribute to Federated Search ranking (they remain searchable via single-resource Search).

**Searchable Tier**:
Which standardized searchable surface a field feeds, declared per field (`search: primary | secondary | none`, omitted = `none`). `primary` is a Document's own high-signal text (e.g. its name); it is matchable whenever the searcher may see the Document at all, and ranks above `secondary`. `secondary` holds lower-signal or denormalized-child text. A match on a Document's own name outranks a match that only landed via a related Resource's name.

**Scoped Searchable Text**:
The `secondary` tier, stored as an array of nested entries each carrying searchable text and an optional scope attribution. The text is populated from `search: secondary` DSL fields; the scope attribution is left empty by the standalone app (so its secondary matches are unscoped) and populated only by a library consumer, whose federated middleware also supplies the caller's scope value on the request. This lets a consumer prevent a Parent shared across tenants from leaking a tenant-specific Child's text to a tenant lacking access, while keeping all tenant policy out of the standalone app. The structure and the Federated Search query are identical in both modes — only whether the scope attribution and filter are present differs. Primary fields need no attribution: a Document's own fields are never narrower in visibility than the Document itself.
