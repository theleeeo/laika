# Indexer

A distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via change notifications. The system listens for "something changed upstream", figures out which search documents are affected, fetches fresh data from the source of truth, and writes the result to Elasticsearch.

## Language

### Versioning

**Version**:
The upstream service's notion of how current a resource is. Carried on a [[notification]] and on every observation of a related resource. Used for version-control style stale-rejection: a notification with an older Version than what is already stored is dropped. `0` means the upstream service does not track versions for this resource.

**Schema Version**:
The versioned shape of an indexed document — which fields are present, which relations are pulled, which index name it lives under (e.g. `a_search_v2`). A single resource type can have multiple Schema Versions in flight simultaneously, used for zero-downtime migrations between document shapes.

**Build Sequence**:
The indexer's own per-resource monotonic counter, bumped on every build of a given resource. Used as the value for Elasticsearch's `external_gte` versioning so that concurrent rebuilds of the same document land in the correct order. Has nothing to do with the upstream — it exists purely to serialise writes to a single ES document across distributed indexer instances.

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
The normal update path: produce and write the document for one or a few specific Resource IDs, respecting the Build Sequence OCC ordering. Triggered by a Notification, by the drift-check re-enqueue, or by another Build's downstream effect.

**Rebuild**:
The reset path: produce and write documents for one, many, or all Resources of a Type without honouring prior state. Used to populate a newly-added Schema Version, to recover from corruption, or to reset documents to a new shape. Always wins over any concurrent Build because it stamps a fresh Build Sequence.

**Document**:
The denormalised search artifact written to Elasticsearch — the result of a Build or a Rebuild. One Document per (Resource, Schema Version) pair.

### Search

**Search** (single-resource):
A query against the indexed Documents of one Resource Type. The caller names the Type; matching, filtering, and reference-relation joins are all scoped to it. The existing search path.

**Federated Search**:
A single query over a caller-supplied set of Resource Types, returning one relevance-ranked list whose hits span Types — "the most relevant one" is simply the top hit. Executed as a single multi-index Elasticsearch query against standardized searchable text the indexer populates at Build time. Per-Type document visibility is enforced by per-Type filters that a federated search middleware supplies on the request, combined as per-index filter groups; the federated middleware chain is independent of the single-resource one. Fields reachable only through a `reference` Relation do not contribute to Federated Search ranking (they remain searchable via single-resource Search).

**Searchable Tier**:
Which standardized searchable surface a field feeds, declared per field (`search: primary | secondary | none`, omitted = `none`). `primary` is a Document's own high-signal text (e.g. its name); it is matchable whenever the searcher may see the Document at all, and ranks above `secondary`. `secondary` holds lower-signal or denormalized-child text. A match on a Document's own name outranks a match that only landed via a related Resource's name.

**Scoped Searchable Text**:
The `secondary` tier, stored as an array of nested entries each carrying searchable text and an optional scope attribution. The text is populated from `search: secondary` DSL fields; the scope attribution is left empty by the standalone app (so its secondary matches are unscoped) and populated only by a library consumer, whose federated middleware also supplies the caller's scope value on the request. This lets a consumer prevent a Parent shared across tenants from leaking a tenant-specific Child's text to a tenant lacking access, while keeping all tenant policy out of the standalone app. The structure and the Federated Search query are identical in both modes — only whether the scope attribution and filter are present differs. Primary fields need no attribution: a Document's own fields are never narrower in visibility than the Document itself.
