# Indexer

A distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via change notifications. The system listens for "something changed upstream", figures out which search documents are affected, fetches fresh data from the source of truth, and writes the result to Elasticsearch.

## Language

### Versioning

The word _version_ shows up in three different senses in this system. They are kept strictly separate.

**Version**:
The upstream service's notion of how current a resource is. Carried on a [[notification]] and on every observation of a related resource. Used for version-control style stale-rejection: a notification with an older Version than what is already stored is dropped. `0` means the upstream service does not track versions for this resource.
_Avoid_: revision, source version, upstream version (in code — these are fine in prose for clarity)

**Schema Version**:
The versioned shape of an indexed document — which fields are present, which relations are pulled, which index name it lives under (e.g. `a_search_v2`). A single resource type can have multiple Schema Versions in flight simultaneously, used for zero-downtime migrations between document shapes.
_Avoid_: generation, format, variant

**Build Sequence**:
The indexer's own per-resource monotonic counter, bumped on every build of a given resource. Used as the value for Elasticsearch's `external_gte` versioning so that concurrent rebuilds of the same document land in the correct order. Has nothing to do with the upstream — it exists purely to serialise writes to a single ES document across distributed indexer instances.
_Avoid_: OCC version, rebuild counter

### The graph

**Resource**:
A single item from an upstream service, identified by Type and ID. The atomic unit the indexer reasons about.

**Relation**:
A directed link from one Resource to another: a Parent contains a Child in its indexed document. The word is used consistently — in the YAML config (`relations:`), in the Store's persisted edges, and in prose.
_Avoid_: edge, link, dependency, association

**Parent / Child**:
The two endpoints of a Relation. The Parent is the Resource whose document includes data from the Child. A single Resource is a Parent in some Relations and a Child in others — these are roles, not types.
_Avoid_: root (do not use — every Resource has its own index, so "root" carried no real distinction and was conflated with "currently rebuilding")

### Operations

**Notification**:
A message from an upstream service that a single Resource changed — carries Type, ID, change Kind, upstream Version, and optional metadata. Deliberately carries no field data; the indexer always re-fetches from upstream when it builds the document.
_Avoid_: event, change, message

**Plan**:
An executable that, given a Resource Type and ID (or no ID, for the listing case), produces the documents the indexer will write. Plans encapsulate all data fetching; the orchestrator only executes them. Each Schema Version of a resource has its own Plan.
_Avoid_: projection, pipeline, query

**Build**:
The normal update path: produce and write the document for one or a few specific Resource IDs, respecting the Build Sequence OCC ordering. Triggered by a Notification, by the drift-check re-enqueue, or by another Build's downstream effect.
_Avoid_: index, sync, reindex (when meaning a Build)

**Rebuild**:
The reset path: produce and write documents for one, many, or all Resources of a Type without honouring prior state. Used to populate a newly-added Schema Version, to recover from corruption, or to reset documents to a new shape. Always wins over any concurrent Build because it stamps a fresh Build Sequence.
_Avoid_: reindex, refresh, recreate

**Document**:
The denormalised search artifact written to Elasticsearch — the result of a Build or a Rebuild. One Document per (Resource, Schema Version) pair.
_Avoid_: record, entity, item
