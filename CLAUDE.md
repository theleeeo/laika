# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Canonical vocabulary lives in [CONTEXT.md](CONTEXT.md); architectural decisions in [docs/adr/](docs/adr/). This file describes the codebase — where things live and how to run them — not the language. Update it when an architecture or behavior change makes it inaccurate.

## Commands

```bash
# Run the application (from repo root)
go run ./app/cmd/indexer

# Run unit tests only (no Docker needed; backend/elasticsearch tests run
# against a mock HTTP transport, not a real cluster)
go test ./core/... ./backend/... ./app/server/... ./app/dsl/...

# Run integration tests (requires Docker for testcontainers):
# storage/postgres and app/tests hit real infra
go test ./storage/... ./app/tests/...

# Run all tests
go test ./...

# Generate Elasticsearch mappings from resource config
go run ./app/cmd/gen-mapping -config resources.yml

# Diff running index mappings against what the current config would generate
# (exits non-zero on actionable drift: added/changed fields or a missing index)
go run ./app/cmd/diff-mapping -config resources.yml -es-addr http://localhost:9200

# Pre-cutover readiness gates for a readVersion bump (ADR 0010): target index
# exists, doc-count parity, stale-backlog age. Exits non-zero when not ready.
go run ./app/cmd/cutover-check -config resources.yml -es-addr http://localhost:9200 -pg-addr postgres://user:pass@localhost/indexer

# Regenerate protobuf bindings (outputs to app/gen/)
buf generate
```

## Architecture

This is a distributed search indexing engine that keeps Elasticsearch Documents in sync with upstream service data via gRPC Notifications.

### Module Structure

The repo uses Go workspaces (`go.work`) with three modules:

| Module | Purpose |
|--------|---------|
| `.` (root) | Core library: `Indexer`, worker pool, Temporal workflows, `SearchBackend`/`Store` interfaces, plus the `storage/postgres` and `backend/elasticsearch` implementation packages, `projection`, `model` |
| `./app` | Standalone app: gRPC wiring (`server/`, `source/`), YAML DSL, entry points, tests |
| `./aggregation` | Streaming aggregation pipeline execution |

The Postgres `Store` and Elasticsearch `SearchBackend` implementations are plain
packages inside the root module (see [ADR 0008](docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md));
only `aggregation` and `app` remain separate modules.

**Library users** depend on the root module (and `aggregation` as needed).
**App users** use the `app` module which wires everything together.

### Core Data Flow

1. gRPC client → `app/server` translates requests into `core.Notification`
2. `core.Indexer.RegisterChanges` (`RegisterChange` is a batch of one) records a batch in one atomic statement (`Store.RegisterChanges`): each accepted Resource's version or tombstone, its **stale mark** and metadata, and the marks of its affected Parent Resources (found via the Relation graph), which leave the Parents' metadata alone. A stale version writes nothing
3. The same statement claims each marked row that has no live **build owner** (`resources.owner_seq`/`owner_since`, lease `Config.OwnerLease`). After the commit, `core` submits an **inline build** per claimed root (of the accepted Resources + affected Parents), and an inline delete per claimed tombstone, to a bounded in-process worker pool; a root with a live owner is only marked, and its owner's follow-up serves the change
4. A build executes a `projection.Plan` (which calls the `source.Provider`; an owned build fetches with the row's metadata as `BeginBuild` returns it, a build that owns nothing with its caller's), writes to ES via `SearchBackend` with the Build Sequence as `external_gte`, replaces each Schema Version's edge set at the Build Sequence (`Store.ReplaceEdges`, which in the same transaction stores the plan's report of the resource's own metadata, `BuildDoc.ResourceMetadata`, on a row that has none), and finishes, guarded by the `stale_seq` captured at `BeginBuild`: an owned build with `FinishOwned`, which clears the mark and the ownership, or — if a change moved `stale_seq` — re-claims the row and submits at most one follow-up, which runs with the row's metadata as its own `BeginBuild` returns it; a notified delete (`deleteOne`) first takes its Build Sequence with `BeginDelete` and deletes nothing when the row is gone or no longer a tombstone, else deletes each version's document at that sequence and the edge sets stamped at or below it, and finishes the same way through `DeleteResourceIfSeq`; a build that owns nothing (a rebuild walk, a direct `idx.Build`) with `ClearStale`. A build whose plans all return nil deletes each version's document and the edge sets at its Build Sequence and finishes, owned or not, with `DeleteResourceIfSeq` too, which removes the row, tombstone or not; a rebuild walk does so after the deleted root's drift check
5. **Slow lane (Temporal):** the `StaleSweep` workflow (schedule `laika-stale-sweep`) rebuilds anything stale past a threshold; explicit rebuilds run as `RebuildWalk` workflows. Both live in `core` on the `laika-indexer` task queue. A single-active-plan walk (single-version type, or a version-targeted backfill) checkpoints a `RebuildCursor` into its activity's heartbeat details, so a retried attempt resumes instead of restarting; multi-plan walks restart from scratch ([ADR 0011](docs/adr/0011-resumable-rebuild-walks-via-heartbeat-cursors.md)). Temporal being down degrades recovery latency only — never hot-path throughput or correctness.

Search path: `app/server/SearcherServer` → `core.Indexer.Search` → `SearchBackend.Search`

### Key Interfaces (root module)

- **`core.SearchBackend`** — implemented by `backend/elasticsearch`; decouples Indexer from ES
- **`core.Store`** — implemented by `storage/postgres`; the relation graph (`ReplaceEdges`, `RemoveResource`, `GetParentResources`, `GetChildResources`) plus stale-mark state and build ownership (`RegisterChanges`, `MarkStale`, `BeginBuild`, `BeginDelete`, `RenewOwners`, `ReleaseOwners`, `FinishOwned`, `ClearStale`, `DeleteResourceIfSeq`, `ListStale`) and the drift check on the Change Sequence (`NextChangeSeq`, `AnyChangedSince`)
- **`app/source.Provider`** — implemented by `app/source.GRPCProvider`; data fetcher used by DSL plans

Both `Store` and `SearchBackend` have exactly one implementation each; the interfaces survive as test seams (unit tests mock them to avoid Docker), not as swap points.

### Key Packages

| Package | Role |
|---------|------|
| `core/` | Orchestration: `Indexer`, inline worker pool, Temporal `StaleSweep`/`RebuildWalk` workflows, `SearchBackend`/`Store` interfaces, `IndexName`/`AliasName` |
| `core/resource/` | Resource/Schema-Version DSL types and validation |
| `model/` | Primitive types (`Resource`) |
| `projection/` | `Plan` type and `BuildDoc` — the aggregation result flowing through Plans |
| `storage/postgres/` | `Store` implementation: relation graph + stale-mark state (root-module package) |
| `backend/elasticsearch/` | `SearchBackend` implementation; mapping generation (root-module package) |
| `app/source/` | `Provider` interface + gRPC implementation |
| `app/server/` | Thin gRPC adapters; translates proto ↔ core types |
| `app/gen/` | Generated protobuf Go bindings — do not edit manually |
| `app/config/` | YAML resource DSL parsing |
| `app/dsl/` | Builds `projection.Plan` trees from resource config + Provider |
| `app/cmd/` | Entry points (`indexer`, `gen-mapping`, `diff-mapping`, `cutover-check`, `cleanup`) |

### Critical Invariants

- **Mark stale before you build**: every build-triggering path (ingest fanout via `RegisterChanges`, drift-check re-build, ADR 0006 parent cascade via `MarkStale`) marks in Postgres *before* submitting the inline build, and submits only what that mark claimed for a build owner. The mark is the durability; the pool is only the accelerator. Reversing the order reintroduces silent loss on shed or crash. A resource's metadata is its own: only a registration's mark of its own item stores the notification's metadata, and a row that has none takes its plans' report from the build's edge write; a Parent mark and `MarkStale` (the cascade, drift re-marks, the rebuild flusher's marks) leave it alone, so a change to one resource never gives another its metadata. A build that owns its resource — an inline build, a follow-up, a sweep build — runs with what the row holds when it begins (`BuildBegun.Metadata`), so a build that waited picks up metadata registered meanwhile and a Parent's build runs with the Parent's own; a row with none builds with none. A build that owns nothing (a rebuild walk, a direct `idx.Build`) fetches with its caller's. See [ADR 0008](docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md).
- **Seq-guarded clear**: a build captures `stale_seq` at `BeginBuild` and clears the mark (`FinishOwned` for an owned build, `ClearStale` for one that owns nothing), or removes the row when it found the resource gone (`DeleteResourceIfSeq`), only if it is unchanged; a newer change that moved it leaves the row stale for the owner's follow-up (`FinishOwned` re-claims it) or the sweep. Never null `stale_since` unconditionally.
- **At-least-once via mark + sweep**: durability is the stale mark plus the Temporal `StaleSweep`, not a job-queue retry count. A resource whose Type was removed from config stays stale forever (logged by the sweep) — this is a known limitation.
- **Distributed-safe**: multiple indexer instances run concurrently; no per-Resource serialization guarantee. Build ownership coalesces inline builds across instances but is not a lock: a lapsed lease costs a duplicate build or delete, or, for a build that writes more than Elasticsearch's `index.gc_deletes` after a delete overtook it, the deleted document back ([ADR 0008](docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md)). A Store statement that locks several `resources` rows locks the existing ones in (type, id) order before its first write, as `RegisterChanges`, `MarkStale`, `RenewOwners` and `ReleaseOwners` do, so they don't deadlock over rows that exist when they start; rows created or removed concurrently still can, and a registration then returns `core.ErrRegistrationAborted`, which the index adapter answers as `Aborted` for the producer to retry. The drift check compares the Change Sequence, never upstream Versions: each build takes a start from it before its fetches (`BeginBuild`, or `NextChangeSeq` once per Rebuild plan walk) and re-schedules if a resource it fetched has `resources.change_seq` above that start. See [ADR 0002](docs/adr/0002-distributed-safety-via-occ-and-drift-check-not-locks.md).
- **Build Sequence drives OCC**: every ES write and delete carries the Resource's Build Sequence (stored in `resources.build_idx`, drawn from the Change Sequence by `BeginBuild` and `BeginDelete`) sent as the `external_gte` version, so concurrent Builds, Rebuilds and deletes of the same Document land in sequence order; a write or delete a newer one rejects is an OCC loss, not an error. `stale_seq` is drawn from the same sequence, so neither number restarts when a row is hard-deleted and recreated.
- **Stale Version rejection**: a Notification with `Version > 0` enables drop-on-stale; `0` means always accept.
- **Relation graph drives fanout**: affected Parent Resources are found by querying the Postgres Relation graph, not static config.
- **Cross-version existence agreement**: a build executes every Schema Version's plan before writing anything, and only unanimity decides existence — all plans nil deletes everywhere, disagreement fails the build and leaves the stale mark for retry. One version's nil must never delete another version's freshly written document. The multi-plan Rebuild walk doesn't hold this yet: it decides plan by plan (seams S15). ADR 0006 Parents are unioned across all plans, never taken from the last one; edges are stored per Schema Version, each version's set from its own plan, and fanout reads their union.
- **All-of-Type Rebuild path**: `BuildRequest.ResourceID == ""` triggers `ListResources` pagination — the Rebuild path that walks every Resource of a Type.
- **Plans encapsulate data fetching**: `core.Indexer` only executes Plans; it never calls `source.Provider` directly. Library users supply their own Plans.
- **Resource configs are validated at the boundary**: `core.New` and `SetPlans` apply defaults and validate the resource config set, refusing an invalid one (an empty set is legal — rejecting it is app policy, enforced by the YAML loader). Everything past that boundary *assumes* the invariants hold — every resource has ≥1 version, `ReadVersionConfig()` never returns nil, relations are consistent — so do not add defensive nil guards for them. Callers must not mutate the configs after handing them over.
- **Checkpoints step only over settled work**: a `RebuildCursor` is reported only by single-active-plan walks, only after a successful flush, and only for fully consumed page boundaries — every resource behind it is settled, or durably marked stale for the sweep. Multi-plan walks never checkpoint and ignore cursors: cross-plan settlement makes their intermediate positions non-durable ([ADR 0011](docs/adr/0011-resumable-rebuild-walks-via-heartbeat-cursors.md)).

### Configuration

- **App config**: `indexer.yml` (override with `APP_CONFIG_PATH` env var). See `example.indexer.yml`.
- **Resource DSL**: `resources.yml` (override with `RESOURCE_CONFIG_PATH`). See `example.resources.yml`.
- **Provider plugin**: external gRPC service implementing `ProviderService` (FetchResource, FetchRelated, ListResources).

### gRPC Services (proto/)

- `index/v1` — `IndexService`: NotifyChange, NotifyChangeBatch, Rebuild
- `search/v1` — `SearchService`: Search, GetCapabilities
- `provider/v1` — `ProviderService`: FetchResource, FetchRelated, ListResources

### Testing

- Unit tests: `core/`, `backend/elasticsearch/`, `app/server/`, `app/dsl/` — no Docker needed (Store/SearchBackend mocked in `core`; the ES client tested against a mock HTTP transport)
- Integration tests: `storage/postgres/`, `app/tests/` — use testcontainers (Docker) for real Postgres + Elasticsearch

Any feature or behavior change must include tests.
