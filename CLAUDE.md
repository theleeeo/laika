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
# exists, every resource with no stale mark has the target version's edge set,
# the doc-count gap is 0 or accepted for its type (-accept-gap a,b),
# stale-backlog age. Exits non-zero when not ready.
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
4. A build executes a `projection.Plan` (which calls the `source.Provider`; an owned build fetches with the row's metadata as `BeginBuild` returns it, a build that owns nothing with its caller's), writes to ES via `SearchBackend` with the Build Sequence as `external_gte`, replaces each Schema Version's edge set at the Build Sequence (`Store.ReplaceEdges`, which in the same transaction stores the plan's report of the resource's own metadata, `BuildDoc.ResourceMetadata`, on a row that has none), and finishes, guarded by the `stale_seq` captured at `BeginBuild`: an owned build with `FinishOwned`, which clears the mark and the ownership, or — if a change moved `stale_seq` — re-claims the row and submits at most one follow-up, which runs with the row's metadata as its own `BeginBuild` returns it; a notified delete (`deleteOne`) first takes its Build Sequence with `BeginDelete` and deletes nothing when the row is gone or no longer a tombstone, else deletes each version's document at that sequence and the edge sets stamped at or below it, and finishes the same way through `DeleteResourceIfSeq`; a build that owns nothing (a rebuild walk, a direct `idx.Build`) with `ClearStale`. A build whose plans disagree — some return a document, some nil — writes the documents it has and, at its Build Sequence, deletes the document of each version whose plan returned nil and empties that version's edge set in the same `ReplaceEdges`, keeps the row and finishes as above ([ADR 0013](docs/adr/0013-each-schema-version-decides-its-documents-existence.md)); a rebuild does the same for an id once every plan's outcome for it has arrived, emptying the sets in a `ReplaceEdges` of their own (`dropNils`). A build whose plans all return nil deletes each version's document and the edge sets at its Build Sequence and finishes, owned or not, with `DeleteResourceIfSeq` too, which removes the row, tombstone or not; a rebuild walk does so after the deleted root's drift check. A rebuild that selects versions and so runs fewer than all of the type's plans with an `Executer` never removes a row: an id its plans all find gone has those versions' documents deleted and their edge sets emptied, keeps its row, and is marked stale for the sweep's build, which runs every plan. Such a rebuild of the whole type ends, once its walk has finished, with a **probe pass** (`probeUncovered`, `core/rebuild_probe_pass.go`): for each selected version whose plan has a `Probe` (one without is skipped, logged at Info), it lists a page at a time the type's rows that aren't tombstones, carry no stale mark, have no edge set of that version and hold the walk's metadata (`Store.ListUncovered`, the predicate the cutover check's coverage counts with), begins each page in one statement (`Store.BeginBuilds`, which begins only rows that still exist and leaves ownership alone) and probes it once with the walk's metadata. An id the probe leaves out is excluded by that version: its document is deleted and its edge set emptied (`ReplaceEdges` declaring nothing) at the Build Sequence taken before the probe, and the row isn't marked. The ids the probe returns are marked stale in one `MarkStale` for the sweep's build
5. **Slow lane (Temporal):** the `StaleSweep` workflow (schedule `laika-stale-sweep`) rebuilds anything stale past a threshold, backing off a resource whose owned build or delete keeps failing; explicit rebuilds run as `RebuildWalk` workflows; the `ForwardWalk` workflow (one schedule per type in `Config.ForwardWalks`, `laika-forward-walk-<type>`) walks a type from its source once per metadata map its config's `Metadata` func returns at that run, as paced `RebuildWalk` children one after another, so a resource created or changed without a notification is indexed; the `ReverseSweep` workflow (one schedule per type in `Config.ReverseSweeps`, `laika-reverse-sweep-<type>`) finds resources deleted at their source without a notification. All live in `core` on the `laika-indexer` task queue. A single-active-plan walk (single-version type, or a version-targeted backfill) checkpoints a `RebuildCursor` into its activity's heartbeat details, so a retried attempt resumes instead of restarting, and stops checkpointing once one of its stale marks failed; multi-plan walks restart from scratch ([ADR 0011](docs/adr/0011-resumable-rebuild-walks-via-heartbeat-cursors.md)). A rebuild whose failed resources are all durably marked stale returns a `RebuildMarkedFailuresError`, which `RunRebuild` fails without retrying (a forward walk counts such a walk as done); one that aborted, one of whose marks failed, or one whose probe pass (item 4) failed to list or begin its rows, is retried. The pass's failed probes, deletes, edge writes and marks are logged and counted but aren't the walk's failures: their ids stay uncovered until the next backfill. The pass keeps no cursor, so a resumed attempt runs it whole again. A forward walk is paced (`ResourceSelector.Pacing`): it asks its plans for pages of its `PageSize` (`BuildRequest.PageSize`), and before taking each page, a later plan's first page included, flushes what it has begun, then waits out the rest of the previous page's `PageInterval` and then while the build pool is pressured; `ForwardWalkNow` runs one run in-process. A reverse sweep lists the type's live rows by id (`Store.ListResources`), asks the source which still exist through the first of the type's plans that has a `Probe`, once per page and actor (each row's metadata), and marks the rest through `scheduleBuild`, so their owned builds, which run every plan, delete each version's document its plan finds gone, and the row when every plan does. The probe answers for its own plan's version only ([ADR 0013](docs/adr/0013-each-schema-version-decides-its-documents-existence.md)); a type whose plans have no `Probe` is skipped with a warning. A `Probe` has one other caller, a version-selected backfill's probe pass (item 4), which asks each selected version's own. The sweep's activity heartbeats the last id of each page whose suspects are marked ([ADR 0012](docs/adr/0012-reverse-sweep-probes-indexed-resources-for-lost-deletes.md)). Temporal being down degrades recovery latency only — never hot-path throughput or correctness.

Search path: `app/server/SearcherServer` (or a library embedder) → `core.Indexer.Search` → the registered `Config.SearchMiddlewares`, then core's `scanPage`, `deriveNestedPath` and `referenceResolve` → `searchBase` → `SearchBackend.Search`. A **scan** (`SearchRequest.Scan`, [ADR 0015](docs/adr/0015-a-scan-reads-one-snapshot-under-the-version-it-pinned.md)) takes the same path on every page: its first page pins the one index the read alias serves (`SearchBackend.GetAliasTargets`) and that index's Schema Version, carried in the context (`core/scan.go`), and every stage reads the pinned version's config instead of `ReadVersionConfig()`; `scanPage` sets its page size (default and cap 1,000), fingerprints the request and wraps the backend's cursor in the page token (`NextPageToken`, continued by `PageToken`), checked against `Config.ScanMaxAge`, the config and the request on each later page. The backend serves it from a point in time on the pinned index with `search_after` (`backend/elasticsearch/scan.go`), IDs and scores only, and refuses it with `core.ErrScanFault` on Elasticsearch below 8.11.0, whose Lucene drops documents missing a numeric sort field from `search_after` pages (the cluster version is read from `GET /` once, on the first scan); `app/tests` runs 8.19.0. `FederatedSearch` has no scan, and the app's `search.v1` doesn't expose one.

### Key Interfaces (root module)

- **`core.SearchBackend`** — implemented by `backend/elasticsearch`; decouples Indexer from ES: document writes and deletes, `Search` (for a scan, decided from `req.Scan` alone, `indexAlias` is the pinned index, `vc` its config and `req.PageToken` the backend's own cursor), `FederatedSearch`, and `GetAliasTargets` (every index an alias points to)
- **`core.Store`** — implemented by `storage/postgres`; the relation graph (`ReplaceEdges`, `RemoveResource`, `GetParentResources`, `GetChildResources`) plus stale-mark state and build ownership (`RegisterChanges`, `MarkStale`, `BeginBuild`, `BeginBuilds` (many existing rows in one statement, no ownership), `BeginDelete`, `RenewOwners`, `ReleaseOwners`, `ReleaseFailed`, `FinishOwned`, `ClearStale`, `DeleteResourceIfSeq`, `ListStale`), the drift check on the Change Sequence (`NextChangeSeq`, `AnyChangedSince`), and the keyset listings of a type's live rows the reverse sweep enumerates (`ListResources`) and of the rows a backfill left without an edge set of its version, which its probe pass asks about (`ListUncovered`)
- **`app/source.Provider`** — implemented by `app/source.GRPCProvider`; data fetcher used by DSL plans

Both `Store` and `SearchBackend` have exactly one implementation each; the interfaces survive as test seams (unit tests mock them to avoid Docker), not as swap points.

### Key Packages

| Package | Role |
|---------|------|
| `core/` | Orchestration: `Indexer`, inline worker pool, Temporal `StaleSweep`/`RebuildWalk`/`ReverseSweep`/`ForwardWalk` workflows (reverse sweeps configured in `Config.ReverseSweeps`, forward walks in `Config.ForwardWalks`), the search middleware chain and scans (`Config.ScanMaxAge`), `SearchBackend`/`Store` interfaces, `IndexName`/`AliasName` |
| `core/resource/` | Resource/Schema-Version DSL types and validation |
| `model/` | Primitive types (`Resource`) |
| `projection/` | `Plan` type and `BuildDoc` — the aggregation result flowing through Plans |
| `storage/postgres/` | `Store` implementation: relation graph + stale-mark state (root-module package) |
| `backend/elasticsearch/` | `SearchBackend` implementation, scans from a point in time included (keep-alive `WithScanKeepAlive`); mapping generation (root-module package) |
| `app/source/` | `Provider` interface + gRPC implementation |
| `app/server/` | Thin gRPC adapters; translates proto ↔ core types |
| `app/gen/` | Generated protobuf Go bindings — do not edit manually |
| `app/config/` | YAML resource DSL parsing |
| `app/dsl/` | Builds `projection.Plan` trees from resource config + Provider |
| `app/cmd/` | Entry points (`indexer`, `gen-mapping`, `diff-mapping`, `cutover-check`, `cleanup`) |

### Critical Invariants

- **Mark stale before you build**: every build-triggering path (ingest fanout via `RegisterChanges`, drift-check re-build, ADR 0006 parent cascade and the reverse sweep's suspects via `MarkStale`) marks in Postgres *before* submitting the inline build, and submits only what that mark claimed for a build owner. The mark is the durability; the pool is only the accelerator. Reversing the order reintroduces silent loss on shed or crash. A resource's metadata is its own: only a registration's mark of its own item stores the notification's metadata, and a row that has none takes its plans' report from the build's edge write; a Parent mark and `MarkStale` (the cascade, drift re-marks, the rebuild flusher's and the probe pass's marks) leave it alone, so a change to one resource never gives another its metadata. A build that owns its resource — an inline build, a follow-up, a sweep build — runs with what the row holds when it begins (`BuildBegun.Metadata`), so a build that waited picks up metadata registered meanwhile and a Parent's build runs with the Parent's own; a row with none builds with none. A build that owns nothing (a rebuild walk, a direct `idx.Build`) fetches with its caller's. See [ADR 0008](docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md).
- **Seq-guarded clear**: a build captures `stale_seq` at `BeginBuild` and clears the mark (`FinishOwned` for an owned build, `ClearStale` for one that owns nothing), or removes the row when it found the resource gone (`DeleteResourceIfSeq`), only if it is unchanged; a newer change that moved it leaves the row stale for the owner's follow-up (`FinishOwned` re-claims it) or the sweep. Never null `stale_since` unconditionally.
- **At-least-once via mark + sweep**: durability is the stale mark plus the Temporal `StaleSweep`, not a job-queue retry count. A failed owned build or delete releases its row through `Store.ReleaseFailed`, which keeps the mark and, unless a registration of the resource itself was accepted since the claim, backs the row off — `Config.SweepBackoff` (5m) after its first failure in a row, doubling up to `Config.SweepBackoffMax` (24h) — so `ListStale` serves it only once its turn has come and a resource that never builds can't keep the sweep from the others. A successful build or delete resets the backoff, except an owned build whose ownership a moved mark took meanwhile (its `FinishOwned` matches no row), and so does a registration of the resource itself; a cancellation, a shed submission or a failed renewal is not a failure. A resource whose Type was removed from config is retried at a growing interval up to the cap, logged at Error from its fifth failure in a row; a tombstone of such a Type is deleted as any other.
- **Distributed-safe**: multiple indexer instances run concurrently; no per-Resource serialization guarantee. Build ownership coalesces inline builds across instances but is not a lock: a lapsed lease costs a duplicate build or delete, or, for a build that writes more than Elasticsearch's `index.gc_deletes` after a delete overtook it, the deleted document back ([ADR 0008](docs/adr/0008-stale-mark-inline-builds-and-temporal-slow-lane.md)). A Store statement that locks several `resources` rows locks the existing ones in (type, id) order before its first write, as `RegisterChanges`, `MarkStale`, `BeginBuilds`, `RenewOwners`, `ReleaseOwners` and `ReleaseFailed` do, so they don't deadlock over rows that exist when they start; rows created or removed concurrently still can, and a registration then returns `core.ErrRegistrationAborted`, which the index adapter answers as `Aborted` for the producer to retry. The drift check compares the Change Sequence, never upstream Versions: each build takes a start from it before its fetches (`BeginBuild`, or `NextChangeSeq` once per Rebuild plan walk) and re-schedules if a resource it fetched has `resources.change_seq` above that start. See [ADR 0002](docs/adr/0002-distributed-safety-via-occ-and-drift-check-not-locks.md).
- **Build Sequence drives OCC**: every ES write and delete carries the Resource's Build Sequence (stored in `resources.build_idx`, drawn from the Change Sequence by `BeginBuild`, `BeginBuilds` and `BeginDelete`) sent as the `external_gte` version, so concurrent Builds, Rebuilds and deletes of the same Document land in sequence order; a write or delete a newer one rejects is an OCC loss, not an error. `stale_seq` is drawn from the same sequence, so neither number restarts when a row is hard-deleted and recreated.
- **Stale Version rejection**: a Notification with `Version > 0` enables drop-on-stale; `0` means always accept.
- **Relation graph drives fanout**: affected Parent Resources are found by querying the Postgres Relation graph, not static config.
- **Each Schema Version decides its own document's existence** ([ADR 0013](docs/adr/0013-each-schema-version-decides-its-documents-existence.md)): a build executes every Schema Version's plan before writing anything, and a plan's nil is its own version's answer, never another's. The build writes the document of each version whose plan returned one and, at the same Build Sequence, deletes the document of each version whose plan returned nil and replaces that version's edge set with an empty one — an empty set, since a version left out of the sets would keep its old children. The row stays while any version has a document, and the build settles as a successful one does. Only when every plan returns nil does it delete every version's document, the edge sets and the row. A failed write or delete fails the build and leaves the mark. A Rebuild walk applies the same rule per id, but only once the id has every plan's outcome: its nils are recorded and applied when it settles, never as the walk meets them, so an id that fails before it settles, is left unfinished, or is partial (an earlier plan's listing omitted it, and an omission is not a nil) applies none and is marked for the sweep (one that fails while settling, or after, may have applied some, and is marked all the same), and one version that lists an id both with and without data in one walk fails it. A rebuild that selects versions, and so runs fewer than all of the type's plans with an `Executer`, never removes the row: an id its plans all find gone has those versions' documents deleted and their edge sets emptied, keeps its row, and is marked stale for the sweep, whose build runs every plan and decides the row; it is not counted as failed. Such a rebuild of the whole type then asks each selected version's `Probe` about the rows its listing left without an edge set of that version (the probe pass, data flow item 4), since a version excludes a resource only through its per-id answers, its fetch's nil and its `Probe`, never through its listing. An id the probe leaves out loses that version's document and gets an empty edge set of it, at a Build Sequence taken before the probe, and isn't marked: the other versions keep their documents, and a resource gone from every version is the reverse sweep's to find. An id the probe returns is marked for the sweep. ADR 0006 Parents and the drift check come from the versions with a document, unioned across their plans, never taken from the last one; edges are stored per Schema Version, each version's set from its own plan, and fanout reads their union. Versions may therefore hold different sets of documents; every search reads through the type's alias, so it shows what the read version's plan says exists.
- **All-of-Type Rebuild path**: `BuildRequest.ResourceID == ""` triggers `ListResources` pagination — the Rebuild path that walks every Resource of a Type, explicitly or as a scheduled forward walk. The DSL plan lists `BuildRequest.PageSize` per page, 100 when it is 0.
- **Plans encapsulate data fetching**: `core.Indexer` only executes Plans; it never calls `source.Provider` directly. Library users supply their own Plans.
- **Resource configs are validated at the boundary**: `core.New` and `SetPlans` apply defaults and validate the resource config set, refusing an invalid one (an empty set is legal — rejecting it is app policy, enforced by the YAML loader). Everything past that boundary *assumes* the invariants hold — every resource has ≥1 version, `ReadVersionConfig()` never returns nil, relations are consistent — so do not add defensive nil guards for them. Callers must not mutate the configs after handing them over.
- **A scan reads only what it pinned** ([ADR 0015](docs/adr/0015-a-scan-reads-one-snapshot-under-the-version-it-pinned.md)): a search stage that reads a resource's config reads the scan state's pinned config when `Scan` is set (`searchVersionConfig`), and fails with `core.ErrScanFault` when the state is missing, for another resource or without a config — never the read version's config, never a pass-through, since a later page built with a newer version's config against the old snapshot can fail open. A new `SearchRequest`, `Filter` or `SortOption` field goes into the scan fingerprint (`scanFingerprint`; a guard test enforces it). No 404 in a scan becomes an empty success: it is `core.ErrCursorExpired`.
- **Checkpoints step only over settled work**: a `RebuildCursor` is reported only by single-active-plan walks, only after a successful flush, only for fully consumed page boundaries, and none once one of the walk's stale marks has failed — every resource behind it is settled, or durably marked stale for the sweep. Multi-plan walks never checkpoint and ignore cursors: cross-plan settlement makes their intermediate positions non-durable ([ADR 0011](docs/adr/0011-resumable-rebuild-walks-via-heartbeat-cursors.md)).

### Configuration

- **App config**: `indexer.yml` (override with `APP_CONFIG_PATH` env var). See `example.indexer.yml`. Its `forward_walks` list enables a type's scheduled forward walk (`resource_type`, `enabled`, `interval`, `page_size`, `page_interval`, and a static list of `metadata` maps); it is decoded strictly from the YAML file, not through viper, so metadata keys keep their case, and has no env overrides. `main` passes the enabled entries as `core.Config.ForwardWalks` and calls `EnsureForwardWalkSchedules` at startup.
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
