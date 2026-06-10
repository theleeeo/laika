# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All Go commands require the `GOEXPERIMENT=jsonv2` flag (Go 1.26.1).

```bash
# Run the application (from repo root)
GOEXPERIMENT=jsonv2 go run ./app/cmd/indexer

# Run all tests — unit tests only (no Docker needed)
GOEXPERIMENT=jsonv2 go test ./core/... ./es/... ./app/server/... ./app/dsl/...

# Run integration tests (requires Docker for testcontainers)
GOEXPERIMENT=jsonv2 go test ./app/tests/...

# Run all tests
GOEXPERIMENT=jsonv2 go test ./...

# Generate Elasticsearch mappings from resource config
go run ./app/cmd/gen-mapping -config resources.yml

# Regenerate protobuf bindings (outputs to app/gen/)
buf generate
```

## Architecture

This is a distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via gRPC change notifications.

### Module Structure

The repo uses Go workspaces (`go.work`) with four modules:

| Module | Purpose |
|--------|---------|
| `.` (root) | Core library: `Indexer`, `SearchBackend` interface, `projection`, `es`, `model` |
| `./app` | Standalone app: gRPC wiring (`server/`, `source/`), YAML DSL, entry points, tests |
| `./aggregation` | Streaming aggregation pipeline execution |
| `./storage/postgres` | Postgres `Store` implementation |

**Library users** depend only on the root module (and aggregation/storage as needed).
**App users** use the `app` module which wires everything together.

### Core Data Flow

1. gRPC client → `app/server` translates requests into `core.Notification`
2. `core.Indexer.RegisterChange` updates Postgres state and finds affected root resources via the relation graph
3. `core` enqueues River jobs (`rebuild`, `delete`, `full_rebuild`)
4. River workers call `Indexer.Build`
5. Build executes `projection.Plan` (which calls the `source.Provider`), writes to ES via `SearchBackend`, updates the relation graph

Search path: `app/server/SearcherServer` → `core.Indexer.Search` → `SearchBackend.Search`

### Key Interfaces (root module)

- **`core.SearchBackend`** — implemented by `es.Client`; decouples Indexer from ES
- **`core.Store`** — implemented by `storage/postgres`; Postgres relation graph
- **`app/source.Provider`** — implemented by `app/source.GRPCProvider`; data fetcher used by DSL plans

### Key Packages

| Package | Role |
|---------|------|
| `core/` | Orchestration: `Indexer`, workers, `SearchBackend` interface, `IndexName`/`AliasName` |
| `core/resource/` | Resource/version DSL types and validation |
| `model/` | Primitive types (`Resource`, `VersionedResource`) |
| `projection/` | `Plan` type and `BuildDoc` — the aggregation result flowing through plans |
| `es/` | `SearchBackend` implementation; mapping generation |
| `app/source/` | `Provider` interface + gRPC implementation |
| `app/server/` | Thin gRPC adapters; translates proto ↔ core types |
| `app/gen/` | Generated protobuf Go bindings — do not edit manually |
| `app/config/` | YAML resource DSL parsing |
| `app/dsl/` | Builds `projection.Plan` trees from resource config + Provider |
| `app/cmd/` | Entry points (`indexer`, `gen-mapping`, `cleanup`, `cutover`) |

### Critical Invariants

- **Distributed-safe**: multiple indexer instances run concurrently; no per-resource serialization guarantee.
- **OCC on writes**: ES writes use `external_gte` version type with `resources.build_idx` to handle concurrent rebuilds safely.
- **Stale-version rejection**: `Version > 0` in a change notification enables rejection; `0` means always accept.
- **Relation graph drives fanout**: affected root resources are found by querying the Postgres relation graph, not static config.
- **Full listing mode**: `BuildRequest.ResourceID == ""` triggers `ListResources` pagination (full rebuild).
- **Plans encapsulate data fetching**: `core.Indexer` only executes plans; it never calls `source.Provider` directly. Library users supply their own plans.

### Configuration

- **App config**: `indexer.yml` (override with `APP_CONFIG_PATH` env var). See `example.indexer.yml`.
- **Resource DSL**: `resources.yml` (override with `RESOURCE_CONFIG_PATH`). See `example.resources.yml`.
- **Provider plugin**: external gRPC service implementing `ProviderService` (FetchResource, FetchRelated, ListResources).

### gRPC Services (proto/)

- `index/v1` — `IndexService`: NotifyChange, NotifyChangeBatch, Rebuild
- `search/v1` — `SearchService`: Search, GetCapabilities
- `provider/v1` — `ProviderService`: FetchResource, FetchRelated, ListResources

### Testing

- Unit tests: `core/`, `es/`, `app/server/`, `app/dsl/` — no Docker needed
- Integration tests: `app/tests/` — use testcontainers (Docker) for real Postgres + Elasticsearch

Any feature or behavior change must include tests.
