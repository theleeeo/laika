# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All Go commands require the `GOEXPERIMENT=jsonv2` flag (Go 1.26.1).

```bash
# Run the application
GOEXPERIMENT=jsonv2 go run ./cmd/indexer

# Run all tests (requires Docker for testcontainers)
GOEXPERIMENT=jsonv2 go test ./...

# Run a single test
GOEXPERIMENT=jsonv2 go test ./tests/ -run TestName

# Generate Elasticsearch mappings from resource config
go run ./cmd/gen-mapping -config resources.yml

# Regenerate protobuf bindings
buf generate
```

## Architecture

This is a distributed search indexing engine that keeps Elasticsearch documents in sync with upstream service data via gRPC change notifications.

### Core Data Flow

1. gRPC client → `server` translates requests into `core.Notification`
2. `core.Indexer.RegisterChange` updates Postgres state and finds affected root resources via the relation graph
3. `core` enqueues River jobs (`rebuild`, `delete`, `full_rebuild`)
4. River workers call `Indexer.Build`
5. Build fetches authoritative data through `source.Provider` (gRPC plugin), writes to ES, updates the relation graph

Search path: `server/SearcherServer` → `core.Indexer.Search` → `es.Client.Search`

### Key Packages

| Package | Role |
|---------|------|
| `core/` | Orchestration: change registration, workers, rebuild/delete/search entry points |
| `app/` | Wiring: config loading (YAML DSL), plan building, dependency injection |
| `app/config/` | YAML resource DSL parsing and validation |
| `app/dsl/` | Builds aggregation plans from resource config |
| `projection/` | Builds denormalized ES documents; resolves affected roots |
| `storage/postgres/` | Postgres persistence: resource state + parent/child relation graph |
| `core/resource/` | Resource/version DSL types (`resources.yml`) and validation |
| `core/source/` | Abstract provider interface |
| `source/` | gRPC `ProviderService` client implementation |
| `es/` | Elasticsearch CRUD, search, alias management, mapping generation |
| `server/` | Thin gRPC adapters for IndexService and SearchService |
| `aggregation/` | Streaming aggregation pipeline execution (separate Go module) |
| `gen/` | Generated protobuf Go bindings — do not edit manually |

### Go Workspace

This repo uses Go workspaces (`go.work`) with multiple modules:
- Root module — main indexer
- `./aggregation` — aggregation execution library
- `./app` — application wiring
- `./storage/postgres` — storage layer
- `./dev/vx-provider` — example provider plugin

### Critical Invariants

- **Distributed-safe**: multiple indexer instances run concurrently; no per-resource serialization guarantee.
- **OCC on writes**: ES rebuild writes use `external_gte` version type with `resources.build_idx` to handle concurrent rebuilds safely.
- **Stale-version rejection**: `Version > 0` in a change notification enables rejection; `0` means always accept.
- **Relation graph drives fanout**: affected root resources are found by querying the Postgres relation graph (`AffectedRoots`), not static config.
- **Full listing mode**: `BuildRequest.ResourceID == ""` triggers `ListResources` pagination (full rebuild).

### Configuration

- **App config**: `indexer.yml` (override with `APP_CONFIG_PATH` env var). See `example.indexer.yml`.
- **Resource DSL**: `resources.yml` (override with `RESOURCE_CONFIG_PATH`). See `example.resources.yml`.
- **Provider plugin**: external gRPC service implementing `ProviderService` (FetchResource, FetchRelated, ListResources).

### gRPC Services (proto/)

- `index/v1` — `IndexService`: NotifyChange, NotifyChangeBatch, Rebuild
- `search/v1` — `SearchService`: Search, GetCapabilities
- `provider/v1` — `ProviderService`: FetchResource, FetchRelated, ListResources

### Testing

Integration tests live in `tests/` and use testcontainers (Docker) to spin up real Postgres and Elasticsearch instances. Any feature or behavior change must include tests.
