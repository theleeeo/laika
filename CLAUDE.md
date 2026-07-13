# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Canonical vocabulary lives in [CONTEXT.md](CONTEXT.md); architectural decisions in [docs/adr/](docs/adr/). This file describes the codebase — where things live and how to run them — not the language.

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

# Diff running index mappings against what the current config would generate
# (exits non-zero on actionable drift: added/changed fields or a missing index)
go run ./app/cmd/diff-mapping -config resources.yml -es-addr http://localhost:9200

# Regenerate protobuf bindings (outputs to app/gen/)
buf generate
```

## Architecture

This is a distributed search indexing engine that keeps Elasticsearch Documents in sync with upstream service data via gRPC Notifications.

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
2. `core.Indexer.RegisterChange` updates Postgres state and finds affected Parent Resources via the Relation graph
3. `core` enqueues River jobs (`build`, `delete`, `rebuild`)
4. River workers call `Indexer.Build`
5. A Build executes a `projection.Plan` (which calls the `source.Provider`), writes to ES via `SearchBackend`, updates the Relation graph

Search path: `app/server/SearcherServer` → `core.Indexer.Search` → `SearchBackend.Search`

### Key Interfaces (root module)

- **`core.SearchBackend`** — implemented by `es.Client`; decouples Indexer from ES
- **`core.Store`** — implemented by `storage/postgres`; Postgres relation graph
- **`app/source.Provider`** — implemented by `app/source.GRPCProvider`; data fetcher used by DSL plans

### Key Packages

| Package | Role |
|---------|------|
| `core/` | Orchestration: `Indexer`, workers, `SearchBackend` interface, `IndexName`/`AliasName` |
| `core/resource/` | Resource/Schema-Version DSL types and validation |
| `model/` | Primitive types (`Resource`, `VersionedResource`) |
| `projection/` | `Plan` type and `BuildDoc` — the aggregation result flowing through Plans |
| `es/` | `SearchBackend` implementation; mapping generation |
| `app/source/` | `Provider` interface + gRPC implementation |
| `app/server/` | Thin gRPC adapters; translates proto ↔ core types |
| `app/gen/` | Generated protobuf Go bindings — do not edit manually |
| `app/config/` | YAML resource DSL parsing |
| `app/dsl/` | Builds `projection.Plan` trees from resource config + Provider |
| `app/cmd/` | Entry points (`indexer`, `gen-mapping`, `diff-mapping`, `cleanup`, `cutover`) |

### Critical Invariants

- **Distributed-safe**: multiple indexer instances run concurrently; no per-Resource serialization guarantee. See [ADR 0002](docs/adr/0002-distributed-safety-via-occ-and-drift-check-not-locks.md).
- **Build Sequence drives OCC**: every ES write carries the Resource's Build Sequence (stored in `resources.build_idx`) sent as the `external_gte` version, so concurrent Builds and Rebuilds of the same Document land in counter order.
- **Stale Version rejection**: a Notification with `Version > 0` enables drop-on-stale; `0` means always accept.
- **Relation graph drives fanout**: affected Parent Resources are found by querying the Postgres Relation graph, not static config.
- **All-of-Type Rebuild path**: `BuildRequest.ResourceID == ""` triggers `ListResources` pagination — the Rebuild path that walks every Resource of a Type.
- **Plans encapsulate data fetching**: `core.Indexer` only executes Plans; it never calls `source.Provider` directly. Library users supply their own Plans.

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
