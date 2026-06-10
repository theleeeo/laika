# Indexer - Agent Guide (Compact)

## Purpose

`github.com/theleeeo/indexer` keeps Elasticsearch documents in sync with upstream service data.

- Input: gRPC change notifications.
- Source of truth for dependency impact: Postgres relation graph.
- Execution: River jobs (`rebuild`, `delete`, `full_rebuild`).
- Output: versioned ES upserts/deletes.

## Core Flow

1. `app/server` translates gRPC requests into `core.Notification`.
2. `core.Indexer.RegisterChange` updates `store` and finds affected roots.
3. `core` enqueues River jobs.
4. Workers call `Indexer.Build`.
5. Build executes `projection.Plan` (which calls `source.Provider`), writes ES via `SearchBackend`, updates relations.

Search path: `app/server/SearcherServer` -> `core.Indexer.Search` -> `SearchBackend.Search`.

## Module Layout

- Root module (core library): `core/`, `projection/`, `model/`, `core/resource/`, `es/`
- `./app`: gRPC wiring (`server/`, `source/`, `gen/`), YAML DSL, entry points, integration tests
- `./aggregation`: aggregation execution library
- `./storage/postgres`: Postgres Store implementation

## Key Packages

- `core/`: orchestration, workers, `SearchBackend` interface, `IndexName`/`AliasName`.
- `projection/`: `Plan` type and `BuildDoc` — aggregation result.
- `core/resource/`: resource/version DSL types.
- `es/`: `SearchBackend` implementation + mapping generation.
- `app/source/`: `Provider` interface + gRPC implementation.
- `app/server/`: thin gRPC adapters; translates proto ↔ core types.
- `app/dsl/`: builds `projection.Plan` trees from resource config + Provider.

## Critical Invariants

- Distributed-safe behavior is required; assume multiple indexer instances.
- No per-resource serialization guarantee; concurrent rebuilds can happen.
- Rebuild writes use ES OCC with `external_gte` and `resources.build_idx`.
- `Version > 0` in notifications enables stale-version rejection.
- Relation graph drives reindex fanout, not static config lookups.
- `BuildRequest.ResourceID == ""` means full listing mode (`ListResources` with pagination).
- `core.Indexer` never calls `source.Provider` directly — it only executes plans.

## Config and Interfaces

- App config: `indexer.yml` (override with `APP_CONFIG_PATH`).
- Resource DSL: `resources.yml` (override with `RESOURCE_CONFIG_PATH`).
- Provider plugin: gRPC `ProviderService`.
- Generated protobuf code is under `app/gen/` (refresh with `buf generate`).

## Commands

```bash
# run the app
GOEXPERIMENT=jsonv2 go run ./app/cmd/indexer

# mapping generation
go run ./app/cmd/gen-mapping -config resources.yml

# regenerate protobuf bindings
buf generate

# unit tests (no Docker)
GOEXPERIMENT=jsonv2 go test ./core/... ./es/... ./app/server/... ./app/dsl/...

# integration tests (Docker required)
GOEXPERIMENT=jsonv2 go test ./app/tests/...
```

## Change Rules

- Update this file when architecture/behavior changes make it inaccurate.
- Any feature or behavior change must include tests.
