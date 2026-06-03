# Indexer - Agent Guide (Compact)

## Purpose

`github.com/theleeeo/indexer` keeps Elasticsearch documents in sync with upstream service data.

- Input: gRPC change notifications.
- Source of truth for dependency impact: Postgres relation graph.
- Execution: River jobs (`rebuild`, `delete`, `full_rebuild`).
- Output: versioned ES upserts/deletes.

## Core Flow

1. `server` translates gRPC requests into `core.Notification`.
2. `core.Indexer.RegisterChange` updates `store` and finds affected roots.
3. `core` enqueues River jobs.
4. Workers call `Indexer.Build`.
5. Build fetches data through `source.Provider`, writes ES, updates relations.

Search path: `server/SearcherServer` -> `core.Indexer.Search` -> `es.Client.Search`.

## Key Packages

- `core/`: orchestration, workers, rebuild/search entry points.
- `projection/`: build denormalized root documents and resolve affected roots.
- `store/`: Postgres resource state + parent/child relation graph.
- `resource/`: resource/version DSL (`resources.yml`) + validation.
- `source/`: provider interface + gRPC provider implementation.
- `es/`: ES CRUD/search client + mapping generation.
- `server/`: thin gRPC adapters.

## Critical Invariants

- Distributed-safe behavior is required; assume multiple indexer instances.
- No per-resource serialization guarantee; concurrent rebuilds can happen.
- Rebuild writes use ES OCC with `external_gte` and `resources.build_idx`.
- `Version > 0` in notifications enables stale-version rejection.
- Relation graph drives reindex fanout (`AffectedRoots`), not static config lookups.
- `BuildRequest.ResourceID == ""` means full listing mode (`ListResources` with pagination).

## Config and Interfaces

- App config: `indexer.yml` (override with `APP_CONFIG_PATH`).
- Resource DSL: `resources.yml` (override with `RESOURCE_CONFIG_PATH`).
- Provider plugin: gRPC `ProviderService`.
- Generated protobuf code is under `gen/` (refresh with `buf generate`).

## Commands

```bash
# required for this repo
GOEXPERIMENT=jsonv2 go run ./cmd/indexer

# mapping generation
go run ./cmd/gen-mapping -config resources.yml

# regenerate protobuf bindings
buf generate

# tests (Docker required for testcontainers)
GOEXPERIMENT=jsonv2 go test ./...
```

## Change Rules

- Update this file when architecture/behavior changes make it inaccurate.
- Any feature or behavior change must include tests.
