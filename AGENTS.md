# Indexer - Agent Guide (Compact)

Canonical vocabulary lives in [CONTEXT.md](CONTEXT.md); architectural decisions in [docs/adr/](docs/adr/). This file describes the codebase, not the language.

## Purpose

`github.com/theleeeo/laika` keeps Elasticsearch Documents in sync with upstream service data.

- Input: gRPC Notifications.
- Source of truth for dependency impact: Postgres Relation graph.
- Execution: River jobs (`build`, `delete`, `full_rebuild`).
- Output: ES upserts/deletes ordered by Build Sequence.

## Core Flow

1. `app/server` translates gRPC requests into `core.Notification`.
2. `core.Indexer.RegisterChange` updates `store` and finds affected Parent Resources.
3. `core` enqueues River jobs.
4. Workers call `Indexer.Build`.
5. A Build executes a `projection.Plan` (which calls `source.Provider`), writes ES via `SearchBackend`, updates Relations.

Search path: `app/server/SearcherServer` -> `core.Indexer.Search` -> `SearchBackend.Search`.

## Module Layout

- Root module (core library): `core/`, `projection/`, `model/`, `core/resource/`, `es/`
- `./app`: gRPC wiring (`server/`, `source/`, `gen/`), YAML DSL, entry points, integration tests
- `./aggregation`: aggregation execution library
- `./storage/postgres`: Postgres Store implementation

## Key Packages

- `core/`: orchestration, workers, `SearchBackend` interface, `IndexName`/`AliasName`.
- `projection/`: `Plan` type and `BuildDoc` — the aggregation result a Plan emits.
- `core/resource/`: Resource/Schema-Version DSL types.
- `es/`: `SearchBackend` implementation + mapping generation.
- `app/source/`: `Provider` interface + gRPC implementation.
- `app/server/`: thin gRPC adapters; translates proto ↔ core types.
- `app/dsl/`: builds `projection.Plan` trees from Resource config + Provider.

## Critical Invariants

- Distributed-safe behavior is required; assume multiple indexer instances. See [ADR 0002](docs/adr/0002-distributed-safety-via-occ-and-drift-check-not-locks.md).
- No per-Resource serialization guarantee; concurrent Builds and Rebuilds can happen.
- Every ES write is stamped with the Resource's Build Sequence (in `resources.build_idx`) sent as the `external_gte` version — that is the OCC ordering.
- A Notification with `Version > 0` enables stale-Version rejection; `0` means always accept.
- Relation graph drives Build fanout, not static config lookups.
- `BuildRequest.ResourceID == ""` is the all-of-Type Rebuild path (`ListResources` with pagination).
- `core.Indexer` never calls `source.Provider` directly — it only executes Plans.

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
