# Laika

**L**eos **AI**-powered **K**onfigurable **A**ggregator

Laika is a distributed search indexing engine that keeps Elasticsearch documents automatically in sync with your upstream services. It listens for change notifications over gRPC, resolves which search documents are affected, fetches the latest data from your services, and rebuilds the relevant Elasticsearch documents — all concurrently and safely across multiple instances.

---

## Use Cases

**Unified search across microservices**
Your data lives in several services. A search result needs fields from all of them — denormalized into a single Elasticsearch document. Laika aggregates those fields at index time so queries stay fast and simple.

**Change-propagation indexing**
When resource `a` changes and it is embedded inside documents for resource `b`, Laika automatically finds every affected `b` document via its relation graph and rebuilds only those — no full scans, no manual fanout logic.

**Zero-downtime index migrations**
Need to change a field's type or add a new relation? Define a new resource version in YAML. Laika writes to all active versions and reads from the designated version, letting you migrate and cut over without downtime.

**Decoupled search layer**
Laika sits between your services and Elasticsearch. Services only need to implement a simple gRPC provider interface; they stay completely unaware of how their data is indexed.

---

## How It Works

### Architecture Overview

```
  Your Services
  ┌──────────────────────────────────────────────────────┐
  │  ProviderService (gRPC)    IndexService (gRPC client) │
  │  FetchResource             NotifyChange               │
  │  FetchRelated              NotifyChangeBatch          │
  │  ListResources             Rebuild                    │
  └──────────┬──────────────────────────┬────────────────┘
             │ data                     │ change events
             ▼                          ▼
  ┌─────────────────────────────────────────────────────┐
  │                        Laika                        │
  │                                                     │
  │  IndexService  ──►  Indexer  ──►  River job queue   │
  │                        │              │             │
  │                   Relation        Build worker      │
  │                    graph  ◄────── Plan execution    │
  │                  (Postgres)            │             │
  │                                  Projection /       │
  │                                  Aggregation        │
  │                                        │            │
  │  SearchService ◄─────────────  Elasticsearch       │
  └─────────────────────────────────────────────────────┘
```

### Change Notification Flow

1. A service calls `NotifyChange` on Laika with a resource type, ID, and change kind (`CREATED`, `UPDATED`, `DELETED`).
2. Laika updates its Postgres relation graph and queries it to find all **root resources** affected by this change.
3. A River job is enqueued for each affected root resource.
4. The build worker executes the aggregation plan: it calls back into your `ProviderService` to fetch the resource and all configured related data.
5. The assembled document is written to Elasticsearch using optimistic concurrency control (`external_gte`) — concurrent rebuilds of the same document are safe.
6. The relation graph is updated with any newly discovered relations.

### Search Flow

Clients send a `Search` RPC to Laika's `SearchService`. Laika routes to the correct Elasticsearch alias (the read version for that resource), executes the query, and returns structured hits. Clients can call `GetCapabilities` to discover which fields are searchable, sortable, and filterable.

### Relation Graph

The heart of Laika's smart fanout. When resource `c` changes, Laika queries Postgres to find every root resource that embeds `c`. Only those documents are rebuilt. This graph is populated incrementally as documents are built, so no static mapping is needed.

---

## Configuration

Laika is configured through two YAML files.

### Runtime Config (`indexer.yml`)

```yaml
grpc:
  addr: ":9000"

es:
  addrs:
    - "http://localhost:9200"
  username: ""
  password: ""

pg:
  addr: "postgres://user:pass@localhost:5432/indexer"

provider:
  addr: "localhost:50051"

resource_config_path: "resources.yml"
```

Every key maps directly to an environment variable by uppercasing and replacing `.` with `_` — for example, `es.addrs` → `ES_ADDRS` (comma-separated). The config file path itself is overridden with `APP_CONFIG_PATH`.

### Resource DSL (`resources.yml`)

This is where you define what gets indexed and how documents are assembled.

```yaml
resources:
  - type: order # resource type name (must match what your ProviderService returns)
    version: 1 # optional, defaults to 1
    fields:
      fields:
        - name: status # keyword by default
        - name: notes
          type: text # full-text search
          query:
            search: false # include in filter but exclude from full-text search
      relations:
        - resource: customer
          key:
            source: order # which resource holds the foreign key
            field: customer_id
          cardinality: one # "one" → object mapping, "many" → nested mapping
          fields:
            - name: name
            - name: email
```

**Versioning** lets you run multiple index shapes concurrently:

```yaml
resources:
  - type: order
    version: 1
    readVersion: 1 # reads go to v1 while v2 is being populated
    fields: { ... }

  - type: order
    version: 2
    fields: { ... } # new shape — writes go here too, reads switch once ready
```

---

## gRPC API

Laika exposes three gRPC services. Proto definitions live in `proto/`.

### IndexService — receive change events

| RPC                 | Description                                           |
| ------------------- | ----------------------------------------------------- |
| `NotifyChange`      | Single resource change notification                   |
| `NotifyChangeBatch` | Batch of change notifications                         |
| `Rebuild`           | Trigger a full rebuild (paginated) of a resource type |

A notification carries the resource type, ID, change kind, and an optional monotonic version for stale-rejection.

### SearchService — query indexed documents

| RPC               | Description                                                       |
| ----------------- | ----------------------------------------------------------------- |
| `Search`          | Full-text search with filters, sorting, and pagination            |
| `GetCapabilities` | Discover searchable, sortable, and filterable fields per resource |

Search supports structured field filters (`EQ`, `IN`), multi-field sorting, and page-based pagination (up to 100 results per page).

### ProviderService — your data contract

Laika calls back into your service via this interface to fetch data during builds. You implement it.

| RPC             | Description                                            |
| --------------- | ------------------------------------------------------ |
| `FetchResource` | Fetch a single resource by type and ID                 |
| `FetchRelated`  | Fetch resources related via a foreign key              |
| `ListResources` | Paginated listing of all resources (for full rebuilds) |

---

## Usage

### Standalone Application

The quickest way to run Laika is as a standalone binary:

```bash
# Run from repo root
APP_CONFIG_PATH=example.indexer.yml GOEXPERIMENT=jsonv2 go run ./app/cmd/indexer
```

You provide `indexer.yml` and `resources.yml`; Laika handles the rest. Your services connect to the `IndexService` to push change events and to the `SearchService` to run queries. You implement one gRPC server (the `ProviderService`) that Laika calls to fetch data.

**Generate an Elasticsearch mapping** from your resource config:

```bash
go run ./app/cmd/gen-mapping -config resources.yml
```

**Regenerate protobuf bindings** after editing proto files:

```bash
buf generate
```

### As a Library

The root module is a standalone Go library. Import it to embed Laika directly or to build custom wiring:

```go
import "github.com/theleeeo/indexer/core"
```

Construct an `Indexer` with your own `SearchBackend`, `Store`, and aggregation plans. Library users supply plans directly; the `app` module's DSL is one way to build them, but not the only way.

---

## Running Tests

```bash
# Unit tests — no Docker required
GOEXPERIMENT=jsonv2 go test ./core/... ./es/... ./app/server/... ./app/dsl/...

# Integration tests — requires Docker (testcontainers spins up Postgres + Elasticsearch)
GOEXPERIMENT=jsonv2 go test ./app/tests/...

# All tests
GOEXPERIMENT=jsonv2 go test ./...
```

---

## Module Layout

| Module               | Purpose                                                                   |
| -------------------- | ------------------------------------------------------------------------- |
| `.` (root)           | Core library: `Indexer`, `SearchBackend` interface, projection, ES client |
| `./app`              | Standalone app: gRPC wiring, YAML DSL, entry points, integration tests    |
| `./aggregation`      | Streaming aggregation pipeline execution                                  |
| `./storage/postgres` | Postgres `Store` implementation (relation graph, resource versioning)     |

Library consumers depend only on the root module (and `aggregation` / `storage` as needed). The `app` module wires everything into a deployable service.
