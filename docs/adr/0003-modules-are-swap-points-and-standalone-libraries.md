# Modules are swap points and standalone libraries

The repository is a Go workspace with four modules. Each module exists for one of two reasons:

**Swap-point modules** — concrete implementations of an interface owned by the root library. `backend/elasticsearch` implements `core.SearchBackend`; `storage/postgres` implements `core.Store`. They live outside the root module so a library user who chooses a different backend (OpenSearch, Algolia, an in-memory store, …) does not transitively pull in the Elasticsearch Go client or `pgx`. The recent extraction of `backend/elasticsearch` out of root was driven by exactly this dep-hygiene concern.

**Standalone libraries** — code that is not specific to this project but is currently developed alongside it. `aggregation` is a generic channel-based pipeline used by the indexer through `projection.Plan.Executer`, but is also reused in other applications (data exports and similar) and may move to its own repository in the future. It is a module so that those other applications can depend on it without carrying the indexer.

The root module deliberately stays free of both categories: no concrete backends, no general-purpose libraries that aren't core to the indexer's interface. The `./app` module is the integration point that depends on everything.

**Implication for contributors:** when adding a new package, ask which category it falls into. A new concrete implementation of a root interface belongs in its own module under `backend/` or `storage/`. A new general-purpose utility that isn't specific to this project either belongs in its own module (if reuse is expected) or stays out entirely. Code that is core indexer logic, including interfaces and orchestration, lives in the root module.
