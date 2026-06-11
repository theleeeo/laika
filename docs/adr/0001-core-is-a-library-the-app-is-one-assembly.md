# Core is a library; the app is one assembly

The root module is a library that knows nothing about gRPC, YAML, or any specific data source. It accepts a set of `projection.Plan` values (which encapsulate data fetching) and an implementation of `core.SearchBackend` and `core.Store`, and runs the build/rebuild/relation-fanout loop on top of them. The `./app` module is one concrete assembly of that library: it parses YAML resource configs, constructs plans via the DSL in `app/dsl`, fetches via a gRPC `source.Provider`, and exposes the result over a gRPC API.

We chose this split because there are two real consumers:

1. The default `./app` — a plug-and-play indexer driven entirely by YAML, useful when the domain is simple and the upstream speaks the standard `ProviderService` gRPC contract.
2. An external enterprise application with a richer domain model that depends on the same core library but bypasses the DSL: it builds resource descriptors as hardcoded Go structs, executes plans that make direct typed gRPC calls (no `map[string]any` hops through a generic provider), and hijacks the Search path to inject required filters.

The cost is the `projection.Plan` abstraction — `core.Indexer` cannot just call a `Provider` directly, and `BuildDoc` flows through a generic `aggregation.Executer`. We accept this cost because collapsing it would force the enterprise assembly to fork the orchestrator or pay a permanent map-conversion tax.

**Implication for contributors:** anything specific to the YAML DSL, the gRPC provider, or the gRPC server belongs in `./app`. The root module stays free of those dependencies.
