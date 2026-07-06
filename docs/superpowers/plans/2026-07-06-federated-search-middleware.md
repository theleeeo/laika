# Federated Search Middleware Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Federated Search its own consumer middleware chain, delete the collect-mode replay of single-resource middleware, and split the harness's authz into one shared policy evaluator feeding thin middlewares for both search paths.

**Architecture:** Core gains `FederatedSearchMiddleware` (composed once in `New`, wrapping the whole federated body including validation) and a trusted per-Type filter channel on `FederatedSearchRequest` (`ResourceFilters`, `SecondaryScope`). The collect-mode mechanism (`collectFilters`/middleware replay) is removed; the group builder reads the request instead. The harness extracts `filtersFor` from its authz middleware and wires both middleware kinds from the same `ResourcePolicy` map.

**Tech Stack:** Go 1.26.x (`GOEXPERIMENT=jsonv2`), Elasticsearch backend, testcontainers for integration tests, vx-bouncer SDK (harness only).

**Spec:** `docs/superpowers/specs/2026-07-06-federated-search-middleware-design.md`

## Global Constraints

- Every Go command needs `GOEXPERIMENT=jsonv2` (Go 1.26.1).
- Tasks 1–4 run in the `laika/` repo (its own git repo); run commands from the laika repo root.
- Tasks 5–6 run in the sibling `harness/` repo (its own git repo, **not** in laika's `go.work`); run its commands from the harness directory with `GOWORK=off GOEXPERIMENT=jsonv2`. Harness consumes laika via `replace github.com/theleeeo/laika => ../laika` in its `go.mod`, so laika changes flow without publishing.
- `ResourceFilters`/federated `SecondaryScope` are core-API-only: no proto changes anywhere in this plan.
- Core stays policy-free: no auth concepts (actor, tenant, enforce) enter laika.
- Commit after each task, in the repo that task touches.

---

### Task 1: Federated middleware chain in core (laika repo)

Add the federated handler/middleware types, a generic chain composer, the `Config` field, and composition in `New`. The existing `FederatedSearch` body becomes the base handler `federatedSearchBase` — unchanged internally, so collect mode still works during this task.

**Files:**
- Modify: `core/search_middleware.go` (new types; genericize the composer)
- Modify: `core/indexer.go` (Config field, chain field, composition in `New`)
- Modify: `core/federated_search.go` (split `FederatedSearch` into thin wrapper + `federatedSearchBase`)
- Test: `core/federated_search_test.go` (append middleware tests)

**Interfaces:**
- Consumes: existing `FederatedSearchRequest`, `FederatedSearchResponse`, `Indexer`, `recordingBackend` + `newFederatedIndexer` test helpers.
- Produces (later tasks rely on these exact names):
  - `type FederatedSearchHandler func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error)`
  - `type FederatedSearchMiddleware func(next FederatedSearchHandler) FederatedSearchHandler`
  - `Config.FederatedSearchMiddlewares []FederatedSearchMiddleware`
  - unexported `chain[H any, M ~func(H) H](base H, mws []M) H`
  - unexported method `(*Indexer) federatedSearchBase(ctx, req) (FederatedSearchResponse, error)`
  - test helper `newFederatedIndexerMW(backend SearchBackend, resources []string, mws ...FederatedSearchMiddleware) *Indexer`

- [ ] **Step 1: Write the failing tests**

Append to `core/federated_search_test.go`:

```go
// newFederatedIndexerMW builds an Indexer with the given resource types (same
// shape as newFederatedIndexer) and federated search middlewares.
func newFederatedIndexerMW(backend SearchBackend, resources []string, mws ...FederatedSearchMiddleware) *Indexer {
	cfgs := make(resource.Configs, 0, len(resources))
	for _, name := range resources {
		cfg := &resource.Config{
			Resource: name,
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{
					{Name: "name", Type: "text"},
					{Name: "region"},
				}},
			},
		}
		cfg.ApplyDefaults()
		cfgs = append(cfgs, cfg)
	}
	return New(Config{
		Resources:                  cfgs,
		ES:                         backend,
		FederatedSearchMiddlewares: mws,
	})
}

func TestFederatedSearch_MiddlewareOrdering(t *testing.T) {
	backend := &recordingBackend{}
	var order []string
	tag := func(name string) FederatedSearchMiddleware {
		return func(next FederatedSearchHandler) FederatedSearchHandler {
			return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
				order = append(order, name)
				return next(ctx, req)
			}
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, tag("A"), tag("B"))

	if _, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	}); err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Fatalf("expected call order [A B], got %v", order)
	}
	if !backend.fedCalled {
		t.Fatal("expected backend.FederatedSearch to be called")
	}
}

func TestFederatedSearch_MiddlewareShortCircuit(t *testing.T) {
	backend := &recordingBackend{}
	denied := errors.New("denied")
	deny := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			return FederatedSearchResponse{}, denied
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, deny)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	})
	if !errors.Is(err, denied) {
		t.Fatalf("expected denied error, got %v", err)
	}
	if backend.fedCalled {
		t.Fatal("backend must not be called when a middleware short-circuits")
	}
}

func TestFederatedSearch_MiddlewareResponseModification(t *testing.T) {
	backend := &recordingBackend{fedResponse: FederatedSearchResult{Total: 1}}
	bump := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			resp, err := next(ctx, req)
			if err != nil {
				return resp, err
			}
			resp.Total += 100
			return resp, nil
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, bump)

	resp, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product"},
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if resp.Total != 101 {
		t.Fatalf("expected modified Total 101, got %d", resp.Total)
	}
}

// The chain wraps the base handler INCLUDING its validation, so an observing
// middleware sees validation failures too (spec: validation-in-base).
func TestFederatedSearch_MiddlewareObservesValidationError(t *testing.T) {
	backend := &recordingBackend{}
	var seen error
	observe := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			resp, err := next(ctx, req)
			seen = err
			return resp, err
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product"}, observe)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{Query: "q"}) // no Resources
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError for empty resource set, got %v", err)
	}
	if !errors.As(seen, &inv) {
		t.Fatalf("middleware should observe the validation error, saw %v", seen)
	}
	if backend.fedCalled {
		t.Fatal("backend must not be called for an invalid request")
	}
}

func TestFederatedSearch_MiddlewareNarrowsResources(t *testing.T) {
	backend := &recordingBackend{}
	narrow := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			req.Resources = []string{"product"}
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"}, narrow)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query: "q", Resources: []string{"product", "order"},
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if len(backend.fedParams.FilterGroups) != 1 || backend.fedParams.FilterGroups[0].Resource != "product" {
		t.Fatalf("expected only the narrowed Type's group, got %+v", backend.fedParams.FilterGroups)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestFederatedSearch_Middleware' -v`
Expected: compile FAIL — `undefined: FederatedSearchMiddleware`, `undefined: FederatedSearchHandler`, unknown field `FederatedSearchMiddlewares`.

- [ ] **Step 3: Implement the types and the generic composer**

Replace the whole of `core/search_middleware.go` with:

```go
package core

import "context"

// SearchHandler executes a search. The innermost handler is the Indexer's own
// validate + normalize + backend call.
type SearchHandler func(ctx context.Context, req SearchRequest) (SearchResponse, error)

// SearchMiddleware wraps a SearchHandler, returning a new one. Middlewares run
// outermost-first in registration order.
type SearchMiddleware func(next SearchHandler) SearchHandler

// FederatedSearchHandler executes a federated search. The innermost handler is
// the Indexer's own validate + group-build + backend call.
type FederatedSearchHandler func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error)

// FederatedSearchMiddleware wraps a FederatedSearchHandler, returning a new
// one. Middlewares run outermost-first in registration order. The federated
// chain is fully independent of the single-resource chain: neither ever runs
// on the other path.
type FederatedSearchMiddleware func(next FederatedSearchHandler) FederatedSearchHandler

// chain composes middlewares around a base handler. The slice runs
// outermost-first: for []{A, B} the resulting call order is A → B → base.
// Instantiate H explicitly at call sites (chain[SearchHandler](...)) so a
// method-value base infers cleanly.
func chain[H any, M ~func(H) H](base H, mws []M) H {
	h := base
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
```

In `core/indexer.go`:

1. Add to `Config` (after `SearchMiddlewares`):

```go
	// FederatedSearchMiddlewares wrap the federated search path, independently
	// of SearchMiddlewares — neither chain ever runs on the other path. They
	// run outermost-first in registration order: []{A, B} executes A → B → the
	// Indexer's own validate/group-build/backend call, so a middleware also
	// observes validation failures. A middleware may deny the request with an
	// error, mutate the FederatedSearchRequest, and inspect or modify the
	// response. The chain is composed once at construction.
	FederatedSearchMiddlewares []FederatedSearchMiddleware
```

2. Add to the `Indexer` struct (after `searchChain`):

```go
	// federatedSearchChain is the composed federated search handler: the
	// registered FederatedSearchMiddlewares wrapped around federatedSearchBase.
	// When no middlewares are registered it equals federatedSearchBase.
	federatedSearchChain FederatedSearchHandler
```

3. In `New`, replace `idx.searchChain = chainSearch(idx.searchBase, mws)` with:

```go
	idx.searchChain = chain[SearchHandler](idx.searchBase, mws)
	idx.federatedSearchChain = chain[FederatedSearchHandler](idx.federatedSearchBase, cfg.FederatedSearchMiddlewares)
```

In `core/federated_search.go`, split the entry point. The existing method keeps its full body but is renamed, and a thin public wrapper is added above it:

```go
// FederatedSearch runs one query across a caller-supplied set of Resource
// Types through the registered federated middleware chain (see
// Config.FederatedSearchMiddlewares). The base handler validates, builds
// per-index filter groups, and queries the backend; see federatedSearchBase.
func (idx *Indexer) FederatedSearch(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
	return idx.federatedSearchChain(ctx, req)
}
```

Rename the old method: `func (idx *Indexer) FederatedSearch(...)` → `func (idx *Indexer) federatedSearchBase(...)`, and adjust the first line of its doc comment from "FederatedSearch runs one query…" to "federatedSearchBase runs one query…" (rest of the comment unchanged in this task).

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestFederatedSearch|TestSearch_' -v`
Expected: all PASS (new middleware tests plus all existing search/federated tests — collect mode is untouched).

- [ ] **Step 5: Run the full unit suite**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... ./app/server/... ./app/dsl/...`
Expected: PASS.

- [ ] **Step 6: Commit (laika repo)**

```bash
git add core/search_middleware.go core/indexer.go core/federated_search.go core/federated_search_test.go
git commit -m "feat(core): dedicated middleware chain for federated search"
```

---

### Task 2: Per-Type filter channel; delete collect mode (laika repo)

`FederatedSearchRequest` gains `ResourceFilters` and `SecondaryScope`; the group builder reads them; `collectFilters`, the middleware replay, `Indexer.searchMiddlewares`, and `SearchRequest.SecondaryScope` are all deleted. Core unit tests are rewritten against the new mechanism.

**Files:**
- Modify: `core/federated_search.go` (request fields; `collectIndexFilterGroups` → `buildIndexFilterGroups`; delete `collectFilters`; doc updates)
- Modify: `core/search_types.go` (remove `SearchRequest.SecondaryScope`)
- Modify: `core/indexer.go` (remove `searchMiddlewares` field and its assignment)
- Test: `core/federated_search_test.go` (rewrite collect-mode tests)

**Interfaces:**
- Consumes: Task 1's `FederatedSearchMiddleware`, `newFederatedIndexerMW`; existing `resolveReferenceFilters(ctx, resourceName string, filters []Filter) (resolved []Filter, matchedNothing bool, err error)`; existing `IndexFilterGroup`.
- Produces (later tasks rely on these exact names):
  - `FederatedSearchRequest.ResourceFilters map[string][]Filter`
  - `FederatedSearchRequest.SecondaryScope string`
  - unexported `(*Indexer) buildIndexFilterGroups(ctx context.Context, resources []string, perType map[string][]Filter) ([]IndexFilterGroup, error)`
  - `SearchRequest` **without** `SecondaryScope` (breaking for anyone setting it; nothing outside core/tests does).

- [ ] **Step 1: Rewrite the collect-mode tests as failing request-driven tests**

In `core/federated_search_test.go`:

**Delete** these tests (their subject is being removed):
- `TestCollectIndexFilterGroups_CapturesFilterWithAlias`
- `TestCollectIndexFilterGroups_PerTypeFiltersIndependent`
- `TestCollectIndexFilterGroups_DeniedTypeFailsClosed` (denial is now Task 1's `TestFederatedSearch_MiddlewareShortCircuit`)
- `TestCollectIndexFilterGroups_HarvestsSecondaryScope`
- `TestCollectIndexFilterGroups_NoScopeChannel_Empty`
- `TestCollectIndexFilterGroups_UnknownResource`

**Delete** the helper `newFederatedIndexer` and change every remaining call site of it to `newFederatedIndexerMW` (same arguments minus any `SearchMiddleware` args — after this task the federated tests never use `SearchMiddleware`).

**Replace** `TestFederatedSearch_WiresCollectedGroupsScopeAndPaging` with:

```go
func TestFederatedSearch_WiresGroupsScopeAndPaging(t *testing.T) {
	backend := &recordingBackend{}
	// A federated middleware scopes each Type and sets the caller's secondary
	// scope — the per-Type channel that replaced collect mode.
	scope := func(next FederatedSearchHandler) FederatedSearchHandler {
		return func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error) {
			req.SecondaryScope = "tenant-7"
			req.ResourceFilters = make(map[string][]Filter, len(req.Resources))
			for _, r := range req.Resources {
				req.ResourceFilters[r] = []Filter{{Field: "fields.tenant_id", Op: FilterOpEq, Value: "tenant-7"}}
			}
			return next(ctx, req)
		}
	}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"}, scope)

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"product", "order"},
		Filters:   []Filter{{Field: "fields.region", Op: FilterOpEq, Value: "eu"}},
		PageSize:  0, // exercises normalization
	})
	if err != nil {
		t.Fatalf("FederatedSearch: %v", err)
	}
	if !backend.fedCalled {
		t.Fatal("expected backend.FederatedSearch to be called")
	}
	p := backend.fedParams
	if p.SecondaryScope != "tenant-7" {
		t.Errorf("SecondaryScope = %q, want tenant-7", p.SecondaryScope)
	}
	if len(p.FilterGroups) != 2 {
		t.Fatalf("expected 2 filter groups, got %d", len(p.FilterGroups))
	}
	for _, g := range p.FilterGroups {
		if g.Alias != AliasName(g.Resource) {
			t.Errorf("group %s alias = %q, want %q", g.Resource, g.Alias, AliasName(g.Resource))
		}
		if len(g.Filters) != 1 || g.Filters[0].Field != "fields.tenant_id" {
			t.Errorf("group %s: expected per-Type visibility filter, got %+v", g.Resource, g.Filters)
		}
	}
	if len(p.Filters) != 1 || p.Filters[0].Field != "fields.region" {
		t.Errorf("global filters not forwarded, got %+v", p.Filters)
	}
	if p.PageSize != 25 {
		t.Errorf("PageSize = %d, want normalized 25", p.PageSize)
	}
}
```

**Add** these new tests:

```go
func TestBuildIndexFilterGroups_TagsFiltersWithAlias(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product", "order"})

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"product", "order"}, map[string][]Filter{
		"product": {{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"}},
		// "order" has no entry: it must get an unfiltered group.
	})
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Resource != "product" || groups[0].Alias != AliasName("product") {
		t.Errorf("group[0] = %+v, want product with its alias", groups[0])
	}
	if len(groups[0].Filters) != 1 || groups[0].Filters[0].Field != "fields.tenant_id" {
		t.Errorf("product group filters = %+v, want the per-Type filter", groups[0].Filters)
	}
	if groups[1].Resource != "order" || len(groups[1].Filters) != 0 || groups[1].MatchNothing {
		t.Errorf("order group = %+v, want unfiltered (Type membership only)", groups[1])
	}
	if backend.called || backend.fedCalled {
		t.Error("group building alone must not hit the backend for plain filters")
	}
}

func TestBuildIndexFilterGroups_UnknownResource(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexerMW(backend, []string{"product"})

	_, err := idx.buildIndexFilterGroups(context.Background(), []string{"nope"}, nil)
	if !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("expected ErrUnknownResource, got %v", err)
	}
}

// newReferenceIndexerMW mirrors the harness domain: "ap" references "pop"
// (population_id == pop.id), so a per-Type filter on pop.fiber_operator_id
// must resolve via a child search into a terms filter on fields.population_id.
func newReferenceIndexerMW(backend SearchBackend, mws ...FederatedSearchMiddleware) *Indexer {
	pop := &resource.Config{
		Resource: "pop",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "name"}, {Name: "fiber_operator_id"}},
		}},
	}
	ap := &resource.Config{
		Resource: "ap",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "population_id"}},
			Relations: []resource.RelationConfig{{
				Resource:    "pop",
				Join:        resource.JoinConfig{Local: "population_id", Foreign: "id"},
				Cardinality: "one",
				Strategy:    resource.StrategyReference,
				Fields:      []resource.FieldConfig{{Name: "fiber_operator_id"}},
			}},
		}},
	}
	pop.ApplyDefaults()
	ap.ApplyDefaults()
	return New(Config{
		Resources:                  resource.Configs{pop, ap},
		ES:                         backend,
		FederatedSearchMiddlewares: mws,
	})
}

func TestBuildIndexFilterGroups_ResolvesReferenceFilter(t *testing.T) {
	backend := &recordingBackend{response: SearchResponse{
		Total: 1,
		Hits:  []SearchHit{{ID: "pop-A"}},
	}}
	idx := newReferenceIndexerMW(backend)

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"ap"}, map[string][]Filter{
		"ap": {{Field: "pop.fiber_operator_id", Op: FilterOpEq, Value: "op-A"}},
	})
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if !backend.called {
		t.Fatal("expected a child search against the referenced Type")
	}
	if len(groups) != 1 || groups[0].MatchNothing {
		t.Fatalf("expected one live group, got %+v", groups)
	}
	fs := groups[0].Filters
	if len(fs) != 1 || fs[0].Field != "fields.population_id" || fs[0].Op != FilterOpIn {
		t.Fatalf("expected folded terms filter on fields.population_id, got %+v", fs)
	}
	if len(fs[0].Values) != 1 || fs[0].Values[0] != "pop-A" {
		t.Fatalf("expected folded child key [pop-A], got %v", fs[0].Values)
	}
}

func TestBuildIndexFilterGroups_ReferenceZeroChildrenMatchNothing(t *testing.T) {
	backend := &recordingBackend{} // child search returns no hits
	idx := newReferenceIndexerMW(backend)

	groups, err := idx.buildIndexFilterGroups(context.Background(), []string{"ap", "pop"}, map[string][]Filter{
		"ap": {{Field: "pop.fiber_operator_id", Op: FilterOpEq, Value: "op-none"}},
	})
	if err != nil {
		t.Fatalf("buildIndexFilterGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if !groups[0].MatchNothing {
		t.Errorf("ap group should be MatchNothing when the reference matches no children, got %+v", groups[0])
	}
	if groups[1].MatchNothing || groups[1].Resource != "pop" {
		t.Errorf("pop group must stay live, got %+v", groups[1])
	}
}
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestBuildIndexFilterGroups|TestFederatedSearch_Wires' -v`
Expected: compile FAIL — `undefined: idx.buildIndexFilterGroups`, unknown fields `ResourceFilters`/`SecondaryScope` on `FederatedSearchRequest`.

- [ ] **Step 3: Implement the request channel and the group builder; delete collect mode**

In `core/federated_search.go`:

1. Replace the `FederatedSearchRequest` type with:

```go
// FederatedSearchRequest is the core-level federated search request: one query
// matched across a caller-supplied, non-empty set of Resource Types, returning
// a single relevance-ranked cross-type list (spec D2).
type FederatedSearchRequest struct {
	Query         string
	Resources     []string
	Filters       []Filter // global filters on root fields.* paths common to every Type (D7)
	Page          int32
	PageSize      int32
	IncludeSource bool

	// ResourceFilters are per-Type visibility filters, keyed by Resource Type
	// name — the channel a trusted federated middleware scopes each Type
	// through. They are core-API-only (never exposed over proto) and exempt
	// from strict request-time filter validation: the same trust model as
	// middleware-appended filters on the single-resource path. Reference-
	// relation paths are resolved via child searches (see
	// buildIndexFilterGroups); a Type with no entry gets an unfiltered group
	// (Type membership only). Denial is the middleware's concern: fail the
	// whole search with an error, or drop the Type from Resources to exclude
	// just it.
	ResourceFilters map[string][]Filter

	// SecondaryScope is the caller's single tenant scope value for the
	// secondary tier (spec D11.2, D14), set by a trusted federated middleware.
	// The query builder weaves it into the nested search_scoped clause. Empty
	// means unscoped secondary (standalone-app behaviour).
	SecondaryScope string
}
```

2. In `federatedSearchBase`, replace

```go
	// Collect each Type's visibility filters + the caller's secondary scope by
	// re-running its single-resource middleware chain in collect mode.
	groups, scope, err := idx.collectIndexFilterGroups(ctx, req.Resources, SearchRequest{Query: req.Query})
	if err != nil {
		return FederatedSearchResponse{}, err
	}
```

with

```go
	// Build each Type's visibility group from the per-Type filters a federated
	// middleware supplied on the request (ResourceFilters).
	groups, err := idx.buildIndexFilterGroups(ctx, req.Resources, req.ResourceFilters)
	if err != nil {
		return FederatedSearchResponse{}, err
	}
```

and in the `FederatedSearchParams` literal change `SecondaryScope: scope,` to `SecondaryScope: req.SecondaryScope,`.

3. Update `federatedSearchBase`'s doc comment: replace the sentence "per-Type document visibility is enforced by collecting each Type's middleware filters in-process (collectIndexFilterGroups) and combining them as per-index filter groups, not by fanning out one query per Type" with "per-Type document visibility is enforced by the per-Type filters a federated middleware supplies on the request (ResourceFilters), combined as per-index filter groups (buildIndexFilterGroups), not by fanning out one query per Type".

4. Update the `IndexFilterGroup` doc comment: replace "the visibility Filters harvested from its single-resource middleware chain" with "the visibility Filters a federated middleware supplied for it (FederatedSearchRequest.ResourceFilters, reference paths resolved)".

5. Replace `collectIndexFilterGroups` (and its entire doc comment) and delete `collectFilters` entirely, adding:

```go
// buildIndexFilterGroups builds a Federated Search's per-index visibility
// groups from the per-Type filters supplied on the request. For each requested
// Type it resolves reference-relation filter paths the same way the
// single-resource path does — a separate child search per reference, folded
// into a terms filter on the parent key (the referenced field is never mapped
// on the parent, so it cannot be a term on the multi-index query) — and tags
// the result with the Type's read alias. A reference that matches no children
// marks the group MatchNothing so that Type is excluded while other Types in
// the federation still return. A Type with no perType entry gets an unfiltered
// group (Type membership only).
//
// Nested-path derivation (deriveNestedPath) is deliberately not applied here,
// matching the former collect mode: per-Type filters targeting
// denormalized-many nested fields are unsupported on the federated path.
func (idx *Indexer) buildIndexFilterGroups(ctx context.Context, resources []string, perType map[string][]Filter) ([]IndexFilterGroup, error) {
	groups := make([]IndexFilterGroup, 0, len(resources))
	for _, name := range resources {
		r := idx.resources.Get(name)
		if r == nil {
			return nil, fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}

		resolved, matchedNothing, err := idx.resolveReferenceFilters(ctx, name, perType[name])
		if err != nil {
			return nil, fmt.Errorf("resolve reference filters for %q: %w", name, err)
		}

		group := IndexFilterGroup{Resource: name, Alias: AliasName(r.Resource)}
		if matchedNothing {
			group.MatchNothing = true
		} else {
			group.Filters = resolved
		}
		groups = append(groups, group)
	}
	return groups, nil
}
```

In `core/search_types.go`: delete the `SecondaryScope` field and its doc comment from `SearchRequest` (lines 50–57).

In `core/indexer.go`: delete the `searchMiddlewares` struct field with its comment (lines 75–79) and the assignment `idx.searchMiddlewares = cfg.SearchMiddlewares` in `New`.

- [ ] **Step 4: Run the core unit suite**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... -v`
Expected: PASS — all new tests green, no remaining references to collect mode or `SearchRequest.SecondaryScope`. If anything still references them, the compiler will list it; fix those sites (they should only be within the files above).

- [ ] **Step 5: Run the full workspace unit suite**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... ./app/server/... ./app/dsl/... github.com/theleeeo/laika/backend/elasticsearch/...`
(The backend module must be named by module path — `./backend/...` matches no packages because it is a nested module.)
Expected: PASS. (`backend/elasticsearch` only reads `FederatedSearchParams.SecondaryScope`, which is unchanged.) Note: `./app/tests/...` will FAIL to compile at this point — its middleware usages are rewritten in Task 3; do not run it yet.

- [ ] **Step 6: Commit (laika repo)**

```bash
git add core/federated_search.go core/federated_search_test.go core/search_types.go core/indexer.go
git commit -m "feat(core)!: per-Type filters on FederatedSearchRequest; remove collect-mode middleware replay"
```

---

### Task 3: Rewrite federated integration tests (laika repo)

The two integration tests that drove per-Type visibility and secondary scope through single-resource middleware now use federated middleware.

**Files:**
- Modify: `app/tests/federated_test.go`

**Interfaces:**
- Consumes: `core.FederatedSearchMiddleware`, `core.FederatedSearchHandler`, `FederatedSearchRequest.ResourceFilters`/`.SecondaryScope` (Tasks 1–2), existing `TestSuite` helpers (`indexRaw`, `setResourceConfig`), `elasticsearch.New`.
- Produces: nothing new — proves end-to-end parity on real ES.

- [ ] **Step 1: Rewrite the middleware wiring in both tests**

In `app/tests/federated_test.go`:

1. In `Test_FederatedSearch_ResolvesReferenceRelationFilter`, replace the `scopeFor` closure with:

```go
	// scopeFor builds an indexer whose federated middleware scopes each Type to
	// a fiber operator the way authz does: population by its own root field,
	// access-point through the referenced population's field.
	scopeFor := func(operator string) *core.Indexer {
		mw := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
			return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
				req.ResourceFilters = map[string][]core.Filter{
					"pop": {{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: operator}},
					"ap":  {{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: operator}},
				}
				return next(ctx, req)
			}
		}
		return core.New(core.Config{
			Resources:                  referenceScopeConfig,
			ES:                         elasticsearch.New(t.esClient, true),
			FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{mw},
		})
	}
```

2. In the same test's subtest ("a reference matching no children excludes only that Type"), replace its `mw` and `idx` with:

```go
		mw := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
			return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
				req.ResourceFilters = map[string][]core.Filter{
					"pop": {{Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: "op-A"}},
					"ap":  {{Field: "pop.fiber_operator_id", Op: core.FilterOpEq, Value: "op-none"}},
				}
				return next(ctx, req)
			}
		}
		idx := core.New(core.Config{
			Resources:                  referenceScopeConfig,
			ES:                         elasticsearch.New(t.esClient, true),
			FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{mw},
		})
```

3. In `Test_FederatedSearch_SecondaryScopeCorrelation`, replace `scopeMW` and `scopedIdx` with:

```go
	// A consumer federated middleware that supplies the caller's tenant scope
	// from context.
	scopeMW := func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
		return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
			if s, ok := ctx.Value(scopeCtxKey{}).(string); ok {
				req.SecondaryScope = s
			}
			return next(ctx, req)
		}
	}
	scopedIdx := core.New(core.Config{
		Resources:                  DefaultResourceConfig,
		ES:                         elasticsearch.New(t.esClient, true),
		FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{scopeMW},
	})
```

All assertions in both tests stay exactly as they are — behavior parity is the point.

- [ ] **Step 2: Run the integration suite (requires Docker)**

Run: `GOEXPERIMENT=jsonv2 go test ./app/tests/...`
Expected: PASS, including `Test_FederatedSearch_CrossTypeRankedAndCounts` (untouched — no middleware), both rewritten tests, and the rest of the suite.

- [ ] **Step 3: Commit (laika repo)**

```bash
git add app/tests/federated_test.go
git commit -m "test(app): drive federated visibility and scope through federated middleware"
```

---

### Task 4: Documentation — supersede collect mode (laika repo)

**Files:**
- Modify: `docs/adr/0007-federated-search-standardized-fields-and-secondary-tier-scoping.md`
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: the shipped mechanism names from Tasks 1–2 (`FederatedSearchMiddlewares`, `ResourceFilters`, `SecondaryScope`).
- Produces: docs matching the code; no code.

- [ ] **Step 1: Add a superseding note to ADR 0007**

At the top of the ADR, immediately after the title line, insert:

```markdown
> **Superseded in part (2026-07-06):** per-Type document visibility is no
> longer harvested by replaying the single-resource middleware chain
> ("collect mode", mechanism point 1 below). Federated Search now has its own
> middleware chain (`Config.FederatedSearchMiddlewares`); a federated
> middleware supplies per-Type filters via
> `FederatedSearchRequest.ResourceFilters` and the scope value via
> `FederatedSearchRequest.SecondaryScope`. Authorization is written once by
> the consumer as a shared policy evaluator feeding both chains. See
> [the federated-search-middleware spec](../superpowers/specs/2026-07-06-federated-search-middleware-design.md).
> Everything else in this ADR stands.
```

- [ ] **Step 2: Update CONTEXT.md's Federated Search vocabulary**

In `CONTEXT.md`:

1. In the Federated Search entry (the paragraph starting "A single query over a caller-supplied set of Resource Types…"), replace the sentence

   "Per-Type document visibility is enforced by running each Type's existing single-resource search middleware in a collect-only mode (enforcing access, harvesting its scope filters) and combining the results as per-index filter groups."

   with

   "Per-Type document visibility is enforced by per-Type filters that a federated search middleware supplies on the request, combined as per-index filter groups; the federated middleware chain is independent of the single-resource one."

2. In the secondary-tier entry (the paragraph describing `search_scoped`), replace "whose middleware also supplies the matching scope filter" with "whose federated middleware also supplies the caller's scope value on the request".

- [ ] **Step 3: Verify no stale references remain**

Run: `grep -rn -i "collect" docs/adr/ CONTEXT.md CLAUDE.md AGENTS.md`
Expected: the only hits are the ADR's historical description below the new superseding note (acceptable — ADRs keep their original text) and unrelated words. If `CLAUDE.md`/`AGENTS.md` mention collect mode, update those sentences the same way as CONTEXT.md.

- [ ] **Step 4: Commit (laika repo)**

```bash
git add docs/adr/0007-federated-search-standardized-fields-and-secondary-tier-scoping.md CONTEXT.md
git commit -m "docs: supersede collect-mode visibility with federated middleware"
```

---

### Task 5: Harness — shared policy evaluator + federated middleware (harness repo)

Extract `filtersFor` from the existing authz middleware and add `FederatedMiddleware`, both driven by the same `ResourcePolicy` map. All commands in this task run from the `harness/` directory with `GOWORK=off GOEXPERIMENT=jsonv2`.

**Files:**
- Modify: `authz/authz.go`
- Test: `authz/authz_test.go`

**Interfaces:**
- Consumes: laika's `core.FederatedSearchMiddleware`, `core.FederatedSearchHandler`, `FederatedSearchRequest.ResourceFilters` (Tasks 1–2, via the `replace => ../laika` directive); existing `ResourcePolicy`, `Middleware`, test helpers (`testPolicies`, `fiberOperatorCtx`, `newEnforcer`, `assertHasFilter`).
- Produces:
  - unexported `filtersFor(ctx context.Context, enf *enforcer.Enforcer, policies map[string]ResourcePolicy, resource string) (context.Context, []core.Filter, error)`
  - `FederatedMiddleware(enf *enforcer.Enforcer, policies map[string]ResourcePolicy) core.FederatedSearchMiddleware` (Task 6 wires it)

- [ ] **Step 1: Write the failing tests**

Append to `authz/authz_test.go`:

```go
func TestFederatedMiddleware_FillsPerTypeFilters(t *testing.T) {
	mc := &mocks.EnforcerClient{}
	mc.On("Enforce", mock.Anything, PopulationEnforceObject, accesspb.Action_ACTION_READ).
		Return(&iampb.EnforceResponse{}, nil)
	mc.On("Enforce", mock.Anything, AccessPointEnforceObject, accesspb.Action_ACTION_READ).
		Return(&iampb.EnforceResponse{}, nil)

	var got core.FederatedSearchRequest
	next := func(_ context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
		got = req
		return core.FederatedSearchResponse{}, nil
	}

	h := FederatedMiddleware(newEnforcer(t, mc), testPolicies)(next)
	_, err := h(fiberOperatorCtx("op-1"), core.FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"population", "access-point"},
	})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	want := map[string]core.Filter{
		"population":   {Field: "fields.fiber_operator_id", Op: core.FilterOpEq, Value: "op-1"},
		"access-point": {Field: "population.fiber_operator_id", Op: core.FilterOpEq, Value: "op-1"},
	}
	if len(got.ResourceFilters) != len(want) {
		t.Fatalf("ResourceFilters = %+v, want entries for %v", got.ResourceFilters, want)
	}
	for res, wf := range want {
		fs := got.ResourceFilters[res]
		if len(fs) != 1 || fs[0] != wf {
			t.Errorf("ResourceFilters[%q] = %+v, want [%+v]", res, fs, wf)
		}
	}
}

// A denial for ANY requested Type fails the whole federated search closed,
// matching the single-resource middleware's behavior.
func TestFederatedMiddleware_DenialFailsClosed(t *testing.T) {
	mc := &mocks.EnforcerClient{}
	mc.On("Enforce", mock.Anything, PopulationEnforceObject, accesspb.Action_ACTION_READ).
		Return(&iampb.EnforceResponse{}, nil)
	mc.On("Enforce", mock.Anything, AccessPointEnforceObject, accesspb.Action_ACTION_READ).
		Return((*iampb.EnforceResponse)(nil), vxerr.New(vxerr.Forbidden, "denied"))

	nextCalled := false
	next := func(context.Context, core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
		nextCalled = true
		return core.FederatedSearchResponse{}, nil
	}

	h := FederatedMiddleware(newEnforcer(t, mc), testPolicies)(next)
	_, err := h(fiberOperatorCtx("op-1"), core.FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"population", "access-point"},
	})
	if err == nil {
		t.Fatal("expected denial to fail the whole federated search, got nil")
	}
	if nextCalled {
		t.Fatal("next must not be called on denial")
	}
}

func TestFederatedMiddleware_UnmappedResourceDenied(t *testing.T) {
	nextCalled := false
	next := func(context.Context, core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
		nextCalled = true
		return core.FederatedSearchResponse{}, nil
	}

	h := FederatedMiddleware(newEnforcer(t, &mocks.EnforcerClient{}), testPolicies)(next)
	_, err := h(fiberOperatorCtx("op-1"), core.FederatedSearchRequest{
		Query:     "q",
		Resources: []string{"population", "does-not-exist"},
	})
	if err == nil {
		t.Fatal("expected error for unmapped resource, got nil")
	}
	if nextCalled {
		t.Fatal("next must not be called on denial")
	}
}

// Both middlewares must derive identical filters for the same (caller,
// resource) pair — they share the policy evaluator.
func TestMiddlewares_DeriveIdenticalFilters(t *testing.T) {
	for _, res := range []string{"population", "access-point"} {
		mc := &mocks.EnforcerClient{}
		mc.On("Enforce", mock.Anything, mock.Anything, accesspb.Action_ACTION_READ).
			Return(&iampb.EnforceResponse{}, nil)
		enf := newEnforcer(t, mc)

		var single core.SearchRequest
		sh := Middleware(enf, testPolicies)(func(_ context.Context, req core.SearchRequest) (core.SearchResponse, error) {
			single = req
			return core.SearchResponse{}, nil
		})
		if _, err := sh(fiberOperatorCtx("op-7"), core.SearchRequest{Resource: res}); err != nil {
			t.Fatalf("%s: single-resource handler: %v", res, err)
		}

		var federated core.FederatedSearchRequest
		fh := FederatedMiddleware(enf, testPolicies)(func(_ context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
			federated = req
			return core.FederatedSearchResponse{}, nil
		})
		if _, err := fh(fiberOperatorCtx("op-7"), core.FederatedSearchRequest{Resources: []string{res}}); err != nil {
			t.Fatalf("%s: federated handler: %v", res, err)
		}

		if len(single.Filters) != 1 || len(federated.ResourceFilters[res]) != 1 ||
			single.Filters[0] != federated.ResourceFilters[res][0] {
			t.Errorf("%s: filters diverge: single=%+v federated=%+v",
				res, single.Filters, federated.ResourceFilters[res])
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run (from `harness/`): `GOWORK=off GOEXPERIMENT=jsonv2 go test ./authz/ -run 'TestFederatedMiddleware|TestMiddlewares_' -v`
Expected: compile FAIL — `undefined: FederatedMiddleware`.

- [ ] **Step 3: Extract filtersFor and implement FederatedMiddleware**

In `authz/authz.go`, add below the `ResourcePolicy` type:

```go
// filtersFor is the shared policy evaluator behind both search middlewares:
// it enforces the resource's policy against vx-bouncer and derives the
// caller's scoping filters. It fails closed: an unmapped resource, an
// enforcer denial, or an actor type absent from the policy's Filters map all
// return an error. The returned context is identity-enriched by Enforce and
// must be used for the remainder of the request.
func filtersFor(ctx context.Context, enf *enforcer.Enforcer, policies map[string]ResourcePolicy, resource string) (context.Context, []core.Filter, error) {
	policy, ok := policies[resource]
	if !ok {
		return ctx, nil, vxerr.New(vxerr.Forbidden,
			"no authorization policy for resource "+resource)
	}

	// Enforce reads auth + actor from ctx, decides object+action for the
	// actor, and returns an identity-enriched ctx we pass downstream.
	ctx, err := enf.Enforce(ctx, policy.Object, accesspb.Action_ACTION_READ)
	if err != nil {
		return ctx, nil, err
	}

	core.LoggerFromContext(ctx).Debug("authz enforced",
		slog.String("object", policy.Object),
		slog.String("action", "read"),
	)

	ac := auth.GetActor(ctx)
	field, ok := policy.Filters[ac.Type]
	if !ok {
		return ctx, nil, vxerr.New(vxerr.Forbidden,
			"actor type not authorized for resource "+resource)
	}

	core.LoggerFromContext(ctx).Debug("authz actor filter derived",
		slog.String("actor_id", ac.Id),
		slog.String("field", field),
	)

	return ctx, []core.Filter{{Field: field, Op: core.FilterOpEq, Value: ac.Id}}, nil
}
```

Replace the body of `Middleware` so it delegates:

```go
// Middleware enforces each single-resource search against vx-bouncer and
// appends the caller's actor filter. It fails closed (see filtersFor).
func Middleware(enf *enforcer.Enforcer, policies map[string]ResourcePolicy) core.SearchMiddleware {
	return func(next core.SearchHandler) core.SearchHandler {
		return func(ctx context.Context, req core.SearchRequest) (core.SearchResponse, error) {
			ctx, filters, err := filtersFor(ctx, enf, policies, req.Resource)
			if err != nil {
				return core.SearchResponse{}, err
			}
			req.Filters = append(req.Filters, filters...)
			return next(ctx, req)
		}
	}
}
```

Add `FederatedMiddleware`:

```go
// FederatedMiddleware enforces every requested Type of a federated search and
// fills the request's per-Type visibility filters (ResourceFilters) from the
// same policy evaluator as Middleware, so both search paths derive identical
// filters for a caller. It fails closed: a denial for ANY requested Type
// fails the whole federated search, matching Middleware's behavior.
func FederatedMiddleware(enf *enforcer.Enforcer, policies map[string]ResourcePolicy) core.FederatedSearchMiddleware {
	return func(next core.FederatedSearchHandler) core.FederatedSearchHandler {
		return func(ctx context.Context, req core.FederatedSearchRequest) (core.FederatedSearchResponse, error) {
			perType := make(map[string][]core.Filter, len(req.Resources))
			for _, res := range req.Resources {
				var (
					filters []core.Filter
					err     error
				)
				ctx, filters, err = filtersFor(ctx, enf, policies, res)
				if err != nil {
					return core.FederatedSearchResponse{}, err
				}
				perType[res] = filters
			}
			req.ResourceFilters = perType
			return next(ctx, req)
		}
	}
}
```

Also update the package doc comment's first sentence to reflect both paths: "Package authz enforces inbound search requests — single-resource and federated — against vx-bouncer and scopes results to the caller's actor."

- [ ] **Step 4: Run the authz tests**

Run (from `harness/`): `GOWORK=off GOEXPERIMENT=jsonv2 go test ./authz/ -v`
Expected: PASS — all five pre-existing `TestMiddleware_*` tests (behavior unchanged by the extraction) plus the four new ones.

- [ ] **Step 5: Commit (harness repo)**

```bash
git add authz/authz.go authz/authz_test.go
git commit -m "feat(authz): shared policy evaluator feeding single-resource and federated middlewares"
```

---

### Task 6: Harness — wire the federated middleware (harness repo)

**Files:**
- Modify: `main.go` (the `core.New(core.Config{...})` literal, around line 155)

**Interfaces:**
- Consumes: `authz.FederatedMiddleware` (Task 5), `core.Config.FederatedSearchMiddlewares` (Task 1).
- Produces: a harness where federated searches are enforced and scoped end-to-end.

- [ ] **Step 1: Wire FederatedSearchMiddlewares in main.go**

In `main.go`, extend the `core.New` config literal:

```go
	idx := core.New(core.Config{
		Plans:     plans,
		Resources: resources,
		ES:        esClientImpl,
		Store:     st,
		// Enforce every search against vx-bouncer by resource, then scope
		// results to the caller's actor. Both chains are fed from the same
		// policy map: the single-resource middleware appends the caller's
		// filters, the federated one fills per-Type ResourceFilters. The HTTP
		// server middleware below populates the caller's auth + actor on the
		// request context.
		SearchMiddlewares: []core.SearchMiddleware{
			authz.Middleware(enf, policies),
		},
		FederatedSearchMiddlewares: []core.FederatedSearchMiddleware{
			authz.FederatedMiddleware(enf, policies),
		},
	})
```

- [ ] **Step 2: Build and run the full harness test suite**

Run (from `harness/`): `GOWORK=off GOEXPERIMENT=jsonv2 go build ./... && GOWORK=off GOEXPERIMENT=jsonv2 go test ./...`
Expected: build OK, all tests PASS (searchsvc tests construct their own `core.Config` without middlewares, so federated behavior there is unchanged).

- [ ] **Step 3: Commit (harness repo)**

```bash
git add main.go
git commit -m "feat: enforce federated search via authz.FederatedMiddleware"
```
