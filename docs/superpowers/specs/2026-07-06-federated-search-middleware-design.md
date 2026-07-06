# Federated Search Middleware — Design

Date: 2026-07-06
Status: approved for implementation

## Goal

Give Federated Search its own consumer middleware chain, separate from the
single-resource search chain, and remove the collect-mode replay mechanism.
Core stays auth-blind: it offers two independent middleware seams, and the
consumer (the harness) implements authorization for both paths on top of one
shared resource→policy evaluator, so the filter-determination logic is written
once and wired into both middlewares.

## Non-goals

- **No policy concept in core.** Authorization stays entirely in the consumer.
  Core never learns what an actor, tenant, or enforce object is.
- **No proto/API surface changes.** The new request fields
  (`ResourceFilters`, federated `SecondaryScope`) are core-API-only, set by
  trusted middleware — never exposed over `search/v1` or the harness proto.
- **No cross-path replay.** Single-resource `SearchMiddlewares` never run on
  the federated path again, and vice versa.

## Background

Today `core.Indexer` has one middleware seam: `Config.SearchMiddlewares`
wraps the single-resource search path. Federated Search has no seam of its
own; instead it *replays* the single-resource middleware chain per requested
Type in collect-only mode (`collectFilters` / `collectIndexFilterGroups`,
ADR 0007) to harvest each Type's visibility filters and the caller's
secondary scope.

That replay couples the two paths: a middleware written for single-resource
search implicitly runs (against a terminal stub) inside every federated
search, cannot tell which path it is on, and cannot shape or observe the
federated request/response as a whole.

## Design

### Core: two independent middleware chains

New types in `core/search_middleware.go`, mirroring the existing pair:

```go
type FederatedSearchHandler func(ctx context.Context, req FederatedSearchRequest) (FederatedSearchResponse, error)

// FederatedSearchMiddleware wraps a FederatedSearchHandler. Middlewares run
// outermost-first in registration order.
type FederatedSearchMiddleware func(next FederatedSearchHandler) FederatedSearchHandler
```

- `Config` gains `FederatedSearchMiddlewares []FederatedSearchMiddleware`.
- The chain is composed once in `New` into `idx.federatedSearchChain`,
  wrapping `federatedSearchBase` — the current `FederatedSearch` body in
  full, **including all validation** (empty-resources check, root-only global
  filter check, unknown-resource check, per-Type filter validation). An
  observing middleware therefore sees validation failures too.
- Consequence of validation-in-base: a middleware-injected *global* filter is
  validated like a caller's (root `fields.*`, valid on every requested Type).
  Per-Type `ResourceFilters` are exempt (trusted channel, below).
- The unexported chain composer becomes generic —
  `chain[H any](base H, mws []func(H) H) H` — shared by both paths. Exported
  types stay concrete.
- No internal middlewares on the federated chain; query mechanics live in the
  base (group building, reference resolution).

### Core: per-Type filter channel on the federated request

Per-Type visibility filters differ by Type (e.g. population is scoped by
`fields.fiber_operator_id`, access-point through
`population.fiber_operator_id`), which the global root-only `Filters` cannot
express. The federated request gains the channel collect mode used to fill
implicitly:

```go
type FederatedSearchRequest struct {
    // ...existing fields...

    // ResourceFilters are per-Type visibility filters, keyed by Resource
    // Type name. Set by trusted federated middleware; never exposed over
    // proto; exempt from strict request-time filter validation (same trust
    // model as middleware-appended filters on the single-resource path).
    ResourceFilters map[string][]Filter

    // SecondaryScope is the caller's single tenant scope value for the
    // secondary tier (spec D11.2, D14), set by trusted federated middleware.
    // Empty means unscoped secondary.
    SecondaryScope string
}
```

The base handler builds `IndexFilterGroup`s directly from
`req.ResourceFilters`: for each requested Type it runs
`resolveReferenceFilters` on that Type's entry (reference-relation paths keep
working exactly as under collect mode, including `MatchNothing` when a
reference filter matches zero children) and tags the result with the Type's
read alias. `req.SecondaryScope` passes straight into
`FederatedSearchParams.SecondaryScope`. A Type with no `ResourceFilters`
entry gets an unfiltered group (Type membership only), matching today's
no-middleware behavior.

Parity note: collect mode never applied `deriveNestedPath` to harvested
filters, and this design keeps that behavior — per-Type filters targeting
denormalized-many nested fields remain unsupported on the federated path.

### Core: collect mode is deleted

- `collectFilters` is removed.
- `collectIndexFilterGroups` loses its middleware-replay half and becomes the
  group builder described above (reads the request instead of running a
  chain).
- `Indexer.searchMiddlewares` (retained only for replay) is removed;
  `Config.SearchMiddlewares` now feeds only the composed single-resource
  chain.
- `SearchRequest.SecondaryScope` is removed — it existed solely as the
  collect-mode harvesting channel (single-resource search always ignored it);
  the field moves to `FederatedSearchRequest`.
- ADR 0007 gets a superseding note on its collect-mode decision, pointing at
  this spec: per-Type visibility is now supplied by federated middleware via
  `ResourceFilters`, not harvested by replaying the single-resource chain.

### Denial semantics stay consumer-owned

Core cannot classify a middleware error as "denied" versus "failed", so any
error from the federated chain fails the whole Federated Search closed. A
consumer middleware that wants a forbidden Type merely *excluded* (other
Types still return) removes it from `req.Resources` before calling next —
the response's `Counts` then omit that Type. Core stays policy-free.

### Harness: one policy evaluator, two thin middlewares

The existing `authz.ResourcePolicy` map (enforce object + per-actor-type
filter field) stays the single source of truth. The enforce-and-derive logic
is extracted from the current middleware into a shared evaluator:

```go
// filtersFor enforces the resource's policy against vx-bouncer and returns
// the actor-scoping filters. Fails closed: unmapped resource, enforcer
// denial, or unmapped actor type all error.
func filtersFor(ctx context.Context, enf *enforcer.Enforcer,
    policies map[string]ResourcePolicy, resource string,
) (context.Context, []core.Filter, error)
```

Two thin wrappers call it:

- `Middleware(enf, policies) core.SearchMiddleware` — the existing one,
  reduced to: evaluate, append filters to `req.Filters`, next.
- `FederatedMiddleware(enf, policies) core.FederatedSearchMiddleware` — new:
  loop `req.Resources`, evaluate each, fill `req.ResourceFilters[type]`,
  next. Denial handling (fail closed vs. drop the Type) is decided here, in
  the harness; initial behavior: fail closed, matching the single-resource
  middleware.

Both middlewares derive identical filters for a given (caller, resource)
pair because they share `filtersFor`.

### Safety consequence (explicit)

Deleting collect mode means registering only `SearchMiddlewares` leaves the
federated path **unfiltered**. An app enforcing auth must register both
middlewares. This is the accepted model: each path is explicitly owned. The
harness wires both in `main.go` from the same policy map.

## Testing

Core unit tests (`core/`, no Docker):

- Federated chain: registration order, request mutation (narrowing
  `Resources`, injecting global filters and `ResourceFilters`),
  short-circuit error, response mutation, and observing a base validation
  error through the chain.
- Group building: `ResourceFilters` with plain and reference-relation paths
  produce the expected `IndexFilterGroup`s (including `MatchNothing` on a
  zero-child reference); `SecondaryScope` pass-through; Types without
  entries get unfiltered groups.
- Existing collect-mode tests are rewritten against the new request-driven
  mechanism.

Integration tests (`app/tests/`, Docker): the existing federated
authz-scoping scenarios re-expressed with a federated middleware setting
`ResourceFilters` instead of relying on replayed single-resource middleware.

Harness tests: both middlewares produce identical filters from one policy
map; federated middleware fails closed on unmapped resource/actor.

## Implementation order

1. Core: generic chain + federated middleware types + `Config` field +
   composition in `New` (chain around the existing body; no behavior change
   yet).
2. Core: `ResourceFilters`/`SecondaryScope` on `FederatedSearchRequest`;
   group builder reads the request; delete collect mode; move
   `SecondaryScope` off `SearchRequest`; update tests.
3. ADR 0007 superseding note.
4. Harness: extract `filtersFor`, add `FederatedMiddleware`, wire both in
   `main.go`, tests.
