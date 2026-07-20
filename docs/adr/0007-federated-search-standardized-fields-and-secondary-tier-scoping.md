# Federated Search rides standardized, Build-time searchable fields; tenancy scoping lives only in the secondary tier and only in library consumers

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

> **Renamed (2026-07-20):** the two standardized surfaces are now named by tier —
> `search` → `search_primary` and `search_scoped` → `search_secondary` — so the
> field names carry the primary/secondary distinction (the reason the split
> exists: a primary match outranks a secondary one) instead of welding
> "secondary" to "scoped". Scoping is an orthogonal capability the secondary tier
> still carries — exercised only by a library consumer, never the standalone app,
> which leaves `scope` empty. Single-resource Search remains primary-only. The
> field names in this ADR are updated to match; the design is otherwise unchanged.

Single-resource Search names one Resource Type and runs a `multi_match` over that
Type's own field set. A search bar needs one query matched across *several* Types
at once, returning a single relevance-ranked cross-type list. Merging raw scores
from per-Type queries over heterogeneous field sets does not produce a trustworthy
ranking (BM25 term statistics and field sets differ per index), and the existing
per-Type `reference`-relation joins and authz middleware do not generalize to a
heterogeneous query for free.

We give **every** index the same two searchable surfaces, populated by the indexer
at **Build time**, and query them with a single multi-index request:

- `search_primary` — flat `text`. The **primary** tier: a Document's own high-signal text.
- `search_secondary` — a `nested` array of `{ text, scope[] }`. The **secondary**
  tier: lower-signal / denormalized-child text, each entry optionally attributed
  to the tenant set that may see it.

Because the fields are identical across indices (same paths, same n-gram
analyzer), a federated query is a uniform `match`, and `dfs_query_then_fetch`
makes cross-type scores genuinely comparable. A per-field `search: primary |
secondary | none` selector drives which surface each field feeds, and the tiers
carry a query-time boost so a Document matched on its own name outranks one matched
only via a related Resource's name.

## Why not the alternatives

- **Fan-out per Type and merge in the client.** Reuses single-resource Search
  wholesale and keeps per-Type authz clean, but merges scores that are only
  approximately comparable and pays an N× over-fetch at pagination depth. Kept as a
  documented *future* execution swap (the standardized fields make it a drop-in),
  not the v1 path. Single-query wins on native pagination and global term stats.
- **Elasticsearch `copy_to` to populate the blobs.** Zero indexer code, but
  `copy_to` cannot cross a `nested` scope — so `many`-relation child text could
  never reach the searchable surface — and it is not recursive. Build-time
  population sits in the denormalization engine that already holds every child
  value in hand, and sidesteps both limits.
- **A flat secondary blob (no scope attribution).** Simpler mapping, but a Parent
  shared across tenants would let a tenant match a tenant-specific Child's text it
  cannot see. Rejected: field-level cross-tenant leakage is a correctness bug, not
  a tuning matter.
- **Bake tenancy into core / the standalone app.** Contradicts
  [ADR 0001](./0001-core-is-a-library-the-app-is-one-assembly.md). Core stays
  policy-free; scoping is a library-consumer concern.

## The tenancy boundary

The multi-tenant leak is confined to the secondary tier. A Document's own (primary)
fields are never narrower in visibility than the Document itself, so once a searcher
may see the Document at all, its primary text is fair game — no attribution needed.
Only denormalized-child text can be narrower, so only `search_secondary` entries carry
a `scope[]`.

This lets us keep **all** tenant policy out of the standalone app:

- The **standalone app** fills each `search_secondary` entry's `text` (from
  `search: secondary` fields) and leaves `scope` **empty** — its secondary matches
  are unscoped, and it stays tenant-agnostic.
- A **library consumer** fills `scope` (via its own Plan) and, at query time,
  supplies the caller's single tenant value, which the query builder weaves into
  the nested clause: an entry matches only when `search_secondary.scope` contains the
  caller *and* its `text` matches. The mapping and the query are identical in both
  modes — only the presence of scope data and the scope value differ.

## Single-query scoping mechanism

Two consumer-supplied inputs reach the one ES query:

1. **Per-Type document visibility → per-`_index` filter groups.** The federated
   handler runs each requested Type's *existing single-resource middleware chain*
   in a collect-only mode: a terminal handler that still lets enforcement
   fail-closed per Type but captures the appended filters instead of hitting ES.
   The builder combines them as `should[ {_index:a ∧ a-filters}, … ]` with
   `minimum_should_match: 1`. This is still one ES query — the per-Type work is
   in-process filter collection, not fan-out — and it reuses the consumer's authz
   middleware verbatim, so authz is written once and serves both search paths.
2. **The correlated secondary scope value**, woven into the nested clause as
   above. This is deliberately *not* a plain top-level filter: a separate nested
   filter would not correlate the matched text with the scope of the entry it
   matched, defeating the isolation.

## Status

Design decision only. Implementation — the `search` tier DSL selector, the n-gram
mapping and standardized fields, Build-time population, the `FederatedSearch` RPC
and core execution, collect-mode middleware reuse, the secondary scope-value
channel, and the `dev/harness` consumer changes (scope population + tenant scope
value) — is tracked as separate follow-up issues. Full design in
[docs/superpowers/specs/2026-07-04-federated-search.md](../superpowers/specs/2026-07-04-federated-search.md).

**Implication for contributors:** do not populate `search_secondary.scope` or apply a
tenant filter in `core` or the standalone app — that is library-consumer policy.
Do not model the secondary scope as an ordinary top-level filter; it must be
correlated inside the nested clause. Keep primary text unattributed.
