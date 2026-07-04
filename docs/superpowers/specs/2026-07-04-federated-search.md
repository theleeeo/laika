# Federated Search

**Status:** Design — approved; implementation tracked in follow-up issues.
**Date:** 2026-07-04
**ADR:** [0007](../../adr/0007-federated-search-standardized-fields-and-secondary-tier-scoping.md)

## Problem

Today's Search is single-resource: `SearchRequest.Resource` is required, and the
full-text query is a `multi_match` over that one Type's searchable fields
(`backend/elasticsearch/search.go`). A search-bar feature needs the opposite: one
query matched across *several* Resource Types at once, returning a single
relevance-ranked list whose top hit is "the most relevant one."

The core tension: the single-resource path (per-field `multi_match`, per-resource
reference-relation joins, per-resource authz middleware) does not generalize to a
heterogeneous cross-type query for free. This spec defines a path that reuses what
works and adds only what's needed.

## Vocabulary

Added to CONTEXT.md: **Federated Search**, **Searchable Tier**, **Scoped
Searchable Text** (see the Search section there). Existing **Search** renamed in
prose to "single-resource Search."

## Decisions

### D1 — Result shape: ranked list across types
Return top-N heterogeneous hits, each tagged with its Resource Type and score,
ordered best-first. "The most relevant one" is `hits[0]`. Cross-type scores are
not naively comparable, so committing to a single winner would be dishonest;
a ranked list is strictly more general.

### D2 — Resource set: caller-supplied, required, non-empty
The caller passes the set of Types to span (it knows the UI context). Empty set
is an `InvalidArgument` error, **not** "all" — federated-over-everything would
silently expose every newly-added Type. No server-side allow-list for v1.

### D3 — Execution: single multi-index query (now); fan-out later
One `_search` across the caller's aliases. Fan-out-and-merge is a **documented
future improvement** to revisit for deep pagination and very high Type counts;
for the search-bar shape (shallow paging, few Types) single-query and fan-out are
near-equal latency, and single-query paginates natively.

### D4 — Standardized searchable fields, populated at Build time
Every index carries the same two searchable surfaces so a federated query is a
uniform `match` and scores are comparable:

- `search` — flat `text`. The **primary** tier: a Document's own high-signal
  text (e.g. its name).
- `search_scoped` — `nested` array of `{ text, scope }`. The **secondary** tier:
  lower-signal / denormalized-child text, optionally attributed to a visibility
  scope.

The indexer **populates these at Build time** (not via ES `copy_to`). `copy_to`
was rejected because it cannot cross a `nested` scope (so `many`-relation child
text could never reach the blob) and is not recursive. Build-time population sits
naturally in the existing denormalization engine, which already holds every child
value in hand, and sidesteps every `copy_to` limitation.

### D5 — `search: primary | secondary | none`, omitted = none (BREAKING)
The per-field `query.search` config becomes a tier selector and the single source
of truth for full-text searchability. **Omitted now means `none`** (previously
`true`), so existing configs must add `search: primary` to every field they want
searchable — a one-time, explicit opt-in migration. Single-resource Search treats
`primary` and `secondary` identically (both feed its existing `multi_match`);
Federated Search routes them to the two standardized fields.

### D6 — Reference-relation fields excluded from Federated Search
Fields reachable only through a `reference` Relation store no child data on the
parent (only the join key), so they cannot be in the parent's Build. They do
**not** contribute to Federated Search ranking. They remain fully searchable via
single-resource Search (two-phase join in `core/reference_search.go`). Documented
boundary; `reference` is the rare advanced strategy for low-count children.

### D7 — Filters: global, common-field only, validated loudly
Federated Search accepts structured filters only on root `fields.*` paths, and
validates up front that **every** requested Type has that field (via the
capabilities data). A filter on a field missing from any requested Type is a loud
`InvalidArgument`, not a silent drop. This covers multi-tenant scoping and shared
dimensions without per-type complexity. (System is multi-tenant.)

### D8 — Tenancy is a library-consumer concern, never in the standalone app
Per ADR 0001 and the existing middleware split: `core` knows only
`SearchMiddleware`/`AddFilter`; the standalone app (`app/cmd/indexer`) wires
`core.New` with zero middlewares (tenant-agnostic); the library consumer (e.g.
`dev/harness`) injects `authz.Middleware`, which enforces per-resource and appends
a scope filter. Federated Search preserves this boundary.

### D9 — Tenant model: multi-valued tenant list, leak confined to secondary
A shared Document carries a *set* of tenants it's visible to; a Child may be
narrower. The leak — a shared Parent's blob letting a tenant match a Child's text
it can't see — exists **only in the secondary tier**. Primary (own) fields are
never narrower in visibility than the Document itself, so they need no
attribution; they are matchable whenever the searcher may see the Document.

### D10 — Secondary tier is scope-attributed nested; app fills text, consumer fills scope
`search_scoped` entries carry `{ text, scope }`. The **standalone app fills
`text`** (from `search: secondary` DSL fields) and leaves `scope` **empty**, so
its secondary matches are unscoped (tenant-agnostic). A **library consumer fills
`scope`** (via its own Plan) and supplies the matching scope value (via
middleware). Structure and query are identical in both modes — only presence of
scope + filter differs. In library mode the consumer builds the field itself.

### D11 — Single-query scoping mechanism
Two things reach the one ES query, both supplied by the consumer:

1. **Per-resource document visibility → per-`_index` filter groups.** The
   federated handler runs each requested Type's *existing single-resource
   middleware chain* in a **collect-only mode**: a terminal handler that lets
   `enf.Enforce` fail-closed per Type and captures the appended filters instead of
   hitting ES. The builder assembles:
   ```
   must:   <text match: search^3 OR nested(search_scoped)>
   filter: should[ {_index:a ∧ <a's filters>}, {_index:b ∧ <b's filters>} ]
           minimum_should_match: 1
   ```
   Still **one ES query** (per-resource work is in-process filter collection, not
   fan-out). Reuses `authz.Middleware` verbatim — authz written once serves both
   search paths.

2. **Correlated secondary scope.** The secondary tier must match text *only inside
   entries the caller may see*: `match(text, q)` AND `scope contains caller` must
   hold **within the same nested entry**. This is not a plain top-level filter (a
   separate nested filter wouldn't correlate text and scope). The federated request
   gets a dedicated channel for the consumer's middleware to set the caller's
   **scope value(s)**, which the builder weaves into the nested clause.

### D12 — Response shape
`FederatedSearchResponse`:
- `hits`: `FederatedHit { resource, id, score, source }`. Keyed by
  (`resource`, `id`) — `id` alone is not globally unique.
- `total`: merged `int64`.
- **per-resource counts**: included from v1 (a `terms` aggregation on `_index` /
  a resource-type field in the same query; no extra round trip). Maps index →
  Type.
- `page` / `page_size`: reused, same 25-default / 100-cap normalization as
  `core/search.go`.
- `include_source`: reused; a search bar often needs only resource/id/score.

### D13 — Score comparability: `dfs_query_then_fetch` + query-time boost
- Use **`dfs_query_then_fetch`** so BM25 uses *global* IDF across all queried
  indices, making cross-type scores genuinely comparable.
- Primary-over-secondary boost expressed at **query time** (`search^3`,
  `search_scoped` unboosted), combined in a `bool.should` with
  `minimum_should_match: 1`. Boost factor is a tunable knob; mechanism is
  query-time so no reindex to tune.

> **Trade-off to revisit — `dfs_query_then_fetch`.** DFS adds a pre-pass round
> across shards to gather global term statistics; it costs extra latency and does
> not scale as gracefully under high query volume or many shards. We accept it for
> v1 because cross-type comparability is the whole point of the feature and the
> cost is negligible at search-bar scale. **Future experiment:** measure federated
> ranking quality *without* DFS (plain `query_then_fetch`) — with standardized
> fields + identical analyzers, per-index IDF skew may be small enough that DFS is
> unnecessary. Flipping `search_type` is a one-line change, hence not an ADR.

### D14 — Secondary scope: single-value caller, array-valued entry
The caller is always on exactly **one** tenant, so the request carries a single
scope value. Each `search_scoped` entry carries a `keyword` **array** (a Child may
be visible to several tenants). The nested clause is
`term { search_scoped.scope: <caller> } AND match(search_scoped.text, q)` — a
`term` on a keyword array matches when the array contains the caller's value. No
array-caller / set-intersection path needed.

### D15 — Substring matching via n-grams (strict requirement)
Typeahead is a launch requirement, and matching the *middle*/*suffix* of
compound name forms (`prefix-middle-suffix`) is required — so edge-ngrams
(prefix-only) are insufficient. `search` and `search_scoped.text` use:
- **Index-time**: `ngram` tokenizer, `token_chars: [letter, digit]` (grams stay
  within a component, never bridging a hyphen/space), `min_gram: 2`,
  `max_gram: 3`, `lowercase` filter. Requires raising index `max_ngram_diff`.
- **Search-time**: standard + `lowercase`, **not** n-grammed — the query term is
  matched against indexed grams, not itself shredded (avoids false-match noise).
- **Precision layer** (for "most relevant", not just any substring hit): each is
  a multi-field with a `.full` standard-analyzed subfield. Query boosts
  whole-token/exact above incidental infix:
  `search.full^9, search^3` (primary), `search_scoped.text.full^3,
  search_scoped.text^1` (secondary). Factors tunable.

> **Trade-off — n-gram index size.** Indexing every 2–3 char substring materially
> grows the index; `min_gram`/`max_gram` are reindex-to-change. Accepted for
> name-like fields; measure and tune.

### D16 — Proto: new RPC on existing `SearchService`
Add `rpc FederatedSearch(FederatedSearchRequest) returns
(FederatedSearchResponse)` to `search/v1/search.proto`'s `SearchService`. New
messages `FederatedSearchRequest` (`query`, repeated `resources`, `filters`,
`page`, `page_size`, `include_source`), `FederatedSearchResponse` (`total`,
repeated `hits`, repeated per-resource count), `FederatedHit`
(`resource`, `id`, `score`, `source`). Reuses existing `Filter`. Single-resource
`Search` and its request/response are left untouched.
