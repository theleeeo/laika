# Relation Strategy: `denormalize` vs `reference`

**Status:** Design — approved in brainstorming, pending spec review.
**Date:** 2026-06-30

## Problem

A Parent denormalizes a Child's fields into its document. When a Child changes,
*every* Parent that contains it is rebuilt — the fanout is driven by the Relation
graph (the persisted Parent→Child edges; see [ADR 0006](../../adr/0006-reverse-relation-discovery-bootstraps-parent-edges.md)).

This is correct but ruinous when a **low-count Child is denormalized into a
high-count Parent**. The motivating shape:

```
C --(a_id)--> A --(b_id)--> B
   0–10 C per A          10000+ A per B
```

Denormalizing `B` onto `A` means one change to a single `B` storms 10000+ `A`
rebuilds. We want to keep `B` searchable from `A` (and transitively from `C`)
*without* paying that rebuild cost.

The fix is a per-Relation **strategy**: keep today's denormalization where
fanout is cheap, and switch to a non-copying **reference** where it is not.

## Vocabulary (to add to CONTEXT.md)

**Strategy**: a property of a Relation choosing how the Child's data reaches the
Parent's searchable surface. Two values:

- **`denormalize`** (default): the Child's selected fields are copied into the
  Parent's Document. Today's behavior, unchanged.
- **`reference`**: the Child's fields are *not* copied. The Parent stores only
  the join key; the Child's fields are resolved at **search time** via a
  two-phase (application-side) join.

`strategy` is distinct from the existing **`cardinality`** (`one` | `many`),
which is *join multiplicity* — whether a Relation resolves to a single nested
object or an array, and which drives the ES object-vs-`nested` mapping. The two
axes are orthogonal: a Relation has both a cardinality and a strategy.

## DSL

`strategy` is an optional field on a relation. Default `denormalize`.

```yaml
relations:
  # cheap fanout (≤10 C per A) — denormalize, and carry A's b_id down onto C
  - resource: a
    join: { local: id, foreign: a_id }
    # ...A fields you want; b_id rides along so it lands on C

  # expensive fanout (10000+ A per B) — reference, keyed off the denormalized b_id
  - resource: b
    strategy: reference
    join: { local: b_id, from: a, foreign: id }
    fields:
      - name: name        # filterable at search time, not copied onto C
```

## What `reference` changes

The strategy changes behavior in four places. The defining property is that a
`reference` relation does **less**, not more — it is largely subtractive.

### 1. Build / Plan — drops the edge (this is what kills the storm)

For a `reference` relation the Parent's Build:

- **does not fetch the Child** (no `FetchRelated` call — a build-time saving),
- **does not write the Parent→Child edge** into the Store, and
- **emits no reverse `Parents`** (ADR 0006 reverse-discovery is skipped).

With no edge, `RegisterChange` → `GetParentResources` never returns these
Parents on a Child change, so they are never enqueued. The high-count Parent
documents are simply never rebuilt when the referenced Child changes. The
Child's own Document reindexes and that is the entire write cost.

Consistency note: when the *key* a reference is built on lives on a denormalized
sibling (the `from:` case below), a change to that sibling still rebuilds the
Parent through the sibling's own (cheap) denormalize edge — so the stored key
stays correct. Only the referenced Child's *fields* are resolved late.

### 2. Mapping — omits the Child's field block

`gen-mapping` does not emit the Child's fields under the Parent index for a
`reference` relation. The Parent needs only the join key, which is already a root
field (natively or via a denormalized sibling). No object/`nested` mapping is
produced for the relation.

### 3. Search — two-phase join via the innermost SearchMiddleware

Reference resolution happens in a **single innermost middleware**
(`referenceResolve`) that runs *after every user middleware* (see "Middleware
model"). It routes each request filter by its `Field` path:

1. A filter naming a `reference` relation's field (`b.name`, `b.tenant_id`) is
   **extracted** onto that Child's own search (rewritten `b.X` → `fields.X`).
2. Every other filter — root fields (`fields.tenant_id`) and `denormalize`
   relation fields (`b.name` when `b` is denormalize) — **stays on the
   primary**.
3. Each Child search is executed, its matching Child IDs collected and folded
   into a `terms` filter on the Parent's join key (`<localKey> IN [...]`), then
   the primary search runs.

Because `reference` is chosen precisely when the Child is low-count, the
intermediate ID set is small — the two-phase join is cheapest exactly where this
strategy is used.

### 4. Capabilities — filter-only

`reference` relation fields are advertised as **filter-only**:
`FilterOps: [Eq, In]`, `Searchable: false`, `Sortable: false`. They live in
another index, so they cannot participate in the Parent's full-text relevance or
sorting. This is a documented, intentional limitation.

## Middleware model (filter routing by field path, innermost-only)

The child sub-search is **not** resolved by re-entering `Indexer.Search`
recursively, and **nothing reference-related runs before the user
middlewares**. Reference handling lives entirely in one innermost middleware:

- **User middlewares run first (outermost).** They add filters via
  `SearchRequest.AddFilter`, which is a plain append. A middleware is **opaque
  to strategy**: it names a field path and lets routing decide where the filter
  lands.
- **`referenceResolve` runs innermost, after every user middleware.** It is the
  only component that knows about reference relations. It routes each filter by
  its field path, then executes the child searches and folds their IDs into the
  primary.

Routing is **explicit and field-path-driven**, with no implicit fan-out:

- A filter on `b.name` filters the denormalized `b` block when `b` is
  `denormalize`, or is extracted onto the referenced `b` search when `b` is
  `reference` — the caller writes the same path either way.
- A filter on a root field (`fields.tenant_id`) scopes the **primary only**; it
  is never copied onto a referenced search.
- To scope a referenced Child, a middleware names that Child's field
  explicitly: `b.tenant_id` routes to the `b` search **only**.

This is strictly simpler than recursive re-entry (no cycle/depth limits) and
than a multi-target fan-out (no shared filter list threaded across
middlewares): the field path *is* the routing instruction.

## Chained references: not supported, not needed

References are expanded **once** (single level). The motivating C→A→B case is
handled not by chaining references but by **key propagation through
denormalization**:

- `C→A` is `denormalize` (≤10 C per A — cheap fanout). This carries A's `b_id`
  down onto C.
- `C→B` is `reference`, with its `local` key sourced `from: a` — the existing
  chained-join mechanism from [ADR 0006](../../adr/0006-reverse-relation-discovery-bootstraps-parent-edges.md).

"All C by B.name" then resolves in a single reference hop:

1. search B index `name:X` → `b_ids` (B is the low-count side — small set)
2. terms filter on C: `b_id IN b_ids`

Making `A` a reference too (true chaining) would explode the middle step to
10000+ `a_ids` per B — the terms-ceiling blowup. Single-level reference with key
propagation is both sufficient and the correct shape.

## Config validation (correctness gate)

Config-load **must verify**, for every `reference` relation, that its `local`
join key is reachable on the searched Document — either:

- a **root field** of the Parent resource, or
- a field carried by a **denormalized sibling** relation referenced via `from:`
  (and that sibling must itself be `strategy: denormalize`, so the key is
  actually present).

If the key is not reachable, the reference cannot be resolved in a single pass;
config-load **rejects the configuration** with a clear error naming the relation
and the missing key. This is a hard error, not a warning.

(Per brainstorming, there is **no** count/scale declaration in the DSL. The
operator chooses `strategy` from their own knowledge of the data; validation only
enforces correctness, not the wisdom of the choice.)

## Out of scope

- Recursive/chained reference resolution (more than one reference hop).
- Sorting or full-text scoring on referenced fields.
- Any automatic strategy selection based on data scale.
- ES-native parent/child `join` fields or `terms`-lookup queries (rejected in
  brainstorming in favor of the application-side join).

## Testing

- **Config validation**: reference key reachable (root field; via `from:`
  denormalized sibling) accepts; missing key, or `from:` pointing at a
  `reference` sibling, rejects.
- **Build**: a `reference` relation writes no Parent→Child edge and emits no
  reverse Parents; a Child change does not enqueue the referencing Parents.
- **Mapping**: `gen-mapping` omits the referenced Child's fields; the join key is
  present.
- **Search middleware**: a filter on a referenced field is extracted to a child
  search and its IDs fold into a `terms` filter on the join key; a root-field
  filter (`fields.tenant_id`) scopes the primary only; a reference-field filter
  (`b.tenant_id`) scopes the child only.
- **Capabilities**: referenced fields advertised filter-only.
- **Integration** (`app/tests`): end-to-end "all C by B.name" over the C→A→B
  shape with B referenced and the `b_id` key propagated via denormalized A;
  changing a B reindexes only B; the query returns the right C.
```
