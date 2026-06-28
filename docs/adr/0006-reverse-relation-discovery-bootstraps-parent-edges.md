# Reverse-relation discovery bootstraps Parent edges for brand-new Children

The Relation graph in the Store is populated only by past Builds: a Parent's outgoing edges are written when that Parent is built and its Plan discovers its Children. `RegisterChange` finds the Parents affected by a change exclusively through `GetParentResources`, which reads those persisted edges. This leaves a gap for a brand-new Child: when a Child is created upstream, no edge to its Parent has ever been written, so the Parent that should now include the Child never gets rebuilt. The system previously relied on the upstream sending a second Notification for the Parent as well — an implicit, undocumented, unenforced contract.

We close the gap inside the indexer rather than in the upstream contract. On every Build of a Child, the indexer derives the affected Parents from a Relation key declared in the YAML and enqueues their Builds. The indexer becomes self-sufficient from the resource config alone; no upstream needs to know about relation endpoints it did not change.

## Why not the alternatives

- **Explicit upstream contract** (document that upstreams MUST notify both endpoints): zero indexer code, but re-introduces exactly the implicit-contract dependency that [ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md) rejected for consistency. A missed notification fails silently — a Child never appears in its Parent and nothing detects it. With many upstreams, every new service must remember the contract. We control all upstreams today, which makes the contract *possible* but not *robust*.
- **Periodic Rebuild as the primary mechanism**: cannot meet the consistency-within-seconds SLO for a Child's first appearance in its Parent unless run at a frequency that defeats the point. Retained only as a future backstop (see below).

## How it works

1. **DSL declares both sides of the join.** A relation is a join between a Parent field and a related-resource field. Today only the Parent side is named (the old `key: { source, field }`) and the related resource's field is hidden inside the Provider's `FetchRelated`. We replace `key` with a single `join` block that names both sides explicitly:

   ```yaml
   # on `a`, relation to c   →   a.id == c.a_id
   - resource: c
     join: { local: id, foreign: a_id }
   # on `a`, relation to b   →   a.b_id == b.id
   - resource: b
     join: { local: b_id, foreign: id }
     cardinality: one
   ```

   `local` is the field on this resource (defaults to the root; add `from: <sibling>` to pull from a sibling relation for chained joins, replacing the old `source`). `foreign` is the field on the related resource. There is no separate reverse-key concept: config-load derives a reverse map `childType → [(parentType, foreignField)]` from the same `join` declarations, and the forward fetch passes `foreign` to `FetchRelated` directly instead of the Provider hardcoding it.

2. **Resolution piggybacks on the Child's own Build — no extra fetch.** A Child Build already fetches the Child's full data; `BuildDoc.Resolved[childType]` holds the *unfiltered* fetched data, so the foreign-key field is present even when it is not part of the indexed document. After the Child's document is written, the indexer reads each declared `foreign` field from the built `BuildDoc` and enqueues a Build for `parentType/<value>`. Resolution does not run on the Notification ingest path, which stays Postgres-only and Provider-free.

3. **The edge graph carries it thereafter.** The enqueued Parent Build runs normally and re-establishes the Parent→Child edge in the Store. Subsequent *updates* to the Child are then found by the existing `GetParentResources` path. Reverse discovery only has to bootstrap the first edge.

This composes with the existing consistency model unchanged: Parent Builds carry their own Build Sequence for ES OCC, run the drift check, and are idempotent under the at-least-once contract — re-emitting a Parent Build is safe.

## Separation of concerns

`core.Indexer` does not gain a `source.Provider` dependency. The reverse-resolution abstraction is supplied to core the same way Plans are — a `ParentResolver` that, given the `BuildDoc` core just produced, returns the `[]model.Resource` Parents to also build. Core enqueues those Builds; it never extracts keys or fetches data itself. The implementation lives in `app/dsl`, is built from the reverse map, and reads the already-fetched data on the `BuildDoc`. See [ADR 0001](./0001-core-is-a-library-the-app-is-one-assembly.md) for why data fetching stays out of core.

## Coverage and the future backstop

Reverse discovery works wherever the Parent key is a field on the Child's own data — the common foreign-key case. Relations whose Parent key cannot be derived from the Child alone (join tables, computed keys) emit nothing and are out of scope here. They are the justification for a future **periodic-Rebuild backstop** (the rejected-as-primary Option C, retained as a safety net): a scheduled Rebuild that converges anything the event-driven path cannot express. The backstop is documented direction, not built as part of this decision.

## Status

This ADR records the design decision only. Implementation — the `join` DSL block (replacing `key`), the reverse map, the `ParentResolver` abstraction, build-time emission, and integration coverage for the create-Child-then-see-it-in-Parent cycle — is tracked as a separate follow-up issue.

**Implication for contributors:** do not satisfy the brand-new-Child case by adding a required Parent Notification to the upstream contract, and do not move reverse resolution onto the `RegisterChange` ingest path. Resolution belongs at Child Build time, driven by config, reusing the fetch the Build already performs.
