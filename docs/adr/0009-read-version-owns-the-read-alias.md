# The config's readVersion owns the read alias

> **Note (2026-10-07, runbook step L2.9):** the consequence that both alias
> targets "serve complete data" assumes lockstep versions, every active
> version's index holding the same resources. Since [ADR
> 0013](0013-each-schema-version-decides-its-documents-existence.md) each
> Schema Version decides its own document's existence, so each index is
> complete for its own version's plan, and two versions whose plans exclude
> different resources hold different sets. An alias flapping between them
> mid-rollout briefly shows the resources one plan includes and hides those it
> excludes. The rest holds: the last instance to start settles the alias.

The read alias in Elasticsearch and `readVersion` in the resource config used to be two sources of truth with different owners. A standalone `cutover` tool flipped the alias, but filter validation, `GetCapabilities`, nested paths, and reference joins all derive from `ReadVersionConfig()` — so after a tool-driven cutover the indexer kept interpreting queries with the old version's schema against the new index until someone also redeployed the config. Worse, `gen-mapping -apply` always re-pointed aliases at its config file's `readVersion`, silently reverting any cutover done behind its back.

The decision: **`readVersion` is the single owner of the alias target; the alias is derived state.**

- **The indexer converges at startup.** `core.ConvergeReadAliases` points every resource's read alias at `IndexName(type, readVersion)` before the server starts taking traffic. It converges in *both* directions — a rollback is just as much a config change as a cutover — and startup fails if convergence fails (ES unreachable, or the target index was never bootstrapped with `gen-mapping`). The one thing it will not do is move an alias off an index the `{type}_search_v{N}` naming scheme does not own: a hand-built alias is skipped with a warning, never destroyed.
- **A cutover is a config change.** Bump `readVersion`, redeploy. Rollback is the same change in reverse. The standalone `cutover` tool is deleted — a second uncoordinated alias writer was the disease, not a feature.
- **`gen-mapping -apply` converges to its config file, guarded.** The tool runs against whatever `-config` file it is handed, which may be a stale checkout — so it refuses to move an alias *backwards*, or off a hand-built target, unless `-force` asserts the file really is the current truth. Creations and forward moves apply without ceremony. The shared decision table is `core.PlanAliasMove`; only the write policy differs between the server (auto-converge, warn on foreign) and the tool (refuse suspicious moves without `-force`).

Consequences:

- The schema/alias split-brain window shrinks from "until someone remembers to redeploy the config" to the rolling-deploy window itself, which is inherent to rolling anything.
- If an old-config instance crash-restarts mid-rollout it can transiently flap the alias back. This is harmless by construction: ADR 0004 keeps every active version's index fully written, so both targets serve complete data, and the last instance to start — always a new-config one once the rollout completes — settles the alias.
- The indexer now refuses to start against an un-bootstrapped cluster instead of limping along until the first write auto-creates a dynamically-mapped index. `gen-mapping` remains the only bootstrap tool.
