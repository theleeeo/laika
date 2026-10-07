# Cutover readiness is a pre-deploy gate, and backfill waits for the rollout

> **Note (2026-10-07, runbook step L2.9):** gate 3's parity assumes lockstep
> versions, every active version holding the same set of documents. Since
> [ADR 0013](0013-each-schema-version-decides-its-documents-existence.md) each
> Schema Version decides its own document's existence, so a version whose
> plan legitimately excludes resources holds fewer documents than the other,
> target or current read version alike, and parity fails the move unless the
> operator raises `-count-tolerance` to cover the gap. The tolerance is one number for the whole run and can't tell that
> gap from an unfinished backfill. The gate is unchanged: whether it should
> change is open point Q24 in laika-dev's `docs/open-points.md`, whose default
> keeps it, and its cost is seams S38 in laika-dev's `docs/seams.md`.

ADR 0009 turned a cutover into a config change: bump `readVersion`, redeploy, and the indexer converges the read alias at startup. What the deleted `cutover` tool never had — and the config change still needs — is a readiness check: nothing stopped an operator from bumping `readVersion` before the target index existed, before the backfill finished, or while the type's ingest was unhealthy. The alias flip is atomic, but flipping to a half-populated index is an instant, silent data-loss event for readers.

The decision: **readiness is checked by a read-only pre-deploy tool, `cutover-check`, and never by startup convergence.**

- **Why not gate the startup convergence?** `ConvergeReadAliases` must stay unconditional. Gating it would turn "deploy the fix" into a deadlock — mid-incident, the rollback config would have to pass gates of its own — and would break the mid-rollout crash-restart tolerance ADR 0009 relies on. The gate belongs where the decision is made (before the `readVersion` bump is deployed), not where it is enacted.
- **The gates** (`core.CheckCutoverReadiness`, one verdict per resource, exit non-zero on any failure):
  1. *alias-state* — the current alias target must be one the naming scheme owns; a hand-built alias cannot be reasoned about.
  2. *target-index* — the proposed `readVersion` index must exist. `gen-mapping` is the only bootstrap tool; a missing index means it never ran.
  3. *doc-count-parity* — the current read index and the target must agree on doc count (within `-count-tolerance`, default 0). ADR 0004 writes every active version in lockstep, so a gap means the backfill has not finished or writes are failing on one side.
  4. *stale-backlog* — no resource of the type stale for longer than `-max-stale-age` (default 10m). A caught-up type clears marks within seconds; an aged mark means an unfinished backfill or unhealthy ingest.
- **Rollback record**: the config file's git history *is* the rollback record — a consequence of ADR 0009 that this ADR just names. Re-running `cutover-check` with the deployed config doubles as the post-cutover soak check (it reports the alias in sync and re-verifies the backlog).

**The ordering invariant** (previously undocumented): **complete the rolling deploy of the version-adding config before starting the backfill.** An indexer instance still running the old config writes only the old versions' indices while bumping the shared Build Sequence. If it builds a resource *after* the backfill walk already visited it, the new index keeps the stale document at a lower `build_idx` — invisible to `diff-mapping`, unrepaired until the next organic change to that resource. If a backfill is discovered to have raced a rolling deploy, re-run it.

> **Note (2026-10-04, runbook step L1.5):** the edge sets rely on the same order. A live build drops the edge sets of Schema Versions its config doesn't declare, guarded by the Build Sequence, so an old-config instance that builds a resource at a higher sequence than the new version's last writer drops that version's set while its index keeps the document. The backfill, run after the rollout completes, rewrites every document of the version and its set. See ADR 0002's L1.5 note.

The migration runbook, in full:

1. Add vN to `resources.yml` (readVersion unchanged) → `gen-mapping -apply`.
2. **Complete** the rolling deploy of that config on every indexer instance.
3. Rebuild vN (the backfill).
4. `diff-mapping` (schema drift) and `cutover-check` with the readVersion-bumped config (readiness). Both exit non-zero on a problem and belong in CI.
5. Deploy the `readVersion` bump. Rollback, if needed, is the same change in reverse — the old version is still fully written (ADR 0004).
6. Soak (`cutover-check` again, now in-sync), then drop the old version from config and run `cleanup`.

Consequences:

- The manual pre-cutover checklist becomes one command with a meaningful exit code; CI can enforce it on the config change itself.
- The gate is advisory by construction — nothing *forces* it to run before a deploy. That is the accepted cost of keeping startup convergence unconditional.
- `cutover-check` needs both Elasticsearch and the Postgres relation store: the stale backlog lives in Postgres, so a pure-ES check cannot see an unhealthy type.
