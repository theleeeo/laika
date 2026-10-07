# Multi-Schema-Version writes for graceful migrations

Every build writes the resulting document to the index of _every_ active Schema Version of the resource, not just the one the read alias currently points to. A resource type with Schema Versions 1 and 2 ends up with both `a_search_v1` and `a_search_v2` containing the document after each rebuild.

The migration model this enables:

1. Add a v2 Schema Version to the YAML alongside the existing v1. Both indices receive writes from then on.
2. Run a `rebuild` for v2 to backfill historical resources into `a_search_v2`.
3. Cut the read alias `a_search` from `a_search_v1` to `a_search_v2` by bumping `readVersion` to 2 and redeploying — the config owns the alias and the indexer converges it at startup (see [ADR 0009](0009-read-version-owns-the-read-alias.md)). The cutover is instantaneous because v2 is already fully populated and warm.
4. Drop v1 from the YAML once the cutover has been validated.

The cost is steady-state write amplification (N writes per build during migrations). The benefit is that breaking changes to the index — added/removed fields, type changes, new or dropped relations — can be rolled out without downtime and without a long cutover window. Given the system's target of consistency-within-seconds, a lazy migration model that only populates v2 on cutover would not meet the cutover-latency expectation.

**Status of the implementation.** The migration flow has been hardened end to end: the backfill pipeline streams in bounded chunks and surfaces per-item failures (ADR 0008 refinements), `readVersion` owns the read alias ([ADR 0009](0009-read-version-owns-the-read-alias.md)), cutover readiness is gated pre-deploy and the deploy/backfill ordering is documented ([ADR 0010](0010-cutover-readiness-is-a-pre-deploy-gate.md)), and each Schema Version decides its own document's existence ([ADR 0013](0013-each-schema-version-decides-its-documents-existence.md), which supersedes the cross-version existence agreement builds enforced until then): a build writes the document of every version whose plan returns one and deletes the document of every version whose plan returns nil, keeping the resource's row while any version has a document, so versions may hold different sets of documents. A dying backfill no longer starts over: a walk with exactly one active plan — a single-version type's full rebuild, or a version-targeted backfill, which is what step 2 above runs — resumes from a heartbeat cursor ([ADR 0011](0011-resumable-rebuild-walks-via-heartbeat-cursors.md)). Known remaining limitation: a multi-plan full rebuild still restarts from scratch, because it has no settled mid-walk position to resume from. `GetCapabilities` advertises every active schema version (with `read_version` marking the serving one), so clients can adopt a not-yet-serving version's fields ahead of the cutover.

**Implication for contributors:** any change that touches the build path must continue to write to every Schema Version's index, and any change that touches read paths must continue to honour the alias as the only public name.
