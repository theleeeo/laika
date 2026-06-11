# Multi-Schema-Version writes for graceful migrations

Every build writes the resulting document to the index of _every_ active Schema Version of the resource, not just the one the read alias currently points to. A resource type with Schema Versions 1 and 2 ends up with both `a_search_v1` and `a_search_v2` containing the document after each rebuild.

The migration model this enables:

1. Add a v2 Schema Version to the YAML alongside the existing v1. Both indices receive writes from then on.
2. Run a `full_rebuild` for v2 to backfill historical resources into `a_search_v2`.
3. Cut the read alias `a_search` from `a_search_v1` to `a_search_v2`. The cutover is instantaneous because v2 is already fully populated and warm.
4. Drop v1 from the YAML once the cutover has been validated.

The cost is steady-state write amplification (N writes per build during migrations). The benefit is that breaking changes to the index — added/removed fields, type changes, new or dropped relations — can be rolled out without downtime and without a long cutover window. Given the system's target of consistency-within-seconds, a lazy migration model that only populates v2 on cutover would not meet the cutover-latency expectation.

**Status of the implementation.** The design intent is established, but edge cases in the migration flow have not yet been fully shaken out — see this as the documented direction, not a closed problem.

**Implication for contributors:** any change that touches the build path must continue to write to every Schema Version's index, and any change that touches read paths must continue to honour the alias as the only public name.
