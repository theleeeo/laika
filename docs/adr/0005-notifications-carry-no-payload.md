# Notifications carry no payload

A `Notification` from an upstream service identifies a Resource (Type + ID), the Kind of change (created / updated / deleted), an upstream Version, and free-form Metadata. It deliberately does not include the changed Resource's field data. The indexer always re-fetches via the Provider when it builds the document.

We chose this for three reasons:

1. **Upstream is the single source of truth.** Re-fetching at build time eliminates any race between "the data the notification was stamped with" and "what upstream currently has". If a notification is delayed in transit, the build still produces a document consistent with the upstream as of the build, not as of the notification.
2. **Tiny upstream contract.** Upstreams only need to publish change identifiers, not their internal data shapes. There is no notification schema to keep in sync with each new field added upstream.
3. **Plan-driven field selection.** Which fields the indexer needs is decided by the Plan (and ultimately the resource config), not by upstream. Decoupling notifications from data lets the index shape evolve without touching the change-publishing side.

The Version field _is_ load-bearing — the drift check (see [ADR 0002](./0002-distributed-safety-via-occ-and-drift-check-not-locks.md)) compares observed Versions against the stored Version written from notifications. Notifications carry "what version of the resource you changed", not "what the resource now contains".

**Future extension.** An optional root-data payload may be added so that, when an upstream chooses to include it, the indexer can write the root fields into the document immediately (giving fast field-level visibility) while the slower relation fanout continues in the background. This sits on top of the current model — when the payload is absent, behaviour is unchanged. Build Sequence and drift-check remain the consistency mechanism.

**Implication for contributors:** do not add required data fields to `Notification`. If a future feature needs upstream-supplied data, it must be optional and the indexer must continue to produce correct documents when it is absent.
