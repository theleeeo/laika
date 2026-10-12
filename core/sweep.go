package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SweepStale rebuilds (or finishes deleting) up to limit resources whose
// stale mark is older than threshold and that no live owner is building,
// synchronously. ListStale claims every entry it returns, so each is an owned
// build or delete: it finishes like an inline one, and a follow-up it hands on
// goes to the pool. The pass finishes its tombstones first, one at a time, and
// then builds its build entries in per-type batches of the type's batch size,
// one Build per batch. An entry whose ownership was lost by the time the pass
// reaches it is skipped: its new owner or a clean row covers it. It returns
// the number of stale entries it listed. Per-resource failures are logged,
// not returned (a batch that fails as a whole is logged once, by Build, ADR
// 0014, its rows at Debug): the mark or tombstone survives, and a failed
// build or delete releases its ownership through Store.ReleaseFailed, which
// backs the row off, so a later sweep retries it once the backoff has passed
// (ListStale skips it until then) and a row that keeps failing never holds
// the head of the backlog. A failed renewal releases the ownership of the tombstone or
// whole batch it covered without a backoff, and the next sweep retries them.
//
// This is the safety net behind the inline build pool (ADR 0008). It runs as
// the body of the StaleSweep Temporal activity; embedders without Temporal
// can drive it from a ticker.
func (idx *Indexer) SweepStale(ctx context.Context, threshold time.Duration, limit int) (int, error) {
	entries, err := idx.st.ListStale(ctx, time.Now().Add(-threshold), limit, idx.ownerLease)
	if err != nil {
		return 0, fmt.Errorf("list stale: %w", err)
	}
	if len(entries) == 0 {
		return 0, nil
	}

	// ListStale claimed every entry at once, so each tombstone, and each
	// batch as a whole, renews its lease when the pass reaches it and is
	// served only while its ownership is still held: a later one's claim may
	// have lapsed while the ones before it ran, and a change since claimed the
	// row — serving it then could delete a recreated document or race its new
	// owner's build. A batch's held ids are built together, each with the
	// metadata its row holds when the build begins (BuildBegun.Metadata), as
	// an inline build of it would.
	var builds []Owned
	for _, e := range entries {
		owned := Owned{Resource: e.Resource, Token: e.Token}
		if !e.Deleted {
			builds = append(builds, owned)
			continue
		}
		if len(idx.renewOwners(ctx, []Owned{owned})) > 0 {
			idx.deleteOne(ctx, e.Resource, e.StaleSeq, e.Token)
		}
	}
	for _, batch := range idx.ownedBatches(builds) {
		held := idx.renewOwners(ctx, batch)
		if len(held) == 0 {
			continue
		}
		// RenewOwners returns the held ones in no set order; they are built
		// in list order.
		isHeld := make(map[Owned]bool, len(held))
		for _, o := range held {
			isHeld[o] = true
		}
		args := BuildArgs{
			ResourceType: batch[0].Type,
			ResourceIds:  make([]string, 0, len(held)),
			OwnerTokens:  make(map[string]int64, len(held)),
		}
		for _, o := range batch {
			if isHeld[o] {
				args.ResourceIds = append(args.ResourceIds, o.Id)
				args.OwnerTokens[o.Id] = o.Token
			}
		}
		// An owned build logs its own failures (Build), so its error is not
		// logged again.
		_ = idx.Build(ctx, args)
	}

	slog.Info("stale sweep pass complete", slog.Int("swept", len(entries)))
	return len(entries), nil
}
