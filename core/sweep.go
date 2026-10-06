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
// goes to the pool. An entry whose ownership was lost by the time the pass
// reaches it is skipped: its new owner or a clean row covers it. It returns
// the number of stale entries it listed. Per-resource failures, a failed
// renewal among them, are logged, not returned: the mark or tombstone
// survives, its ownership is released, and the next sweep retries it.
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

	// Builds run one entry at a time, each an owned build that fetches with
	// the metadata its row holds when the build begins (BuildBegun.Metadata),
	// as an inline build of it would. ListStale claimed the whole batch at
	// once, so each entry renews its lease when the pass reaches it, and is
	// served only if its ownership is still held: a later entry's claim may
	// have lapsed while the ones before it ran, and a change since claimed the
	// row — serving it then could delete a recreated document or race its new
	// owner's build.
	for _, e := range entries {
		if len(idx.renewOwners(ctx, []Owned{{Resource: e.Resource, Token: e.Token}})) == 0 {
			continue
		}
		if e.Deleted {
			idx.deleteOne(ctx, e.Resource, e.StaleSeq, e.Token)
			continue
		}
		if err := idx.Build(ctx, BuildArgs{
			ResourceType: e.Type,
			ResourceIds:  []string{e.Id},
			OwnerTokens:  map[string]int64{e.Id: e.Token},
		}); err != nil {
			slog.Warn("sweep build failed; resource remains stale",
				slog.String("type", e.Type), slog.String("id", e.Id), slog.String("error", err.Error()))
		}
	}

	slog.Info("stale sweep pass complete", slog.Int("swept", len(entries)))
	return len(entries), nil
}
