package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SweepStale rebuilds (or finishes deleting) up to limit resources whose
// stale mark is older than threshold, synchronously. It returns the number of
// stale entries it attempted. Per-resource failures are logged, not returned:
// the mark or tombstone survives and the next sweep retries it.
//
// This is the safety net behind the inline build pool (ADR 0008). It runs as
// the body of the StaleSweep Temporal activity; embedders without Temporal
// can drive it from a ticker.
func (idx *Indexer) SweepStale(ctx context.Context, threshold time.Duration, limit int) (int, error) {
	entries, err := idx.st.ListStale(ctx, time.Now().Add(-threshold), limit)
	if err != nil {
		return 0, fmt.Errorf("list stale: %w", err)
	}
	if len(entries) == 0 {
		return 0, nil
	}

	byType := make(map[string][]string)
	for _, e := range entries {
		if e.Deleted {
			idx.deleteOne(ctx, e.Resource, e.StaleSeq)
			continue
		}
		byType[e.Type] = append(byType[e.Type], e.Id)
	}

	for resourceType, ids := range byType {
		if err := idx.Build(ctx, BuildArgs{ResourceType: resourceType, ResourceIds: ids}); err != nil {
			slog.Warn("sweep build failed; resources remain stale",
				slog.String("type", resourceType), slog.String("error", err.Error()))
		}
	}

	slog.Info("stale sweep pass complete", slog.Int("swept", len(entries)))
	return len(entries), nil
}
