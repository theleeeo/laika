package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
)

// scheduleBuild durably marks roots stale, then opportunistically builds them
// inline on the pool. The mark always lands before the build attempt, so a
// shed submission, failed build, or crash is recovered by the stale sweep.
// See ADR 0008.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource, metadata map[string]string) error {
	if len(roots) == 0 {
		return nil
	}

	if err := idx.st.MarkStale(ctx, roots); err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}

	for resourceType, ids := range groupResourceIDsByType(roots) {
		args := BuildArgs{ResourceType: resourceType, ResourceIds: ids, Metadata: metadata}
		submitted := idx.pool.trySubmit(func(taskCtx context.Context) {
			if err := idx.Build(taskCtx, args); err != nil {
				slog.Warn("inline build failed; resources remain stale for sweep",
					slog.String("type", args.ResourceType),
					slog.String("error", err.Error()),
				)
			}
		})
		if !submitted {
			slog.Info("build pool saturated; resources left stale for sweep",
				slog.String("type", resourceType),
				slog.Int("count", len(ids)),
			)
		}
	}

	return nil
}
