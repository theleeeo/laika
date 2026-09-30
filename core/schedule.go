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
//
// wait selects the submission: true waits for a slot while the pool is
// pressured (only RegisterChange with WaitForSlot, a caller outside the
// pool); false sheds on a full queue. Cascades from inside running builds
// must pass false — a task waiting on its own pool could deadlock it.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource, metadata map[string]string, wait bool) error {
	if err := idx.markStale(ctx, roots, metadata); err != nil {
		return err
	}
	idx.submitBuilds(ctx, roots, metadata, wait)
	return nil
}

// markStale is scheduleBuild's durable half. Callers that submit other work
// too (RegisterChange's delete) run it first, so no mark waits behind a
// submission.
func (idx *Indexer) markStale(ctx context.Context, roots []model.Resource, metadata map[string]string) error {
	if len(roots) == 0 {
		return nil
	}
	if err := idx.st.MarkStale(ctx, roots, metadata); err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}
	return nil
}

// submitBuilds is scheduleBuild's opportunistic half: one inline build per
// resource type. roots must already be marked stale.
func (idx *Indexer) submitBuilds(ctx context.Context, roots []model.Resource, metadata map[string]string, wait bool) {
	for resourceType, ids := range groupResourceIDsByType(roots) {
		args := BuildArgs{ResourceType: resourceType, ResourceIds: ids, Metadata: metadata}
		submitted := idx.submit(ctx, wait, func(taskCtx context.Context) {
			if err := idx.Build(taskCtx, args); err != nil {
				slog.Warn("inline build failed; resources remain stale for sweep",
					slog.String("type", args.ResourceType),
					slog.String("error", err.Error()),
				)
			}
		})
		if !submitted {
			slog.Info(notSubmittedMsg(wait, "resources left stale for sweep"),
				slog.String("type", resourceType),
				slog.Int("count", len(ids)),
			)
		}
	}
}

// submit hands task to the pool: waiting for a slot when wait is set, else
// shedding on a full queue. Either way a false return leaves the already
// marked work to the sweep.
func (idx *Indexer) submit(ctx context.Context, wait bool, task func(context.Context)) bool {
	if wait {
		return idx.pool.submitWait(ctx, task)
	}
	return idx.pool.trySubmit(task)
}

// notSubmittedMsg is the Info log for a submission the pool did not accept.
func notSubmittedMsg(wait bool, outcome string) string {
	if wait {
		return "wait for a build slot ended by cancellation or shutdown; " + outcome
	}
	return "build pool saturated; " + outcome
}
