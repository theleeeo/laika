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
// It is the cascade path, whose submissions shed on a full queue and never
// wait: build Parents and drift re-builds run inside pool tasks, where a task
// waiting on its own pool could deadlock it, and the rebuild flusher runs in a
// rebuild walk, which must not stall behind producer backpressure.
func (idx *Indexer) scheduleBuild(ctx context.Context, roots []model.Resource, metadata map[string]string) error {
	if len(roots) == 0 {
		return nil
	}
	if _, err := idx.st.MarkStale(ctx, roots, metadata, idx.ownerLease); err != nil {
		return fmt.Errorf("marking %d resources stale: %w", len(roots), err)
	}
	idx.submitBuilds(ctx, roots, metadata)
	return nil
}

// submitBuilds is scheduleBuild's opportunistic half: one inline build per
// resource type. roots must already be marked stale.
func (idx *Indexer) submitBuilds(ctx context.Context, roots []model.Resource, metadata map[string]string) {
	for resourceType, ids := range groupResourceIDsByType(roots) {
		if !idx.submitBuildArgs(ctx, BuildArgs{ResourceType: resourceType, ResourceIds: ids, Metadata: metadata}, false) {
			slog.Info(notSubmittedMsg(false, "resources left stale for sweep"),
				slog.String("type", resourceType),
				slog.Int("count", len(ids)),
			)
		}
	}
}

// submitBuild submits one inline build of res, already marked stale, with
// its own metadata.
func (idx *Indexer) submitBuild(ctx context.Context, res model.Resource, metadata map[string]string, wait bool) {
	if !idx.submitBuildArgs(ctx, BuildArgs{ResourceType: res.Type, ResourceIds: []string{res.Id}, Metadata: metadata}, wait) {
		slog.Info(notSubmittedMsg(wait, "resource left stale for sweep"),
			slog.String("type", res.Type), slog.String("id", res.Id))
	}
}

func (idx *Indexer) submitBuildArgs(ctx context.Context, args BuildArgs, wait bool) bool {
	return idx.submit(ctx, wait, func(taskCtx context.Context) {
		if err := idx.Build(taskCtx, args); err != nil {
			slog.Warn("inline build failed; resources remain stale for sweep",
				slog.String("type", args.ResourceType),
				slog.String("error", err.Error()),
			)
		}
	})
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

func groupResourceIDsByType(roots []model.Resource) map[string][]string {
	idsByType := make(map[string][]string, len(roots))

	for _, root := range roots {
		idsByType[root.Type] = append(idsByType[root.Type], root.Id)
	}

	return idsByType
}
