package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
)

// RegisterChange handles a single change notification from a source service.
// It durably records the change and its affected roots (upsert/tombstone plus
// stale marks), then opportunistically builds them inline on the pool. Marks
// always land before the build attempt, so anything shed or lost to a crash is
// recovered by the stale sweep. See ADR 0008.
func (idx *Indexer) RegisterChange(ctx context.Context, n Notification) error {
	if err := idx.verifyResourceConfig(n); err != nil {
		return err
	}

	res := model.Resource{Type: n.ResourceType, Id: n.ResourceID}

	var deleteSeq int64
	if n.Kind == ChangeDeleted {
		seq, err := idx.st.MarkDeleted(ctx, res)
		if err != nil {
			return fmt.Errorf("mark deleted %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
		deleteSeq = seq
	} else {
		if err := idx.st.UpsertResource(ctx, res, n.Version); err != nil {
			return fmt.Errorf("upsert resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	}

	parents, err := idx.st.GetParentResources(ctx, res)
	if err != nil {
		return fmt.Errorf("getting parents: %w", err)
	}

	slog.Info("registering change",
		"resource_type", n.ResourceType,
		"resource_id", n.ResourceID,
		"kind", n.Kind.String(),
		"affected_parents", len(parents),
	)

	roots := make([]model.Resource, 0, len(parents)+1)
	roots = append(roots, parents...)

	if n.Kind == ChangeDeleted {
		if !idx.pool.trySubmit(func(taskCtx context.Context) {
			idx.deleteOne(taskCtx, res, deleteSeq)
		}) {
			slog.Info("pool saturated; tombstone left for sweep",
				slog.String("type", res.Type), slog.String("id", res.Id))
		}
	} else {
		roots = append(roots, res)
	}

	return idx.scheduleBuild(ctx, roots, n.Metadata)
}

func groupResourceIDsByType(roots []model.Resource) map[string][]string {
	idsByType := make(map[string][]string, len(roots))

	for _, root := range roots {
		idsByType[root.Type] = append(idsByType[root.Type], root.Id)
	}

	return idsByType
}
