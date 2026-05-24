package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/model"
)

// RegisterChange handles a single change notification from a source service.
// It determines which root search documents are affected and enqueues
// rebuild (or delete) jobs for each.
func (idx *Indexer) RegisterChange(ctx context.Context, n Notification) error {
	if err := idx.verifyResourceConfig(n); err != nil {
		return err
	}

	// Track the resource itself in the resources table.
	res := model.Resource{Type: n.ResourceType, Id: n.ResourceID}
	if n.Kind == ChangeDeleted {
		if err := idx.st.DeleteResource(ctx, res); err != nil {
			return fmt.Errorf("delete resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	} else {
		if err := idx.st.UpsertResource(ctx, res, n.Version); err != nil {
			return fmt.Errorf("upsert resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	}

	roots := []model.Resource{
		// The resource itself is always affected
		{Type: n.ResourceType, Id: n.ResourceID},
	}

	// Determine which parents are affected.
	parents, err := idx.st.GetParentResources(ctx, res)
	if err != nil {
		return fmt.Errorf("getting parents: %w", err)
	}
	roots = append(roots, parents...)

	slog.Info("registering change",
		"resource_type", n.ResourceType,
		"resource_id", n.ResourceID,
		"kind", n.Kind.String(),
		"affected_roots", len(roots),
	)

	for _, root := range roots {
		// If this is a delete of a root resource itself, enqueue a delete job.
		if n.Kind == ChangeDeleted && root.Type == n.ResourceType && root.Id == n.ResourceID {
			if _, err := idx.river.Insert(ctx, DeleteArgs{
				ResourceType: root.Type,
				ResourceID:   root.Id,
				Metadata:     n.Metadata,
			}, nil); err != nil {
				return fmt.Errorf("enqueueing delete for root %s|%s: %w", root.Type, root.Id, err)
			}
			continue
		}

		// TODO: Group per resource type
		if _, err := idx.river.Insert(ctx, BuildArgs{
			ResourceType: root.Type,
			ResourceIds:  []string{root.Id},
			Metadata:     n.Metadata,
		}, nil); err != nil {
			return fmt.Errorf("enqueueing rebuild for root %s|%s: %w", root.Type, root.Id, err)
		}
	}

	return nil
}
