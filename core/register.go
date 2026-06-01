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

	// Determine which parents are affected.
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
		if _, err := idx.river.Insert(ctx, DeleteArgs{
			ResourceType: n.ResourceType,
			ResourceID:   n.ResourceID,
			Metadata:     n.Metadata,
		}, nil); err != nil {
			return fmt.Errorf("enqueueing delete for root %s|%s: %w", n.ResourceType, n.ResourceID, err)
		}
	} else {
		roots = append(roots, res)
	}

	idsByType := groupResourceIDsByType(roots)

	for resourceType, resourceIDs := range idsByType {
		if _, err := idx.river.Insert(ctx, BuildArgs{
			ResourceType: resourceType,
			ResourceIds:  resourceIDs,
			Metadata:     n.Metadata,
		}, nil); err != nil {
			return fmt.Errorf("enqueueing rebuild for root type %s with %d ids: %w", resourceType, len(resourceIDs), err)
		}
	}

	return nil
}

func groupResourceIDsByType(roots []model.Resource) map[string][]string {
	idsByType := make(map[string][]string, len(roots))

	for _, root := range roots {
		idsByType[root.Type] = append(idsByType[root.Type], root.Id)
	}

	return idsByType
}
