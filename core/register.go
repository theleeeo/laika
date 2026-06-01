package core

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/theleeeo/indexer/model"
)

func groupResourceIDsByType(roots []model.Resource) map[string][]string {
	idsByType := make(map[string][]string, len(roots))
	seenByType := make(map[string]map[string]struct{}, len(roots))

	for _, root := range roots {
		seenIDs, ok := seenByType[root.Type]
		if !ok {
			seenIDs = make(map[string]struct{})
			seenByType[root.Type] = seenIDs
		}

		if _, alreadySeen := seenIDs[root.Id]; alreadySeen {
			continue
		}

		seenIDs[root.Id] = struct{}{}
		idsByType[root.Type] = append(idsByType[root.Type], root.Id)
	}

	for resourceType := range idsByType {
		sort.Strings(idsByType[resourceType])
	}

	return idsByType
}

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

	idsByType := groupResourceIDsByType(roots)

	if n.Kind == ChangeDeleted {
		if _, err := idx.river.Insert(ctx, DeleteArgs{
			ResourceType: n.ResourceType,
			ResourceID:   n.ResourceID,
			Metadata:     n.Metadata,
		}, nil); err != nil {
			return fmt.Errorf("enqueueing delete for root %s|%s: %w", n.ResourceType, n.ResourceID, err)
		}

		delete(idsByType, n.ResourceType)
	}

	resourceTypes := make([]string, 0, len(idsByType))
	for resourceType := range idsByType {
		resourceTypes = append(resourceTypes, resourceType)
	}
	sort.Strings(resourceTypes)

	for _, resourceType := range resourceTypes {
		resourceIDs := idsByType[resourceType]
		if len(resourceIDs) == 0 {
			continue
		}

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
