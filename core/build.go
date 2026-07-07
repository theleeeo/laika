package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

func (idx *Indexer) Build(ctx context.Context, params BuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	cfg := idx.resources.Get(params.ResourceType)
	if cfg == nil {
		return fmt.Errorf("resource type %q: %w", params.ResourceType, ErrUnknownResource)
	}

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		return fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}

	var failed int
	for _, id := range params.ResourceIds {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		occVersion, err := idx.st.NextRebuildCounter(ctx, model.Resource{Type: params.ResourceType, Id: id})
		if err != nil {
			logger.Warn("failed to increment rebuild counter", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}

		// TODO: Build multiple documents in a batch.
		if err := idx.buildOne(ctx, plans, params.ResourceType, id, params.Metadata, occVersion); err != nil {
			logger.Warn("build failed", slog.String("id", id), slog.String("error", err.Error()))
			failed++
		}
	}

	if failed > 0 {
		logger.Warn("build complete with failures", slog.Int("total", len(params.ResourceIds)), slog.Int("failed", failed))
	}
	return nil
}

func (idx *Indexer) buildOne(ctx context.Context, plans []projection.Plan, resourceType, resourceID string, metadata map[string]string, occVersion int64) error {
	if occVersion <= 0 {
		return fmt.Errorf("invalid occ version %d for %s/%s", occVersion, resourceType, resourceID)
	}

	if err := idx.st.RemoveResource(ctx, model.Resource{Type: resourceType, Id: resourceID}); err != nil {
		return fmt.Errorf("removing relations: %w", err)
	}

	var allRelations []model.VersionedResource
	var builtDoc projection.BuildDoc

	for _, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: resourceType,
			ResourceID:   resourceID,
			Metadata:     metadata,
		})

		var result projection.BuildDoc
		for r := range ch {
			if r.Err != nil {
				return r.Err
			}
			if len(r.Items) > 0 {
				result = r.Items[0]
				break
			}
		}

		// Resource no longer exists at source — delete from all versions.
		if result.Doc == nil {
			return idx.handleDelete(ctx, RebuildPayload{
				ResourceType: resourceType,
				ResourceID:   resourceID,
			})
		}

		allRelations = append(allRelations, result.Relations...)
		builtDoc = result

		indexName := IndexName(resourceType, plan.Version)
		if err := idx.es.Upsert(ctx, indexName, resourceID, result.Doc, occVersion); err != nil {
			return fmt.Errorf("upsert %s/%s to %s: %w", resourceType, resourceID, indexName, err)
		}
	}

	// Persist relation edges (unversioned — just the resource identity).
	plainRelations := make([]model.Resource, len(allRelations))
	for i, r := range allRelations {
		plainRelations[i] = r.Resource
	}
	if err := idx.st.AddChildResources(ctx,
		model.Resource{Type: resourceType, Id: resourceID},
		plainRelations,
	); err != nil {
		return fmt.Errorf("persist relations for %s/%s: %w", resourceType, resourceID, err)
	}

	// Reverse-relation discovery: bootstrap Parent edges for a brand-new Child.
	//
	// The Child just built may not yet appear in a Parent that should include
	// it, because no Parent→Child edge has ever been persisted — so the
	// RegisterChange fanout could not reach the Parent. The Plan derived the
	// affected Parents from the Child's own fetched data onto the BuildDoc;
	// enqueue their Builds. Each Parent Build re-establishes the edge, so
	// subsequent Child updates are found via GetParentResources without this
	// path. See ADR 0006.
	// TODO: Will there be a double enqueue now for all Parents that already have the edge? Should we check for existing edges first?
	if err := idx.enqueueParents(ctx, builtDoc.Parents, metadata); err != nil {
		return err
	}

	// Drift check.
	//
	// Compare the version we observed from the provider for each child against
	// the version currently stored in the resources table (written by
	// RegisterChange). If a child's stored version is higher than what we
	// fetched, a concurrent update occurred while our edge was missing and the
	// parent fanout could not reach us. Re-enqueue to converge.
	if len(allRelations) > 0 {
		drift, err := idx.st.AnyResourceVersionDrifted(ctx, allRelations)
		if err != nil {
			return fmt.Errorf("drift check for %s/%s: %w", resourceType, resourceID, err)
		}
		if drift {
			slog.Info("child drift detected, re-enqueueing build",
				slog.String("type", resourceType),
				slog.String("id", resourceID),
			)
			if _, err := idx.river.Insert(ctx, BuildArgs{
				ResourceType: resourceType,
				ResourceIds:  []string{resourceID},
				Metadata:     metadata,
			}, nil); err != nil {
				return fmt.Errorf("re-enqueue after drift for %s/%s: %w", resourceType, resourceID, err)
			}
		}
	}

	return nil
}

// enqueueParents enqueues a Build for each Parent the Plan derived from the
// built Child document. The enqueued Builds run normally — carrying their own
// Build Sequence for ES OCC and re-running the drift check — and are idempotent
// under the at-least-once contract, so re-emitting a Parent Build is safe.
func (idx *Indexer) enqueueParents(ctx context.Context, parents []model.Resource, metadata map[string]string) error {
	for _, parent := range parents {
		if _, err := idx.river.Insert(ctx, BuildArgs{
			ResourceType: parent.Type,
			ResourceIds:  []string{parent.Id},
			Metadata:     metadata,
		}, nil); err != nil {
			return fmt.Errorf("enqueue parent build %s/%s: %w", parent.Type, parent.Id, err)
		}
	}

	return nil
}

func (idx *Indexer) rebuild(ctx context.Context, params RebuildArgs) error {
	if len(params.ResourceIDs) > 0 {
		return idx.rebuildByIDs(ctx, params)
	}
	return idx.rebuildAll(ctx, params)
}

func (idx *Indexer) rebuildByIDs(ctx context.Context, params RebuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		return fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}

	resourceRelations := make(map[string][]model.VersionedResource)
	var items []BulkItem
	var failed int

	for _, id := range params.ResourceIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		root := model.Resource{Type: params.ResourceType, Id: id}

		if err := idx.st.RemoveResource(ctx, root); err != nil {
			logger.Warn("failed to remove relations", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}

		occVersion, err := idx.st.NextRebuildCounter(ctx, root)
		if err != nil {
			logger.Warn("failed to increment rebuild counter", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}

		skipRelations := false
		for _, plan := range plans {
			if plan.Executer == nil {
				continue
			}
			ch := plan.Execute(ctx, projection.BuildRequest{
				ResourceType: params.ResourceType,
				ResourceID:   id,
				Metadata:     params.Metadata,
			})

			var result projection.BuildDoc
			var planErr error
			for r := range ch {
				if r.Err != nil {
					planErr = r.Err
					break
				}
				if len(r.Items) > 0 {
					result = r.Items[0]
					break
				}
			}
			if planErr != nil {
				logger.Warn("plan execution failed", slog.String("id", id), slog.Int("plan_version", plan.Version), slog.String("error", planErr.Error()))
				failed++
				skipRelations = true
				break
			}

			// Source returned no data — delete from all versions and stop processing this ID.
			if result.Doc == nil {
				if err := idx.handleDelete(ctx, RebuildPayload{
					ResourceType: params.ResourceType,
					ResourceID:   id,
				}); err != nil {
					logger.Warn("delete missing resource", slog.String("id", id), slog.String("error", err.Error()))
					failed++
				}
				skipRelations = true
				break
			}

			resourceRelations[id] = append(resourceRelations[id], result.Relations...)
			items = append(items, BulkItem{
				Index:   IndexName(params.ResourceType, plan.Version),
				ID:      id,
				Doc:     result.Doc,
				Version: occVersion,
			})
		}

		if skipRelations {
			delete(resourceRelations, id)
		}
	}

	if len(items) > 0 {
		if err := idx.es.BulkUpsert(ctx, items); err != nil {
			return fmt.Errorf("bulk upsert: %w", err)
		}
	}

	for id, rels := range resourceRelations {
		plain := make([]model.Resource, len(rels))
		for i, r := range rels {
			plain[i] = r.Resource
		}
		if err := idx.st.AddChildResources(ctx, model.Resource{Type: params.ResourceType, Id: id}, plain); err != nil {
			logger.Warn("failed to persist relations", slog.String("id", id), slog.String("error", err.Error()))
			failed++
		}
	}

	logger.Info("targeted rebuild complete", slog.Int("total", len(params.ResourceIDs)), slog.Int("failed", failed))
	return nil
}

func (idx *Indexer) rebuildAll(ctx context.Context, params RebuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		return fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}

	resourceRelations := make(map[string][]model.VersionedResource)
	cleaned := make(map[string]bool)
	occVersions := make(map[string]int64)

	var items []BulkItem
	var failed int

	for _, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: params.ResourceType,
			ResourceID:   "",
			Metadata:     params.Metadata,
		})

		for page := range ch {
			if page.Err != nil {
				return fmt.Errorf("plan execution for %s v%d: %w", params.ResourceType, plan.Version, page.Err)
			}

			for _, doc := range page.Items {
				if err := ctx.Err(); err != nil {
					return err
				}

				id := doc.Root.Id

				if !cleaned[id] {
					if err := idx.st.RemoveResource(ctx, doc.Root); err != nil {
						logger.Warn("failed to remove relations", slog.String("id", id), slog.String("error", err.Error()))
						failed++
						continue
					}

					occVersion, err := idx.st.NextRebuildCounter(ctx, doc.Root)
					if err != nil {
						logger.Warn("failed to increment rebuild counter", slog.String("id", id), slog.String("error", err.Error()))
						failed++
						continue
					}

					occVersions[id] = occVersion
					cleaned[id] = true
				}

				resourceRelations[id] = append(resourceRelations[id], doc.Relations...)
				occVersion := occVersions[id]

				items = append(items, BulkItem{
					Index:   IndexName(params.ResourceType, plan.Version),
					ID:      id,
					Doc:     doc.Doc,
					Version: occVersion,
				})
			}
		}
	}

	if err := idx.es.BulkUpsert(ctx, items); err != nil {
		return fmt.Errorf("bulk upsert: %w", err)
	}

	for id, rels := range resourceRelations {
		plain := make([]model.Resource, len(rels))
		for i, r := range rels {
			plain[i] = r.Resource
		}
		if err := idx.st.AddChildResources(ctx, model.Resource{Type: params.ResourceType, Id: id}, plain); err != nil {
			logger.Warn("failed to persist relations", slog.String("id", id), slog.String("error", err.Error()))
			failed++
		}
	}

	logger.Info("build complete", slog.Int("total", len(cleaned)), slog.Int("failed", failed))
	return nil
}
