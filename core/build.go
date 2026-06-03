package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/projection"
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

	var allRelations []model.Resource

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

		indexName := es.IndexName(resourceType, plan.Version)
		if err := idx.es.Upsert(ctx, indexName, resourceID, result.Doc, occVersion); err != nil {
			return fmt.Errorf("upsert %s/%s to %s: %w", resourceType, resourceID, indexName, err)
		}
	}

	if err := idx.st.AddChildResources(ctx,
		model.Resource{Type: resourceType, Id: resourceID},
		allRelations,
	); err != nil {
		return fmt.Errorf("persist relations for %s/%s: %w", resourceType, resourceID, err)
	}

	return nil
}

func (idx *Indexer) rebuild(ctx context.Context, params FullRebuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		return fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}

	resourceRelations := make(map[string][]model.Resource)
	cleaned := make(map[string]bool)
	occVersions := make(map[string]int64)

	var items []es.BulkItem
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

				items = append(items, es.BulkItem{
					Index:   es.IndexName(params.ResourceType, plan.Version),
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
		if err := idx.st.AddChildResources(ctx, model.Resource{Type: params.ResourceType, Id: id}, rels); err != nil {
			logger.Warn("failed to persist relations", slog.String("id", id), slog.String("error", err.Error()))
			failed++
		}
	}

	logger.Info("build complete", slog.Int("total", len(cleaned)), slog.Int("failed", failed))
	return nil
}
