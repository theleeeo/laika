package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
)

// RebuildPayload is used as the payload for delete jobs and as the parameter
// to handleDelete when Build detects a resource no longer exists.
type RebuildPayload struct {
	ResourceType string
	ResourceID   string
}

// handleDelete removes the document from Elasticsearch and cleans up relations in PG.
func (idx *Indexer) handleDelete(ctx context.Context, p RebuildPayload) error {
	logger := slog.With(slog.String("jobType", "delete"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	cfg := idx.resources.Get(p.ResourceType)
	for _, v := range cfg.SortedVersions() {
		indexName := IndexName(p.ResourceType, v)
		if err := idx.es.Delete(ctx, indexName, p.ResourceID); err != nil {
			return fmt.Errorf("delete %s/%s from %s: %w", p.ResourceType, p.ResourceID, indexName, err)
		}
	}

	// Remove relation edges from PG so stale roots are no longer affected.
	if err := idx.st.RemoveResource(ctx, model.Resource{Type: p.ResourceType, Id: p.ResourceID}); err != nil {
		return fmt.Errorf("clean up relations for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("deleted document")
	return nil
}

// deleteOne removes the resource's documents and edges, then hard-deletes the
// tombstoned row if no newer change arrived. Failures are logged, not
// returned: the tombstone stays stale and the sweep retries it.
func (idx *Indexer) deleteOne(ctx context.Context, res model.Resource, staleSeq int64) {
	if err := idx.handleDelete(ctx, RebuildPayload{ResourceType: res.Type, ResourceID: res.Id}); err != nil {
		slog.Warn("inline delete failed; tombstone remains for sweep",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
		return
	}
	if err := idx.st.DeleteResourceIfSeq(ctx, res, staleSeq); err != nil {
		slog.Warn("tombstone cleanup failed; sweep will retry",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
	}
}
