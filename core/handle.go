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
// buildSeq is the deleting path's Build Sequence: every version's delete
// carries it as its version, and it bounds the edge removal, so neither
// undoes what a build at a higher Build Sequence wrote.
func (idx *Indexer) handleDelete(ctx context.Context, p RebuildPayload, buildSeq int64) error {
	logger := slog.With(slog.String("jobType", "delete"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	// A type dropped from config has an unknowable version set: its ES
	// documents live in de-configured indices, which are cleanup's territory.
	// The relation edges and the tombstone must still be finished — the sweep
	// serves oldest-first, so refusing here would wedge it forever.
	cfg := idx.resources.Get(p.ResourceType)
	if cfg == nil {
		logger.Warn("deleting resource of a type no longer in config; leaving its de-configured indices to cleanup")
	} else {
		for _, v := range cfg.SortedVersions() {
			indexName := IndexName(p.ResourceType, v)
			if err := idx.es.Delete(ctx, indexName, p.ResourceID, buildSeq); err != nil {
				return fmt.Errorf("delete %s/%s from %s: %w", p.ResourceType, p.ResourceID, indexName, err)
			}
		}
	}

	// Remove relation edges from PG so stale roots are no longer affected.
	if err := idx.st.RemoveResource(ctx, model.Resource{Type: p.ResourceType, Id: p.ResourceID}, buildSeq); err != nil {
		return fmt.Errorf("clean up relations for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("deleted document")
	return nil
}

// deleteOne removes the resource's documents and edges, then hard-deletes the
// tombstoned row if no newer change arrived. token is the delete's ownership
// (0 = none). Before deleting anything it bumps the row's Build Sequence with
// BeginDelete, while the row is still a tombstone: the ES deletes and the edge
// removal carry the bumped sequence, so they never undo a newer build. A
// newer mark that kept the row deleted — a child's registration marking it
// as a Parent, a newer delete — does not stop it, so steady child traffic
// cannot keep a deleted Parent searchable. A delete BeginDelete reports
// superseded — a recreate or a finished delete got to the row first —
// deletes nothing and only finishes. Either way a newer change registered
// meanwhile, which its ownership kept from submitting, comes back from
// DeleteResourceIfSeq, guarded by staleSeq, the tombstone's mark, as the
// follow-up — a build for a recreate, a delete for a row still deleted — and
// is submitted. Failures are logged, not returned: the tombstone stays stale,
// its ownership is released, and the next change or the sweep retries it.
func (idx *Indexer) deleteOne(ctx context.Context, res model.Resource, staleSeq, token int64) {
	owned := []Owned{{Resource: res, Token: token}}
	if token == 0 {
		owned = nil
	}
	begun, err := idx.st.BeginDelete(ctx, res, token)
	if err != nil {
		slog.Warn("begin delete failed; tombstone remains for sweep",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
		idx.releaseOwners(ctx, owned)
		return
	}
	if begun.Superseded {
		slog.Info("delete superseded by a recreate or a finished delete; deleting nothing",
			slog.String("type", res.Type), slog.String("id", res.Id))
	} else if err := idx.handleDelete(ctx, RebuildPayload{ResourceType: res.Type, ResourceID: res.Id}, begun.BuildIdx); err != nil {
		slog.Warn("inline delete failed; tombstone remains for sweep",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
		idx.releaseOwners(ctx, owned)
		return
	}
	fu, err := idx.st.DeleteResourceIfSeq(ctx, res, staleSeq, token)
	if err != nil {
		slog.Warn("tombstone cleanup failed; sweep will retry",
			slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
		idx.releaseOwners(ctx, owned)
		return
	}
	idx.submitFollowUp(ctx, res, fu)
}
