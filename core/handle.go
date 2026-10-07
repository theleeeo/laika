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

	// Elasticsearch rejects a delete below a newer document's version and
	// SearchBackend.Delete returns nil for it, so this claims no deletion: the
	// ES client logs each document it actually deleted.
	logger.Info("delete path finished", slog.Int64("buildSeq", buildSeq))
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
// and an owned delete is released through releaseFailed, which backs its row
// off, so the sweep retries it once the backoff has passed, or the next
// change claims it first; one whose failure a cancellation or shutdown
// caused is released without a backoff.
func (idx *Indexer) deleteOne(ctx context.Context, res model.Resource, staleSeq, token int64) {
	// fail ends the delete after err: an owned delete is released through
	// releaseFailed, which logs it with its attempt count; one that owns
	// nothing is logged here.
	fail := func(err error) {
		if token == 0 {
			slog.Warn("delete failed; tombstone remains for sweep",
				slog.String("type", res.Type), slog.String("id", res.Id), slog.String("error", err.Error()))
			return
		}
		idx.releaseFailed(ctx, "delete", []Owned{{Resource: res, Token: token}}, err)
	}
	begun, err := idx.st.BeginDelete(ctx, res, token)
	if err != nil {
		fail(fmt.Errorf("begin delete: %w", err))
		return
	}
	if begun.Superseded {
		slog.Info("delete superseded by a recreate or a finished delete; deleting nothing",
			slog.String("type", res.Type), slog.String("id", res.Id))
	} else if err := idx.handleDelete(ctx, RebuildPayload{ResourceType: res.Type, ResourceID: res.Id}, begun.BuildIdx); err != nil {
		fail(err)
		return
	}
	fu, err := idx.st.DeleteResourceIfSeq(ctx, res, staleSeq, token)
	if err != nil {
		fail(fmt.Errorf("tombstone cleanup: %w", err))
		return
	}
	idx.submitFollowUp(ctx, res, fu)
}
