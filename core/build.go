package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

type BuildArgs struct {
	ResourceType string            `json:"resource_type"`
	ResourceIds  []string          `json:"resource_ids,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	// OwnerTokens holds, per id, the owner token the build was claimed
	// under (Store.MarkStale, RegisterChanges, ListStale). An id without one
	// builds unowned: it claims nothing and finishes with ClearStale.
	OwnerTokens map[string]int64 `json:"owner_tokens,omitempty"`
}

type RebuildArgs struct {
	ResourceType string            `json:"resource_type"`
	Versions     []int             `json:"versions"`
	ResourceIDs  []string          `json:"resource_ids,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// Build builds each of params' ids. An id with an owner token (OwnerTokens)
// is an owned inline build: BeginBuild renews its lease, and a success
// finishes it with FinishOwned, submitting the follow-up FinishOwned hands on.
// An id without one owns nothing and finishes with ClearStale. A failed owned
// id releases its ownership and keeps its mark, so the next change claims it
// or the sweep rebuilds it; so does every owned id left unfinished when ctx
// ends.
func (idx *Indexer) Build(ctx context.Context, params BuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	cfg := idx.resources.Get(params.ResourceType)
	if cfg == nil {
		idx.releaseOwners(ctx, params.owned(params.ResourceIds))
		return fmt.Errorf("resource type %q: %w", params.ResourceType, ErrUnknownResource)
	}

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		idx.releaseOwners(ctx, params.owned(params.ResourceIds))
		return fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}

	var failed int
	for i, id := range params.ResourceIds {
		if ctx.Err() != nil {
			idx.releaseOwners(ctx, params.owned(params.ResourceIds[i:]))
			return ctx.Err()
		}

		res := model.Resource{Type: params.ResourceType, Id: id}
		token := params.OwnerTokens[id]
		begun, err := idx.st.BeginBuild(ctx, res, token)
		if err != nil {
			logger.Warn("failed to begin build", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			idx.releaseOwners(ctx, params.owned([]string{id}))
			continue
		}

		// TODO: Build multiple documents in a batch.
		if err := idx.buildOne(ctx, plans, params.ResourceType, id, params.Metadata, begun.BuildIdx, begun.Start); err != nil {
			logger.Warn("build failed", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			idx.releaseOwners(ctx, params.owned([]string{id}))
			continue
		}

		if token == 0 {
			// Race-safe: a no-op if a newer change bumped stale_seq mid-build —
			// including buildOne's own drift re-mark, which must survive this
			// clear.
			if err := idx.st.ClearStale(ctx, res, begun.StaleSeq); err != nil {
				logger.Warn("clear stale failed; sweep may rebuild redundantly",
					slog.String("id", id), slog.String("error", err.Error()))
			}
			continue
		}

		// Race-safe the same way: a change that moved stale_seq mid-build —
		// a registration this owner kept from submitting, or buildOne's own
		// drift re-mark — re-claims the row for one follow-up instead.
		fu, err := idx.st.FinishOwned(ctx, res, begun.StaleSeq, token)
		if err != nil {
			logger.Warn("finishing owned build failed; resource remains stale for sweep",
				slog.String("id", id), slog.String("error", err.Error()))
			idx.releaseOwners(ctx, params.owned([]string{id}))
			continue
		}
		idx.submitFollowUp(ctx, res, fu)
	}

	if failed > 0 {
		logger.Warn("build complete with failures", slog.Int("total", len(params.ResourceIds)), slog.Int("failed", failed))
	}
	return nil
}

// owned is the ownership params holds over ids: those with an owner token.
func (params BuildArgs) owned(ids []string) []Owned {
	var out []Owned
	for _, id := range ids {
		if token := params.OwnerTokens[id]; token != 0 {
			out = append(out, Owned{Resource: model.Resource{Type: params.ResourceType, Id: id}, Token: token})
		}
	}
	return out
}

// versionedDoc pairs a plan's Schema Version with the document it built.
type versionedDoc struct {
	version int
	doc     projection.BuildDoc
}

// executeAllPlans runs every active plan for one resource and splits the
// outcomes into built documents and the versions whose plan returned no data.
func executeAllPlans(ctx context.Context, plans []projection.Plan, req projection.BuildRequest) (docs []versionedDoc, missing []int, err error) {
	for _, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		result, err := executePlan(ctx, plan, req)
		if err != nil {
			return nil, nil, err
		}
		if result.Doc == nil {
			missing = append(missing, plan.Version)
			continue
		}
		docs = append(docs, versionedDoc{version: plan.Version, doc: result})
	}
	return docs, missing, nil
}

func (idx *Indexer) buildOne(ctx context.Context, plans []projection.Plan, resourceType, resourceID string, metadata map[string]string, occVersion, start int64) error {
	if occVersion <= 0 {
		return fmt.Errorf("invalid occ version %d for %s/%s", occVersion, resourceType, resourceID)
	}

	// Execute every version's plan before deciding anything: existence is a
	// property of the resource, not of one Schema Version, so one plan's nil
	// must never delete what another plan just wrote — and relations and
	// Parents are collected from every plan, not just the last.
	docs, missing, err := executeAllPlans(ctx, plans, projection.BuildRequest{
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Metadata:     metadata,
	})
	if err != nil {
		return err
	}

	// Every plan agrees the resource is gone at source — delete everywhere.
	if len(docs) == 0 && len(missing) > 0 {
		return idx.handleDelete(ctx, RebuildPayload{
			ResourceType: resourceType,
			ResourceID:   resourceID,
		})
	}
	// Plans disagree on existence: a transient source inconsistency or a
	// broken plan. Fail the build without writing — the stale mark survives
	// and the retry converges on the source's real state.
	if len(missing) > 0 {
		return fmt.Errorf("plans for %s/%s disagree on existence: version(s) %v returned no data; leaving stale for retry", resourceType, resourceID, missing)
	}

	var allRelations []model.VersionedResource
	var parents []model.Resource
	seenParents := make(map[model.Resource]bool)
	for _, vd := range docs {
		allRelations = append(allRelations, vd.doc.Relations...)
		for _, p := range vd.doc.Parents {
			if !seenParents[p] {
				seenParents[p] = true
				parents = append(parents, p)
			}
		}
	}

	for _, vd := range docs {
		indexName := IndexName(resourceType, vd.version)
		if err := idx.es.Upsert(ctx, indexName, resourceID, vd.doc.Doc, occVersion); err != nil {
			// An OCC loss is benign: a concurrent build with a newer Build
			// Sequence already wrote fresher data to this index. The
			// seq-guarded finish (ClearStale or FinishOwned) keeps recovery
			// correct if the winner served a different change.
			if !errors.Is(err, ErrVersionConflict) {
				return fmt.Errorf("upsert %s/%s to %s: %w", resourceType, resourceID, indexName, err)
			}
			slog.Debug("build superseded by newer write",
				slog.String("type", resourceType), slog.String("id", resourceID), slog.String("index", indexName))
		}
	}

	// Store each version's edges at this build's Build Sequence — also for a
	// version whose write lost above: the Store keeps, per version, the set
	// of the highest Build Sequence, as Elasticsearch keeps the document
	// (ADR 0002). This build ran every configured plan, so it declares their
	// versions, and the sets of versions the config no longer has are
	// dropped.
	sets := make([]EdgeSet, len(docs))
	for i, vd := range docs {
		sets[i] = EdgeSet{SchemaVersion: vd.version, Children: relationResources(vd.doc.Relations)}
	}
	declared := make([]int, len(plans))
	for i, p := range plans {
		declared[i] = p.Version
	}
	if err := idx.st.ReplaceEdges(ctx, model.Resource{Type: resourceType, Id: resourceID}, occVersion, sets, declared); err != nil {
		return fmt.Errorf("replace edges for %s/%s: %w", resourceType, resourceID, err)
	}

	// Reverse-relation discovery (ADR 0006): mark-first schedule of the
	// Parents the Plans derived from the Child's own data, unioned across
	// every Schema Version's plan.
	if err := idx.scheduleBuild(ctx, parents, metadata); err != nil {
		return err
	}

	// Drift check (ADR 0002). start is the Change Sequence value BeginBuild
	// took before the fetches. A child whose change_seq exceeds it had a
	// change accepted after the build started that the fetch may have missed
	// — e.g. a child only this build found, whose fanout ran before this
	// build stored its edge and so could not reach us. Re-schedule (mark
	// first) to converge. An owned build is the root's live
	// owner, so its re-mark submits nothing and its FinishOwned runs the
	// follow-up; an unowned one claims the root and submits it, unless
	// another build owns it. The root is not checked: its BeginBuild precedes
	// its own fetch, so a root change numbered below start is seen by the
	// fetch, and one above it bumped stale_seq, so the guarded finish leaves
	// the mark for the follow-up build.
	if len(allRelations) > 0 {
		checks := make([]ChangeCheck, len(allRelations))
		for i, r := range allRelations {
			checks[i] = ChangeCheck{Resource: r.Resource, Start: start}
		}
		drift, err := idx.st.AnyChangedSince(ctx, checks)
		if err != nil {
			return fmt.Errorf("drift check for %s/%s: %w", resourceType, resourceID, err)
		}
		if drift {
			slog.Info("child drift detected, re-enqueueing build",
				slog.String("type", resourceType),
				slog.String("id", resourceID),
			)
			if err := idx.scheduleBuild(ctx, []model.Resource{{Type: resourceType, Id: resourceID}}, metadata); err != nil {
				return fmt.Errorf("re-schedule after drift for %s/%s: %w", resourceType, resourceID, err)
			}
		}
	}

	return nil
}

// relationResources is the resources of relations, without their versions:
// an edge set's Children.
func relationResources(relations []model.VersionedResource) []model.Resource {
	out := make([]model.Resource, len(relations))
	for i, r := range relations {
		out[i] = r.Resource
	}
	return out
}

func (idx *Indexer) rebuild(ctx context.Context, params RebuildArgs, resume rebuildResume) error {
	if len(params.ResourceIDs) > 0 {
		if resume.start != nil {
			slog.Warn("rebuild cursor ignored for a targeted rebuild; its input is bounded, so restarting from scratch is the recovery",
				slog.String("type", params.ResourceType),
				slog.Int("plan_version", resume.start.PlanVersion))
		}
		return idx.rebuildByIDs(ctx, params)
	}
	return idx.rebuildAll(ctx, params, resume)
}

func (idx *Indexer) rebuildByIDs(ctx context.Context, params RebuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans, err := idx.plansForRebuild(params)
	if err != nil {
		return err
	}
	expected := 0
	for _, p := range plans {
		if p.Executer != nil {
			expected++
		}
	}

	fl := newRebuildFlusher(idx, params.ResourceType, params.Metadata)

	for _, id := range params.ResourceIDs {
		if ctx.Err() != nil {
			fl.salvage(ctx)
			return ctx.Err()
		}

		root := model.Resource{Type: params.ResourceType, Id: id}

		begun, err := idx.st.BeginBuild(ctx, root, 0)
		if err != nil {
			logger.Warn("failed to begin build", slog.String("id", id), slog.String("error", err.Error()))
			fl.fail(ctx, id)
			continue
		}
		// BeginBuild precedes this root's fetch, so its start covers the
		// root: only its children are checked.
		fl.begin(id, begun, driftBase{start: begun.Start}, expected)

		// Same existence rule as the live path (buildOne): all selected plans
		// run first, and only unanimity decides — a nil from one version must
		// not delete what another version's plan just built.
		docs, missing, planErr := executeAllPlans(ctx, plans, projection.BuildRequest{
			ResourceType: params.ResourceType,
			ResourceID:   id,
			Metadata:     params.Metadata,
		})
		if planErr != nil {
			logger.Warn("plan execution failed", slog.String("id", id), slog.String("error", planErr.Error()))
			fl.fail(ctx, id)
			continue
		}

		// Every selected plan agrees: gone at source — delete from all versions.
		if len(docs) == 0 && len(missing) > 0 {
			fl.discard(id)
			if err := idx.handleDelete(ctx, RebuildPayload{
				ResourceType: params.ResourceType,
				ResourceID:   id,
			}); err != nil {
				logger.Warn("delete missing resource", slog.String("id", id), slog.String("error", err.Error()))
				fl.fail(ctx, id)
			}
			continue
		}
		if len(missing) > 0 {
			logger.Warn("plans disagree on existence; leaving resource stale for retry",
				slog.String("id", id), slog.Any("versions_without_data", missing))
			fl.fail(ctx, id)
			continue
		}

		for _, vd := range docs {
			if err := fl.add(ctx, BulkItem{
				Index:   IndexName(params.ResourceType, vd.version),
				ID:      id,
				Doc:     vd.doc.Doc,
				Version: begun.BuildIdx,
			}, vd.version, vd.doc.Relations); err != nil {
				fl.salvage(ctx)
				return err
			}
		}
	}

	if err := fl.finish(ctx); err != nil {
		fl.salvage(ctx)
		return err
	}

	logger.Info("targeted rebuild complete", slog.Int("total", len(params.ResourceIDs)), slog.Int("failed", fl.failed))
	return fl.errorIfFailed()
}

func (idx *Indexer) rebuildAll(ctx context.Context, params RebuildArgs, resume rebuildResume) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans, err := idx.plansForRebuild(params)
	if err != nil {
		return err
	}

	// Only a walk with exactly one active plan has resumable positions. With
	// several, a resource first seen by an early plan stays unsettled until
	// the later plans' documents land, so every mid-walk position steps over
	// begun-but-unsettled resources — and an attempt resuming past one whose
	// remaining listing no longer emits it would leave it with some versions'
	// documents and edge sets refreshed, others not, and no stale mark. With
	// one plan, a page boundary is reached only once its resources have
	// settled or been marked stale.
	activePlans, activeIdx := 0, -1
	for i, p := range plans {
		if p.Executer != nil {
			activePlans++
			activeIdx = i
		}
	}
	resumable := activePlans == 1

	// Resolve the resume cursor. Anything this walk cannot address — several
	// active plans, or a version it does not run (the config or the selector
	// changed between attempts) — restarts from scratch: that is always safe,
	// resuming from a wrong position is not.
	startToken := ""
	switch {
	case resume.start == nil:
	case !resumable:
		logger.Warn("rebuild cursor ignored; resumable walks have exactly one active plan; restarting walk from scratch",
			slog.Int("plan_version", resume.start.PlanVersion),
			slog.Int("active_plans", activePlans))
	case resume.start.PlanVersion != plans[activeIdx].Version:
		logger.Warn("rebuild cursor names a version this walk does not run; restarting walk from scratch",
			slog.Int("plan_version", resume.start.PlanVersion),
			slog.Int("walk_plan_version", plans[activeIdx].Version))
	default:
		startToken = resume.start.PageToken
		logger.Info("resuming rebuild walk from cursor",
			slog.Int("plan_version", resume.start.PlanVersion),
			slog.String("page_token", startToken))
	}

	fl := newRebuildFlusher(idx, params.ResourceType, params.Metadata)

	// completed is the last fully consumed page boundary. The flusher's
	// afterFlush hook checkpoints it: right after a flush, everything before
	// that boundary is durably written and settled. Mid-page flushes
	// checkpoint the previous boundary — conservative, never ahead of what was
	// flushed.
	checkpointing := resumable && resume.checkpoint != nil
	var completed *RebuildCursor
	if checkpointing {
		fl.afterFlush = func() {
			if completed != nil {
				resume.checkpoint(*completed)
			}
		}
	}

	for planIdx, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		// Documents a resource first seen in this walk still expects: this
		// plan plus the active plans after it — earlier walks can no longer
		// emit it.
		expected := 0
		for _, p := range plans[planIdx:] {
			if p.Executer != nil {
				expected++
			}
		}

		// The walk start: every root this walk begins, and every child its
		// pages carry, is drift-checked from here. It must precede Execute —
		// the pipeline may fetch pages as soon as it is called, ahead of the
		// loop below and so ahead of the roots' BeginBuild.
		walkStart, err := idx.st.NextChangeSeq(ctx)
		if err != nil {
			fl.salvage(ctx)
			return fmt.Errorf("walk start for %s v%d: %w", params.ResourceType, plan.Version, err)
		}

		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: params.ResourceType,
			ResourceID:   "",
			Metadata:     params.Metadata,
			// Non-empty only on a resumable walk, whose sole active plan this
			// is; every other walk starts its plans at the listing's head.
			PageToken: startToken,
		})

		for page := range ch {
			if page.Err != nil {
				fl.salvage(ctx)
				return fmt.Errorf("plan execution for %s v%d: %w", params.ResourceType, plan.Version, page.Err)
			}

			for _, doc := range page.Items {
				if err := ctx.Err(); err != nil {
					fl.salvage(ctx)
					return err
				}

				id := doc.Root.Id
				if id == "" {
					logger.Warn("plan emitted a document without a root id; skipping",
						slog.Int("plan_version", plan.Version))
					continue
				}

				// Source listed the resource but returned no data — hand it
				// to the delete path, which removes every version's document.
				if doc.Doc == nil {
					fl.discard(id)
					if err := idx.handleDelete(ctx, RebuildPayload{
						ResourceType: params.ResourceType,
						ResourceID:   id,
					}); err != nil {
						logger.Warn("delete missing resource", slog.String("id", id), slog.String("error", err.Error()))
						fl.fail(ctx, id)
					}
					continue
				}

				if !fl.tracked(id) {
					begun, err := idx.st.BeginBuild(ctx, doc.Root, 0)
					if err != nil {
						logger.Warn("failed to begin build", slog.String("id", id), slog.String("error", err.Error()))
						fl.fail(ctx, id)
						continue
					}
					// Measured from the walk start, not begun.Start: the
					// root's page was fetched before this BeginBuild, so the
					// root checks itself too (driftBase.checkRoot). Begun
					// only on first sighting, a root seen by several plans
					// keeps the start of the walk that first fetched it.
					fl.begin(id, begun, driftBase{start: walkStart, checkRoot: true}, expected)
				}

				occVersion, ok := fl.occ(id)
				if !ok {
					continue
				}

				if err := fl.add(ctx, BulkItem{
					Index:   IndexName(params.ResourceType, plan.Version),
					ID:      id,
					Doc:     doc.Doc,
					Version: occVersion,
				}, plan.Version, doc.Relations); err != nil {
					fl.salvage(ctx)
					return err
				}
			}

			// The page is fully consumed: its token becomes the checkpointable
			// boundary at the next flush. Non-string tokens (a hand-rolled
			// executer) cannot be resumed from and are never checkpointed —
			// the walk still runs, it just restarts this plan on retry.
			if tok, ok := page.NextPageToken.(string); ok && checkpointing {
				completed = &RebuildCursor{PlanVersion: plan.Version, PageToken: tok}
			}
		}

		// The page channel closed. That means the listing was exhausted — or
		// the context ended and the pipeline stopped: a cancelled producer
		// abandons its terminal error when no receiver is parked on the
		// channel, which is exactly where this loop is while it flushes. An
		// ended context here therefore means an unwalked remainder, and
		// reporting success would let a caller (the RunRebuild activity) record
		// a half-done backfill as finished.
		if err := ctx.Err(); err != nil {
			fl.salvage(ctx)
			return err
		}
	}

	if err := fl.finish(ctx); err != nil {
		fl.salvage(ctx)
		return err
	}

	logger.Info("rebuild complete", slog.Int("failed", fl.failed))
	return fl.errorIfFailed()
}
