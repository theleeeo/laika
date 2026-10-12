package core

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

type BuildArgs struct {
	ResourceType string   `json:"resource_type"`
	ResourceIds  []string `json:"resource_ids,omitempty"`
	// Metadata is the fetch context of the ids that build without an owner
	// token: a build that owns nothing fetches with its caller's metadata.
	// An owned id ignores it and fetches with its row's metadata as
	// BeginBuilds returns it (BuildBegun.Metadata), so owned builds are
	// submitted without any.
	Metadata map[string]string `json:"metadata,omitempty"`
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
	// Pacing paces an all-of-type walk (ResourceSelector.Pacing); a
	// targeted rebuild ignores it.
	Pacing *WalkPacing `json:"pacing,omitempty"`
}

// Build builds each of params' ids, in batches (ADR 0014): it cuts them into
// chunks of the type's batch size (batchSize), begins each chunk in one
// BeginBuilds in BeginInsert, groups the begun ids by the metadata they fetch
// with, and asks each plan about each group once. Each id keeps its own
// Build Sequence, write, edge set and finish (buildBatch).
//
// An id with an owner token (OwnerTokens) is an owned inline build: its begin
// renews its lease, its fetches run with the row's metadata as BeginBuilds
// returns it, and a success finishes it with FinishOwned. While the call
// runs, every owned id it hasn't finished — the ones still waiting for their
// batch included — is renewed (RenewOwners) every third of the OwnerLease,
// from one interval after the call starts until the id finishes, so a build
// of any length keeps its rows (ADR 0014). A failed renewal is logged and
// retried at the next tick, releasing nothing; an id a renewal reports lost
// is renewed no more and built on, its finish matching nothing, as a lapsed
// build's does. The renewal stops before Build returns. An id without one
// owns nothing: it fetches with params.Metadata and finishes with
// ClearStale. The build follow-ups the call's finishes hand on are collected
// and submitted together when Build returns, per type in tasks of at most
// the type's batch size (submitOwnedBuilds); a delete follow-up is submitted
// as its finish hands it on.
//
// A plan's nil is its own Schema Version's answer (ADR 0013): an id some of
// whose plans returned a document and others nil has those documents
// written and, at its Build Sequence, the nil versions' documents deleted
// and their edge sets emptied, and finishes as above, keeping its row. An id
// whose plans all returned nil is gone at source: its documents are deleted,
// and it finishes with DeleteResourceIfSeq instead, which removes its row —
// owned or not, tombstone or not — and hands on the follow-up when a change
// moved the mark.
//
// A failed owned id — a type no longer configured or without plans included
// — keeps its mark and is released through releaseFailed, which backs its
// row off, so the sweep rebuilds it once the backoff has passed, or the next
// change claims it first. A plan's error for one id fails that id alone. A
// batch whose BeginBuilds or plan call fails fails every id in it: one Error
// line names the type, the metadata, the count, the first ten ids and the
// error, and its owned ids are released with a backoff, their per-row lines
// at Debug. A batch of one id fails as that id alone, its line carrying its
// attempt count and logged at Error from the fifth attempt in a row, as any
// single failure's is. A failure after the plans — the bulk request, the
// Parent mark, the drift check — fails each id it reaches with that id's own
// line. Every owned id left unfinished when ctx ends, and one whose
// failure the ending caused, is released without a backoff, and Build
// returns ctx's error. Build logs every failure of an owned id itself; the
// error it returns — the type unconfigured or without plans, or ctx's end —
// is for a caller whose ids own nothing, and a caller of an owned build need
// not log it again.
func (idx *Indexer) Build(ctx context.Context, params BuildArgs) error {
	cfg := idx.resources.Get(params.ResourceType)
	if cfg == nil {
		err := fmt.Errorf("resource type %q: %w", params.ResourceType, ErrUnknownResource)
		idx.releaseFailed(ctx, "build", params.owned(params.ResourceIds), err)
		return err
	}

	plans := idx.plans[params.ResourceType]
	if len(plans) == 0 {
		err := fmt.Errorf("no plans for resource type %q", params.ResourceType)
		idx.releaseFailed(ctx, "build", params.owned(params.ResourceIds), err)
		return err
	}

	// Every owned id keeps its lease while the call runs, renewed every third
	// of the lease until it finishes; the renewal stops before Build returns.
	renewal := idx.startBuildRenewal(ctx, params.owned(params.ResourceIds))
	defer renewal.stop()

	b := &batchBuild{
		idx:     idx,
		params:  params,
		plans:   plans,
		renewal: renewal,
		logger:  slog.With(slog.String("type", params.ResourceType)),
	}
	// The collected build follow-ups go out however the call ends: each was
	// re-claimed by its finish, and a shed submission releases it.
	defer func() { idx.submitOwnedBuilds(ctx, b.followUps) }()

	batches := idBatches(params.ResourceIds, idx.batchSize(params.ResourceType))
	total := 0
	for i, ids := range batches {
		if ctx.Err() != nil {
			var rest []string
			for _, later := range batches[i:] {
				rest = append(rest, later...)
			}
			b.endedUnfinished(ctx, rest)
			return ctx.Err()
		}
		b.buildBatch(ctx, ids)
		total += len(ids)
	}

	if b.failed > 0 {
		b.logger.Warn("build complete with failures", slog.Int("total", total), slog.Int("failed", b.failed))
	}
	if b.cancelled {
		return ctx.Err()
	}
	return nil
}

// endedUnfinished releases the owned ids of ids, which ctx's end left
// unbuilt, without a backoff, logging them once at Info; their marks stay.
func (b *batchBuild) endedUnfinished(ctx context.Context, ids []string) {
	for _, id := range ids {
		b.renewal.finished(id)
	}
	if unfinished := b.params.owned(ids); len(unfinished) > 0 {
		b.logger.Info("build ended by cancellation or shutdown; its unfinished ids are released without a backoff, their marks stay",
			slog.Int("count", len(unfinished)), slog.String("error", ctx.Err().Error()))
		b.idx.releaseOwners(ctx, unfinished)
	}
}

// batchBuild is one Build call's state across its batches.
type batchBuild struct {
	idx     *Indexer
	params  BuildArgs
	plans   []projection.Plan
	renewal *buildRenewal
	logger  *slog.Logger

	// failed counts the call's failed ids; cancelled records a failure
	// reached once ctx had ended, so Build returns ctx's error.
	failed    int
	cancelled bool
	// followUps are the build follow-ups the call's finishes handed on,
	// submitted together when the call ends.
	followUps []Owned
}

// batchEntry is one begun id of a batch.
type batchEntry struct {
	id       string
	res      model.Resource
	token    int64
	begun    BuildBegun
	metadata map[string]string
	outcome  idOutcome
	// done is set once the id has finished or failed.
	done bool
}

// metadataGroup is a batch's ids that fetch with one metadata.
type metadataGroup struct {
	metadata map[string]string
	entries  []*batchEntry
}

// failedBatchLogIDs is how many of a failed batch's ids its Error line names.
const failedBatchLogIDs = 10

// buildBatch builds one chunk of ids. It begins them in one BeginBuilds,
// groups them by fetch metadata — the row's for an owned id, the caller's
// otherwise; maps compare by content, nil equal to empty — and asks every
// plan about each group once (executeAllPlans). A failed BeginBuilds fails
// the chunk, a failed plan call its group (failBatch). The ids every plan
// answered are then written in buildOn's order.
func (b *batchBuild) buildBatch(ctx context.Context, ids []string) {
	begins := make([]Begin, len(ids))
	for i, id := range ids {
		begins[i] = Begin{Resource: model.Resource{Type: b.params.ResourceType, Id: id}, Token: b.params.OwnerTokens[id]}
	}
	begun, err := b.idx.st.BeginBuilds(ctx, BeginInsert, begins)
	if err == nil && len(begun) != len(begins) {
		err = fmt.Errorf("store answered %d of %d begins", len(begun), len(begins))
	}
	if err != nil {
		// The metadata is known only when every id owns nothing: then it is
		// the caller's.
		b.failBatch(ctx, ids, b.params.Metadata, len(b.params.owned(ids)) == 0, fmt.Errorf("begin build: %w", err))
		return
	}

	var groups []*metadataGroup
	entries := make([]*batchEntry, len(ids))
	for i, id := range ids {
		e := &batchEntry{id: id, res: begins[i].Resource, token: begins[i].Token, begun: begun[i]}
		// An owned build runs with what the row holds when it begins, so one
		// that waited in the pool queue or the sweep picks up metadata
		// registered meanwhile; one that owns nothing runs with its caller's.
		e.metadata = b.params.Metadata
		if e.token != 0 {
			e.metadata = begun[i].Metadata
		}
		entries[i] = e
		var g *metadataGroup
		for _, cand := range groups {
			if maps.Equal(cand.metadata, e.metadata) {
				g = cand
				break
			}
		}
		if g == nil {
			g = &metadataGroup{metadata: e.metadata}
			groups = append(groups, g)
		}
		g.entries = append(g.entries, e)
	}

	// Every plan runs for a group before the batch writes anything: whether
	// a row stays depends on every plan's outcome, and the drift check and
	// the Parents (ADR 0006) are unioned over every plan's document.
	var answered []*batchEntry
	for _, g := range groups {
		groupIDs := make([]string, len(g.entries))
		for i, e := range g.entries {
			groupIDs[i] = e.id
		}
		outcomes, err := executeAllPlans(ctx, b.plans, projection.BuildRequest{
			ResourceType: b.params.ResourceType,
			ResourceIDs:  groupIDs,
			Metadata:     g.metadata,
		})
		if err != nil {
			b.failBatch(ctx, groupIDs, g.metadata, true, err)
			for _, e := range g.entries {
				e.done = true
			}
			continue
		}
		for _, e := range g.entries {
			e.outcome = outcomes[e.id]
			answered = append(answered, e)
		}
	}
	b.buildOn(ctx, answered)
}

// buildOn writes and finishes a batch's answered entries, each at its own
// Build Sequence, in the order a single build's writes go:
//
//  1. Each id every plan returned nil for is deleted (handleDelete) and
//     finishes with DeleteResourceIfSeq.
//  2. The others' documents go in one BulkUpsert. A rejected item fails its
//     id only; a version conflict is an OCC loss, which the backend doesn't
//     report.
//  3. Each nil version's document is deleted.
//  4. Each id's edge sets are replaced (ReplaceEdges).
//  5. The Parents of every id (ADR 0006) are marked in one scheduleBuild;
//     then one drift check covers every id's fetched children, narrowing to
//     the drifted roots on a hit, which are re-marked in one scheduleBuild.
//     A failure of either fails every id not yet finished.
//  6. Each id finishes: FinishOwned, or ClearStale for one that owns nothing.
//
// An id one step fails takes no further step.
func (b *batchBuild) buildOn(ctx context.Context, entries []*batchEntry) {
	resourceType := b.params.ResourceType
	var standing []*batchEntry
	for _, e := range entries {
		switch {
		case e.outcome.err != nil:
			b.fail(ctx, e, "build failed", e.outcome.err)
		case e.begun.BuildIdx <= 0:
			b.fail(ctx, e, "build failed", fmt.Errorf("invalid occ version %d for %s/%s", e.begun.BuildIdx, resourceType, e.id))
		case len(e.outcome.docs) == 0 && len(e.outcome.missing) > 0:
			// 1. Every plan finds the resource gone at source — delete
			// everywhere.
			b.finishGone(ctx, e)
		default:
			standing = append(standing, e)
		}
	}

	// 2. One bulk write of every standing id's documents, each at its own
	// Build Sequence.
	var items []BulkItem
	for _, e := range standing {
		for _, vd := range e.outcome.docs {
			items = append(items, BulkItem{
				Index:   IndexName(resourceType, vd.version),
				ID:      e.id,
				Doc:     vd.doc.Doc,
				Version: e.begun.BuildIdx,
			})
		}
	}
	if len(items) > 0 {
		failures, err := b.idx.es.BulkUpsert(ctx, items)
		if err != nil {
			// A request-level failure: nothing can be assumed written.
			var written []*batchEntry
			for _, e := range standing {
				if len(e.outcome.docs) > 0 {
					written = append(written, e)
				}
			}
			b.failMany(ctx, written, fmt.Errorf("bulk upsert: %w", err))
		}
		rejected := make(map[string]BulkFailure, len(failures))
		for _, f := range failures {
			if _, seen := rejected[f.ID]; !seen {
				rejected[f.ID] = f
			}
		}
		for _, e := range standing {
			if f, ok := rejected[e.id]; ok && !e.done {
				b.fail(ctx, e, "build failed", fmt.Errorf("upsert %s/%s to %s rejected (%d): %s", resourceType, e.id, f.Index, f.Status, f.Reason))
			}
		}
	}

	// 3. The versions whose plans returned nil while others returned a
	// document (ADR 0013): each such version's document goes, at the id's
	// Build Sequence, while the row stays for the versions with one. A
	// delete a newer document rejects is an OCC loss, which
	// SearchBackend.Delete reports as nil; any error fails the id before its
	// edges are replaced, leaving the mark, as a failed write does.
	for _, e := range standing {
		if e.done {
			continue
		}
		for _, v := range e.outcome.missing {
			indexName := IndexName(resourceType, v)
			if err := b.idx.es.Delete(ctx, indexName, e.id, e.begun.BuildIdx); err != nil {
				b.fail(ctx, e, "build failed", fmt.Errorf("delete %s/%s from %s, whose plan returned no data: %w", resourceType, e.id, indexName, err))
				break
			}
		}
	}

	// 4. Store each version's edges at the id's Build Sequence — also for a
	// version whose write or delete lost an OCC race: the Store keeps, per
	// version, the set of the highest Build Sequence, as Elasticsearch keeps
	// the document (ADR 0002). A version whose plan returned nil gets an
	// empty set, which replaces its stored children; only declaring it would
	// leave them. The build ran every configured plan, so it declares their
	// versions, and the sets of versions the config no longer has are
	// dropped. The same write stores the plans' report — the first non-empty
	// one of the versions with a document, in plan order — as the row's
	// metadata if the row has none, before the drift check and the cascade:
	// a later owned build of the resource that no registration gave metadata
	// fetches as it.
	declared := make([]int, len(b.plans))
	for i, p := range b.plans {
		declared[i] = p.Version
	}
	for _, e := range standing {
		if e.done {
			continue
		}
		docs, missing := e.outcome.docs, e.outcome.missing
		sets := make([]EdgeSet, 0, len(docs)+len(missing))
		for _, vd := range docs {
			sets = append(sets, EdgeSet{SchemaVersion: vd.version, Children: vd.doc.Relations})
		}
		for _, v := range missing {
			sets = append(sets, EdgeSet{SchemaVersion: v})
		}
		if err := b.idx.st.ReplaceEdges(ctx, e.res, e.begun.BuildIdx, sets, declared, firstReport(docs)); err != nil {
			b.fail(ctx, e, "build failed", fmt.Errorf("replace edges for %s/%s: %w", resourceType, e.id, err))
		}
	}

	standing = slices.DeleteFunc(standing, func(e *batchEntry) bool { return e.done })
	if len(standing) == 0 {
		return
	}

	// 5. Reverse-relation discovery (ADR 0006): one mark-first schedule of
	// the Parents the plans derived from the ids' own data, unioned across
	// every id and every Schema Version's plan that returned a document. The
	// mark carries none of the batch's metadata: a Parent's build runs with
	// the Parent's own.
	var parents []model.Resource
	seenParents := make(map[model.Resource]bool)
	for _, e := range standing {
		for _, vd := range e.outcome.docs {
			for _, p := range vd.doc.Parents {
				if !seenParents[p] {
					seenParents[p] = true
					parents = append(parents, p)
				}
			}
		}
	}
	if err := b.idx.scheduleBuild(ctx, parents); err != nil {
		b.failMany(ctx, standing, err)
		return
	}
	if err := b.checkDrift(ctx, standing); err != nil {
		b.failMany(ctx, standing, err)
		return
	}

	// 6. Each id finishes, seq-guarded by its own stale_seq.
	for _, e := range standing {
		b.finish(ctx, e)
	}
}

// checkDrift is the ADR 0002 drift check of a batch's standing ids, on the
// Change Sequence: one query over every child each id's plans fetched, each
// measured from its id's start, the Change Sequence value its begin took
// before the fetches. A child whose change_seq exceeds it had a change
// accepted after the build started that the fetch may have missed — e.g. a
// child only this build found, whose fanout ran before this build stored its
// edge and so could not reach the root. On a hit the check narrows with one
// query per id, as the rebuild flusher's does, and the drifted roots are
// re-scheduled (mark first) in one scheduleBuild to converge; a hit no
// narrowing query confirms re-schedules every checked root. An owned build
// is its root's live owner, so its re-mark submits nothing and its
// FinishOwned runs the follow-up; an unowned one claims the root and submits
// it, unless another build owns it. A root is not checked: its begin
// precedes its own fetch, so a root change numbered below its start is seen
// by the fetch, and one above it moved stale_seq, so the guarded finish
// leaves the mark for the follow-up build. The re-mark leaves the roots'
// metadata alone, so a registration that committed after the edge write
// keeps its metadata for the follow-up (Q16).
func (b *batchBuild) checkDrift(ctx context.Context, standing []*batchEntry) error {
	var all []ChangeCheck
	var checked []*batchEntry
	perID := make(map[*batchEntry][]ChangeCheck)
	for _, e := range standing {
		for _, vd := range e.outcome.docs {
			for _, r := range vd.doc.Relations {
				perID[e] = append(perID[e], ChangeCheck{Resource: r, Start: e.begun.Start})
			}
		}
		if len(perID[e]) > 0 {
			checked = append(checked, e)
			all = append(all, perID[e]...)
		}
	}
	if len(all) == 0 {
		return nil
	}
	drift, err := b.idx.st.AnyChangedSince(ctx, all)
	if err != nil {
		return fmt.Errorf("drift check: %w", err)
	}
	if !drift {
		return nil
	}
	var drifted []*batchEntry
	for _, e := range checked {
		hit := true
		if len(checked) > 1 {
			if hit, err = b.idx.st.AnyChangedSince(ctx, perID[e]); err != nil {
				return fmt.Errorf("drift check of %s/%s: %w", e.res.Type, e.id, err)
			}
		}
		if hit {
			drifted = append(drifted, e)
		}
	}
	// A hit no narrowing query confirms — a checked row gone between the
	// queries reads as unchanged — re-marks every checked root: a redundant
	// rebuild is safe, a missed one isn't.
	if len(drifted) == 0 {
		drifted = checked
	}
	roots := make([]model.Resource, len(drifted))
	for i, e := range drifted {
		b.logger.Info("child drift detected, re-enqueueing build", slog.String("id", e.id))
		roots[i] = e.res
	}
	if err := b.idx.scheduleBuild(ctx, roots); err != nil {
		return fmt.Errorf("re-schedule after drift: %w", err)
	}
	return nil
}

// finishGone deletes an id every plan returned nil for — its documents and
// edge sets at its Build Sequence — and finishes it with
// DeleteResourceIfSeq, which removes its row. Race-safe as the other
// finishes: a change that moved stale_seq mid-build keeps the row, and an
// owner re-claims it for the follow-up — a build for a recreate. A token of
// 0 re-claims nothing.
func (b *batchBuild) finishGone(ctx context.Context, e *batchEntry) {
	if err := b.idx.handleDelete(ctx, RebuildPayload{ResourceType: e.res.Type, ResourceID: e.id}, e.begun.BuildIdx); err != nil {
		b.fail(ctx, e, "build failed", err)
		return
	}
	e.done = true
	b.renewal.finished(e.id)
	fu, err := b.idx.st.DeleteResourceIfSeq(ctx, e.res, e.begun.StaleSeq, e.token)
	if err != nil {
		b.fail(ctx, e, "removing the row of a resource gone at source failed; it stays until a build of it finds the resource gone again",
			fmt.Errorf("removing the row of a resource gone at source: %w", err))
		return
	}
	b.followUp(ctx, e.res, fu)
}

// finish ends a built id's build, guarded by its own stale_seq.
func (b *batchBuild) finish(ctx context.Context, e *batchEntry) {
	e.done = true
	if e.token == 0 {
		// Race-safe: a no-op if a newer change bumped stale_seq mid-build —
		// including the batch's own drift re-mark, which must survive this
		// clear.
		if err := b.idx.st.ClearStale(ctx, e.res, e.begun.StaleSeq); err != nil {
			b.logger.Warn("clear stale failed; sweep may rebuild redundantly",
				slog.String("id", e.id), slog.String("error", err.Error()))
		}
		return
	}
	// Race-safe the same way: a change that moved stale_seq mid-build — a
	// registration this owner kept from submitting, or the batch's own drift
	// re-mark — re-claims the row for one follow-up instead.
	b.renewal.finished(e.id)
	fu, err := b.idx.st.FinishOwned(ctx, e.res, e.begun.StaleSeq, e.token)
	if err != nil {
		b.fail(ctx, e, "finishing owned build failed", fmt.Errorf("finish owned build: %w", err))
		return
	}
	b.followUp(ctx, e.res, fu)
}

// followUp takes on the follow-up a finish handed on: a build is collected
// for the call's one submission, a delete submitted now.
func (b *batchBuild) followUp(ctx context.Context, res model.Resource, fu FollowUp) {
	switch {
	case fu.Token == 0:
	case fu.Deleted:
		b.idx.submitFollowUp(ctx, res, fu)
	default:
		b.followUps = append(b.followUps, Owned{Resource: res, Token: fu.Token})
	}
}

// fail ends e's build after err: an owned id is released through
// releaseFailed, which logs it with its attempt count; an id that owns
// nothing is logged here as msg.
func (b *batchBuild) fail(ctx context.Context, e *batchEntry, msg string, err error) {
	e.done = true
	b.failed++
	if ctx.Err() != nil {
		b.cancelled = true
	}
	b.renewal.finished(e.id)
	if e.token == 0 {
		b.logger.Warn(msg, slog.String("id", e.id), slog.String("error", err.Error()))
		return
	}
	b.idx.releaseFailed(ctx, "build", []Owned{{Resource: e.res, Token: e.token}}, err)
}

// failMany fails entries after one err, each logged on its own line as fail
// does, their owned ids released in one releaseFailed.
func (b *batchBuild) failMany(ctx context.Context, entries []*batchEntry, err error) {
	var owned []Owned
	for _, e := range entries {
		e.done = true
		b.failed++
		b.renewal.finished(e.id)
		if e.token == 0 {
			b.logger.Warn("build failed", slog.String("id", e.id), slog.String("error", err.Error()))
			continue
		}
		owned = append(owned, Owned{Resource: e.res, Token: e.token})
	}
	if ctx.Err() != nil {
		b.cancelled = true
	}
	b.idx.releaseFailed(ctx, "build", owned, err)
}

// failBatch fails every id of a batch whose BeginBuilds or plan call failed
// with err. Once ctx has ended the failure is not the batch's: its owned ids
// are released without a backoff, logged once at Info. Otherwise one Error
// line names the type, the metadata when it is known (knownMetadata: a
// plan call's group, or a failed begin of ids that all own nothing), the
// count, the first ids and the error, and the owned ids are
// released with a backoff, their per-row lines at Debug.
//
// A batch of one id fails as that id alone (fail), its line naming its
// attempt count as any single failure's does.
func (b *batchBuild) failBatch(ctx context.Context, ids []string, metadata map[string]string, knownMetadata bool, err error) {
	if len(ids) == 1 {
		res := model.Resource{Type: b.params.ResourceType, Id: ids[0]}
		b.fail(ctx, &batchEntry{id: ids[0], res: res, token: b.params.OwnerTokens[ids[0]]}, "build failed", err)
		return
	}
	b.failed += len(ids)
	for _, id := range ids {
		b.renewal.finished(id)
	}
	owned := b.params.owned(ids)
	attrs := []any{slog.Int("count", len(ids)), slog.Any("ids", ids[:min(len(ids), failedBatchLogIDs)])}
	if knownMetadata {
		attrs = append(attrs, slog.Any("metadata", metadata))
	}
	attrs = append(attrs, slog.String("error", err.Error()))
	if ctx.Err() != nil {
		b.cancelled = true
		b.logger.Info("build batch ended by cancellation or shutdown; its owned ids are released without a backoff, their marks stay", attrs...)
		b.idx.releaseOwners(ctx, owned)
		return
	}
	b.logger.Error("build batch failed; every id in it failed, and its owned ids are released with a backoff for the stale sweep", attrs...)
	b.idx.releaseFailedBatch(ctx, "build", owned, err)
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

const (
	renewDuringBuildFailedMsg = "renewing an owned build's leases failed; retried at the next tick"
	renewDuringBuildLostMsg   = "build ownership lost while building; the build goes on, its finish matches nothing and the new owner's build serves the resource"
)

// buildRenewal renews the leases of an owned Build call's ids while the call
// runs (ADR 0014), so a build of any length keeps them: a delete registered
// meanwhile only marks the row, and the owner's follow-up deletes after the
// write. Its set is the call's owned ids not yet finished; one leaves it when
// Build finishes it (finished) or a renewal reports it lost. A nil
// buildRenewal — a call that owns nothing — renews nothing.
type buildRenewal struct {
	idx    *Indexer
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	set map[string]Owned // by id: a Build call is of one type
}

// startBuildRenewal starts renewing owned in a goroutine of its own, every
// third of the OwnerLease, the first one interval from now (BeginBuilds renews
// each id as it begins), until ctx ends or stop. It returns nil, starting
// nothing, when owned is empty.
func (idx *Indexer) startBuildRenewal(ctx context.Context, owned []Owned) *buildRenewal {
	if len(owned) == 0 {
		return nil
	}
	r := &buildRenewal{idx: idx, done: make(chan struct{}), set: make(map[string]Owned, len(owned))}
	for _, o := range owned {
		r.set[o.Id] = o
	}
	ctx, r.cancel = context.WithCancel(ctx)
	// A floor keeps an absurdly short lease from a ticker of zero, which
	// panics.
	ticks, stopTicker := idx.renewTicker(max(idx.ownerLease/3, time.Millisecond))
	go func() {
		defer close(r.done)
		defer stopTicker()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticks:
				r.renew(ctx)
			}
		}
	}()
	return r
}

// renew renews the set's ownerships once. A failed renewal is logged and
// changes nothing: the next tick retries the same set, and nothing is
// released or skipped — unlike renewOwners at a dequeue or in the sweep,
// which hold work back until it runs. An id the renewal reports lost leaves
// the set, logged once; its build goes on, and its finish matches nothing.
// One finished while the renewal ran isn't reported.
func (r *buildRenewal) renew(ctx context.Context) {
	r.mu.Lock()
	owned := make([]Owned, 0, len(r.set))
	for _, o := range r.set {
		owned = append(owned, o)
	}
	r.mu.Unlock()
	if len(owned) == 0 {
		return
	}

	held, err := r.idx.st.RenewOwners(ctx, owned)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn(renewDuringBuildFailedMsg, slog.String("type", owned[0].Type),
				slog.Int("count", len(owned)), slog.String("error", err.Error()))
		}
		return
	}
	kept := make(map[Owned]bool, len(held))
	for _, o := range held {
		kept[o] = true
	}
	var lost []Owned
	r.mu.Lock()
	for _, o := range owned {
		if kept[o] || r.set[o.Id] != o {
			continue
		}
		delete(r.set, o.Id)
		lost = append(lost, o)
	}
	r.mu.Unlock()
	for _, o := range lost {
		slog.Info(renewDuringBuildLostMsg,
			slog.String("type", o.Type), slog.String("id", o.Id), slog.Int64("token", o.Token))
	}
}

// finished takes id out of the set: Build is finishing it, or has failed or
// released it.
func (r *buildRenewal) finished(id string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.set, id)
	r.mu.Unlock()
}

// stop ends the renewal and waits for it, so none outlives the Build call.
func (r *buildRenewal) stop() {
	if r == nil {
		return
	}
	r.cancel()
	<-r.done
}

// versionedDoc pairs a plan's Schema Version with the document it built.
type versionedDoc struct {
	version int
	doc     projection.BuildDoc
}

// idOutcome is one asked id's outcome over every active plan: the documents
// its plans built and the versions whose plan returned nil, or the error of
// the first plan that failed it, which leaves it no documents and no nils.
type idOutcome struct {
	docs    []versionedDoc
	missing []int
	err     error
}

// executeAllPlans runs every active plan for req's ids and gives each asked
// id its outcome: its built documents and the versions whose plan returned
// no data, or its error. Each plan's answer is its own version's (ADR 0013):
// a document is that version's to write, a nil that version's to delete. An
// id one plan fails is failed whatever the others answer. Every plan runs
// before the caller writes anything; an error of a plan's call fails every
// asked id, and executeAllPlans returns it with no outcomes.
func executeAllPlans(ctx context.Context, plans []projection.Plan, req projection.BuildRequest) (map[string]idOutcome, error) {
	outcomes := make(map[string]idOutcome, len(req.ResourceIDs))
	for _, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		answers, err := executePlan(ctx, plan, req)
		if err != nil {
			return nil, err
		}
		for id, answer := range answers {
			o := outcomes[id]
			switch {
			case o.err != nil:
			case answer.Err != nil:
				o = idOutcome{err: answer.Err}
			case answer.Doc == nil:
				o.missing = append(o.missing, plan.Version)
			default:
				o.docs = append(o.docs, versionedDoc{version: plan.Version, doc: answer})
			}
			outcomes[id] = o
		}
	}
	return outcomes, nil
}

// firstReport is the resource's own metadata as its plans report it: the
// first non-empty BuildDoc.ResourceMetadata of docs, in plan order; nil when
// none reports any.
func firstReport(docs []versionedDoc) map[string]string {
	for _, vd := range docs {
		if len(vd.doc.ResourceMetadata) > 0 {
			return vd.doc.ResourceMetadata
		}
	}
	return nil
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

// rebuildByIDs builds the selected ids onto a rebuild flusher, a batch of
// the type's batch size at a time: each batch is begun in one BeginBuilds in
// BeginInsert, every token 0, and each selected plan is asked about it once.
// A batch whose BeginBuilds or plan call fails fails each of its ids, logged
// once for the batch; one the rebuild's context ended instead salvages what
// was begun and returns the context's error.
func (idx *Indexer) rebuildByIDs(ctx context.Context, params RebuildArgs) error {
	logger := slog.With(slog.String("type", params.ResourceType))

	plans, err := idx.plansForRebuild(params)
	if err != nil {
		return err
	}
	expected := countActive(plans)

	fl := newRebuildFlusher(idx, params.ResourceType)
	fl.mayDelete = idx.runsEveryActivePlan(params.ResourceType, plans)

	// failBatch fails each id of a batch the store or a plan call failed as
	// a whole, in one log line rather than one per id.
	failBatch := func(msg string, ids []string, err error) {
		logger.Error(msg,
			slog.Int("count", len(ids)),
			slog.Any("ids", ids[:min(len(ids), 10)]),
			slog.String("error", err.Error()))
		for _, id := range ids {
			fl.fail(ctx, id)
		}
	}

	// Each id is served once, however often the selector lists it (idBatches
	// drops a repeat): a repeat would begin it again over its flusher entry,
	// so a failed id could be re-begun, counted and marked again.
	total := 0
	for _, ids := range idBatches(params.ResourceIDs, idx.batchSize(params.ResourceType)) {
		if ctx.Err() != nil {
			fl.salvage(ctx)
			return ctx.Err()
		}
		total += len(ids)

		begins := make([]Begin, len(ids))
		for i, id := range ids {
			begins[i] = Begin{Resource: model.Resource{Type: params.ResourceType, Id: id}}
		}
		begun, err := idx.st.BeginBuilds(ctx, BeginInsert, begins)
		if err == nil && len(begun) != len(ids) {
			err = fmt.Errorf("the store returned %d begun builds for %d ids", len(begun), len(ids))
		}
		if err != nil {
			// A begin the rebuild's end cut short is no failed batch: its
			// ids were never begun, and a retry of the rebuild builds them.
			if ctx.Err() != nil {
				fl.salvage(ctx)
				return ctx.Err()
			}
			failBatch("failed to begin a batch of builds", ids, err)
			continue
		}
		// BeginBuilds precedes the batch's fetch, so each id's start covers
		// the id itself: only its children are checked.
		for i, id := range ids {
			fl.begin(id, begun[i], driftBase{start: begun[i].Start}, expected, false)
		}

		// Same existence rule as the live path (Build, ADR 0013): every
		// selected plan runs for the batch first, and each plan's outcome is
		// its own version's — a nil deletes that version's document and
		// empties its edge set, never another version's. An id a plan left
		// unanswered gets an error from executePlan, never a nil, and fails.
		outcomes, err := executeAllPlans(ctx, plans, projection.BuildRequest{
			ResourceType: params.ResourceType,
			ResourceIDs:  ids,
			Metadata:     params.Metadata,
		})
		if err != nil {
			// A plan call the rebuild's end cut short is no failed batch:
			// salvage marks the batch's begun ids with the rest.
			if ctx.Err() != nil {
				fl.salvage(ctx)
				return ctx.Err()
			}
			failBatch("plan execution failed for a batch", ids, err)
			continue
		}

		for i, id := range ids {
			outcome := outcomes[id]
			if outcome.err != nil {
				logger.Warn("plan execution failed", slog.String("id", id), slog.String("error", outcome.err.Error()))
				fl.fail(ctx, id)
				continue
			}

			// The nils count first, each its own version's outcome
			// (rebuildFlusher.gone), so the id settles when its documents
			// flush, with the nils applied (dropNils). Every selected plan nil
			// settles it at once: a rebuild running every plan with an
			// Executer deletes it from all versions, and the row with them
			// (removeGone); one that selected fewer versions deletes their
			// documents and leaves it marked for the sweep, whose build runs
			// every plan (leaveGone).
			for _, v := range outcome.missing {
				fl.gone(ctx, id, v)
			}
			for _, vd := range outcome.docs {
				if err := fl.add(ctx, BulkItem{
					Index:   IndexName(params.ResourceType, vd.version),
					ID:      id,
					Doc:     vd.doc.Doc,
					Version: begun[i].BuildIdx,
				}, vd.version, vd.doc.Relations, vd.doc.ResourceMetadata); err != nil {
					fl.salvage(ctx)
					return err
				}
			}
		}
	}

	if err := fl.finish(ctx); err != nil {
		fl.salvage(ctx)
		return err
	}

	logger.Info("targeted rebuild complete", slog.Int("total", total), slog.Int("failed", fl.failed))
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
	// the later plans' outcomes arrive, so every mid-walk position steps over
	// begun-but-unsettled resources — and an attempt resuming past one whose
	// remaining listing no longer emits it would leave it with some versions'
	// documents and edge sets refreshed, others not, and no stale mark. With
	// one plan, a page boundary is reached only once its resources have
	// settled or been marked stale.
	activePlans, activeIdx, firstActiveIdx := 0, -1, -1
	for i, p := range plans {
		if p.Executer != nil {
			activePlans++
			activeIdx = i
			if firstActiveIdx == -1 {
				firstActiveIdx = i
			}
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

	fl := newRebuildFlusher(idx, params.ResourceType)
	// With several plans an id settles on its last plan's outcome, and a
	// later sighting is held to it (rebuildFlusher.keepSettled).
	fl.keepSettled = activePlans > 1
	fl.mayDelete = idx.runsEveryActivePlan(params.ResourceType, plans)

	// completed is the last fully consumed page boundary. The flusher's
	// afterFlush hook checkpoints it: right after a flush, everything before
	// that boundary is durably written and settled, or failed and durably
	// marked stale. Mid-page flushes checkpoint the previous boundary —
	// conservative, never ahead of what was flushed. A paced walk flushes at
	// each page boundary before it waits (walkPacer.beforePage), so it
	// checkpoints each boundary it waits at. Once a mark of the walk
	// has failed (rebuildFlusher.markFailed) it checkpoints nothing more: a
	// resource may have no mark, and a retry must resume before it.
	checkpointing := resumable && resume.checkpoint != nil
	var completed *RebuildCursor
	if checkpointing {
		fl.afterFlush = func() {
			if completed != nil && !fl.markFailed {
				resume.checkpoint(*completed)
			}
		}
	}

	pacer := &walkPacer{idx: idx, pacing: params.Pacing, flush: fl.flush}

	for planIdx, plan := range plans {
		if plan.Executer == nil {
			continue
		}
		// Plan outcomes — a document or a nil — a resource first seen in this
		// walk still expects: this plan plus the active plans after it —
		// earlier walks can no longer emit it. A resource first seen after
		// the first active plan's walk was omitted by an earlier listing, so
		// its outcomes are not every plan's: it is partial and fails rather
		// than settles (pendingResource.partial). A single-plan walk is never
		// partial.
		partial := planIdx != firstActiveIdx
		expected := 0
		for _, p := range plans[planIdx:] {
			if p.Executer != nil {
				expected++
			}
		}

		// A paced walk waits before the plan's first page — before Execute,
		// which may fetch it at once — and so before the walk start, which
		// it would only widen. It flushes what an earlier plan left pending
		// first (walkPacer.beforePage).
		if err := pacer.beforePage(ctx); err != nil {
			fl.salvage(ctx)
			return err
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
			ResourceIDs:  nil,
			Metadata:     params.Metadata,
			// Non-empty only on a resumable walk, whose sole active plan this
			// is; every other walk starts its plans at the listing's head.
			PageToken: startToken,
			// The pacing's page size on a paced walk; 0, the plan's own,
			// otherwise.
			PageSize: pacer.pageSize(),
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

				// Begun on first sighting — by a document or a nil alike — so
				// every plan's outcome for the resource lands in one flusher
				// entry, at one Build Sequence. A failed resource keeps its
				// entry, so it is never begun again.
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
					fl.begin(id, begun, driftBase{start: walkStart, checkRoot: true}, expected, partial)
				}

				// Source listed the resource but returned no data: that is
				// this plan's outcome for it, and its own version's answer
				// (ADR 0013), applied once every expected plan's outcome has
				// arrived: with another plan's document it deletes this
				// version's document and empties its edge set, and the row
				// stays; with every plan nil the resource is deleted, row
				// included — or its versions' documents are, and it is left
				// marked for the sweep, if the walk runs fewer than every
				// plan with an Executer (mayDelete). A document of this
				// version contradicts it and fails the resource
				// (rebuildFlusher.gone).
				if doc.Doc == nil {
					fl.gone(ctx, id, plan.Version)
					continue
				}

				occVersion, _, ok := fl.occ(id)
				if !ok {
					continue
				}

				if err := fl.add(ctx, BulkItem{
					Index:   IndexName(params.ResourceType, plan.Version),
					ID:      id,
					Doc:     doc.Doc,
					Version: occVersion,
				}, plan.Version, doc.Relations, doc.ResourceMetadata); err != nil {
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

			// A page with a next is followed by one: a paced walk flushes
			// the page and waits before taking the next — the plan's pipeline
			// may already have fetched up to its stage depth ahead. The
			// flush checkpoints this page's boundary (afterFlush). The final
			// page (nil token) waits for nothing — another plan's first page
			// waits before its Execute.
			if page.NextPageToken != nil {
				if err := pacer.beforePage(ctx); err != nil {
					fl.salvage(ctx)
					return err
				}
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

	// A walk that runs fewer than every plan with an Executer walked only
	// its versions' listings: it asks each one's Probe about the rows its
	// listing left out (probeUncovered). Its failures aren't the walk's
	// (fl.failed); a failed listing or begin fails the rebuild, retryably,
	// logging what the walk and the pass did until then.
	attrs := []any{slog.Int("failed", fl.failed)}
	if !fl.mayDelete {
		counts, err := idx.probeUncovered(ctx, params, plans)
		if len(counts) > 0 {
			pass := make([]any, len(counts))
			for i, c := range counts {
				pass[i] = c.logAttr()
			}
			attrs = append(attrs, slog.Group("probe_pass", pass...))
		}
		if err != nil {
			logger.Warn(probePassFailedMsg, append(attrs, slog.String("error", err.Error()))...)
			return err
		}
	}

	logger.Info("rebuild complete", attrs...)
	return fl.errorIfFailed()
}
