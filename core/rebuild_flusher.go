package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// plansForRebuild resolves the plans a rebuild executes. An empty Versions
// selects every plan; otherwise only the selected versions' plans run.
func (idx *Indexer) plansForRebuild(params RebuildArgs) ([]projection.Plan, error) {
	all := idx.plans[params.ResourceType]
	if len(all) == 0 {
		return nil, fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}
	if len(params.Versions) == 0 {
		return all, nil
	}

	want := make(map[int]bool, len(params.Versions))
	for _, v := range params.Versions {
		want[v] = true
	}
	var selected []projection.Plan
	for _, p := range all {
		if want[p.Version] {
			selected = append(selected, p)
			delete(want, p.Version)
		}
	}
	for v := range want {
		return nil, fmt.Errorf("resource %q has no plan for version %d", params.ResourceType, v)
	}
	return selected, nil
}

// executePlan runs the plan for a single resource ID and returns its first
// document. A zero-value result with a nil Doc means the source no longer has
// the resource.
func executePlan(ctx context.Context, plan projection.Plan, req projection.BuildRequest) (projection.BuildDoc, error) {
	var result projection.BuildDoc
	for r := range plan.Execute(ctx, req) {
		if r.Err != nil {
			return projection.BuildDoc{}, r.Err
		}
		if len(r.Items) > 0 {
			result = r.Items[0]
			break
		}
	}
	return result, nil
}

// detachedMarkTimeout bounds a stale mark made on a context detached from the
// rebuild's cancellation (a failed resource's mark, salvage).
const detachedMarkTimeout = 30 * time.Second

// pendingResource tracks a resource mid-rebuild: begun (Build Sequence bumped)
// but not yet fully flushed.
type pendingResource struct {
	occVersion int64
	staleSeq   int64
	drift      driftBase
	// remaining counts the plan documents not yet flushed successfully; the
	// resource completes — every expected document and its version's edge
	// set stored, stale mark cleared — when it hits 0.
	remaining int
	failed    bool
	// metadata is the resource row's metadata as its last ReplaceEdges
	// returned it; its drift re-mark carries it when non-empty, else the
	// rebuild's metadata.
	metadata map[string]string
}

// driftBase is where a begun resource's drift check measures from. It is per
// resource, never flusher state: one chunk can settle resources begun at
// different starts, and a plan walk's resources cross chunk and plan
// boundaries.
type driftBase struct {
	// start is a Change Sequence value taken before any of the resource's
	// fetches.
	start int64
	// checkRoot adds the resource itself to its check. A plan walk fetches a
	// root's page before the root's BeginBuild: a root change accepted in
	// between is missing from the page, yet the page is written under the
	// newer Build Sequence and the change's stale mark is cleared. Checking
	// the root against the walk start re-builds it. A root begun before its
	// own fetch (rebuildByIDs) is covered by its start and checks its
	// children only.
	checkRoot bool
}

// pendingItem is one document awaiting a bulk flush: its Schema Version, the
// relations its plan discovered, stored as that version's edge set only after
// the document landed, and its plan's report of the resource's own metadata
// (BuildDoc.ResourceMetadata), stored with the edge sets.
type pendingItem struct {
	BulkItem
	version   int
	relations []model.Resource
	reported  map[string]string
}

// rebuildFlusher streams a rebuild's documents to the backend in bounded bulk
// chunks. Each flush stores, per resource, the edge sets of the versions whose
// documents landed in it, at the resource's Build Sequence. The rest of a
// resource's bookkeeping — the ADR 0002 drift check on the Change Sequence,
// the seq-guarded stale clear — waits until every document of that resource
// has flushed. A resource whose document is rejected is durably marked stale
// instead of cleared, so the sweep recovers it, and the rebuild reports the
// failure instead of success.
type rebuildFlusher struct {
	idx          *Indexer
	resourceType string
	metadata     map[string]string
	chunkSize    int

	pending []pendingItem
	state   map[string]*pendingResource
	failed  int
	// afterFlush, when set, runs after every successful flush of a non-empty
	// chunk. rebuildAll checkpoints the walk cursor here: right after a flush,
	// everything queued before the last completed page boundary is durably
	// written, so that boundary is safe to resume from.
	afterFlush func()
}

func newRebuildFlusher(idx *Indexer, resourceType string, metadata map[string]string) *rebuildFlusher {
	return &rebuildFlusher{
		idx:          idx,
		resourceType: resourceType,
		metadata:     metadata,
		chunkSize:    idx.rebuildChunkSize,
		state:        make(map[string]*pendingResource),
	}
}

func (f *rebuildFlusher) root(id string) model.Resource {
	return model.Resource{Type: f.resourceType, Id: id}
}

// begin registers a resource after BeginBuild: its Build Sequence and the
// stale_seq its clear is guarded by come from begun, its drift check measures
// from drift (begun.Start is not read). expected is the number of plan
// documents that must flush before the resource completes.
func (f *rebuildFlusher) begin(id string, begun BuildBegun, drift driftBase, expected int) {
	f.state[id] = &pendingResource{occVersion: begun.BuildIdx, staleSeq: begun.StaleSeq, drift: drift, remaining: expected}
}

// tracked reports whether the resource has been begun and not yet settled.
func (f *rebuildFlusher) tracked(id string) bool {
	return f.state[id] != nil
}

// occ returns the Build Sequence captured when the resource was begun, and
// the stale_seq its finish is guarded by.
func (f *rebuildFlusher) occ(id string) (occVersion, staleSeq int64, ok bool) {
	p := f.state[id]
	if p == nil {
		return 0, 0, false
	}
	return p.occVersion, p.staleSeq, true
}

// add queues one plan document of Schema Version version, with its plan's
// report reported. Flushes when the chunk bound is reached.
func (f *rebuildFlusher) add(ctx context.Context, item BulkItem, version int, relations []model.Resource, reported map[string]string) error {
	p := f.state[item.ID]
	if p == nil || p.failed {
		return nil
	}
	f.pending = append(f.pending, pendingItem{BulkItem: item, version: version, relations: relations, reported: reported})
	if len(f.pending) >= f.chunkSize {
		return f.flush(ctx)
	}
	return nil
}

// discard forgets the resource without bookkeeping — queued documents are
// dropped. Used when the delete path takes over a resource mid-rebuild.
func (f *rebuildFlusher) discard(id string) {
	f.dropPending(id)
	delete(f.state, id)
}

// fail records a resource the rebuild could not serve: its queued documents
// are dropped and it is durably marked stale so the sweep recovers it — on a
// context detached from the rebuild's cancellation (markStale). A
// resource that was never begun (e.g. a failed BeginBuild) gets a failed
// entry, so a later plan walk cannot re-begin it and complete it with only a
// subset of its versions written.
func (f *rebuildFlusher) fail(ctx context.Context, id string) {
	p := f.state[id]
	if p == nil {
		p = &pendingResource{}
		f.state[id] = p
	}
	if p.failed {
		return
	}
	p.failed = true
	f.failed++
	f.dropPending(id)
	f.markStale(ctx, id)
}

// removeRow finishes a resource the rebuild deleted as gone at source: it
// hard-deletes its row, guarded by the stale_seq staleSeq the rebuild
// captured at its BeginBuild, so a change that moved the mark keeps the row
// for its own build. A rebuild owns nothing, so it hands on no follow-up. A
// failed removal fails the resource: its mark brings the sweep, whose build
// finds it gone and removes the row.
func (f *rebuildFlusher) removeRow(ctx context.Context, id string, staleSeq int64) {
	if _, err := f.idx.st.DeleteResourceIfSeq(ctx, f.root(id), staleSeq, 0); err != nil {
		slog.Warn("removing the row of a resource gone at source failed; failing the resource",
			slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
		f.fail(ctx, id)
	}
}

func (f *rebuildFlusher) dropPending(id string) {
	kept := f.pending[:0]
	for _, it := range f.pending {
		if it.ID != id {
			kept = append(kept, it)
		}
	}
	f.pending = kept
}

// docKey identifies one document of a bulk chunk.
func docKey(index, id string) string { return index + "/" + id }

// markStale durably marks one resource stale on a context detached from the
// rebuild's cancellation: the walk may have been cancelled after the failure
// it reacts to, and a checkpoint reported after this mark steps over the
// resource, so the mark is its only recovery.
func (f *rebuildFlusher) markStale(ctx context.Context, id string) {
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedMarkTimeout)
	defer cancel()
	if _, err := f.idx.st.MarkStale(mctx, []model.Resource{f.root(id)}, 0); err != nil {
		slog.Error("failed to mark rebuilt resource stale; sweep cannot recover it",
			slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
	}
}

// flush writes the pending chunk, stores each resource's edge sets for the
// versions whose documents landed, and settles every resource whose documents
// have all landed: check its fetched children (and, for a plan walk's root,
// the root) for changes accepted after its start, clear its stale mark.
// Returns an error only for request-level write failures, where nothing can
// be assumed written — the caller aborts and salvages.
func (f *rebuildFlusher) flush(ctx context.Context) error {
	if len(f.pending) == 0 {
		return nil
	}
	chunk := f.pending
	f.pending = nil

	items := make([]BulkItem, len(chunk))
	inChunk := make(map[string]bool, len(chunk))
	for i, it := range chunk {
		items[i] = it.BulkItem
		inChunk[docKey(it.Index, it.ID)] = true
	}
	failures, err := f.idx.es.BulkUpsert(ctx, items)
	if err != nil {
		return fmt.Errorf("bulk upsert: %w", err)
	}
	// Rejection is per document: a resource's other versions' documents may
	// have landed. A failure naming no document of the chunk is taken as a
	// rejection of every document of its id.
	rejectedDoc := make(map[string]bool, len(failures))
	rejectedID := make(map[string]bool)
	for _, bf := range failures {
		key := docKey(bf.Index, bf.ID)
		rejectedDoc[key] = true
		if !inChunk[key] {
			rejectedID[bf.ID] = true
		}
		slog.Warn("rebuild document rejected; resource stays stale for sweep",
			slog.String("index", bf.Index), slog.String("id", bf.ID),
			slog.Int("status", bf.Status), slog.String("reason", bf.Reason))
	}

	// Group the landed documents per resource, in order of first appearance,
	// and note the resources with a rejected document.
	var order []string
	landed := make(map[string][]pendingItem)
	hasRejected := make(map[string]bool)
	for _, it := range chunk {
		p := f.state[it.ID]
		if p == nil || p.failed {
			continue
		}
		if _, seen := landed[it.ID]; !seen && !hasRejected[it.ID] {
			order = append(order, it.ID)
		}
		if rejectedDoc[docKey(it.Index, it.ID)] || rejectedID[it.ID] {
			hasRejected[it.ID] = true
			continue
		}
		landed[it.ID] = append(landed[it.ID], it)
	}

	// Store the edge set of every landed document's version at the
	// resource's Build Sequence — an empty set too, which replaces the stored
	// one with no edges — also for a resource with a rejected sibling
	// document: what landed is what Elasticsearch now holds (ruling R2).
	// Versions without a landed document keep their stored sets. The same
	// write stores the resource's report — the first non-empty one among its
	// landed documents, in chunk order — as its row's metadata if the row has
	// none; across flushes, the first stored stays.
	for _, id := range order {
		docs := landed[id]
		if len(docs) == 0 {
			continue
		}
		sets := make([]EdgeSet, 0, len(docs))
		at := make(map[int]int, len(docs))
		var reported map[string]string
		for _, it := range docs {
			if reported == nil && len(it.reported) > 0 {
				reported = it.reported
			}
			set := EdgeSet{SchemaVersion: it.version, Children: it.relations}
			// A version listed twice in one chunk: its later document is the
			// one the bulk write left in place.
			if i, ok := at[it.version]; ok {
				sets[i] = set
				continue
			}
			at[it.version] = len(sets)
			sets = append(sets, set)
		}
		if err := f.idx.st.ReplaceEdges(ctx, f.root(id), f.state[id].occVersion, sets, nil, reported); err != nil {
			// The replace may have failed because the walk's context ended;
			// fail marks on a detached context all the same.
			slog.Warn("failed to replace edges; failing the resource", slog.String("id", id), slog.String("error", err.Error()))
			f.fail(ctx, id)
			continue
		}
	}

	// Resources with a rejected document are failed after their landed
	// documents' sets are stored.
	for _, id := range order {
		if hasRejected[id] {
			f.fail(ctx, id)
		}
	}

	// Per-document settlement of the resources still standing: drift checks
	// — the root once per chunk, its children per document — and the count
	// of documents still expected.
	driftCheck := make(map[string][]ChangeCheck)
	rootChecked := make(map[string]bool)
	for _, id := range order {
		p := f.state[id]
		if p == nil || p.failed {
			continue
		}
		for _, it := range landed[id] {
			if p.drift.checkRoot && !rootChecked[id] {
				rootChecked[id] = true
				driftCheck[id] = append(driftCheck[id], ChangeCheck{Resource: f.root(id), Start: p.drift.start})
			}
			for _, r := range it.relations {
				driftCheck[id] = append(driftCheck[id], ChangeCheck{Resource: r, Start: p.drift.start})
			}
			if p.remaining > 0 {
				p.remaining--
			}
		}
	}

	f.checkDrift(ctx, driftCheck)

	// Complete resources whose every expected document has flushed. A root
	// whose drift re-mark failed was failed by checkDrift and is skipped.
	for _, it := range chunk {
		p := f.state[it.ID]
		if p == nil || p.failed || p.remaining > 0 {
			continue
		}
		// Seq-guarded: a notification that landed mid-rebuild — or the drift
		// re-mark above — moved stale_seq and turns this into a no-op.
		if err := f.idx.st.ClearStale(ctx, f.root(it.ID), p.staleSeq); err != nil {
			slog.Warn("clear stale failed; sweep may rebuild redundantly",
				slog.String("id", it.ID), slog.String("error", err.Error()))
		}
		delete(f.state, it.ID)
	}

	if f.afterFlush != nil {
		f.afterFlush()
	}
	return nil
}

// checkDrift is the ADR 0002 drift check for a flushed chunk, on the Change
// Sequence. driftCheck maps each root with landed documents to its checks,
// each carrying the root's start: every child its documents fetched and, for
// a plan walk's root, the root itself. A root with a checked resource whose
// change_seq exceeds that start had a change accepted after its fetch began
// that the fetch may have missed — e.g. a child only this rebuild found,
// whose fanout ran before the root's edge to it was stored and so could not
// reach the root — and must re-build to converge.
//
// One batched query serves the common no-drift case; a hit narrows with one
// query per root, and each changed root is re-marked and re-built via the
// mark-first primitive, carrying the root's metadata as its ReplaceEdges
// returned it, or the rebuild's when the row has none or the root had no
// ReplaceEdges (the walk's nil-doc delete path). A root whose re-mark fails
// is failed instead: clearing it would leave its possibly outdated document
// with no mark for the sweep. fail retries its mark on a context detached
// from cancellation — the re-mark may have failed because the walk's context
// ended — and the rebuild reports the failure.
func (f *rebuildFlusher) checkDrift(ctx context.Context, driftCheck map[string][]ChangeCheck) {
	if len(driftCheck) == 0 {
		return
	}
	var all []ChangeCheck
	for _, checks := range driftCheck {
		all = append(all, checks...)
	}
	drifted, err := f.idx.st.AnyChangedSince(ctx, all)
	if err == nil && !drifted {
		return
	}

	for id, checks := range driftCheck {
		perResource, perErr := drifted, err
		if len(driftCheck) > 1 {
			perResource, perErr = f.idx.st.AnyChangedSince(ctx, checks)
		}
		// On a drift-check error, re-mark rather than risk a silent
		// convergence gap: a redundant rebuild is safe, a missed one is not.
		if perErr != nil || perResource {
			metadata := f.metadata
			if p := f.state[id]; p != nil && len(p.metadata) > 0 {
				metadata = p.metadata
			}
			if err := f.idx.scheduleBuild(ctx, []model.Resource{f.root(id)}, metadata); err != nil {
				slog.Warn("drift re-schedule failed; failing the resource", slog.String("id", id), slog.String("error", err.Error()))
				f.fail(ctx, id)
			}
		}
	}
}

// finish flushes the remainder and settles resources that never received all
// their expected documents (a plan walk skipped them, e.g. deleted upstream
// mid-walk). Their Build Sequence was bumped and only some of their versions'
// documents and edge sets were refreshed — they must converge via the sweep,
// so they are marked stale.
func (f *rebuildFlusher) finish(ctx context.Context) error {
	if err := f.flush(ctx); err != nil {
		return err
	}
	for id, p := range f.state {
		delete(f.state, id)
		if p.failed {
			continue
		}
		slog.Warn("rebuild left resource incomplete; marking stale for sweep",
			slog.String("type", f.resourceType), slog.String("id", id))
		f.failed++
		f.markStale(ctx, id)
	}
	return nil
}

// salvage durably marks every unfinished resource stale after an abort. It
// runs on a detached context: the abort may stem from cancellation, and these
// marks are the only durable recovery for resources that were begun but not
// fully written.
func (f *rebuildFlusher) salvage(ctx context.Context) {
	if len(f.state) == 0 {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedMarkTimeout)
	defer cancel()

	roots := make([]model.Resource, 0, len(f.state))
	for id := range f.state {
		roots = append(roots, f.root(id))
	}
	if _, err := f.idx.st.MarkStale(sctx, roots, 0); err != nil {
		slog.Error("failed to mark unfinished rebuild resources stale; sweep cannot recover them",
			slog.String("type", f.resourceType), slog.Int("count", len(roots)), slog.String("error", err.Error()))
	}
	f.state = make(map[string]*pendingResource)
}

// errorIfFailed converts accumulated per-resource failures into a rebuild
// error, so the Temporal workflow surfaces the failure instead of a silent
// partial success. The failed resources are already marked stale.
func (f *rebuildFlusher) errorIfFailed() error {
	if f.failed == 0 {
		return nil
	}
	return fmt.Errorf("rebuild of %s failed %d resource(s); they are marked stale for sweep recovery", f.resourceType, f.failed)
}
