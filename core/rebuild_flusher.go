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

// pendingResource is a rebuild's entry for one resource: begun (Build
// Sequence bumped) and unsettled; failed (begun or not), kept until finish;
// or, in a multi-plan walk, settled and kept with how it settled. A partial
// entry — first sighted by a plan after the walk's first — is failed instead
// of settled.
type pendingResource struct {
	occVersion int64
	staleSeq   int64
	drift      driftBase
	// partial marks a resource a multi-plan walk first sighted after its
	// first active plan: the earlier plans' listings omitted it, so its
	// outcomes are not every plan's and decide nothing. When its last
	// expected outcome arrives it is failed — marked for the sweep, whose
	// build runs every plan — never deleted or completed. What its documents
	// wrote stays written, their edge sets stored.
	partial bool
	// remaining counts the plans whose outcome for the resource is still
	// outstanding: a plan's document counts once it flushed successfully, a
	// plan's nil (the source listed the resource without data) as the walk
	// meets it — each plan once, however often its listing repeats the
	// resource. The resource settles when it hits 0: on documents it
	// completes — every expected document and its version's edge set
	// stored, stale mark cleared — and on nils it is deleted (gone).
	remaining int
	// lastCounted is the Schema Version of the plan whose outcome counted
	// last (0: none). Plans arrive in order, so an outcome of that version is
	// a repeat and counts no further.
	lastCounted int
	// sawDoc records a document of the resource queued for a flush, sawNil a
	// plan's nil. Only unanimity decides existence, as in the live build: a
	// document and a nil fail the resource.
	sawDoc bool
	sawNil bool
	// settled is how the resource settled, kept by a multi-plan walk until
	// finish (keepSettled); a single-plan walk drops a settled entry.
	settled settlement
	// failed marks a resource the rebuild could not serve. Its entry stays
	// until finish (or salvage), so nothing later re-begins, writes, deletes
	// or counts it again.
	failed bool
}

// settlement is how a resource settled: unsettled, completed on documents, or
// deleted on nils.
type settlement int

const (
	unsettled settlement = iota
	completed
	deleted
)

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
// failure instead of success. A plan walk counts a plan's nil as that plan's
// outcome too (gone): a resource every expected plan found gone is deleted,
// one whose plans disagree fails. Its marks carry no metadata: the rebuild's
// Metadata is the walk's fetch context, never a resource's own.
type rebuildFlusher struct {
	idx          *Indexer
	resourceType string
	chunkSize    int

	pending []pendingItem
	state   map[string]*pendingResource
	failed  int
	// keepSettled keeps a settled resource's entry until finish, recording
	// how it settled. A multi-plan walk sets it: a later sighting of an id
	// that settled is held to that outcome — a repeat of it is ignored, the
	// other outcome fails the id — rather than begun afresh, so the result
	// never depends on where the chunks end. A single-plan walk drops a
	// settled entry, keeping its state to O(chunk): a later sighting there
	// is the newer fetch and decides the id afresh.
	keepSettled bool
	// afterFlush, when set, runs after every successful flush of a non-empty
	// chunk. rebuildAll checkpoints the walk cursor here: right after a flush,
	// everything queued before the last completed page boundary is durably
	// written, so that boundary is safe to resume from.
	afterFlush func()
}

func newRebuildFlusher(idx *Indexer, resourceType string) *rebuildFlusher {
	return &rebuildFlusher{
		idx:          idx,
		resourceType: resourceType,
		chunkSize:    idx.rebuildChunkSize,
		state:        make(map[string]*pendingResource),
	}
}

func (f *rebuildFlusher) root(id string) model.Resource {
	return model.Resource{Type: f.resourceType, Id: id}
}

// begin registers a resource after BeginBuild: its Build Sequence and the
// stale_seq its clear is guarded by come from begun, its drift check measures
// from drift (begun.Start is not read). expected is the number of plans
// whose outcome — a flushed document or a nil — settles the resource, and
// partial fails it then instead (pendingResource.partial).
func (f *rebuildFlusher) begin(id string, begun BuildBegun, drift driftBase, expected int, partial bool) {
	f.state[id] = &pendingResource{occVersion: begun.BuildIdx, staleSeq: begun.StaleSeq, drift: drift, remaining: expected, partial: partial}
}

// failPartial fails a partial resource whose last expected outcome arrived.
func (f *rebuildFlusher) failPartial(ctx context.Context, id string) {
	slog.Warn("an earlier plan's listing omitted the resource; leaving it stale for the sweep",
		slog.String("type", f.resourceType), slog.String("id", id))
	f.fail(ctx, id)
}

// tracked reports whether the resource has an entry: begun and not yet
// settled, failed, or settled in a multi-plan walk (keepSettled).
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
// report reported. Flushes when the chunk bound is reached. A document of a
// failed or completed resource is dropped; one of a resource a plan found
// gone — deleted, or awaiting the later plans' outcomes — fails it unwritten,
// so no version is written without the others.
func (f *rebuildFlusher) add(ctx context.Context, item BulkItem, version int, relations []model.Resource, reported map[string]string) error {
	p := f.state[item.ID]
	if p == nil || p.failed || p.settled == completed {
		return nil
	}
	if p.sawNil {
		slog.Warn("plans disagree on existence; leaving resource stale for the sweep",
			slog.String("type", f.resourceType), slog.String("id", item.ID),
			slog.Int("version_with_data", version), slog.Int("version_without_data", p.lastCounted))
		f.fail(ctx, item.ID)
		return nil
	}
	p.sawDoc = true
	f.pending = append(f.pending, pendingItem{BulkItem: item, version: version, relations: relations, reported: reported})
	if len(f.pending) >= f.chunkSize {
		return f.flush(ctx)
	}
	return nil
}

// discard forgets the resource without bookkeeping — queued documents are
// dropped. Used when the delete path takes over a resource. A failed
// resource is never dropped: its entry keeps it failed until finish.
func (f *rebuildFlusher) discard(id string) {
	if p := f.state[id]; p != nil && p.failed {
		return
	}
	f.dropPending(id)
	delete(f.state, id)
}

// gone counts a plan's nil — the source listed the resource but returned no
// data — as plan version's outcome for the resource id. A failed or deleted
// resource ignores it, and a repeated nil of one plan's listing counts once.
// A resource with a document — queued, flushed, or completed in a multi-plan
// walk (keepSettled) — fails: its queued documents are dropped, what already
// flushed stays written, and the sweep's build, which runs every plan,
// resolves it. When the last expected plan's nil settles the resource, every
// plan found it gone and it is deleted (removeGone) — unless it is partial,
// and the plans that omitted it were never asked: then it fails.
func (f *rebuildFlusher) gone(ctx context.Context, id string, version int) {
	p := f.state[id]
	if p == nil || p.failed || p.settled == deleted {
		return
	}
	if p.sawDoc {
		slog.Warn("plans disagree on existence; leaving resource stale for the sweep",
			slog.String("type", f.resourceType), slog.String("id", id), slog.Int("version_without_data", version))
		f.fail(ctx, id)
		return
	}
	if p.lastCounted == version {
		return
	}
	p.sawNil, p.lastCounted = true, version
	p.remaining--
	if p.remaining > 0 {
		return
	}
	if p.partial {
		f.failPartial(ctx, id)
		return
	}
	f.removeGone(ctx, id, p)
}

// removeGone deletes a resource every expected plan found gone: every
// version's document and its edges at the Build Sequence the walk began it
// at (handleDelete), then the drift check of the root, then its row
// (removeRow). The page that listed it may have been fetched before that
// Build Sequence was taken: a recreate built and settled in between wrote
// below it, and the delete removed it. So the root is checked against its
// own start — that of the walk that began it, which precedes every fetch of
// it — as a root the walk settles checks itself (driftBase.checkRoot); a hit
// or a failed check re-marks and re-builds it, at worst redundantly. The
// re-mark moves stale_seq, so the guarded row delete keeps the row; a root
// whose re-mark failed was failed and keeps it too. A failed step fails the
// resource.
func (f *rebuildFlusher) removeGone(ctx context.Context, id string, p *pendingResource) {
	if err := f.idx.handleDelete(ctx, RebuildPayload{ResourceType: f.resourceType, ResourceID: id}, p.occVersion); err != nil {
		slog.Warn("delete missing resource", slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
		f.fail(ctx, id)
		return
	}
	f.checkDrift(ctx, map[string][]ChangeCheck{id: {{Resource: f.root(id), Start: p.drift.start}}})
	if !p.failed {
		f.removeRow(ctx, id, p.staleSeq)
	}
	f.settle(id, deleted)
}

// settle records that the resource settled as how: a multi-plan walk keeps
// its entry until finish (keepSettled), a single-plan walk drops it. A
// resource that failed on the way stays failed.
func (f *rebuildFlusher) settle(id string, how settlement) {
	p := f.state[id]
	if p == nil || p.failed {
		return
	}
	if f.keepSettled {
		p.settled = how
		return
	}
	delete(f.state, id)
}

// fail records a resource the rebuild could not serve: its queued documents
// are dropped and it is durably marked stale so the sweep recovers it — on a
// context detached from the rebuild's cancellation (markStale). It counts
// and marks a resource once, however often it fails: the failed entry stays
// until finish. A resource that was never begun (e.g. a failed BeginBuild)
// gets a failed entry too, so a later plan walk cannot re-begin it and
// complete it with only a subset of its versions written, or delete it.
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
		if p == nil || p.failed || p.settled != unsettled {
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
	// of plans whose outcome is still expected, which a plan's repeated
	// document doesn't lower again.
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
			if it.version != p.lastCounted && p.remaining > 0 {
				p.lastCounted = it.version
				p.remaining--
			}
		}
	}

	f.checkDrift(ctx, driftCheck)

	// Complete resources whose every expected document has flushed — a
	// partial one fails instead. A root whose drift re-mark failed was
	// failed by checkDrift and is skipped.
	for _, it := range chunk {
		p := f.state[it.ID]
		if p == nil || p.failed || p.settled != unsettled || p.remaining > 0 {
			continue
		}
		if p.partial {
			f.failPartial(ctx, it.ID)
			continue
		}
		// Seq-guarded: a notification that landed mid-rebuild — or the drift
		// re-mark above — moved stale_seq and turns this into a no-op.
		if err := f.idx.st.ClearStale(ctx, f.root(it.ID), p.staleSeq); err != nil {
			slog.Warn("clear stale failed; sweep may rebuild redundantly",
				slog.String("id", it.ID), slog.String("error", err.Error()))
		}
		f.settle(it.ID, completed)
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
// mark-first primitive. The re-mark leaves the root's metadata alone, and the
// re-build it claims is owned and fetches with the root's own metadata, not
// the rebuild's — none when the row has none. A root whose re-mark fails
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
			if err := f.idx.scheduleBuild(ctx, []model.Resource{f.root(id)}); err != nil {
				slog.Warn("drift re-schedule failed; failing the resource", slog.String("id", id), slog.String("error", err.Error()))
				f.fail(ctx, id)
			}
		}
	}
}

// finish flushes the remainder and settles resources that never received
// every expected plan's outcome: an earlier plan's document or nil began
// them, and a later plan's walk never listed them. Their Build Sequence was
// bumped and only some of their versions' documents and edge sets were
// refreshed, or none after an earlier plan's nil — they must converge via the
// sweep, so they are marked stale. Failed resources were marked when they
// failed, and settled ones (keepSettled) need no mark.
func (f *rebuildFlusher) finish(ctx context.Context) error {
	if err := f.flush(ctx); err != nil {
		return err
	}
	for id, p := range f.state {
		delete(f.state, id)
		if p.failed || p.settled != unsettled {
			continue
		}
		slog.Warn("rebuild left resource incomplete; marking stale for sweep",
			slog.String("type", f.resourceType), slog.String("id", id))
		f.failed++
		f.markStale(ctx, id)
	}
	return nil
}

// salvage durably marks every unsettled resource stale after an abort. It
// runs on a detached context: the abort may stem from cancellation, and these
// marks are the only durable recovery for resources that were begun but not
// fully written. A failed resource was marked when it failed, and a settled
// one (keepSettled) needs no mark.
func (f *rebuildFlusher) salvage(ctx context.Context) {
	if len(f.state) == 0 {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedMarkTimeout)
	defer cancel()

	roots := make([]model.Resource, 0, len(f.state))
	for id, p := range f.state {
		if p.failed || p.settled != unsettled {
			continue
		}
		roots = append(roots, f.root(id))
	}
	f.state = make(map[string]*pendingResource)
	if len(roots) == 0 {
		return
	}
	if _, err := f.idx.st.MarkStale(sctx, roots, 0); err != nil {
		slog.Error("failed to mark unfinished rebuild resources stale; sweep cannot recover them",
			slog.String("type", f.resourceType), slog.Int("count", len(roots)), slog.String("error", err.Error()))
	}
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
