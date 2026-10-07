package core

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// plansForRebuild resolves the plans a rebuild executes. An empty Versions
// selects every plan; otherwise only the selected versions' plans run.
// runsEveryActivePlan tells whether they may remove a resource's row.
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

// runsEveryActivePlan reports whether plans — a subset of resourceType's
// plans, as plansForRebuild selects them — include every one of its plans
// that has an Executer. The sweep's build runs exactly those
// (executeAllPlans), so only then may a rebuild remove the row of a resource
// its plans all find gone (rebuildFlusher.mayDelete).
func (idx *Indexer) runsEveryActivePlan(resourceType string, plans []projection.Plan) bool {
	return countActive(plans) == countActive(idx.plans[resourceType])
}

// countActive counts the plans that have an Executer.
func countActive(plans []projection.Plan) int {
	n := 0
	for _, p := range plans {
		if p.Executer != nil {
			n++
		}
	}
	return n
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
	// first active plan: the earlier plans' listings omitted it, and an
	// omission is not a nil, so its outcomes are not every plan's and decide
	// nothing. When its last expected outcome arrives it is failed — marked
	// for the sweep, whose build runs every plan — never deleted or
	// completed, and its plans' nils are not applied. What its documents
	// wrote stays written, their edge sets stored.
	partial bool
	// remaining counts the plans whose outcome for the resource is still
	// outstanding: a plan's document counts once it flushed successfully, a
	// plan's nil (the source listed the resource without data) as the walk
	// meets it — each plan once, however often its listing repeats the
	// resource. The resource settles when it hits 0, on every plan's outcome
	// (ADR 0013): with a document it completes — every expected document and
	// its version's edge set stored, the nils applied (dropNils), stale mark
	// cleared, row kept — and on nils alone it is deleted, row included
	// (removeGone), or, when the rebuild may not remove the row (mayDelete),
	// its versions' documents are deleted and it is left marked for the
	// sweep (leaveGone). A partial resource fails instead.
	remaining int
	// lastCounted is the Schema Version of the plan whose document counted
	// last (0: none). Documents count in the order they were queued, which is
	// plan order, so a document of that version is a repeat and counts no
	// further.
	lastCounted int
	// lastDoc is the Schema Version of the last document of the resource
	// queued for a flush (0: none; versions start at 1). Whether the
	// resource saw a document (sawDoc) decides whether it keeps its row when
	// it settles. A nil of that version contradicts it (ruling R3).
	lastDoc int
	// nils are the versions whose plans returned nil for the resource, in
	// plan order; a repeated nil of one is not added again, and a document of
	// one contradicts it (ruling R3). They are applied only when the resource
	// settles (dropNils, removeGone), so a queued document and a delete of
	// one resource never meet, and a resource that fails or never receives
	// every outcome deletes nothing.
	nils []int
	// settled is how the resource settled, kept by a multi-plan walk until
	// finish (keepSettled); a single-plan walk drops a settled entry.
	settled settlement
	// failed marks a resource the rebuild could not serve. Its entry stays
	// until finish (or salvage), so nothing later re-begins, writes, deletes
	// or counts it again.
	failed bool
}

// sawDoc reports whether a plan returned a document of the resource.
func (p *pendingResource) sawDoc() bool { return p.lastDoc != 0 }

// settlement is how a resource settled: unsettled, completed on a document,
// or deleted on nils alone — which a rebuild that may not remove the row
// settles by deleting its versions' documents and leaving the resource
// marked for the sweep (rebuildFlusher.leaveGone).
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
// the seq-guarded stale clear — waits until every plan's outcome for that
// resource has arrived and its documents have flushed. A resource whose
// document is rejected is durably marked stale instead of cleared, so the
// sweep recovers it, and the rebuild reports the failure instead of success.
// A plan's nil is that plan's outcome too (gone), and its own version's
// answer (ADR 0013): when the resource settles with a document, each nil
// deletes its version's document and empties its version's edge set at the
// resource's Build Sequence (dropNils), and the row stays; a resource every
// expected plan found gone is deleted, row included — or, when the rebuild
// runs fewer than every plan with an Executer (mayDelete), has those
// versions' documents deleted and is left marked for the sweep. A partial
// resource (pendingResource.partial), and one whose version contradicts
// itself (ruling R3), fails instead. Its marks carry no metadata: the
// rebuild's Metadata is the walk's fetch context, never a resource's own.
type rebuildFlusher struct {
	idx          *Indexer
	resourceType string
	chunkSize    int

	pending []pendingItem
	state   map[string]*pendingResource
	failed  int
	// markFailed records that a mark meant to hand a resource to the sweep
	// failed — fail's, finish's or salvage's — so a resource may have no
	// mark: the rebuild's error stays retryable (errorIfFailed) and a
	// single-plan walk's checkpoint steps over nothing more (rebuildAll). A
	// failed mark that fail retries (leaveGone's, a drift re-mark) is not
	// recorded unless the retry fails too.
	markFailed bool
	// keepSettled keeps a settled resource's entry until finish, recording
	// how it settled. A multi-plan walk sets it: a later sighting of an id
	// that settled — its last plan's listing repeating it — is held to that
	// plan's outcome — a repeat of it is ignored, the other outcome fails the
	// id (ruling R3) — rather than begun afresh, so the result never depends
	// on where the chunks end. A single-plan walk drops a settled entry,
	// keeping its state to O(chunk): a later sighting there is the newer
	// fetch and decides the id afresh.
	keepSettled bool
	// mayDelete lets the rebuild remove the row of a resource every plan it
	// runs found gone (removeGone). It is set when those plans are every plan
	// of the type that has an Executer (Indexer.runsEveryActivePlan) — the
	// plans the sweep's build runs. It guards only the row: each plan's nil
	// deletes its own version's document either way. A rebuild that selected
	// fewer versions has not asked the others, whose documents keep the row,
	// so it deletes its versions' documents of such a resource and leaves it
	// marked for the sweep (leaveGone), whose build runs every plan and
	// decides the row.
	mayDelete bool
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
// failed resource is dropped, and so is a repeat of the document a resource
// completed on. A document of a version whose plan returned nil for the
// resource contradicts that nil and fails the resource unwritten (ruling R3);
// another version's nil does not stop it.
func (f *rebuildFlusher) add(ctx context.Context, item BulkItem, version int, relations []model.Resource, reported map[string]string) error {
	p := f.state[item.ID]
	if p == nil || p.failed {
		return nil
	}
	if slices.Contains(p.nils, version) {
		f.contradicts(ctx, item.ID, version)
		return nil
	}
	// Only the last plan's listing can sight a settled resource again, and
	// its document, unless it contradicts that plan's nil, is a repeat.
	if p.settled != unsettled {
		return nil
	}
	p.lastDoc = version
	f.pending = append(f.pending, pendingItem{BulkItem: item, version: version, relations: relations, reported: reported})
	if len(f.pending) >= f.chunkSize {
		return f.flush(ctx)
	}
	return nil
}

// gone counts a plan's nil — the source listed the resource but returned no
// data — as plan version's outcome for the resource id. It is that version's
// answer alone (ADR 0013), recorded and applied when the resource settles. A
// failed resource ignores it, and so does one that settled, unless it
// contradicts the document the resource completed on; a repeated nil of one
// plan's listing counts once. A nil of a version whose document the resource
// already has — queued, flushed, or completed in a multi-plan walk
// (keepSettled) — contradicts it and fails the resource (ruling R3): its
// queued documents are dropped, what already flushed stays written, and the
// sweep's build, which runs every plan, resolves it.
//
// When the last expected plan's nil settles the resource, it fails if it is
// partial, and the plans that omitted it were never asked. Otherwise, with a
// document — every one of which has flushed, or remaining would not have
// reached 0 — it completes here (completeOnNil); and on nils alone it is
// deleted (removeGone) if the rebuild runs every plan with an Executer
// (mayDelete), and otherwise has its versions' documents deleted and is left
// marked for the sweep (leaveGone).
func (f *rebuildFlusher) gone(ctx context.Context, id string, version int) {
	p := f.state[id]
	if p == nil || p.failed {
		return
	}
	if p.lastDoc == version {
		f.contradicts(ctx, id, version)
		return
	}
	if p.settled != unsettled || slices.Contains(p.nils, version) {
		return
	}
	p.nils = append(p.nils, version)
	p.remaining--
	if p.remaining > 0 {
		return
	}
	switch {
	case p.partial:
		f.failPartial(ctx, id)
	case p.sawDoc():
		f.completeOnNil(ctx, id, p)
	case !f.mayDelete:
		f.leaveGone(ctx, id, p)
	default:
		f.removeGone(ctx, id, p)
	}
}

// contradicts fails a resource one version's plan listed both with and
// without data in one walk (ruling R3): applying both would queue a bulk
// write and a delete of one document at one Build Sequence.
func (f *rebuildFlusher) contradicts(ctx context.Context, id string, version int) {
	slog.Warn("a schema version's plan listed the resource both with and without data; leaving it stale for the sweep",
		slog.String("type", f.resourceType), slog.String("id", id), slog.Int("version", version))
	f.fail(ctx, id)
}

// dropNils applies the nils of a resource that settles (pendingResource.nils):
// each such version's document is deleted at the resource's Build Sequence —
// a newer write's rejection is an OCC loss, which Delete does not report —
// and then their edge sets are replaced with empty ones in one write at the
// same sequence, which keeps each set's stamp, so an older build's edges
// cannot return. Other versions' documents and edge sets, and the row, are
// left alone. A failed delete or write fails the resource, leaving the mark
// for the sweep; the caller checks p.failed.
func (f *rebuildFlusher) dropNils(ctx context.Context, id string, p *pendingResource) {
	if len(p.nils) == 0 {
		return
	}
	sets := make([]EdgeSet, len(p.nils))
	for i, v := range p.nils {
		index := IndexName(f.resourceType, v)
		if err := f.idx.es.Delete(ctx, index, id, p.occVersion); err != nil {
			slog.Warn("deleting the document of a version whose plan returned no data failed; failing the resource",
				slog.String("type", f.resourceType), slog.String("id", id), slog.String("index", index), slog.String("error", err.Error()))
			f.fail(ctx, id)
			return
		}
		sets[i] = EdgeSet{SchemaVersion: v}
	}
	if err := f.idx.st.ReplaceEdges(ctx, f.root(id), p.occVersion, sets, nil, nil); err != nil {
		slog.Warn("emptying the edge sets of the versions whose plans returned no data failed; failing the resource",
			slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
		f.fail(ctx, id)
	}
}

// completeOnNil completes a resource whose last expected outcome is a nil
// arriving after its documents flushed — only flush completes one whose last
// outcome is a document. It applies the nils (dropNils), checks the root
// against its start when it is a plan walk's root, as removeGone does after
// its delete (driftBase.checkRoot) — the children were checked when the
// documents flushed — and clears the mark. A failed step fails the resource.
func (f *rebuildFlusher) completeOnNil(ctx context.Context, id string, p *pendingResource) {
	f.dropNils(ctx, id, p)
	if p.failed {
		return
	}
	if p.drift.checkRoot {
		f.checkDrift(ctx, map[string][]ChangeCheck{id: {{Resource: f.root(id), Start: p.drift.start}}})
		if p.failed {
			return
		}
	}
	f.complete(ctx, id, p)
}

// complete settles a resource every expected outcome of which arrived, its
// documents flushed and its nils applied: its stale mark is cleared, guarded
// by the stale_seq its BeginBuild captured, and its row stays.
func (f *rebuildFlusher) complete(ctx context.Context, id string, p *pendingResource) {
	// Seq-guarded: a notification that landed mid-rebuild — or a drift
	// re-mark — moved stale_seq and turns this into a no-op.
	if err := f.idx.st.ClearStale(ctx, f.root(id), p.staleSeq); err != nil {
		slog.Warn("clear stale failed; sweep may rebuild redundantly",
			slog.String("id", id), slog.String("error", err.Error()))
	}
	f.settle(id, completed)
}

// leaveGone hands the sweep a resource every plan of a rebuild that may not
// remove the row (mayDelete) found gone: those plans are a subset of the
// type's, and the versions they did not run may still have the resource, so
// only a build running every plan may remove the row (ADR 0013). It deletes
// its plans' versions' documents and empties their edge sets (dropNils),
// keeps the row, and marks the resource stale, once, on a context detached
// from the rebuild's cancellation (markStale): the sweep's build runs every
// plan and decides the row. The resource gets no drift check: the mark,
// taken after the deletes, already brings the sweep's build, at a higher
// Build Sequence. It is not a failure, and it settles as a deleted resource
// does (settle): a multi-plan walk holds it to that outcome, a single-plan
// walk drops it — its checkpoint may step over it, durably marked. A failed
// delete or write fails it, and so does a failed mark (fail retries the mark
// and counts it), so the rebuild reports it.
func (f *rebuildFlusher) leaveGone(ctx context.Context, id string, p *pendingResource) {
	slog.Info("every plan the rebuild runs found the resource gone; it runs fewer than every plan with an executer, so it deletes their versions' documents and leaves the row marked for the sweep, whose build runs them all",
		slog.String("type", f.resourceType), slog.String("id", id))
	f.dropNils(ctx, id, p)
	if p.failed {
		return
	}
	if err := f.markStale(ctx, id); err != nil {
		f.fail(ctx, id)
		return
	}
	f.settle(id, deleted)
}

// removeGone deletes a resource every expected plan found gone, in a rebuild
// that runs every plan with an Executer (mayDelete): every
// version's document and its edges at the Build Sequence the walk began it
// at (handleDelete), then, for a plan walk's root, the drift check of the
// root, then its row (removeRow). The page that listed it may have been
// fetched before that Build Sequence was taken: a recreate built and settled
// in between wrote below it, and the delete removed it. So the root is
// checked against its own start — that of the walk that began it, which
// precedes every fetch of it — as a root the walk settles checks itself
// (driftBase.checkRoot); a hit or a failed check re-marks and re-builds it,
// at worst redundantly. A root begun before its own fetch (rebuildByIDs) is
// not checked. The re-mark moves stale_seq, so the guarded row delete keeps
// the row; a root whose re-mark failed was failed and keeps it too. A failed
// step fails the resource.
func (f *rebuildFlusher) removeGone(ctx context.Context, id string, p *pendingResource) {
	if err := f.idx.handleDelete(ctx, RebuildPayload{ResourceType: f.resourceType, ResourceID: id}, p.occVersion); err != nil {
		slog.Warn("delete missing resource", slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
		f.fail(ctx, id)
		return
	}
	if p.drift.checkRoot {
		f.checkDrift(ctx, map[string][]ChangeCheck{id: {{Resource: f.root(id), Start: p.drift.start}}})
	}
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
// are dropped and it is marked stale so the sweep recovers it — on a context
// detached from the rebuild's cancellation (markStale). A failed mark is
// recorded in markFailed: the resource may have no mark, so the rebuild stays
// retryable and a walk checkpoints nothing more. It counts
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
	if err := f.markStale(ctx, id); err != nil {
		f.markFailed = true // logged; the failure is counted either way
	}
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
// resource, so the mark is its only recovery. It logs a failed mark and
// returns its error: fail and finish record it (markFailed) and go on,
// leaveGone fails the resource, which retries the mark.
func (f *rebuildFlusher) markStale(ctx context.Context, id string) error {
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedMarkTimeout)
	defer cancel()
	if _, err := f.idx.st.MarkStale(mctx, []model.Resource{f.root(id)}, 0); err != nil {
		slog.Error("failed to mark rebuilt resource stale; sweep cannot recover it",
			slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
		return err
	}
	return nil
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
	// document doesn't lower again. A plan's nil was counted when the walk
	// met it.
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

	// Apply the nils of every resource this flush settles (dropNils) — those
	// whose every expected outcome has arrived — before the drift check, so
	// the check follows their deletes. A partial resource applies none: it
	// fails below. One whose delete or write failed was failed and is not
	// checked.
	for _, id := range order {
		p := f.state[id]
		if p == nil || p.failed || p.remaining > 0 || p.partial {
			continue
		}
		if f.dropNils(ctx, id, p); p.failed {
			delete(driftCheck, id)
		}
	}

	f.checkDrift(ctx, driftCheck)

	// Complete resources whose every expected outcome has arrived and every
	// document flushed — a partial one fails instead. A root whose drift
	// re-mark failed was failed by checkDrift and is skipped.
	for _, it := range chunk {
		p := f.state[it.ID]
		if p == nil || p.failed || p.settled != unsettled || p.remaining > 0 {
			continue
		}
		if p.partial {
			f.failPartial(ctx, it.ID)
			continue
		}
		f.complete(ctx, it.ID, p)
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
// them, and a later plan's walk never listed them — an omission, which is
// not a nil. Their Build Sequence was bumped and only some of their versions'
// documents and edge sets were refreshed, and none of their nils applied —
// they must converge via the sweep, so they are marked stale. Failed resources were marked when they
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
		if err := f.markStale(ctx, id); err != nil {
			f.markFailed = true // logged; the failure is counted either way
		}
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
		f.markFailed = true
		slog.Error("failed to mark unfinished rebuild resources stale; sweep cannot recover them",
			slog.String("type", f.resourceType), slog.Int("count", len(roots)), slog.String("error", err.Error()))
	}
}

// errorIfFailed converts accumulated per-resource failures into a rebuild
// error, so the Temporal workflow surfaces the failure instead of a silent
// partial success. When every failed resource is durably marked stale it is a
// RebuildMarkedFailuresError, which RunRebuild does not retry: the sweep
// recovers them. When a mark failed (markFailed) it is a plain error, which
// RunRebuild retries: a resource may have no mark, and the sweep may not
// recover it.
func (f *rebuildFlusher) errorIfFailed() error {
	if f.failed == 0 {
		return nil
	}
	if f.markFailed {
		return fmt.Errorf("rebuild of %s failed %d resource(s); some of their stale marks failed, so the sweep may not recover every one", f.resourceType, f.failed)
	}
	return &RebuildMarkedFailuresError{ResourceType: f.resourceType, Count: f.failed}
}
