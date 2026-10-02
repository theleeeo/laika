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
// selects every plan — a full rebuild. Otherwise only the selected versions'
// plans run, and full is false: the caller must then merge relation edges
// instead of wiping them, because the non-targeted versions' plans do not run
// and wiping would drop the edges only they discover.
func (idx *Indexer) plansForRebuild(params RebuildArgs) (plans []projection.Plan, full bool, err error) {
	all := idx.plans[params.ResourceType]
	if len(all) == 0 {
		return nil, false, fmt.Errorf("no plans for resource type %q", params.ResourceType)
	}
	if len(params.Versions) == 0 {
		return all, true, nil
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
		return nil, false, fmt.Errorf("resource %q has no plan for version %d", params.ResourceType, v)
	}
	return selected, len(selected) == len(all), nil
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

// pendingResource tracks a resource mid-rebuild: begun (Build Sequence bumped
// and, on a full rebuild, edges wiped) but not yet fully flushed.
type pendingResource struct {
	occVersion int64
	staleSeq   int64
	// start is the Change Sequence value the resource's drift check measures
	// its children against: a value taken before any of its fetches. It is
	// per resource, never flusher state — one chunk can settle resources
	// begun at different starts.
	start int64
	// remaining counts the plan documents not yet flushed successfully; the
	// resource completes — edges final, stale mark cleared — when it hits 0.
	remaining int
	failed    bool
}

// pendingItem is one document awaiting a bulk flush, carrying the relations
// its plan discovered so they are persisted only after the document landed.
type pendingItem struct {
	BulkItem
	relations []model.VersionedResource
}

// rebuildFlusher streams a rebuild's documents to the backend in bounded bulk
// chunks and defers each resource's bookkeeping — edge persistence, the
// ADR 0002 drift check on the Change Sequence, the seq-guarded stale clear —
// until every document of that resource has flushed. A resource whose
// document is rejected is durably marked stale instead of cleared, so the
// sweep recovers it, and the rebuild reports the failure instead of success.
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

// begin registers a resource after BeginBuild. start is the Change Sequence
// value its drift check measures from; expected is the number of plan
// documents that must flush before the resource completes.
func (f *rebuildFlusher) begin(id string, occVersion, staleSeq, start int64, expected int) {
	f.state[id] = &pendingResource{occVersion: occVersion, staleSeq: staleSeq, start: start, remaining: expected}
}

// tracked reports whether the resource has been begun and not yet settled.
func (f *rebuildFlusher) tracked(id string) bool {
	return f.state[id] != nil
}

// occ returns the Build Sequence captured when the resource was begun.
func (f *rebuildFlusher) occ(id string) (int64, bool) {
	p := f.state[id]
	if p == nil {
		return 0, false
	}
	return p.occVersion, true
}

// add queues one plan document. Flushes when the chunk bound is reached.
func (f *rebuildFlusher) add(ctx context.Context, item BulkItem, relations []model.VersionedResource) error {
	p := f.state[item.ID]
	if p == nil || p.failed {
		return nil
	}
	f.pending = append(f.pending, pendingItem{BulkItem: item, relations: relations})
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
// are dropped and it is durably marked stale so the sweep recovers it. A
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

func (f *rebuildFlusher) dropPending(id string) {
	kept := f.pending[:0]
	for _, it := range f.pending {
		if it.ID != id {
			kept = append(kept, it)
		}
	}
	f.pending = kept
}

func (f *rebuildFlusher) markStale(ctx context.Context, id string) {
	if err := f.idx.st.MarkStale(ctx, []model.Resource{f.root(id)}, f.metadata); err != nil {
		slog.Error("failed to mark rebuilt resource stale; sweep cannot recover it",
			slog.String("type", f.resourceType), slog.String("id", id), slog.String("error", err.Error()))
	}
}

// flush writes the pending chunk and settles every resource whose documents
// have all landed: persist its edges, check its fetched children for changes
// accepted after its start, clear its stale mark. Returns an error only for
// request-level write failures, where nothing can be assumed written — the
// caller aborts and salvages.
func (f *rebuildFlusher) flush(ctx context.Context) error {
	if len(f.pending) == 0 {
		return nil
	}
	chunk := f.pending
	f.pending = nil

	items := make([]BulkItem, len(chunk))
	for i, it := range chunk {
		items[i] = it.BulkItem
	}
	failures, err := f.idx.es.BulkUpsert(ctx, items)
	if err != nil {
		return fmt.Errorf("bulk upsert: %w", err)
	}
	rejected := make(map[string]bool, len(failures))
	for _, bf := range failures {
		rejected[bf.ID] = true
		slog.Warn("rebuild document rejected; resource stays stale for sweep",
			slog.String("index", bf.Index), slog.String("id", bf.ID),
			slog.Int("status", bf.Status), slog.String("reason", bf.Reason))
	}

	// Per-document settlement: persist edges for landed documents, fail
	// resources with rejected ones.
	driftCheck := make(map[string][]ChangeCheck)
	for _, it := range chunk {
		p := f.state[it.ID]
		if p == nil || p.failed {
			continue
		}
		if rejected[it.ID] {
			f.fail(ctx, it.ID)
			continue
		}
		if len(it.relations) > 0 {
			// AddChildResources merges (insert-if-absent), which both the
			// full rebuild (edges wiped at begin) and the targeted rebuild
			// (edges of non-targeted versions preserved) rely on.
			plain := make([]model.Resource, len(it.relations))
			for i, r := range it.relations {
				plain[i] = r.Resource
			}
			if err := f.idx.st.AddChildResources(ctx, f.root(it.ID), plain); err != nil {
				slog.Warn("failed to persist relations", slog.String("id", it.ID), slog.String("error", err.Error()))
				f.fail(ctx, it.ID)
				continue
			}
			for _, r := range plain {
				driftCheck[it.ID] = append(driftCheck[it.ID], ChangeCheck{Resource: r, Start: p.start})
			}
		}
		if p.remaining > 0 {
			p.remaining--
		}
	}

	f.checkDrift(ctx, driftCheck)

	// Complete resources whose every expected document has flushed.
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
// Sequence. driftCheck maps each settled root to its checks: every child its
// documents fetched, with the start of the build that fetched it. A root with
// any resource checked whose change_seq exceeds the start of the build that
// fetched it had a change accepted after that fetch began — possibly while
// its edges were missing, so fanout could not reach it — and must re-build to
// converge. One batched
// query serves the common no-drift case; a hit narrows with one query per
// root, and each changed root is re-marked and re-built via the mark-first
// primitive.
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
			if err := f.idx.scheduleBuild(ctx, []model.Resource{f.root(id)}, f.metadata); err != nil {
				slog.Warn("drift re-schedule failed", slog.String("id", id), slog.String("error", err.Error()))
			}
		}
	}
}

// finish flushes the remainder and settles resources that never received all
// their expected documents (a plan walk skipped them, e.g. deleted upstream
// mid-walk). Their Build Sequence was bumped and, on a full rebuild, their
// edges wiped — they must converge via the sweep, so they are marked stale.
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
// marks are the only durable recovery for resources whose edges were already
// wiped.
func (f *rebuildFlusher) salvage(ctx context.Context) {
	if len(f.state) == 0 {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	roots := make([]model.Resource, 0, len(f.state))
	for id := range f.state {
		roots = append(roots, f.root(id))
	}
	if err := f.idx.st.MarkStale(sctx, roots, f.metadata); err != nil {
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
