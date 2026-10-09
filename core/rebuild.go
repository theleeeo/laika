package core

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.temporal.io/sdk/client"
)

// ResourceSelector identifies a set of resources and versions to rebuild.
type ResourceSelector struct {
	ResourceType string
	// Versions selects the Schema Versions whose plans the rebuild runs;
	// empty runs every plan. Each plan's nil deletes its own version's
	// document (ADR 0013). A rebuild that leaves out a version with an
	// executing plan never removes a resource's row: a resource its plans all
	// find gone loses their versions' documents and is left marked stale for
	// the sweep, whose build runs every plan and decides the row. Such a
	// rebuild of the whole type (no ResourceIDs), once its walk has
	// finished, asks each selected version's plan's Probe, a page at a time,
	// about the type's rows its listing left without an edge set of the
	// version that hold Metadata: an id the probe leaves out loses that
	// version's document and edge set, unmarked; an id it returns is marked
	// stale for the sweep. A selected version without a Probe is skipped
	// (ADR 0013's Q24 note).
	Versions    []int
	ResourceIDs []string
	// Metadata is the rebuild's own fetch context (e.g. the acting tenant),
	// passed to every provider call its walk makes. It is never stored on a
	// resource's row: the owned re-builds the walk's drift re-marks claim
	// fetch with their rows' own metadata.
	Metadata map[string]string
	// Pacing, when set, paces an all-of-type walk: the forward walk's
	// scheduled runs set it from their ForwardWalkConfig. Nil walks unpaced,
	// as an explicit rebuild does.
	Pacing *WalkPacing
}

// WalkPacing paces an all-of-type rebuild walk as ReverseSweepConfig paces
// the reverse sweep. Before the walk takes each page it waits while the build
// pool is pressured; after a page it waits out the rest of PageInterval,
// measured from the page's start. It paces the walk taking pages, not the
// plan's fetches: the plan's pipeline may already have fetched up to its
// stage depth ahead (about a page per stage) while the walk waits. The walk
// flushes what it has pending before each wait, so no resource it began
// waits unwritten.
type WalkPacing struct {
	// PageSize is passed to the walk's plans as
	// projection.BuildRequest.PageSize.
	PageSize int
	// PageInterval is a page's rate budget.
	PageInterval time.Duration
}

// RebuildMarkedFailuresErrorType is the type of the non-retryable Temporal
// application error RunRebuild returns for a RebuildMarkedFailuresError; its
// details are the count of failed resources.
const RebuildMarkedFailuresErrorType = "RebuildMarkedFailures"

// RebuildMarkedFailuresError is a rebuild's error when it failed resources
// and every one of them is durably marked stale for the sweep: no mark the
// rebuild made failed. Retrying the rebuild would add nothing the sweep
// doesn't, so RunRebuild returns it as non-retryable. A rebuild that aborted,
// or one of whose marks failed, returns another error.
type RebuildMarkedFailuresError struct {
	ResourceType string
	// Count is the number of resources the rebuild failed.
	Count int
}

func (e *RebuildMarkedFailuresError) Error() string {
	return fmt.Sprintf("rebuild of %s failed %d resource(s); they are marked stale for sweep recovery", e.ResourceType, e.Count)
}

// RebuildCursor marks a settled position in a rebuild walk: the plan with
// Schema Version PlanVersion has walked and flushed every page before
// PageToken, and every resource those pages listed has settled — its documents
// written and its stale mark cleared, or it is durably marked stale for the
// sweep. An empty PageToken means the start of PlanVersion's walk. A walk
// resumed from a cursor re-enters the listing at PageToken (ADR 0011).
//
// PageToken must identify a position that is stable across attempts — a cursor
// is redeemed minutes to hours after it was recorded. Keyset or last-ID tokens
// qualify; an offset token does not, because upstream deletions in between
// shift later rows forward and the resumed walk then starts past resources it
// never walked, leaving them un-rebuilt and unmarked.
type RebuildCursor struct {
	PlanVersion int    `json:"plan_version"`
	PageToken   string `json:"page_token"`
}

// rebuildResume carries a walk's resume state: where to start (nil = from
// scratch) and where to report checkpoints (nil = don't checkpoint).
type rebuildResume struct {
	start      *RebuildCursor
	checkpoint func(RebuildCursor)
}

// RebuildNow synchronously rebuilds the selected resources in-process. It is
// the body of the RebuildWalk activity, and is exported for embedders and
// tests that want rebuild semantics without Temporal.
func (idx *Indexer) RebuildNow(ctx context.Context, selectors []ResourceSelector) error {
	if err := idx.validateSelectors(selectors); err != nil {
		return err
	}
	for _, sel := range selectors {
		if err := idx.rebuild(ctx, RebuildArgs{
			ResourceType: sel.ResourceType,
			Versions:     sel.Versions,
			ResourceIDs:  sel.ResourceIDs,
			Metadata:     sel.Metadata,
			Pacing:       sel.Pacing,
		}, rebuildResume{}); err != nil {
			return fmt.Errorf("rebuild %s: %w", sel.ResourceType, err)
		}
	}
	return nil
}

// RebuildNowResumable is RebuildNow for a single selector with crash-resume
// support: checkpoint (optional) receives a cursor whenever everything before
// that position has durably settled, and start (optional) resumes a walk from
// such a cursor. The Temporal RunRebuild activity persists cursors as
// heartbeat details, so a retried attempt continues where the dead one
// stopped instead of restarting the walk.
//
// Cursors apply only to an all-of-type walk with exactly one active plan — a
// single-version resource, or a version-targeted backfill. A walk over several
// versions has no settled mid-walk position: a resource first seen by an early
// plan stays unsettled until every later plan's outcome for it — a document
// that landed, or a nil — has arrived, and an attempt resuming past it whose
// remaining listing omits it would leave it with some versions' documents and
// edge sets refreshed, others not, its nils unapplied, and no stale mark.
// Such walks, and targeted (by-ID) rebuilds, ignore
// start and never checkpoint — they restart from scratch, as before (ADR 0011).
//
// A selector's Pacing paces an all-of-type walk (walkPacer): its plans are
// asked for pages of PageSize, and before the walk takes each page it waits
// out the rest of the previous page's PageInterval and while the build pool
// is pressured; the plan's pipeline may already have fetched up to its stage
// depth ahead. Before each wait it flushes its pending chunk, so it writes,
// and a single-plan walk checkpoints, at every page boundary it waits at,
// whatever RebuildChunkSize is. A targeted rebuild ignores it.
//
// Resuming also requires the plan's Executer to honour
// projection.BuildRequest.PageToken: one that ignores it restarts from the head
// of the listing on every resume. That is safe — a full re-walk settles
// everything it touches — but the walk is not actually resumable.
func (idx *Indexer) RebuildNowResumable(ctx context.Context, sel ResourceSelector, start *RebuildCursor, checkpoint func(RebuildCursor)) error {
	if err := idx.validateSelectors([]ResourceSelector{sel}); err != nil {
		return err
	}
	if err := idx.rebuild(ctx, RebuildArgs{
		ResourceType: sel.ResourceType,
		Versions:     sel.Versions,
		ResourceIDs:  sel.ResourceIDs,
		Metadata:     sel.Metadata,
		Pacing:       sel.Pacing,
	}, rebuildResume{start: start, checkpoint: checkpoint}); err != nil {
		return fmt.Errorf("rebuild %s: %w", sel.ResourceType, err)
	}
	return nil
}

// Rebuild validates the selectors and starts one durable RebuildWalk workflow
// per selector, returning the workflow IDs (inspect/retry/cancel in the
// Temporal UI).
func (idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) ([]string, error) {
	if err := idx.validateSelectors(selectors); err != nil {
		return nil, err
	}
	workflowIDs := make([]string, 0, len(selectors))
	for _, sel := range selectors {
		run, err := idx.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: idx.taskQueue,
		}, rebuildWalkWorkflowName, sel)
		if err != nil {
			return workflowIDs, fmt.Errorf("start rebuild workflow for %s: %w", sel.ResourceType, err)
		}
		workflowIDs = append(workflowIDs, run.GetID())
	}
	return workflowIDs, nil
}

func (idx *Indexer) validateSelectors(selectors []ResourceSelector) error {
	if len(selectors) == 0 {
		return &InvalidArgumentError{Msg: "at least one selector is required"}
	}
	for _, sel := range selectors {
		cfg := idx.resources.Get(sel.ResourceType)
		if cfg == nil {
			return fmt.Errorf("resource type %q: %w", sel.ResourceType, ErrUnknownResource)
		}
		for _, v := range sel.Versions {
			if cfg.GetVersion(v) == nil {
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q has no version %d", sel.ResourceType, v)}
			}
		}
		if p := sel.Pacing; p != nil {
			if p.PageSize < 0 || p.PageInterval < 0 {
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q: negative walk page size or page interval", sel.ResourceType)}
			}
			if p.PageSize > math.MaxInt32 {
				// A listing's page size is an int32 on the provider contract.
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q: walk page size %d above %d", sel.ResourceType, p.PageSize, math.MaxInt32)}
			}
		}
	}
	return nil
}

// walkPacer paces an all-of-type rebuild walk by its selector's WalkPacing;
// a nil pacing paces nothing, and an unpaced walk flushes only at its chunk
// size.
type walkPacer struct {
	idx    *Indexer
	pacing *WalkPacing
	// flush writes the walk's pending chunk (rebuildFlusher.flush); a paced
	// walk runs it before every wait.
	flush func(context.Context) error
	// pageStart is when the walk asked for its last page; zero before the
	// first.
	pageStart time.Time
}

// pageSize is the listing page size the walk asks its plans for: the
// pacing's, or 0 — the plan's own — when unpaced.
func (w *walkPacer) pageSize() int {
	if w.pacing == nil {
		return 0
	}
	return w.pacing.PageSize
}

// beforePage runs before the walk takes a page: before a plan's Execute,
// which may fetch its first page at once, and after a page that has a next.
// It first flushes the walk's pending chunk, so no id the walk has begun —
// its Build Sequence taken — waits unwritten across the pacing: a notified
// delete of it that outlasted Elasticsearch's index.gc_deletes before the
// walk's write at the older Build Sequence would bring the deleted document
// back (seams S16). The flush checkpoints a single-plan walk at the page
// boundary just consumed, as any flush does; a flush error is returned for
// the walk to abort on. It then waits out the rest of the previous page's PageInterval, measured from
// that page's start, then waits while the build pool is pressured, then
// starts the new page's clock. Called only when another page follows, it
// never waits after the walk's last page. It holds back the walk taking
// pages, not the plan's fetches: within a plan, the pipeline (a goroutine
// per stage over unbuffered channels) may already have fetched up to its
// stage depth ahead — listings and relation fetches for about a page per
// stage — while the walk waits. A ctx that ends during a wait returns its
// error.
func (w *walkPacer) beforePage(ctx context.Context) error {
	if w.pacing == nil {
		return nil
	}
	if err := w.flush(ctx); err != nil {
		return err
	}
	if !w.pageStart.IsZero() {
		if err := w.idx.waitPageInterval(ctx, w.pacing.PageInterval-time.Since(w.pageStart)); err != nil {
			return err
		}
	}
	if err := w.idx.awaitPoolRelief(ctx, "rebuild walk"); err != nil {
		return err
	}
	w.pageStart = time.Now()
	return nil
}
