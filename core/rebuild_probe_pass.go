package core

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// defaultProbePassPageSize is the number of rows a probe pass lists, begins
// and probes per page when its walk has no page size of its own — the size a
// DSL plan lists when BuildRequest.PageSize is 0.
const defaultProbePassPageSize = 100

// probePassFailedMsg is the Warn a rebuild logs, in place of "rebuild
// complete", when its probe pass ends with an error: the walk's failures and
// what the pass did until then.
const probePassFailedMsg = "rebuild walk complete; its probe pass failed"

// probePassCounts is what a probe pass did for one Schema Version.
type probePassCounts struct {
	version int
	// asked is the number of ids the pass probed: listed and begun.
	asked int
	// excluded is the number of ids the probe left out whose version's
	// document was deleted and edge set emptied.
	excluded int
	// marked is the number of ids the probe returned that were marked stale.
	marked int
	// failed is the number of asked ids the pass wrote nothing more for: a
	// failed probe's, a failed delete or edge-set write's, a failed mark's.
	failed int
}

func (c probePassCounts) logAttr() slog.Attr {
	return slog.Group(fmt.Sprintf("v%d", c.version),
		slog.Int("asked", c.asked),
		slog.Int("excluded", c.excluded),
		slog.Int("marked", c.marked),
		slog.Int("failed", c.failed),
	)
}

// probeUncovered is a whole-type walk's probe pass (ADR 0013's Q24 note): a
// walk that runs fewer than every plan with an Executer (mayDelete false)
// walked only its selected versions' listings, so a row a version's listing
// left out got no edge set of that version, and no decision about its
// document. Once the walk has finished, the pass asks each selected version's
// plan's Probe about those rows, in order of plans; a selected version whose
// plan has no Probe is logged at Info and skipped, and a plan without an
// Executer, which no walk runs, is not asked. It keeps no cursor: a resumed
// attempt (ADR 0011) runs it whole again.
//
// For each version it lists, in id order, the type's rows that aren't
// tombstones, have no stale mark, have no edge set of the version, and hold
// the walk's metadata (Store.ListUncovered), in pages of the walk's page size
// — the pacing's, defaultProbePassPageSize when it has none — ending after a
// short page. A paced walk paces the pass's pages as its own: the first page
// of each version's pass follows no page of the pass and starts at once, and
// each later one waits as walkPacer.beforePage does — after a full page, so
// it waits too before a listing that comes back empty. Each page:
//
//   - Begins its rows in one statement (Store.BeginBuilds), which takes each
//     one's Build Sequence before the probe, as a build takes it before its
//     fetch (ADR 0002). A row gone or tombstoned since the listing isn't
//     begun, and is neither probed nor written. Nor is a row begun with
//     other metadata than the walk's (nil and empty equal): a registration
//     changed it since the listing, and that registration's build, which
//     fetches with it, decides the row. Neither counts as asked.
//   - Probes the begun ids in one call with the walk's metadata.
//   - Of an id the probe leaves out, which the version excludes, deletes the
//     version's document at the id's Build Sequence — an OCC loss is no
//     error — and replaces the version's edge set with an empty one at that
//     sequence, declaring nothing, so no other version's set is touched, as
//     rebuildFlusher.dropNils does. It doesn't mark the row: a change
//     registered since the begin has, and its build decides.
//   - Marks every id the probe returns stale in one MarkStale without a
//     claim, as a rebuild hands a resource to the sweep (rebuildFlusher
//     markStale), so the sweep's build, which runs every plan, decides it.
//
// A failed probe, a failed delete or edge-set write of an id, or a failed
// mark writes nothing more for those ids: they stay uncovered, which fails
// the cutover's coverage closed, and the next backfill asks again. These are
// logged and counted, and are not the walk's failures (rebuildFlusher.failed),
// so they don't make RunRebuild retry. A failed listing or begin ends the
// pass with an error the rebuild returns. ctx is checked only after a full
// page, before the next one, and when a probe fails: an ended ctx then ends
// the pass with its error. Within a page it is not checked, so a write of
// that page that fails on the ended ctx is counted as failed like any other.
//
// No lock is held across the probe: BeginBuilds is one statement and the
// listing locks nothing.
func (idx *Indexer) probeUncovered(ctx context.Context, params RebuildArgs, plans []projection.Plan) ([]probePassCounts, error) {
	// The walk has finished: nothing is pending, so a wait has nothing to
	// flush.
	pacer := &walkPacer{idx: idx, pacing: params.Pacing, flush: func(context.Context) error { return nil }}
	pageSize := pacer.pageSize()
	if pageSize <= 0 {
		pageSize = defaultProbePassPageSize
	}
	var md map[string]string
	if len(params.Metadata) > 0 {
		md = params.Metadata
	}

	var all []probePassCounts
	for _, plan := range plans {
		// A plan without an Executer runs in no walk: no listing of it ran,
		// so it left out nothing to ask its Probe about.
		if plan.Executer == nil {
			continue
		}
		if plan.Probe == nil {
			slog.Info("probe pass skipped: the selected version's plan has no Probe; the rows its listing left out stay uncovered",
				slog.String("type", params.ResourceType), slog.Int("version", plan.Version))
			continue
		}
		counts := probePassCounts{version: plan.Version}
		err := idx.probeVersionUncovered(ctx, params.ResourceType, plan, md, pageSize, pacer, &counts)
		all = append(all, counts)
		if err != nil {
			return all, fmt.Errorf("probe pass of %s v%d: %w", params.ResourceType, plan.Version, err)
		}
	}
	return all, nil
}

// probeVersionUncovered runs probeUncovered's pass for plan's version,
// counting into counts.
func (idx *Indexer) probeVersionUncovered(ctx context.Context, resourceType string, plan projection.Plan, md map[string]string, pageSize int, pacer *walkPacer, counts *probePassCounts) error {
	after := ""
	pacer.startPage()
	for {
		page, err := idx.st.ListUncovered(ctx, resourceType, plan.Version, md, after, pageSize)
		if err != nil {
			return fmt.Errorf("listing after %q: %w", after, err)
		}
		if len(page) == 0 {
			return nil
		}
		if err := idx.probeUncoveredPage(ctx, resourceType, plan, md, page, counts); err != nil {
			return err
		}
		if len(page) < pageSize {
			return nil
		}
		after = page[len(page)-1].Id
		// Another page follows a full one — though its listing may come back
		// empty: a ctx that ended stops the pass, and a paced one waits.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := pacer.beforePage(ctx); err != nil {
			return err
		}
	}
}

// probeUncoveredPage begins, probes and serves one listed page of
// probeVersionUncovered.
func (idx *Indexer) probeUncoveredPage(ctx context.Context, resourceType string, plan projection.Plan, md map[string]string, page []ListedResource, counts *probePassCounts) error {
	roots := make([]model.Resource, len(page))
	for i, r := range page {
		roots[i] = r.Resource
	}
	begun, err := idx.st.BeginBuilds(ctx, roots)
	if err != nil {
		return fmt.Errorf("beginning %d row(s): %w", len(roots), err)
	}
	if len(begun) != len(roots) {
		return fmt.Errorf("beginning %d row(s): the store returned %d", len(roots), len(begun))
	}

	// Ruling R2: a row BeginBuilds didn't begin is gone or a tombstone since
	// the listing; it is neither probed nor written. Ruling R7: so is a row
	// begun with other metadata than the walk's (nil and empty equal) — a
	// registration of it since the listing, whose owned build fetches with
	// that metadata and decides it. A probe asked with the walk's metadata
	// would answer for metadata the row no longer holds, and a delete at this
	// newer Build Sequence would beat that build's write unmarked.
	seqs := make(map[string]int64, len(roots))
	var ids []string
	for i, b := range begun {
		if b.BuildIdx == 0 || metadataKey(b.Metadata) != metadataKey(md) {
			continue
		}
		seqs[roots[i].Id] = b.BuildIdx
		ids = append(ids, roots[i].Id)
	}
	if len(ids) == 0 {
		return nil
	}
	counts.asked += len(ids)

	// The probe gets a copy: it may use its ids as it likes.
	present, err := plan.Probe(ctx, slices.Clone(ids), md)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("probe pass: the probe failed; its ids stay uncovered until the next backfill",
			slog.String("type", resourceType), slog.Int("version", plan.Version),
			slog.Int("ids", len(ids)), slog.String("error", err.Error()))
		counts.failed += len(ids)
		return nil
	}
	found := make(map[string]bool, len(present))
	for _, id := range present {
		found[id] = true
	}

	var returned []model.Resource
	index := IndexName(resourceType, plan.Version)
	for _, id := range ids {
		root := model.Resource{Type: resourceType, Id: id}
		if found[id] {
			returned = append(returned, root)
			continue
		}
		// Excluded by the version: its document goes, and its edge set is
		// emptied, at the Build Sequence taken before the probe; the row
		// isn't marked (ADR 0013's Q24 note).
		seq := seqs[id]
		if err := idx.es.Delete(ctx, index, id, seq); err != nil {
			slog.Warn("probe pass: deleting the document of an id the version excludes failed; it stays uncovered",
				slog.String("type", resourceType), slog.String("id", id), slog.String("index", index), slog.String("error", err.Error()))
			counts.failed++
			continue
		}
		if err := idx.st.ReplaceEdges(ctx, root, seq, []EdgeSet{{SchemaVersion: plan.Version}}, nil, nil); err != nil {
			slog.Warn("probe pass: emptying the edge set of an id the version excludes failed; it stays uncovered",
				slog.String("type", resourceType), slog.String("id", id), slog.Int("version", plan.Version), slog.String("error", err.Error()))
			counts.failed++
			continue
		}
		counts.excluded++
	}

	if len(returned) == 0 {
		return nil
	}
	// Returned by the probe though the version's listing left them out: the
	// sweep's build, which runs every plan, decides them. The mark claims
	// nothing, as a rebuild's hand-off doesn't, and runs detached from the
	// rebuild's cancellation, as its marks do (rebuildFlusher.markStale).
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedMarkTimeout)
	defer cancel()
	if _, err := idx.st.MarkStale(mctx, returned, 0); err != nil {
		slog.Warn("probe pass: marking the ids the probe returned failed; they stay uncovered",
			slog.String("type", resourceType), slog.Int("version", plan.Version),
			slog.Int("ids", len(returned)), slog.String("error", err.Error()))
		counts.failed += len(returned)
		return nil
	}
	counts.marked += len(returned)
	return nil
}
