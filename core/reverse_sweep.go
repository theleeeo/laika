package core

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

const (
	// defaultReverseSweepInterval is how often a type's ReverseSweep
	// schedule runs when its ReverseSweepConfig leaves Interval at zero.
	defaultReverseSweepInterval = 24 * time.Hour
	// defaultReverseSweepPageSize is the number of rows listed and probed
	// per page when ReverseSweepConfig.PageSize is zero.
	defaultReverseSweepPageSize = 200
	// defaultReverseSweepPageInterval is a page's rate budget when
	// ReverseSweepConfig.PageInterval is zero.
	defaultReverseSweepPageInterval = time.Second
	// defaultReverseSweepBackoff is how long the sweep waits before it
	// checks the pool's pressure again.
	defaultReverseSweepBackoff = time.Second
)

// ReverseSweepConfig enables the reverse sweep for one resource type and
// paces it (ADR 0012). The sweep needs a plan of the type with a Probe; a
// type with none is skipped with a warning at each run.
type ReverseSweepConfig struct {
	// Interval is how often the type's ReverseSweep schedule runs. Default
	// 24h.
	Interval time.Duration
	// PageSize is the number of rows listed, and probed, per page. Default
	// 200.
	PageSize int
	// PageInterval is a page's rate budget: a page that finishes sooner
	// waits out the rest before the next page starts, so the sweep probes at
	// most PageSize ids per PageInterval. Default 1s.
	PageInterval time.Duration
}

// ReverseSweepResult is what one run of a type's reverse sweep did.
type ReverseSweepResult struct {
	// Listed is the number of rows the run listed.
	Listed int
	// Suspects is the number of listed ids no successful probe returned:
	// each was marked stale and, if the mark claimed it, submitted as an
	// owned build.
	Suspects int
	// FailedProbes is the number of probe calls that failed: each one's ids
	// were skipped, neither marked nor built, and the next run probes them
	// again.
	FailedProbes int
	// Unprobed is the number of ids those failed probes skipped.
	Unprobed int
}

// reverseSweepConfigs validates Config.ReverseSweeps against the resource
// configs and applies its defaults.
func reverseSweepConfigs(in map[string]ReverseSweepConfig, resources resource.Configs) (map[string]ReverseSweepConfig, error) {
	out := make(map[string]ReverseSweepConfig, len(in))
	for typ, c := range in {
		if resources.Get(typ) == nil {
			return nil, fmt.Errorf("reverse sweep for resource type %q: %w", typ, ErrUnknownResource)
		}
		if c.Interval < 0 || c.PageSize < 0 || c.PageInterval < 0 {
			return nil, fmt.Errorf("reverse sweep for resource type %q: negative interval, page size or page interval", typ)
		}
		if c.Interval == 0 {
			c.Interval = defaultReverseSweepInterval
		}
		if c.PageSize == 0 {
			c.PageSize = defaultReverseSweepPageSize
		}
		if c.PageInterval == 0 {
			c.PageInterval = defaultReverseSweepPageInterval
		}
		out[typ] = c
	}
	return out, nil
}

// reverseSweepBackoffMsg is the Debug message of each wait the sweep makes
// while the pool reports pressure.
const reverseSweepBackoffMsg = "reverse sweep backing off: build pool pressured"

// ReverseSweepNow runs one pass of resourceType's reverse sweep in-process,
// from its first id to its last. It is the body of the ReverseSweep activity,
// as RebuildNow is RebuildWalk's, and is exported for embedders and tests that
// want it without Temporal.
func (idx *Indexer) ReverseSweepNow(ctx context.Context, resourceType string) (ReverseSweepResult, error) {
	return idx.ReverseSweepResumable(ctx, resourceType, "", nil)
}

// ReverseSweepResumable is ReverseSweepNow starting after the id after (empty
// means the first id), calling checkpoint with a page's last id once every
// suspect of that page is durably marked: a run resumed from it skips no id
// (ADR 0011's pattern). A nil checkpoint reports nothing.
//
// The run probes with the Probe of the type's first plan that has one; a
// type with none is skipped with a warning and a zero result. A type
// Resources doesn't configure is an error wrapping ErrUnknownResource, and
// one without a Config.ReverseSweeps entry an error too.
//
// It walks the type's live rows in keyset pages of PageSize ids
// (Store.ListResources), ending after a page shorter than that. A page's ids
// are grouped by their row's metadata — as a registration of the row stored
// it or, on a row that had none, its plans' report — rows without metadata
// in a group of their own, and each group is probed in one call with its
// metadata (nil for the group without). An id a probe doesn't return is a
// suspect; an id it returns but wasn't asked about is ignored. A probe that
// fails is warned about and counted (FailedProbes, and its ids in Unprobed),
// and its ids are skipped, neither marked nor built, for the next run to
// probe again; the rest of the page goes on. A probe that fails once ctx has
// ended ends the run with ctx's error instead.
//
// A page's suspects go through one scheduleBuild: one MarkStale marks them
// all, claiming each that has no live owner, and only then is an owned build
// of the claimed ones submitted, so only suspects are ever marked, and the
// mark comes first (ADR 0008). Each build fetches with the metadata its row
// holds at BeginBuild, not the page's — a suspect whose metadata a
// registration changed since the listing is built with the new one — and one
// whose every plan returns nil has its documents deleted at its Build
// Sequence and its row removed, through Build's all-plans-nil path; one that
// does exist is rebuilt, which is harmless. A suspect with a live owner is
// that owner's follow-up, and a build the pool sheds releases its claim and
// leaves the mark to the StaleSweep. Once the page is marked it is
// checkpointed at its last listed id; a failed mark ends the run with its
// error and the result so far, the page not checkpointed.
//
// Pacing: before each page, the first included, the run waits while the
// build pool is pressured (its queue at Config.QueueHighWater), checking
// again every reverseSweepBackoff: it starts no page while the pool is
// pressured. After a page it waits out the rest of PageInterval, measured
// from the page's start, so it probes at most PageSize ids per PageInterval;
// it doesn't wait after the last page. A ctx that ends during a wait, or
// between pages, ends the run with ctx's error and the result so far.
//
// No lock is held across a probe or a fetch (ADR 0002): the listing claims
// and locks nothing, and the run's own writes are its suspects' marks; its
// builds run as any owned build does.
func (idx *Indexer) ReverseSweepResumable(ctx context.Context, resourceType, after string, checkpoint func(after string)) (ReverseSweepResult, error) {
	var res ReverseSweepResult
	if idx.resources.Get(resourceType) == nil {
		return res, fmt.Errorf("reverse sweep of resource type %q: %w", resourceType, ErrUnknownResource)
	}
	cfg, ok := idx.reverseSweeps[resourceType]
	if !ok {
		return res, fmt.Errorf("reverse sweep of resource type %q: not enabled in Config.ReverseSweeps", resourceType)
	}
	probe := firstProbe(idx.plans[resourceType])
	if probe == nil {
		slog.Warn("reverse sweep skipped: no plan of the type has a Probe", slog.String("type", resourceType))
		return res, nil
	}

	for {
		if err := idx.awaitPoolRelief(ctx); err != nil {
			return res, err
		}
		pageStart := time.Now()
		page, err := idx.st.ListResources(ctx, resourceType, after, cfg.PageSize)
		if err != nil {
			return res, fmt.Errorf("reverse sweep of resource type %q: listing after %q: %w", resourceType, after, err)
		}
		res.Listed += len(page)
		if len(page) == 0 {
			break
		}

		suspects, err := idx.probePage(ctx, probe, resourceType, page, &res)
		if err != nil {
			return res, err
		}
		if err := idx.scheduleBuild(ctx, suspects); err != nil {
			return res, fmt.Errorf("reverse sweep of resource type %q: %w", resourceType, err)
		}
		res.Suspects += len(suspects)
		after = page[len(page)-1].Id
		if checkpoint != nil {
			checkpoint(after)
		}

		if len(page) < cfg.PageSize {
			break
		}
		if err := sleepCtx(ctx, cfg.PageInterval-time.Since(pageStart)); err != nil {
			return res, err
		}
	}

	slog.Info("reverse sweep pass complete",
		slog.String("type", resourceType),
		slog.Int("listed", res.Listed),
		slog.Int("suspects", res.Suspects),
		slog.Int("failed_probes", res.FailedProbes),
		slog.Int("unprobed", res.Unprobed),
	)
	return res, nil
}

// firstProbe is the Probe of the first of plans that has one, in the order
// given; nil when none has.
func firstProbe(plans []projection.Plan) func(context.Context, []string, map[string]string) ([]string, error) {
	for _, p := range plans {
		if p.Probe != nil {
			return p.Probe
		}
	}
	return nil
}

// probePage probes page's ids, one call per group of rows with equal
// metadata, in the order each group first appears, and returns the suspects:
// every probed id its probe didn't return, in page order within each group. A
// failed probe is counted in res and its ids skipped, unless ctx has ended,
// which ends the page with ctx's error.
func (idx *Indexer) probePage(ctx context.Context, probe func(context.Context, []string, map[string]string) ([]string, error), resourceType string, page []ListedResource, res *ReverseSweepResult) ([]model.Resource, error) {
	var suspects []model.Resource
	for _, g := range groupByMetadata(page) {
		present, err := probe(ctx, g.ids, g.metadata)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			slog.Warn("reverse sweep probe failed; its ids are skipped until the next run",
				slog.String("type", resourceType),
				slog.Int("ids", len(g.ids)),
				slog.String("error", err.Error()),
			)
			res.FailedProbes++
			res.Unprobed += len(g.ids)
			continue
		}
		found := make(map[string]bool, len(present))
		for _, id := range present {
			found[id] = true
		}
		for _, id := range g.ids {
			if !found[id] {
				suspects = append(suspects, model.Resource{Type: resourceType, Id: id})
			}
		}
	}
	return suspects, nil
}

// probeGroup is the ids of a page whose rows hold the same metadata.
type probeGroup struct {
	metadata map[string]string
	ids      []string
}

// groupByMetadata groups page's ids by their row's metadata, nil and empty
// alike in one group probed with nil, in the order each group first appears.
func groupByMetadata(page []ListedResource) []probeGroup {
	var groups []probeGroup
	at := make(map[string]int)
	for _, r := range page {
		key := metadataKey(r.Metadata)
		i, ok := at[key]
		if !ok {
			var md map[string]string
			if len(r.Metadata) > 0 {
				md = r.Metadata
			}
			i = len(groups)
			at[key] = i
			groups = append(groups, probeGroup{metadata: md})
		}
		groups[i].ids = append(groups[i].ids, r.Id)
	}
	return groups
}

// metadataKey is an encoding of md equal for equal maps: its pairs in key
// order, each key and value quoted. Nil and empty encode alike.
func metadataKey(md map[string]string) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(md)) {
		b.WriteString(strconv.Quote(k))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(md[k]))
		b.WriteByte(';')
	}
	return b.String()
}

// awaitPoolRelief returns once the build pool is not pressured, checking
// every reverseSweepBackoff, or with ctx's error once ctx ends.
func (idx *Indexer) awaitPoolRelief(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !idx.pool.pressured() {
			return nil
		}
		slog.Debug(reverseSweepBackoffMsg, slog.Duration("backoff", idx.reverseSweepBackoff))
		if err := sleepCtx(ctx, idx.reverseSweepBackoff); err != nil {
			return err
		}
	}
}

// sleepCtx waits d, returning at once when d isn't positive, or with ctx's
// error once ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
