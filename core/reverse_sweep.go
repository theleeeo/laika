package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/theleeeo/laika/core/resource"
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
	// Suspects is the number of listed ids no probe returned: each was
	// marked stale and, if the mark claimed it, submitted as an owned build.
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

// errReverseSweepNotImplemented is the contract stub's; L2.4 lane B
// replaces it.
var errReverseSweepNotImplemented = errors.New("reverse sweep: not implemented")

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
func (idx *Indexer) ReverseSweepResumable(ctx context.Context, resourceType, after string, checkpoint func(after string)) (ReverseSweepResult, error) {
	return ReverseSweepResult{}, errReverseSweepNotImplemented
}
