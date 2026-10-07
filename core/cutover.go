package core

import (
	"context"
	"fmt"
	"time"

	"github.com/theleeeo/laika/core/resource"
)

// CutoverBackend is the narrow read-only Elasticsearch surface the cutover
// readiness check needs. *elasticsearch.Client implements it.
type CutoverBackend interface {
	// GetAlias returns the concrete index the alias points to, or "" if the
	// alias does not exist.
	GetAlias(ctx context.Context, aliasName string) (string, error)
	// IndexExists reports whether the concrete index exists.
	IndexExists(ctx context.Context, indexName string) (bool, error)
	// CountDocs returns the number of documents in the concrete index.
	CountDocs(ctx context.Context, indexName string) (int64, error)
}

// StaleCounter is the narrow store surface the readiness check needs: the
// size and age of one resource type's stale backlog. *postgres.Store
// implements it.
type StaleCounter interface {
	// CountStale returns how many resources of the type have a stale mark
	// older than before, and the oldest such mark (zero when count is 0).
	CountStale(ctx context.Context, resourceType string, before time.Time) (count int, oldest time.Time, err error)
}

// DefaultMaxStaleAge is the stale-backlog age ReadinessOptions falls back to.
const DefaultMaxStaleAge = 10 * time.Minute

// Names of the readiness checks a ResourceReadiness can carry.
const (
	CheckAliasState   = "alias-state"
	CheckTargetIndex  = "target-index"
	CheckDocParity    = "doc-count-parity"
	CheckStaleBacklog = "stale-backlog"
)

// ReadinessOptions tunes the cutover readiness gates.
type ReadinessOptions struct {
	// CountTolerance is the absolute doc-count difference between the current
	// read index and the target index that the parity gate tolerates.
	CountTolerance int64
	// MaxStaleAge fails the stale-backlog gate when any resource of the type
	// has been stale longer than this. Zero means DefaultMaxStaleAge.
	MaxStaleAge time.Duration
}

// ReadinessCheck is one gate's verdict for one resource.
type ReadinessCheck struct {
	Name   string
	OK     bool
	Detail string
}

// ResourceReadiness is the readiness verdict for one resource's cutover to
// its config ReadVersion.
type ResourceReadiness struct {
	Resource     string
	Alias        string
	CurrentIndex string // "" when the alias does not exist yet
	TargetIndex  string
	Move         AliasMove
	Ready        bool
	Checks       []ReadinessCheck
}

// CheckCutoverReadiness runs the pre-cutover gates for every resource,
// treating each config's ReadVersion as the proposed alias target. It is
// read-only: run it with the bumped readVersion config *before* deploying it
// (the deployed indexer converges the alias unconditionally, ADR 0009).
//
// Gates, in order — a resource stops at its first structurally-failed gate:
//
//  1. alias-state: the current alias target must be assessable. A hand-built
//     target cannot be reasoned about and fails outright.
//  2. target-index: the ReadVersion index must exist (gen-mapping bootstraps).
//  3. doc-count-parity: for a forward or backward move, the current read
//     index and the target must agree on doc count within CountTolerance. A
//     gap means the backfill has not finished, writes are failing on one
//     side, or one version's plan legitimately excludes resources the
//     other's includes: each version decides its own document's existence
//     (ADR 0013), and the gate can't tell these apart, so such a cutover
//     needs a CountTolerance that covers the gap.
//  4. stale-backlog: no resource of the type may have been stale longer than
//     MaxStaleAge — a caught-up type clears marks within seconds, so an old
//     mark means an unfinished backfill or unhealthy ingest.
//
// A fetch error fails the gate it prevented, never the whole run: every
// resource is always reported.
func CheckCutoverReadiness(ctx context.Context, es CutoverBackend, staleness StaleCounter, resources resource.Configs, opts ReadinessOptions) []ResourceReadiness {
	maxStaleAge := opts.MaxStaleAge
	if maxStaleAge == 0 {
		maxStaleAge = DefaultMaxStaleAge
	}

	out := make([]ResourceReadiness, 0, len(resources))
	for _, cfg := range resources {
		r := ResourceReadiness{
			Resource:    cfg.Resource,
			Alias:       AliasName(cfg.Resource),
			TargetIndex: IndexName(cfg.Resource, cfg.ReadVersion),
		}

		fail := func(name, format string, args ...any) {
			r.Checks = append(r.Checks, ReadinessCheck{Name: name, Detail: fmt.Sprintf(format, args...)})
		}
		pass := func(name, format string, args ...any) {
			r.Checks = append(r.Checks, ReadinessCheck{Name: name, OK: true, Detail: fmt.Sprintf(format, args...)})
		}
		done := func() {
			r.Ready = true
			for _, c := range r.Checks {
				r.Ready = r.Ready && c.OK
			}
			out = append(out, r)
		}

		current, err := es.GetAlias(ctx, r.Alias)
		if err != nil {
			fail(CheckAliasState, "get alias %s: %v", r.Alias, err)
			done()
			continue
		}
		r.CurrentIndex = current
		r.Move = PlanAliasMove(cfg.Resource, current, cfg.ReadVersion)
		if r.Move == AliasForeign {
			fail(CheckAliasState, "alias %s points at hand-built index %q; readiness cannot be assessed (see gen-mapping -force)", r.Alias, current)
			done()
			continue
		}
		pass(CheckAliasState, "%s: %s", describeMove(r.Move), aliasTransition(current, r.TargetIndex))

		exists, err := es.IndexExists(ctx, r.TargetIndex)
		if err != nil {
			fail(CheckTargetIndex, "check index %s: %v", r.TargetIndex, err)
			done()
			continue
		}
		if !exists {
			fail(CheckTargetIndex, "target index %s does not exist — bootstrap it with gen-mapping", r.TargetIndex)
			done()
			continue
		}
		pass(CheckTargetIndex, "target index %s exists", r.TargetIndex)

		// Parity only compares two distinct, naming-scheme-owned indices: a
		// forward or backward move. A create has nothing to compare against;
		// in-sync would compare the index with itself.
		if r.Move == AliasForward || r.Move == AliasBackward {
			currentCount, err := es.CountDocs(ctx, current)
			if err != nil {
				fail(CheckDocParity, "count docs in %s: %v", current, err)
			} else if targetCount, err := es.CountDocs(ctx, r.TargetIndex); err != nil {
				fail(CheckDocParity, "count docs in %s: %v", r.TargetIndex, err)
			} else {
				diff := max(currentCount-targetCount, targetCount-currentCount)
				detail := fmt.Sprintf("%s has %d docs, %s has %d (diff %d, tolerance %d)",
					current, currentCount, r.TargetIndex, targetCount, diff, opts.CountTolerance)
				if diff <= opts.CountTolerance {
					pass(CheckDocParity, "%s", detail)
				} else {
					fail(CheckDocParity, "%s — has the backfill rebuild finished?", detail)
				}
			}
		} else {
			pass(CheckDocParity, "not applicable: %s", describeMove(r.Move))
		}

		count, oldest, err := staleness.CountStale(ctx, cfg.Resource, time.Now().Add(-maxStaleAge))
		switch {
		case err != nil:
			fail(CheckStaleBacklog, "count stale %q resources: %v", cfg.Resource, err)
		case count > 0:
			fail(CheckStaleBacklog, "%d %q resource(s) stale for longer than %s (oldest since %s) — backfill unfinished or ingest unhealthy",
				count, cfg.Resource, maxStaleAge, oldest.UTC().Format(time.RFC3339))
		default:
			pass(CheckStaleBacklog, "no %q resource stale for longer than %s", cfg.Resource, maxStaleAge)
		}

		done()
	}
	return out
}

func describeMove(move AliasMove) string {
	switch move {
	case AliasInSync:
		return "alias already at target"
	case AliasCreate:
		return "alias will be created"
	case AliasForward:
		return "forward cutover"
	case AliasBackward:
		return "rollback"
	case AliasForeign:
		return "hand-built alias"
	default:
		return fmt.Sprintf("unknown move %d", move)
	}
}

func aliasTransition(current, target string) string {
	if current == "" {
		return "(none) -> " + target
	}
	if current == target {
		return current
	}
	return current + " -> " + target
}
