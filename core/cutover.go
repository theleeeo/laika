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
// size and age of one resource type's stale backlog, and how much of the type
// a Schema Version has answered for. *postgres.Store implements it.
type StaleCounter interface {
	// CountStale returns how many resources of the type have a stale mark
	// older than before, and the oldest such mark (zero when count is 0).
	CountStale(ctx context.Context, resourceType string, before time.Time) (count int, oldest time.Time, err error)
	// CountMissingEdgeSets returns how many resources of the type have a row
	// that isn't a tombstone, and how many of those with no stale mark lack
	// an edge set of the Schema Version.
	CountMissingEdgeSets(ctx context.Context, resourceType string, schemaVersion int) (rows, missing int, err error)
}

// DefaultMaxStaleAge is the stale-backlog age ReadinessOptions falls back to.
const DefaultMaxStaleAge = 10 * time.Minute

// Names of the readiness checks a ResourceReadiness can carry.
const (
	CheckAliasState   = "alias-state"
	CheckTargetIndex  = "target-index"
	CheckCoverage     = "coverage"
	CheckDocGap       = "doc-gap"
	CheckStaleBacklog = "stale-backlog"
)

// ReadinessOptions tunes the cutover readiness gates.
type ReadinessOptions struct {
	// AcceptGap names the resource types whose document gap the operator
	// has verified: the doc-gap gate passes them whatever the gap. It passes
	// no other gate.
	AcceptGap map[string]bool
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
//  3. coverage: for a forward or backward move, every resource row of the
//     type with no stale mark, tombstones aside, must carry an edge set of
//     the target's Schema Version. Every build that runs the version's plan
//     writes one, an empty set for a nil (ADR 0013), so a row without one
//     hasn't had the version's answer: the backfill hasn't reached it. Stale
//     rows are the stale-backlog gate's. Nothing passes this gate but the
//     sets.
//  4. doc-gap: for a forward or backward move, the target's document count
//     minus the current read index's. Each version decides its own
//     document's existence (ADR 0013), so a covered target may still hold a
//     different set of documents; a gap other than 0 fails unless the type
//     is in AcceptGap, the operator having verified it.
//  5. stale-backlog: no resource of the type may have been stale longer than
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

		// Coverage and the gap gate only a move between two distinct,
		// naming-scheme-owned indices, forward or backward: a create has no
		// current read index to compare with, and in-sync has already moved.
		if r.Move == AliasForward || r.Move == AliasBackward {
			rows, missing, coverageErr := staleness.CountMissingEdgeSets(ctx, cfg.Resource, cfg.ReadVersion)
			switch {
			case coverageErr != nil:
				fail(CheckCoverage, "count %q resources lacking a v%d edge set: %v", cfg.Resource, cfg.ReadVersion, coverageErr)
			case missing > 0:
				fail(CheckCoverage, "%d of %d %q resources with no stale mark lack a v%d edge set — has the backfill finished?",
					missing, rows, cfg.Resource, cfg.ReadVersion)
			default:
				pass(CheckCoverage, "every %q resource with no stale mark has a v%d edge set (%d resources)",
					cfg.Resource, cfg.ReadVersion, rows)
			}

			currentCount, err := es.CountDocs(ctx, current)
			if err != nil {
				fail(CheckDocGap, "count docs in %s: %v", current, err)
			} else if targetCount, err := es.CountDocs(ctx, r.TargetIndex); err != nil {
				fail(CheckDocGap, "count docs in %s: %v", r.TargetIndex, err)
			} else {
				gap := targetCount - currentCount
				ofRows := ""
				if coverageErr == nil {
					ofRows = fmt.Sprintf(", of %d %q resources", rows, cfg.Resource)
				}
				detail := fmt.Sprintf("%s holds %d, %s holds %d%s (gap %+d)",
					current, currentCount, r.TargetIndex, targetCount, ofRows, gap)
				switch {
				case gap == 0:
					pass(CheckDocGap, "%s", detail)
				case opts.AcceptGap[cfg.Resource]:
					pass(CheckDocGap, "%s (accepted)", detail)
				default:
					fail(CheckDocGap, "%s — verify it, then rerun with -accept-gap %s", detail, cfg.Resource)
				}
			}
		} else {
			pass(CheckCoverage, "not applicable: %s", describeMove(r.Move))
			pass(CheckDocGap, "not applicable: %s", describeMove(r.Move))
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
