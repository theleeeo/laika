package elasticsearch

import (
	"fmt"
	"io"
)

// IndexDiff pairs an index name with the outcome of diffing its expected mapping
// against the running cluster. Exists is false when the index has not been
// created yet, in which case Diff is zero.
type IndexDiff struct {
	Index  string
	Exists bool
	Diff   MappingDiff
}

// HasDrift reports whether the set of diffs contains anything that requires
// action: a field the config would add or change, or an index that does not yet
// exist. Removed fields alone are informational and do not count.
func HasDrift(diffs []IndexDiff) bool {
	for _, d := range diffs {
		if !d.Exists || d.Diff.Drift() {
			return true
		}
	}
	return false
}

// ReportDiffs writes a human-readable, per-index summary of diffs to w and
// returns HasDrift(diffs).
func ReportDiffs(w io.Writer, diffs []IndexDiff) bool {
	for _, d := range diffs {
		switch {
		case !d.Exists:
			fmt.Fprintf(w, "%s  (not created yet)\n", d.Index)
		case d.Diff.Empty():
			fmt.Fprintf(w, "%s  in sync\n", d.Index)
		default:
			fmt.Fprintln(w, d.Index)
			for _, f := range d.Diff.Added {
				fmt.Fprintf(w, "  + %-32s %s (added)\n", f.Path, f.ExpectedType)
			}
			for _, f := range d.Diff.Changed {
				fmt.Fprintf(w, "  ~ %-32s %s → %s (CHANGED — reindex required)\n", f.Path, f.ActualType, f.ExpectedType)
			}
			for _, f := range d.Diff.Removed {
				fmt.Fprintf(w, "  - %-32s %s (removed from config)\n", f.Path, f.ActualType)
			}
		}
	}
	return HasDrift(diffs)
}
