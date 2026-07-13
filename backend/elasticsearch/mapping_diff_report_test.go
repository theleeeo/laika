package elasticsearch

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasDrift_AddedOrChangedOrMissing(t *testing.T) {
	require.False(t, HasDrift(nil), "no diffs → no drift")

	inSync := []IndexDiff{{Index: "a_v1", Exists: true}}
	require.False(t, HasDrift(inSync))

	removedOnly := []IndexDiff{{Index: "a_v1", Exists: true, Diff: MappingDiff{
		Removed: []FieldDiff{{Path: "fields.x", ActualType: "keyword", Kind: DiffRemoved}},
	}}}
	require.False(t, HasDrift(removedOnly), "removed-only is informational, not drift")

	added := []IndexDiff{{Index: "a_v1", Exists: true, Diff: MappingDiff{
		Added: []FieldDiff{{Path: "fields.y", ExpectedType: "keyword", Kind: DiffAdded}},
	}}}
	require.True(t, HasDrift(added))

	changed := []IndexDiff{{Index: "a_v1", Exists: true, Diff: MappingDiff{
		Changed: []FieldDiff{{Path: "fields.z", ExpectedType: "text", ActualType: "keyword", Kind: DiffChanged}},
	}}}
	require.True(t, HasDrift(changed))

	missing := []IndexDiff{{Index: "a_v1", Exists: false}}
	require.True(t, HasDrift(missing), "a not-yet-created index counts as drift")
}

func TestReportDiffs_HumanOutput(t *testing.T) {
	diffs := []IndexDiff{
		{Index: "in_sync_v1", Exists: true},
		{Index: "missing_v1", Exists: false},
		{Index: "drift_v1", Exists: true, Diff: MappingDiff{
			Added:   []FieldDiff{{Path: "fields.phone", ExpectedType: "keyword", Kind: DiffAdded}},
			Changed: []FieldDiff{{Path: "fields.zip", ExpectedType: "text", ActualType: "keyword", Kind: DiffChanged}},
			Removed: []FieldDiff{{Path: "fields.legacy", ActualType: "keyword", Kind: DiffRemoved}},
		}},
	}

	var buf bytes.Buffer
	drift := ReportDiffs(&buf, diffs)
	require.True(t, drift, "return value must match HasDrift")

	out := buf.String()
	require.Contains(t, out, "in_sync_v1")
	require.Contains(t, out, "in sync")
	require.Contains(t, out, "missing_v1")
	require.Contains(t, out, "not created")
	require.Contains(t, out, "fields.phone")
	require.Contains(t, out, "fields.zip")
	require.Contains(t, out, "keyword") // actual type of the changed field
	require.Contains(t, out, "text")    // expected type of the changed field
	require.Contains(t, out, "fields.legacy")
	// The changed field must be flagged as reindex-requiring.
	require.True(t, strings.Contains(strings.ToLower(out), "reindex"), "changed fields must warn about reindex, got:\n%s", out)
}
