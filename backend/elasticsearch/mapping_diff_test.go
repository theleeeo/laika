package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

// Helpers mirroring the two mapping shapes DiffMapping must handle.

func mappingsDoc(props map[string]any) map[string]any {
	return map[string]any{"properties": props}
}
func leaf(t string) map[string]any { return map[string]any{"type": t} }
func object(props map[string]any) map[string]any {
	// How GenerateMapping emits an object container: explicit "type": "object".
	return map[string]any{"type": "object", "properties": props}
}
func esObject(props map[string]any) map[string]any {
	// How Elasticsearch echoes an object container back: "type" omitted, since
	// object is the implicit default.
	return map[string]any{"properties": props}
}
func nested(props map[string]any) map[string]any {
	return map[string]any{"type": "nested", "properties": props}
}

func TestDiffMapping_InSync_ObjectTypeNormalized(t *testing.T) {
	// Expected uses explicit object type; the running index (ES) omits it.
	// These must be treated as equal, not a spurious "changed".
	expected := mappingsDoc(map[string]any{
		"fields": object(map[string]any{"name": leaf("keyword")}),
	})
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{"name": leaf("keyword")}),
	})

	d := DiffMapping(expected, actual)
	require.True(t, d.Empty(), "expected no diff, got %+v", d)
	require.False(t, d.Drift())
}

func TestDiffMapping_AddedField(t *testing.T) {
	expected := mappingsDoc(map[string]any{
		"fields": object(map[string]any{
			"name":  leaf("keyword"),
			"phone": leaf("keyword"),
		}),
	})
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{"name": leaf("keyword")}),
	})

	d := DiffMapping(expected, actual)
	require.True(t, d.Drift())
	require.Len(t, d.Added, 1)
	require.Equal(t, FieldDiff{Path: "fields.phone", ExpectedType: "keyword", Kind: DiffAdded}, d.Added[0])
	require.Empty(t, d.Changed)
	require.Empty(t, d.Removed)
}

func TestDiffMapping_ChangedType(t *testing.T) {
	expected := mappingsDoc(map[string]any{
		"fields": object(map[string]any{"zip": leaf("text")}),
	})
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{"zip": leaf("keyword")}),
	})

	d := DiffMapping(expected, actual)
	require.True(t, d.Drift())
	require.Len(t, d.Changed, 1)
	require.Equal(t, FieldDiff{Path: "fields.zip", ExpectedType: "text", ActualType: "keyword", Kind: DiffChanged}, d.Changed[0])
	require.Empty(t, d.Added)
}

func TestDiffMapping_RemovedField_NotDrift(t *testing.T) {
	expected := mappingsDoc(map[string]any{
		"fields": object(map[string]any{"name": leaf("keyword")}),
	})
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{
			"name":      leaf("keyword"),
			"legacy_id": leaf("keyword"),
		}),
	})

	d := DiffMapping(expected, actual)
	require.False(t, d.Drift(), "removed-only fields must not count as drift")
	require.False(t, d.Empty(), "removed field is still a difference")
	require.Len(t, d.Removed, 1)
	require.Equal(t, FieldDiff{Path: "fields.legacy_id", ActualType: "keyword", Kind: DiffRemoved}, d.Removed[0])
}

func TestDiffMapping_NestedRelationSubtree(t *testing.T) {
	// A nested relation's child field changes type; the nested container itself
	// matches and must not be reported.
	expected := mappingsDoc(map[string]any{
		"owner": nested(map[string]any{
			"id":    leaf("keyword"),
			"email": leaf("text"),
		}),
	})
	actual := mappingsDoc(map[string]any{
		"owner": nested(map[string]any{
			"id":    leaf("keyword"),
			"email": leaf("keyword"),
		}),
	})

	d := DiffMapping(expected, actual)
	require.Len(t, d.Changed, 1)
	require.Equal(t, FieldDiff{Path: "owner.email", ExpectedType: "text", ActualType: "keyword", Kind: DiffChanged}, d.Changed[0])
	require.Empty(t, d.Added)
	require.Empty(t, d.Removed)
}

func TestDiffMapping_MultipleDiffsSortedByPath(t *testing.T) {
	expected := mappingsDoc(map[string]any{
		"fields": object(map[string]any{
			"alpha": leaf("keyword"), // added
			"gamma": leaf("keyword"), // added
		}),
	})
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{}),
	})

	d := DiffMapping(expected, actual)
	require.Len(t, d.Added, 2)
	require.Equal(t, "fields.alpha", d.Added[0].Path, "diffs must be sorted by path")
	require.Equal(t, "fields.gamma", d.Added[1].Path)
}

// DiffMapping must accept a full generated mapping (with top-level "mappings"
// and "settings") as the expected side, ignoring settings and analyzer keys, and
// diff it against the bare mappings object GetMapping returns.
func TestDiffMapping_UnwrapsGeneratedMappingAndIgnoresAnalyzers(t *testing.T) {
	vc := &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "name", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}},
	}
	expected := GenerateMapping(vc) // full doc: has "settings" and "mappings"

	// Running index as ES would report it: object container without a type,
	// search surfaces carry only their type (analyzer/subfield keys omitted).
	actual := mappingsDoc(map[string]any{
		"fields": esObject(map[string]any{"name": leaf("keyword")}),
		"search_primary": leaf("text"),
		"search_secondary": nested(map[string]any{
			"text":  leaf("text"),
			"scope": leaf("keyword"),
		}),
	})

	d := DiffMapping(expected, actual)
	require.True(t, d.Empty(), "generated mapping should diff clean against equivalent ES mapping, got %+v", d)
}
