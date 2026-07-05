package dsl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
)

// primary builds a FieldConfig fed to the primary search tier.
func primaryField(name string) resource.FieldConfig {
	return resource.FieldConfig{Name: name, Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}
}

// secondary builds a FieldConfig fed to the secondary search_scoped tier.
func secondaryField(name string) resource.FieldConfig {
	return resource.FieldConfig{Name: name, Query: resource.QueryConfig{Search: resource.SearchTierSecondary}}
}

func TestPopulateSearch_PrimaryRootFieldFeedsFlatSearch(t *testing.T) {
	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{
			primaryField("name"),
			{Name: "internal_id"}, // omitted tier → none, excluded
		},
	}
	doc := projection.BuildDoc{Doc: map[string]any{
		"fields": map[string]any{"name": "Acme Corp", "internal_id": "x-999"},
	}}

	out := populateStandardizedSearchFields(vc, doc)

	require.Equal(t, []string{"Acme Corp"}, out.Doc["search"])
	require.NotContains(t, out.Doc["search"], "x-999")
	require.Nil(t, out.Doc["search_scoped"], "no secondary fields → no scoped entries")
}

func TestPopulateSearch_SecondaryChildFieldFeedsScopedEntryPerRow(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource: "tags",
			Fields:   []resource.FieldConfig{secondaryField("label")},
		}},
	}
	doc := projection.BuildDoc{Doc: map[string]any{
		"fields": map[string]any{},
		"tags": []map[string]any{
			{"id": "t1", "label": "red"},
			{"id": "t2", "label": "blue"},
		},
	}}

	out := populateStandardizedSearchFields(vc, doc)

	require.Nil(t, out.Doc["search"], "secondary text must not leak into the flat primary surface")
	scoped := out.Doc["search_scoped"].([]map[string]any)
	require.Len(t, scoped, 2, "one scoped entry per child row")
	require.Equal(t, []string{"red"}, scoped[0]["text"])
	require.Equal(t, []string{"blue"}, scoped[1]["text"])
}

// The standalone app never attributes scope: every scoped entry it emits has an
// empty scope array (spec D10).
func TestPopulateSearch_ScopeEmptyInAppMode(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource: "notes",
			Fields:   []resource.FieldConfig{secondaryField("body")},
		}},
	}
	doc := projection.BuildDoc{Doc: map[string]any{
		"fields": map[string]any{},
		"notes":  []map[string]any{{"id": "n1", "body": "hidden text"}},
	}}

	out := populateStandardizedSearchFields(vc, doc)

	scoped := out.Doc["search_scoped"].([]map[string]any)
	require.Len(t, scoped, 1)
	require.Equal(t, []string{}, scoped[0]["scope"], "app leaves scope empty (unscoped)")
}

// A reference relation is never denormalized into the parent Build, so its
// fields cannot contribute — even a secondary tier selector is ignored (D6).
func TestPopulateSearch_ReferenceRelationExcluded(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource: "b",
			Strategy: resource.StrategyReference,
			Fields:   []resource.FieldConfig{secondaryField("name")},
		}},
	}
	// Even if data were somehow present under the relation key, it must be skipped.
	doc := projection.BuildDoc{Doc: map[string]any{
		"fields": map[string]any{},
		"b":      []map[string]any{{"id": "b1", "name": "should not appear"}},
	}}

	out := populateStandardizedSearchFields(vc, doc)

	require.Nil(t, out.Doc["search"])
	require.Nil(t, out.Doc["search_scoped"])
}

func TestPopulateSearch_PrimaryChildAndSecondaryRootMix(t *testing.T) {
	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{secondaryField("aka")},
		Relations: []resource.RelationConfig{{
			Resource: "owner",
			Fields:   []resource.FieldConfig{primaryField("name")},
		}},
	}
	doc := projection.BuildDoc{Doc: map[string]any{
		"fields": map[string]any{"aka": "nickname"},
		"owner":  map[string]any{"id": "o1", "name": "Alice"}, // cardinality:one shape
	}}

	out := populateStandardizedSearchFields(vc, doc)

	// Primary child field lands in the flat surface.
	require.Equal(t, []string{"Alice"}, out.Doc["search"])
	// Secondary root field lands in a single scoped entry.
	scoped := out.Doc["search_scoped"].([]map[string]any)
	require.Len(t, scoped, 1)
	require.Equal(t, []string{"nickname"}, scoped[0]["text"])
}

func TestPopulateSearch_NilDocPassesThrough(t *testing.T) {
	vc := &resource.VersionConfig{Fields: []resource.FieldConfig{primaryField("name")}}
	out := populateStandardizedSearchFields(vc, projection.BuildDoc{Doc: nil})
	require.Nil(t, out.Doc)
}

// End-to-end: a plan built from config populates the standardized surfaces on
// the executed document, proving the terminal stage is wired into the chain.
func TestBuildPlan_PopulatesStandardizedSearchFields(t *testing.T) {
	prov := newMockProvider()
	prov.resources["order|1"] = map[string]any{"id": "1", "number": "ORD-1", "customer_id": "c1"}
	prov.related["customer|c1"] = []map[string]any{{"id": "c1", "name": "Alice"}}

	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{primaryField("number")},
		Relations: []resource.RelationConfig{{
			Resource:    "customer",
			Cardinality: "one",
			Join:        resource.JoinConfig{Local: "customer_id", Foreign: "id"},
			Fields:      []resource.FieldConfig{secondaryField("name")},
		}},
	}

	doc := execSinglePlan(t, prov, vc, "order", "1")

	require.Equal(t, []string{"ORD-1"}, doc.Doc["search"])
	scoped := doc.Doc["search_scoped"].([]map[string]any)
	require.Len(t, scoped, 1)
	require.Equal(t, []string{"Alice"}, scoped[0]["text"])
	require.Equal(t, []string{}, scoped[0]["scope"])
}
