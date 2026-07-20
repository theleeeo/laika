package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

func TestGenerateMappingOmitsReference(t *testing.T) {
	vc := &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "b_id"}},
		Relations: []resource.RelationConfig{
			{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
		},
	}
	m := GenerateMapping(vc)
	props := m["mappings"].(map[string]any)["properties"].(map[string]any)
	if _, ok := props["b"]; ok {
		t.Fatal("reference relation 'b' must not appear in the mapping")
	}
	if _, ok := props["fields"]; !ok {
		t.Fatal("root fields must still be mapped")
	}
}

// relType returns the ES "type" of a top-level relation property in a mapping.
func relType(t *testing.T, m map[string]any, rel string) string {
	t.Helper()
	props := m["mappings"].(map[string]any)["properties"].(map[string]any)
	relMap, ok := props[rel].(map[string]any)
	require.True(t, ok, "relation %q missing from mapping properties", rel)
	typ, ok := relMap["type"].(string)
	require.True(t, ok, "relation %q has no string type", rel)
	return typ
}

func TestGenerateMapping_ManyCardinality_Nested(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource:    "tags",
			Cardinality: "many",
			Fields:      []resource.FieldConfig{{Name: "label"}},
		}},
	}
	require.Equal(t, "nested", relType(t, GenerateMapping(vc), "tags"))
}

func TestGenerateMapping_DefaultCardinality_Nested(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource: "tags",
			// Cardinality empty → defaults to many → nested.
			Fields: []resource.FieldConfig{{Name: "label"}},
		}},
	}
	require.Equal(t, "nested", relType(t, GenerateMapping(vc), "tags"))
}

func TestGenerateMapping_OneCardinality_Object(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource:    "owner",
			Cardinality: "one",
			Fields:      []resource.FieldConfig{{Name: "email"}},
		}},
	}
	require.Equal(t, "object", relType(t, GenerateMapping(vc), "owner"))
}

func TestGenerateMapping_RelationProperties_IncludeIDAndFields(t *testing.T) {
	vc := &resource.VersionConfig{
		Relations: []resource.RelationConfig{{
			Resource:    "owner",
			Cardinality: "one",
			Fields: []resource.FieldConfig{
				{Name: "email"},                // default type → keyword
				{Name: "age", Type: "integer"}, // explicit type
			},
		}},
	}
	props := GenerateMapping(vc)["mappings"].(map[string]any)["properties"].(map[string]any)
	ownerProps := props["owner"].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, "keyword", ownerProps["id"].(map[string]any)["type"])
	require.Equal(t, "keyword", ownerProps["email"].(map[string]any)["type"])
	require.Equal(t, "integer", ownerProps["age"].(map[string]any)["type"])
}

func TestGenerateMapping_RootFieldsWrappedWithESType(t *testing.T) {
	vc := &resource.VersionConfig{
		Fields: []resource.FieldConfig{
			{Name: "title"},              // default → keyword
			{Name: "body", Type: "text"}, // explicit
		},
	}
	props := GenerateMapping(vc)["mappings"].(map[string]any)["properties"].(map[string]any)
	fields := props["fields"].(map[string]any)
	require.Equal(t, "object", fields["type"])
	fieldProps := fields["properties"].(map[string]any)
	require.Equal(t, "keyword", fieldProps["title"].(map[string]any)["type"])
	require.Equal(t, "text", fieldProps["body"].(map[string]any)["type"])
}

func TestGenerateMapping_StandardizedSearchSurfaces(t *testing.T) {
	// Standardized surfaces exist on every index, independent of field config.
	props := GenerateMapping(&resource.VersionConfig{})["mappings"].(map[string]any)["properties"].(map[string]any)

	// Primary: flat n-grammed text with a standard-analyzed .full subfield.
	search := props["search_primary"].(map[string]any)
	require.Equal(t, "text", search["type"])
	require.Equal(t, ngramIndexAnalyzer, search["analyzer"])
	require.Equal(t, ngramSearchAnalyzer, search["search_analyzer"])
	searchFull := search["fields"].(map[string]any)["full"].(map[string]any)
	require.Equal(t, "text", searchFull["type"])
	require.Equal(t, "standard", searchFull["analyzer"])

	// Secondary: nested { text (same analyzer + .full), scope keyword[] }.
	secondary := props["search_secondary"].(map[string]any)
	require.Equal(t, "nested", secondary["type"])
	secondaryProps := secondary["properties"].(map[string]any)
	secondaryText := secondaryProps["text"].(map[string]any)
	require.Equal(t, "text", secondaryText["type"])
	require.Equal(t, ngramIndexAnalyzer, secondaryText["analyzer"])
	require.Equal(t, ngramSearchAnalyzer, secondaryText["search_analyzer"])
	require.Equal(t, "standard", secondaryText["fields"].(map[string]any)["full"].(map[string]any)["analyzer"])
	// scope is a keyword field; keyword natively accepts arrays.
	require.Equal(t, "keyword", secondaryProps["scope"].(map[string]any)["type"])
}

func TestGenerateMapping_NgramAnalysisSettings(t *testing.T) {
	settings := GenerateMapping(&resource.VersionConfig{})["settings"].(map[string]any)

	// max_ngram_diff must cover the tokenizer's gram range or ES rejects it.
	require.Equal(t, ngramMaxGram-ngramMinGram, settings["index"].(map[string]any)["max_ngram_diff"])

	analysis := settings["analysis"].(map[string]any)

	tok := analysis["tokenizer"].(map[string]any)[ngramTokenizer].(map[string]any)
	require.Equal(t, "ngram", tok["type"])
	require.Equal(t, ngramMinGram, tok["min_gram"])
	require.Equal(t, ngramMaxGram, tok["max_gram"])
	require.Equal(t, []any{"letter", "digit"}, tok["token_chars"])

	analyzers := analysis["analyzer"].(map[string]any)
	// Index-time: n-gram tokenizer + lowercase.
	idx := analyzers[ngramIndexAnalyzer].(map[string]any)
	require.Equal(t, ngramTokenizer, idx["tokenizer"])
	require.Equal(t, []any{"lowercase"}, idx["filter"])
	// Search-time: standard tokenizer + lowercase, NOT n-grammed.
	srch := analyzers[ngramSearchAnalyzer].(map[string]any)
	require.Equal(t, "standard", srch["tokenizer"])
	require.Equal(t, []any{"lowercase"}, srch["filter"])
}

// GenerateMappings (used by gen-mapping for the all-resources path) must produce
// one entry per version keyed by IndexName, each with the correct relation type.
func TestGenerateMappings_PerVersionWithCardinality(t *testing.T) {
	cfgs := resource.Configs{{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{Version: 1, Relations: []resource.RelationConfig{{
				Resource: "c", Cardinality: "many", Fields: []resource.FieldConfig{{Name: "number"}},
			}}},
			{Version: 2, Relations: []resource.RelationConfig{{
				Resource: "b", Cardinality: "one", Fields: []resource.FieldConfig{{Name: "name"}},
			}}},
		},
	}}

	all := GenerateMappings(cfgs)
	v1 := core.IndexName("a", 1)
	v2 := core.IndexName("a", 2)
	require.Contains(t, all, v1)
	require.Contains(t, all, v2)
	require.Equal(t, "nested", relType(t, all[v1], "c"))
	require.Equal(t, "object", relType(t, all[v2], "b"))
}
