package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

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
				{Name: "email"},               // default type → keyword
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
			{Name: "title"},             // default → keyword
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
