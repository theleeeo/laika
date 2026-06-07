package es

import "github.com/theleeeo/indexer/core/resource"

// GenerateMapping builds an Elasticsearch index mapping from a version config.
func GenerateMapping(vc *resource.VersionConfig) map[string]any {
	fieldsProps := make(map[string]any, len(vc.Fields))
	for _, f := range vc.Fields {
		fieldsProps[f.Name] = map[string]any{
			"type": f.ESType(),
		}
	}

	properties := map[string]any{
		"fields": map[string]any{
			"type":       "object",
			"properties": fieldsProps,
		},
	}

	for _, rel := range vc.Relations {
		relProps := make(map[string]any, len(rel.Fields)+1)
		relProps["id"] = map[string]any{"type": "keyword"}
		for _, f := range rel.Fields {
			relProps[f.Name] = map[string]any{
				"type": f.ESType(),
			}
		}

		relType := "nested"
		if !rel.IsMany() {
			relType = "object"
		}

		properties[rel.Resource] = map[string]any{
			"type":       relType,
			"properties": relProps,
		}
	}

	return map[string]any{
		"mappings": map[string]any{
			"properties": properties,
		},
	}
}

// GenerateMappings builds ES index mappings for all resource configs.
// Returns a map of versioned index name -> mapping.
// Each version in the config produces a separate entry with its own schema.
func GenerateMappings(configs resource.Configs) map[string]map[string]any {
	result := make(map[string]map[string]any)
	for _, cfg := range configs {
		for _, vc := range cfg.Versions {
			result[IndexName(cfg.Resource, vc.Version)] = GenerateMapping(&vc)
		}
	}
	return result
}
