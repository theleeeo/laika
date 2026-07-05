package elasticsearch

import (
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// n-gram analyzer parameters for the standardized searchable surfaces. Grams
// stay within a component (letter/digit runs), never bridging a hyphen or
// space, so the middle and suffix of compound name forms are matchable.
// min/max_gram are reindex-to-change; see spec D15.
const (
	ngramMinGram = 2
	ngramMaxGram = 3

	// ngramIndexAnalyzer shreds indexed text into 2–3 char grams. The query
	// term is matched against those grams but is not itself shredded, so it uses
	// ngramSearchAnalyzer (standard + lowercase) at search time.
	ngramIndexAnalyzer  = "search_ngram"
	ngramSearchAnalyzer = "search_ngram_query"
	ngramTokenizer      = "search_ngram_tokenizer"
)

// searchSettings is the index settings block defining the n-gram analysis chain
// shared by `search` and `search_scoped.text`. max_ngram_diff must be at least
// max_gram - min_gram or ES rejects the tokenizer.
func searchSettings() map[string]any {
	return map[string]any{
		"index": map[string]any{
			"max_ngram_diff": ngramMaxGram - ngramMinGram,
		},
		"analysis": map[string]any{
			"tokenizer": map[string]any{
				ngramTokenizer: map[string]any{
					"type":        "ngram",
					"min_gram":    ngramMinGram,
					"max_gram":    ngramMaxGram,
					"token_chars": []any{"letter", "digit"},
				},
			},
			"analyzer": map[string]any{
				ngramIndexAnalyzer: map[string]any{
					"type":      "custom",
					"tokenizer": ngramTokenizer,
					"filter":    []any{"lowercase"},
				},
				ngramSearchAnalyzer: map[string]any{
					"type":      "custom",
					"tokenizer": "standard",
					"filter":    []any{"lowercase"},
				},
			},
		},
	}
}

// searchableTextField is the mapping for an n-grammed searchable surface: a
// substring-matchable `text` body plus a `.full` standard-analyzed subfield the
// query boosts for whole-token/exact precision over incidental infix hits.
func searchableTextField() map[string]any {
	return map[string]any{
		"type":            "text",
		"analyzer":        ngramIndexAnalyzer,
		"search_analyzer": ngramSearchAnalyzer,
		"fields": map[string]any{
			"full": map[string]any{
				"type":     "text",
				"analyzer": "standard",
			},
		},
	}
}

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
		// Standardized searchable surfaces. Every index carries both so a
		// federated query is a uniform match with comparable scores; the
		// indexer populates them at Build time (spec D4, populated in #12).
		//
		// search — flat primary tier: a Document's own high-signal text.
		"search": searchableTextField(),
		// search_scoped — nested secondary tier: lower-signal / denormalized
		// child text, each entry optionally attributed to visibility scopes.
		// scope is a keyword array (a Child may be visible to several tenants);
		// a term on it matches when the array contains the caller's value.
		"search_scoped": map[string]any{
			"type": "nested",
			"properties": map[string]any{
				"text":  searchableTextField(),
				"scope": map[string]any{"type": "keyword"},
			},
		},
	}

	for _, rel := range vc.Relations {
		if rel.IsReference() {
			// reference relations store no child fields on the parent; only the
			// join key (a root field) is needed, and it is already mapped above.
			continue
		}

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
		"settings": searchSettings(),
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
			result[core.IndexName(cfg.Resource, vc.Version)] = GenerateMapping(&vc)
		}
	}
	return result
}
