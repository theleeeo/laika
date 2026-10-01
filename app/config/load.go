package config

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/theleeeo/laika/core/resource"
)

// LoadConfig reads a resource config file at the given path and returns
// the parsed Configs with defaults applied.
func LoadConfig(path string) (resource.Configs, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	return ParseConfig(data)
}

// rawFile is the top-level YAML structure.
type rawFile struct {
	Resources []rawEntry `yaml:"resources"`
}

// rawEntry represents a single entry in the resources list.
// The canonical shape is flat, identical with and without a version key:
// Fields is a []FieldConfig list, Relations and NestedBlocks are sibling keys.
// Versioned entries may instead use the legacy nested shape, where Fields is
// a VersionConfig object containing {fields, relations, nestedBlocks}.
type rawEntry struct {
	Type         string                       `yaml:"type"`
	Version      int                          `yaml:"version,omitempty"`
	ReadVersion  int                          `yaml:"readVersion,omitempty"`
	Fields       any                          `yaml:"fields"`
	Relations    []resource.RelationConfig    `yaml:"relations,omitempty"`
	NestedBlocks []resource.NestedBlockConfig `yaml:"nestedBlocks,omitempty"`
}

// ParseConfig parses resource config YAML bytes into Configs and warns about
// every version with no primary-tier field.
func ParseConfig(data []byte) (resource.Configs, error) {
	var raw rawFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}
	if len(raw.Resources) == 0 {
		return nil, fmt.Errorf("at least one resource required")
	}

	configMap := make(map[string]*resource.Config)
	var configOrder []string

	for _, entry := range raw.Resources {
		if entry.Type == "" {
			return nil, fmt.Errorf("resource entry missing type")
		}

		cfg, exists := configMap[entry.Type]
		if !exists {
			cfg = &resource.Config{
				Resource: entry.Type,
			}
			configMap[entry.Type] = cfg
			configOrder = append(configOrder, entry.Type)
		}

		if entry.ReadVersion != 0 {
			cfg.ReadVersion = entry.ReadVersion
		}

		version := entry.Version
		if version == 0 {
			version = 1
		}

		vc, err := parseEntrySchema(entry)
		if err != nil {
			return nil, fmt.Errorf("resource %q version %d: %w", entry.Type, version, err)
		}
		vc.Version = version

		// Check for duplicate version.
		for _, existing := range cfg.Versions {
			if existing.Version == version {
				return nil, fmt.Errorf("resource %q version %d defined more than once", entry.Type, version)
			}
		}
		cfg.Versions = append(cfg.Versions, *vc)
	}

	configs := make(resource.Configs, 0, len(configOrder))
	for _, name := range configOrder {
		cfg := configMap[name]
		cfg.ApplyDefaults()
		configs = append(configs, cfg)
	}
	configs.WarnMissingPrimaryTier(slog.Default())

	return configs, nil
}

// parseEntrySchema extracts a VersionConfig from a raw YAML entry.
//
// The canonical flat shape — "fields" is a []FieldConfig list, "relations"
// and "nestedBlocks" are sibling keys — works with and without a version key,
// so adding `version:` never restructures the entry. A "fields" key holding a
// mapping instead of a list selects the legacy nested shape, an object with
// {fields, relations, nestedBlocks} sub-keys (i.e. a VersionConfig); there the
// sibling keys would be silently shadowed, so combining the two is an error.
func parseEntrySchema(entry rawEntry) (*resource.VersionConfig, error) {
	if entry.Fields == nil {
		return &resource.VersionConfig{Relations: entry.Relations, NestedBlocks: entry.NestedBlocks}, nil
	}

	b, err := yaml.Marshal(entry.Fields)
	if err != nil {
		return nil, fmt.Errorf("re-marshal fields: %w", err)
	}

	if _, nested := entry.Fields.(map[string]any); nested {
		if len(entry.Relations) > 0 || len(entry.NestedBlocks) > 0 {
			return nil, fmt.Errorf("entry uses both the nested fields shape ({fields, relations, nestedBlocks} under \"fields\") and sibling relations/nestedBlocks keys; use one shape")
		}
		var vc resource.VersionConfig
		if err := yaml.Unmarshal(b, &vc); err != nil {
			return nil, fmt.Errorf("parse version schema: %w", err)
		}
		return &vc, nil
	}

	var fields []resource.FieldConfig
	if err := yaml.Unmarshal(b, &fields); err != nil {
		return nil, fmt.Errorf("parse fields: %w", err)
	}
	return &resource.VersionConfig{
		Fields:       fields,
		Relations:    entry.Relations,
		NestedBlocks: entry.NestedBlocks,
	}, nil
}
