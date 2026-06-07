package config

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/theleeeo/indexer/core/resource"
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
// For versioned entries (version > 0), Fields is a VersionConfig object
// containing {fields, relations}. For unversioned entries, Fields is
// a []FieldConfig list and Relations is a sibling key.
type rawEntry struct {
	Type        string                    `yaml:"type"`
	Version     int                       `yaml:"version,omitempty"`
	ReadVersion int                       `yaml:"readVersion,omitempty"`
	Fields      any                       `yaml:"fields"`
	Relations   []resource.RelationConfig `yaml:"relations,omitempty"`
}

// ParseConfig parses resource config YAML bytes into Configs.
func ParseConfig(data []byte) (resource.Configs, error) {
	var raw rawFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
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

	return configs, nil
}

// parseEntrySchema extracts a VersionConfig from a raw YAML entry.
//
// For versioned entries (entry.Version > 0), the "fields" YAML key is an
// object with {fields, relations} sub-keys (i.e. a VersionConfig).
//
// For unversioned entries, "fields" is a direct []FieldConfig list and
// "relations" is a sibling key on the entry.
func parseEntrySchema(entry rawEntry) (*resource.VersionConfig, error) {
	if entry.Fields == nil {
		return &resource.VersionConfig{Relations: entry.Relations}, nil
	}

	b, err := yaml.Marshal(entry.Fields)
	if err != nil {
		return nil, fmt.Errorf("re-marshal fields: %w", err)
	}

	if entry.Version > 0 {
		// Versioned: fields is {fields: [...], relations: [...]}
		var vc resource.VersionConfig
		if err := yaml.Unmarshal(b, &vc); err != nil {
			return nil, fmt.Errorf("parse version schema: %w", err)
		}
		return &vc, nil
	}

	// Unversioned: fields is a direct [...] list
	var fields []resource.FieldConfig
	if err := yaml.Unmarshal(b, &fields); err != nil {
		return nil, fmt.Errorf("parse fields: %w", err)
	}
	return &resource.VersionConfig{
		Fields:    fields,
		Relations: entry.Relations,
	}, nil
}
