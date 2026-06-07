package resource

import (
	"fmt"
	"sort"
)

type Configs []*Config

func (c Configs) Get(resource string) *Config {
	for _, rc := range c {
		if rc.Resource == resource {
			return rc
		}
	}
	return nil
}

// VersionConfig holds the schema definition for a single version of a resource.
type VersionConfig struct {
	Version   int              `yaml:"version"`
	Fields    []FieldConfig    `yaml:"fields"`
	Relations []RelationConfig `yaml:"relations"`
}

// GetSearchableFields returns the list of ES field paths that are included
// in multi_match full-text search for this version.
func (vc *VersionConfig) GetSearchableFields() []string {
	var fields []string
	for _, f := range vc.Fields {
		if f.Query.Search == nil || *f.Query.Search {
			fields = append(fields, "fields."+f.Name)
		}
	}

	for _, r := range vc.Relations {
		for _, f := range r.Fields {
			if f.Query.Search == nil || *f.Query.Search {
				fields = append(fields, fmt.Sprintf("%s.%s", r.Resource, f.Name))
			}
		}
	}

	return fields
}

// GetRelation returns the relation config for the given resource name, or nil.
func (vc *VersionConfig) GetRelation(resource string) *RelationConfig {
	for _, r := range vc.Relations {
		if r.Resource == resource {
			return &r
		}
	}
	return nil
}

type Config struct {
	Resource string `yaml:"resource"`

	// Versions holds the schema definitions for each version of the resource.
	Versions []VersionConfig `yaml:"versions,omitempty"`

	// ReadVersion is the version whose index the read alias points to.
	// Must match one of the Versions entries. Defaults to the lowest version.
	ReadVersion int `yaml:"readVersion"`
}

// SortedVersions returns the version numbers in ascending order.
func (c *Config) SortedVersions() []int {
	versions := make([]int, 0, len(c.Versions))
	for _, vc := range c.Versions {
		versions = append(versions, vc.Version)
	}
	sort.Ints(versions)
	return versions
}

// GetVersion returns the VersionConfig for the given version number, or nil.
func (c *Config) GetVersion(v int) *VersionConfig {
	for i := range c.Versions {
		if c.Versions[i].Version == v {
			return &c.Versions[i]
		}
	}
	return nil
}

// ReadVersionConfig returns the VersionConfig for the active read version.
func (c *Config) ReadVersionConfig() *VersionConfig {
	return c.GetVersion(c.ReadVersion)
}

// HasRelationTo reports whether any version of this resource has a relation
// to the given resource type.
func (c *Config) HasRelationTo(resourceType string) bool {
	for _, vc := range c.Versions {
		for _, rel := range vc.Relations {
			if rel.Resource == resourceType {
				return true
			}
		}
	}
	return false
}

// ApplyDefaults fills in zero-value fields with sensible defaults.
// Call this after unmarshalling config from YAML.
func (c *Config) ApplyDefaults() {
	if c.ReadVersion == 0 {
		c.ReadVersion = c.SortedVersions()[0]
	}
}

type FieldConfig struct {
	Name  string      `yaml:"name"`
	Type  string      `yaml:"type"` // ES field type; defaults to "keyword"
	Query QueryConfig `yaml:"query"`
}

func (f FieldConfig) ESType() string {
	if f.Type == "" {
		return "keyword"
	}
	return f.Type
}

type QueryConfig struct {
	// Default true
	Search *bool `yaml:"search"`
}

type KeyConfig struct {
	// Source is the name of the resource to extract the lookup key from.
	// Use the root resource's own name to extract from root data,
	// or a sibling relation name to extract from its resolved data.
	Source string `yaml:"source"`

	// Field is the field name to extract from the source resource's data.
	// The extracted value is passed to the provider.
	Field string `yaml:"field"`
}

type RelationConfig struct {
	Resource    string        `yaml:"resource"`
	Key         KeyConfig     `yaml:"key"`
	Cardinality string        `yaml:"cardinality"` // "one" or "many"; defaults to "many"
	Fields      []FieldConfig `yaml:"fields"`
}

func (r RelationConfig) IsMany() bool {
	return r.Cardinality != "one"
}
