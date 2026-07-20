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
	Version      int                 `yaml:"version"`
	Fields       []FieldConfig       `yaml:"fields"`
	Relations    []RelationConfig    `yaml:"relations"`
	NestedBlocks []NestedBlockConfig `yaml:"nestedBlocks,omitempty"`
}

// GetSearchableFields returns the list of ES field paths that are included
// in multi_match full-text search for this version.
func (vc *VersionConfig) GetSearchableFields() []string {
	var fields []string
	for _, f := range vc.Fields {
		if f.Query.IsSearchable() {
			fields = append(fields, "fields."+f.Name)
		}
	}

	for _, r := range vc.Relations {
		for _, f := range r.Fields {
			if f.Query.IsSearchable() {
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

// GetNestedBlock returns the nested block config for the given name, or nil.
func (vc *VersionConfig) GetNestedBlock(name string) *NestedBlockConfig {
	for i := range vc.NestedBlocks {
		if vc.NestedBlocks[i].Name == name {
			return &vc.NestedBlocks[i]
		}
	}
	return nil
}

// ScopedNestedBlocks returns the tenant-scoped nested blocks of this version.
func (vc *VersionConfig) ScopedNestedBlocks() []NestedBlockConfig {
	var out []NestedBlockConfig
	for _, b := range vc.NestedBlocks {
		if b.IsScoped() {
			out = append(out, b)
		}
	}
	return out
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

// SearchTier selects which standardized searchable surface a field feeds.
// Single-resource Search matches only the primary surface; Federated Search
// matches both tiers and ranks a primary hit above a secondary one. `none`
// excludes the field from full-text search entirely.
type SearchTier string

const (
	// SearchTierNone excludes the field from full-text search. It is the value
	// an omitted selector resolves to.
	SearchTierNone SearchTier = "none"
	// SearchTierPrimary routes the field to the primary `search_primary` surface:
	// a Document's own high-signal text.
	SearchTierPrimary SearchTier = "primary"
	// SearchTierSecondary routes the field to the secondary `search_secondary`
	// surface: lower-signal / denormalized-child text.
	SearchTierSecondary SearchTier = "secondary"
)

type QueryConfig struct {
	// Search selects the full-text searchable tier this field feeds:
	// "primary", "secondary", or "none". Omitted means "none".
	Search SearchTier `yaml:"search"`
}

// Tier returns the field's search tier, resolving an omitted (empty) selector
// to SearchTierNone.
func (q QueryConfig) Tier() SearchTier {
	if q.Search == "" {
		return SearchTierNone
	}
	return q.Search
}

// IsSearchable reports whether the field feeds full-text search at all,
// i.e. its tier is primary or secondary.
func (q QueryConfig) IsSearchable() bool {
	t := q.Tier()
	return t == SearchTierPrimary || t == SearchTierSecondary
}

// JoinConfig names both sides of the join between a resource and a related
// resource. The relation matches rows where the Local field on this resource
// equals the Foreign field on the related resource.
type JoinConfig struct {
	// Local is the field whose value identifies the related resources.
	// By default it is read from the root resource; set From to read it
	// from a sibling relation instead (for chained joins).
	Local string `yaml:"local"`

	// Foreign is the field on the related resource that Local's value is
	// matched against. It is passed to the provider as the lookup key field.
	Foreign string `yaml:"foreign"`

	// From names a sibling relation to read Local from. Empty means the
	// Local field is read from the root resource.
	From string `yaml:"from"`
}

// Relation strategies decide how a child's data reaches the parent's
// searchable surface. denormalize copies the child's fields into the parent
// document (default). reference stores only the join key and resolves the
// child's fields at search time via a two-phase join.
const (
	StrategyDenormalize = "denormalize"
	StrategyReference   = "reference"
)

type RelationConfig struct {
	Resource string     `yaml:"resource"`
	Join     JoinConfig `yaml:"join"`
	// TODO: No default for cardinality; we should require it to be explicit in the config.
	Cardinality string        `yaml:"cardinality"` // "one" or "many"; defaults to "many"
	Strategy    string        `yaml:"strategy"`    // "denormalize" (default) or "reference"
	Fields      []FieldConfig `yaml:"fields"`
}

func (r RelationConfig) IsMany() bool {
	return r.Cardinality != "one"
}

// IsReference reports whether this relation is resolved at search time rather
// than denormalized into the parent document.
func (r RelationConfig) IsReference() bool {
	return r.Strategy == StrategyReference
}

// LocalSource returns the name of the resource the Local join field is read
// from: the sibling relation named by Join.From, or the root resource when
// From is empty.
func (r RelationConfig) LocalSource(root string) string {
	if r.Join.From != "" {
		return r.Join.From
	}
	return root
}

// NestedBlockConfig declares a nested object field on a document that is not a
// relation to another indexed resource — its entries are assembled by the
// resource's Plan (e.g. one entry per tenant). When ScopeKey is set the block
// is tenant-scoped: the search layer always correlates the caller's Scope onto
// <Name>.<ScopeKey> for any filter or visibility check on the block, so one
// tenant can never match another's entry (fail-closed on empty Scope).
type NestedBlockConfig struct {
	Name     string        `yaml:"name"`
	ScopeKey string        `yaml:"scopeKey"`
	Fields   []FieldConfig `yaml:"fields"`
}

// IsScoped reports whether the block is tenant-scoped.
func (b NestedBlockConfig) IsScoped() bool { return b.ScopeKey != "" }
