package elasticsearch

import "sort"

// DiffKind classifies how a single field path differs between the mapping the
// current config would generate and the mapping of the running index.
type DiffKind string

const (
	// DiffAdded: the field exists in the config but not in the running index.
	// Elasticsearch accepts these via a PUT _mapping update — safe.
	DiffAdded DiffKind = "added"
	// DiffChanged: the field exists in both but its type differs. Elasticsearch
	// rejects a type change on an existing field — the index must be reindexed.
	DiffChanged DiffKind = "changed"
	// DiffRemoved: the field exists in the running index but not in the config.
	// Informational only; Elasticsearch never drops a field on its own.
	DiffRemoved DiffKind = "removed"
)

// FieldDiff is a single differing field path. ExpectedType is empty for removed
// fields; ActualType is empty for added fields.
type FieldDiff struct {
	Path         string
	ExpectedType string
	ActualType   string
	Kind         DiffKind
}

// MappingDiff is the set of field differences for one index.
type MappingDiff struct {
	Added   []FieldDiff
	Changed []FieldDiff
	Removed []FieldDiff
}

// Drift reports actionable drift: fields the config adds or changes. Removed
// fields do not count — Elasticsearch never removes them on its own.
func (d MappingDiff) Drift() bool {
	return len(d.Added) > 0 || len(d.Changed) > 0
}

// Empty reports whether there is no difference of any kind, including removed.
func (d MappingDiff) Empty() bool {
	return len(d.Added) == 0 && len(d.Changed) == 0 && len(d.Removed) == 0
}

// DiffMapping compares the mapping the config would generate (expected) against
// a running index mapping (actual). Both may be either a full mapping document
// (with a top-level "mappings" key, as GenerateMapping returns) or a bare
// "mappings" object (with a top-level "properties" key, as GetMapping returns).
// Only field types are compared; the settings/analysis block is ignored.
func DiffMapping(expected, actual map[string]any) MappingDiff {
	exp := flattenProperties(propertiesOf(expected), "")
	act := flattenProperties(propertiesOf(actual), "")

	var d MappingDiff
	for path, expType := range exp {
		actType, ok := act[path]
		switch {
		case !ok:
			d.Added = append(d.Added, FieldDiff{Path: path, ExpectedType: expType, Kind: DiffAdded})
		case actType != expType:
			d.Changed = append(d.Changed, FieldDiff{Path: path, ExpectedType: expType, ActualType: actType, Kind: DiffChanged})
		}
	}
	for path, actType := range act {
		if _, ok := exp[path]; !ok {
			d.Removed = append(d.Removed, FieldDiff{Path: path, ActualType: actType, Kind: DiffRemoved})
		}
	}

	sortByPath(d.Added)
	sortByPath(d.Changed)
	sortByPath(d.Removed)
	return d
}

// propertiesOf extracts the "properties" object from a mapping, tolerating both
// a full mapping document (with a "mappings" wrapper) and a bare mappings object.
func propertiesOf(m map[string]any) map[string]any {
	if inner, ok := m["mappings"].(map[string]any); ok {
		m = inner
	}
	props, _ := m["properties"].(map[string]any)
	return props
}

// flattenProperties walks a properties tree into a flat path -> type map. Object
// and nested containers contribute their own type at their path and recurse into
// their nested "properties". Elasticsearch omits "type" on object containers
// (object is the implicit default), so a typeless node with properties is
// normalized to "object" — this is what keeps the diff free of false positives.
func flattenProperties(props map[string]any, prefix string) map[string]string {
	out := make(map[string]string)
	for name, raw := range props {
		node, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		typ, _ := node["type"].(string)
		child, hasChild := node["properties"].(map[string]any)
		if typ == "" && hasChild {
			typ = "object"
		}
		if typ != "" {
			out[path] = typ
		}
		if hasChild {
			for k, v := range flattenProperties(child, path) {
				out[k] = v
			}
		}
	}
	return out
}

func sortByPath(fs []FieldDiff) {
	sort.Slice(fs, func(i, j int) bool { return fs[i].Path < fs[j].Path })
}
