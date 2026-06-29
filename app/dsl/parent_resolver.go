package dsl

import (
	"slices"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
)

// identityField is the field name that carries a resource's own identity. A
// parent→child relation is reverse-discoverable only when the join matches on
// this field on the parent (Join.Local == identityField): then the parent's
// resource id equals the child's foreign-field value.
const identityField = "id"

// parentRef names a parent type reachable from a child, together with the field
// on the child whose value is that parent's resource id.
type parentRef struct {
	parentType   string
	foreignField string
}

// buildReverseMap derives the reverse-relation map from the join declarations
// across all resource versions: childType → the parents derivable from it.
//
// A relation contributes an entry only when its parent key is derivable from
// the child alone — i.e. the join reads the parent's own identity field
// (Join.Local == "id") directly from the parent (Join.From == ""). Relations
// where the parent holds the foreign key, or that chain through a sibling, are
// not invertible and contribute nothing.
func buildReverseMap(resources resource.Configs) map[string][]parentRef {
	reverse := make(map[string][]parentRef)

	for _, cfg := range resources {
		for _, vc := range cfg.Versions {
			for _, rel := range vc.Relations {
				if rel.Join.From != "" || rel.Join.Local != identityField {
					continue
				}
				ref := parentRef{parentType: cfg.Resource, foreignField: rel.Join.Foreign}
				if !slices.Contains(reverse[rel.Resource], ref) {
					reverse[rel.Resource] = append(reverse[rel.Resource], ref)
				}
			}
		}
	}

	return reverse
}

// deriveParents resolves the parent resources reachable from the just-fetched
// root data. For each invertible relation pointing at the root's type, it reads
// the foreign field from the root's own resolved data; that value is the
// parent's resource id. It reads only data already present on the BuildDoc and
// performs no fetching.
func deriveParents(refs []parentRef, rows []map[string]any) []model.Resource {
	if len(refs) == 0 || len(rows) == 0 {
		return nil
	}

	var parents []model.Resource
	seen := make(map[model.Resource]bool)
	for _, row := range rows {
		for _, ref := range refs {
			val, ok := row[ref.foreignField]
			if !ok {
				continue
			}
			id, ok := val.(string)
			if !ok || id == "" {
				continue
			}
			res := model.Resource{Type: ref.parentType, Id: id}
			if !seen[res] {
				seen[res] = true
				parents = append(parents, res)
			}
		}
	}

	return parents
}
