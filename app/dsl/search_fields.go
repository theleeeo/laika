package dsl

import (
	"fmt"

	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/projection"
)

// populateStandardizedSearchFields fills the two standardized searchable
// surfaces on a built document from the resource config's per-field `search`
// tier selectors (spec D4/D9/D10). It runs as a terminal Build stage, after all
// relations are denormalized into the Doc.
//
//   - search_primary: flat array of every `search: primary` field's text (the
//     Document's own high-signal fields).
//   - search_secondary: nested array, one entry per source of `search: secondary`
//     text. Each entry is { text, scope }; the standalone app leaves scope
//     empty (unscoped, tenant-agnostic). A library consumer fills scope via its
//     own Plan instead.
//
// reference-relation fields never reach here: they are not denormalized into
// the parent Build, so their data is absent from Doc (spec D6).
func populateStandardizedSearchFields(vc *resource.VersionConfig, doc projection.BuildDoc) projection.BuildDoc {
	// A nil Doc marks a missing/deleted root; nothing to populate.
	if doc.Doc == nil {
		return doc
	}

	var primary []string
	var secondary []map[string]any

	// Root fields: primary feeds the flat surface; secondary fields collapse
	// into a single secondary entry (the root's own lower-signal text).
	rootFields, _ := doc.Doc["fields"].(map[string]any)
	var rootSecondary []string
	for _, f := range vc.Fields {
		switch f.Query.Tier() {
		case resource.SearchTierPrimary:
			primary = append(primary, textValues(rootFields[f.Name])...)
		case resource.SearchTierSecondary:
			rootSecondary = append(rootSecondary, textValues(rootFields[f.Name])...)
		}
	}
	if len(rootSecondary) > 0 {
		secondary = append(secondary, secondaryEntry(rootSecondary))
	}

	// Denormalized relations: primary child fields feed the flat surface;
	// secondary child fields produce one secondary entry per child row, so a
	// library consumer can attribute each child's scope independently.
	for _, rel := range vc.Relations {
		if rel.IsReference() {
			continue // D6: reference fields are not in the parent Build.
		}

		var primaryNames, secondaryNames []string
		for _, f := range rel.Fields {
			switch f.Query.Tier() {
			case resource.SearchTierPrimary:
				primaryNames = append(primaryNames, f.Name)
			case resource.SearchTierSecondary:
				secondaryNames = append(secondaryNames, f.Name)
			}
		}
		if len(primaryNames) == 0 && len(secondaryNames) == 0 {
			continue
		}

		for _, row := range childRows(doc.Doc[rel.Resource]) {
			for _, name := range primaryNames {
				primary = append(primary, textValues(row[name])...)
			}
			var entryText []string
			for _, name := range secondaryNames {
				entryText = append(entryText, textValues(row[name])...)
			}
			if len(entryText) > 0 {
				secondary = append(secondary, secondaryEntry(entryText))
			}
		}
	}

	if len(primary) > 0 {
		doc.Doc["search_primary"] = primary
	}
	if len(secondary) > 0 {
		doc.Doc["search_secondary"] = secondary
	}
	return doc
}

// secondaryEntry builds a search_secondary entry with an empty scope. The
// standalone app never attributes scope (spec D10); the empty array matches the
// nested keyword mapping and leaves the entry unscoped.
func secondaryEntry(text []string) map[string]any {
	return map[string]any{
		"text":  text,
		"scope": []string{},
	}
}

// textValues flattens a field value into its constituent text strings. Nested
// arrays are flattened; empty strings are dropped; non-string scalars are
// stringified so an explicitly search-tiered non-text field still contributes.
func textValues(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	case []string:
		return t
	case []any:
		var out []string
		for _, e := range t {
			out = append(out, textValues(e)...)
		}
		return out
	default:
		return []string{fmt.Sprint(t)}
	}
}

// childRows normalizes a relation's denormalized value in the Doc into a slice
// of rows, covering both the "many" (array) and "one" (single object) shapes.
func childRows(v any) []map[string]any {
	switch t := v.(type) {
	case []map[string]any:
		return t
	case map[string]any:
		return []map[string]any{t}
	default:
		return nil
	}
}
