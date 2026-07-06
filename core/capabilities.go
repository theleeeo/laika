package core

import (
	"fmt"

	"github.com/theleeeo/laika/core/resource"
)

// GetCapabilities returns the search capabilities for all configured resources.
func (idx *Indexer) GetCapabilities() CapabilitiesResponse {
	resp := CapabilitiesResponse{}

	for _, rc := range idx.resources {
		cap := ResourceCapability{Resource: rc.Resource}
		vc := rc.ReadVersionConfig()

		for _, f := range vc.Fields {
			cap.Fields = append(cap.Fields, fieldCapability("fields."+f.Name, f))
		}

		for _, rel := range vc.Relations {
			for _, f := range rel.Fields {
				fc := fieldCapability(fmt.Sprintf("%s.%s", rel.Resource, f.Name), f)
				if rel.IsReference() {
					// Resolved via a child search at query time: never part of the
					// parent's full-text surface or sort, and negation ops are not
					// offered because the join gives them "some joined child
					// differs" semantics instead of the document-level "no child
					// matches" (see withoutNegations).
					fc.Searchable = false
					fc.Sortable = false
					fc.FilterOps = withoutNegations(fc.FilterOps)
				}
				cap.Fields = append(cap.Fields, fc)
			}
		}

		resp.Resources = append(resp.Resources, cap)
	}

	return resp
}

func fieldCapability(path string, f resource.FieldConfig) FieldCapability {
	esType := f.ESType()
	return FieldCapability{
		Field:      path,
		Type:       esType,
		Searchable: f.Query.IsSearchable(),
		Sortable:   esType != "text",
		FilterOps:  OpsForField(f),
	}
}
