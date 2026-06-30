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
					fc.Searchable = false
					fc.Sortable = false
					fc.FilterOps = []FilterOp{FilterOpEq, FilterOpIn}
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
	searchable := f.Query.Search == nil || *f.Query.Search

	fc := FieldCapability{
		Field:      path,
		Type:       esType,
		Searchable: searchable,
		Sortable:   esType != "text",
	}

	if esType != "text" {
		fc.FilterOps = []FilterOp{FilterOpEq, FilterOpIn}
	}

	return fc
}
