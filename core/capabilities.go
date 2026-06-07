package core

import (
	"fmt"

	"github.com/theleeeo/indexer/core/resource"
	"github.com/theleeeo/indexer/gen/search/v1"
)

// GetCapabilities returns the search capabilities for all configured resources,
// describing available fields, their types, supported filter operations, and
// whether they are searchable or sortable.
func (idx *Indexer) GetCapabilities() *search.GetCapabilitiesResponse {
	resp := &search.GetCapabilitiesResponse{}

	for _, rc := range idx.resources {
		cap := &search.ResourceCapability{
			Resource: rc.Resource,
		}

		vc := rc.ReadVersionConfig()

		for _, f := range vc.Fields {
			cap.Fields = append(cap.Fields, fieldCapability("fields."+f.Name, f))
		}

		for _, rel := range vc.Relations {
			for _, f := range rel.Fields {
				cap.Fields = append(cap.Fields, fieldCapability(fmt.Sprintf("%s.%s", rel.Resource, f.Name), f))
			}
		}

		resp.Resources = append(resp.Resources, cap)
	}

	return resp
}

func fieldCapability(path string, f resource.FieldConfig) *search.FieldCapability {
	esType := f.ESType()
	searchable := f.Query.Search == nil || *f.Query.Search

	fc := &search.FieldCapability{
		Field:      path,
		Type:       esType,
		Searchable: searchable,
		Sortable:   esType != "text",
	}

	// Term-based filters work on keyword, numeric, date, and boolean types.
	// They do not work reliably on analyzed text fields.
	if esType != "text" {
		fc.FilterOps = []search.FilterOp{
			search.FilterOp_FILTER_OP_EQ,
			search.FilterOp_FILTER_OP_IN,
		}
	}

	return fc
}
