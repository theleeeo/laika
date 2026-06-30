package core

import (
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

func TestGetCapabilities_Empty(t *testing.T) {
	idx := New(Config{})
	resp := idx.GetCapabilities()

	if len(resp.Resources) != 0 {
		t.Fatalf("expected 0 resources, got %d", len(resp.Resources))
	}
}

func TestGetCapabilities_SingleResource(t *testing.T) {
	cfg := &resource.Config{
		Resource: "product",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "title", Type: "text"},
					{Name: "status"},
				},
			},
		},
	}
	cfg.ApplyDefaults()
	idx := New(Config{
		Resources: resource.Configs{cfg},
	})

	resp := idx.GetCapabilities()
	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(resp.Resources))
	}

	rc := resp.Resources[0]
	if rc.Resource != "product" {
		t.Fatalf("expected resource 'product', got %q", rc.Resource)
	}
	if len(rc.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(rc.Fields))
	}

	// title — text field
	assertField(t, rc.Fields[0], "fields.title", "text", true, false, nil)

	// status — keyword field (default)
	assertField(t, rc.Fields[1], "fields.status", "keyword", true, true,
		[]FilterOp{FilterOpEq, FilterOpIn})
}

func TestGetCapabilities_WithRelations(t *testing.T) {
	cfg := &resource.Config{
		Resource: "order",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "order_number"},
				},
				Relations: []resource.RelationConfig{
					{
						Resource: "customer",
						Fields: []resource.FieldConfig{
							{Name: "name", Type: "text"},
							{Name: "tier"},
						},
					},
				},
			},
		},
	}
	cfg.ApplyDefaults()
	idx := New(Config{
		Resources: resource.Configs{cfg},
	})

	resp := idx.GetCapabilities()
	rc := resp.Resources[0]
	if len(rc.Fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(rc.Fields))
	}

	assertField(t, rc.Fields[0], "fields.order_number", "keyword", true, true,
		[]FilterOp{FilterOpEq, FilterOpIn})
	assertField(t, rc.Fields[1], "customer.name", "text", true, false, nil)
	assertField(t, rc.Fields[2], "customer.tier", "keyword", true, true,
		[]FilterOp{FilterOpEq, FilterOpIn})
}

func TestGetCapabilities_SearchDisabled(t *testing.T) {
	falseVal := false
	cfg := &resource.Config{
		Resource: "item",
		Versions: []resource.VersionConfig{
			{
				Version: 1,
				Fields: []resource.FieldConfig{
					{Name: "code", Query: resource.QueryConfig{Search: &falseVal}},
				},
			},
		},
	}
	cfg.ApplyDefaults()
	idx := New(Config{
		Resources: resource.Configs{cfg},
	})

	resp := idx.GetCapabilities()
	f := resp.Resources[0].Fields[0]
	assertField(t, f, "fields.code", "keyword", false, true,
		[]FilterOp{FilterOpEq, FilterOpIn})
}

func TestGetCapabilities_MultipleResources(t *testing.T) {
	cfgA := &resource.Config{Resource: "a", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "x"}}}}}
	cfgB := &resource.Config{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "y", Type: "integer"}}}}}
	cfgA.ApplyDefaults()
	cfgB.ApplyDefaults()
	idx := New(Config{
		Resources: resource.Configs{cfgA, cfgB},
	})

	resp := idx.GetCapabilities()
	if len(resp.Resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(resp.Resources))
	}
	if resp.Resources[0].Resource != "a" || resp.Resources[1].Resource != "b" {
		t.Fatalf("unexpected resource names: %q, %q", resp.Resources[0].Resource, resp.Resources[1].Resource)
	}

	bField := resp.Resources[1].Fields[0]
	assertField(t, bField, "fields.y", "integer", true, true,
		[]FilterOp{FilterOpEq, FilterOpIn})
}

func TestReferenceFieldsAreFilterOnly(t *testing.T) {
	cfg := &resource.Config{
		Resource:   "c",
		ReadVersion: 1,
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{
					Resource: "b",
					Strategy: resource.StrategyReference,
					Join: resource.JoinConfig{Local: "b_id", Foreign: "id"},
					Fields: []resource.FieldConfig{{Name: "name"}},
				},
			},
		}},
	}
	cfg.ApplyDefaults()
	idx := New(Config{
		Resources: resource.Configs{cfg},
	})

	caps := idx.GetCapabilities()
	var fc *FieldCapability
	for i := range caps.Resources {
		for j := range caps.Resources[i].Fields {
			if caps.Resources[i].Fields[j].Field == "b.name" {
				fc = &caps.Resources[i].Fields[j]
			}
		}
	}
	if fc == nil {
		t.Fatal("reference field b.name must still be advertised")
	}
	if fc.Searchable || fc.Sortable {
		t.Fatalf("reference field must be filter-only, got searchable=%v sortable=%v", fc.Searchable, fc.Sortable)
	}
	if len(fc.FilterOps) != 2 {
		t.Fatalf("reference field must support Eq+In filters, got %v", fc.FilterOps)
	}
}

func assertField(t *testing.T, f FieldCapability, wantField, wantType string, wantSearchable, wantSortable bool, wantOps []FilterOp) {
	t.Helper()
	if f.Field != wantField {
		t.Errorf("field: got %q, want %q", f.Field, wantField)
	}
	if f.Type != wantType {
		t.Errorf("type for %q: got %q, want %q", wantField, f.Type, wantType)
	}
	if f.Searchable != wantSearchable {
		t.Errorf("searchable for %q: got %v, want %v", wantField, f.Searchable, wantSearchable)
	}
	if f.Sortable != wantSortable {
		t.Errorf("sortable for %q: got %v, want %v", wantField, f.Sortable, wantSortable)
	}
	if len(f.FilterOps) != len(wantOps) {
		t.Errorf("filter_ops count for %q: got %d, want %d", wantField, len(f.FilterOps), len(wantOps))
		return
	}
	for i, op := range wantOps {
		if f.FilterOps[i] != op {
			t.Errorf("filter_ops[%d] for %q: got %v, want %v", i, wantField, f.FilterOps[i], op)
		}
	}
}
