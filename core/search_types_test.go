package core

import "testing"

func TestAddFilterAppendsToPrimary(t *testing.T) {
	req := SearchRequest{Resource: "c"}
	req.AddFilter(Filter{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"})

	if len(req.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(req.Filters))
	}
	if req.Filters[0].Field != "fields.tenant_id" || req.Filters[0].Value != "t1" {
		t.Fatalf("unexpected filter: %+v", req.Filters[0])
	}
}
