package core

import (
	"slices"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

func TestFilterOpString(t *testing.T) {
	cases := map[FilterOp]string{
		FilterOpEq:        "eq",
		FilterOpIn:        "in",
		FilterOpNeq:       "neq",
		FilterOpNotIn:     "not_in",
		FilterOpGt:        "gt",
		FilterOpGte:       "gte",
		FilterOpLt:        "lt",
		FilterOpLte:       "lte",
		FilterOpPrefix:    "prefix",
		FilterOpSuffix:    "suffix",
		FilterOpContains:  "contains",
		FilterOpExists:    "exists",
		FilterOpNotExists: "not_exists",
		FilterOp(99):      "unknown",
	}
	for op, want := range cases {
		if got := op.String(); got != want {
			t.Errorf("FilterOp(%d).String() = %q, want %q", int(op), got, want)
		}
	}
}

func TestFilterOpIsNegation(t *testing.T) {
	negations := []FilterOp{FilterOpNeq, FilterOpNotIn, FilterOpNotExists}
	positives := []FilterOp{FilterOpEq, FilterOpIn, FilterOpGt, FilterOpGte, FilterOpLt,
		FilterOpLte, FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists}
	for _, op := range negations {
		if !op.IsNegation() {
			t.Errorf("%s must be a negation", op)
		}
	}
	for _, op := range positives {
		if op.IsNegation() {
			t.Errorf("%s must not be a negation", op)
		}
	}
}

func TestOpsForField(t *testing.T) {
	cases := []struct {
		typ  string
		want []FilterOp
	}{
		{"keyword", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists, FilterOpNotExists}},
		{"long", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"date", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"boolean", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpExists, FilterOpNotExists}},
		{"ip", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"text", nil},
	}
	for _, c := range cases {
		got := OpsForField(resource.FieldConfig{Name: "f", Type: c.typ})
		if !slices.Equal(got, c.want) {
			t.Errorf("OpsForField(%q) = %v, want %v", c.typ, got, c.want)
		}
	}
}

func TestOpsForField_ReturnsCopy(t *testing.T) {
	a := OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"})
	a[0] = FilterOp(99)
	b := OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"})
	if b[0] != FilterOpEq {
		t.Error("OpsForField must return a fresh copy, not the shared table slice")
	}
}

func TestWithoutNegations(t *testing.T) {
	got := withoutNegations(OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"}))
	want := []FilterOp{FilterOpEq, FilterOpIn, FilterOpPrefix, FilterOpSuffix,
		FilterOpContains, FilterOpExists}
	if !slices.Equal(got, want) {
		t.Errorf("withoutNegations(keyword ops) = %v, want %v", got, want)
	}
}
