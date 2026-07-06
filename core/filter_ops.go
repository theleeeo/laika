package core

import (
	"slices"

	"github.com/theleeeo/laika/core/resource"
)

// filterOpNames drives FilterOp.String; indexed by the constants in
// search_types.go.
var filterOpNames = [...]string{
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
}

func (op FilterOp) String() string {
	if int(op) >= 0 && int(op) < len(filterOpNames) && filterOpNames[op] != "" {
		return filterOpNames[op]
	}
	return "unknown"
}

// IsNegation reports whether the op excludes documents. Negations are
// document-level: on a nested many-relation the must_not wraps the nested
// query, so "b.state neq active" means "no b child is active" (spec:
// Semantics / Negation on relations).
func (op FilterOp) IsNegation() bool {
	return op == FilterOpNeq || op == FilterOpNotIn || op == FilterOpNotExists
}

// opsByFamily maps each field-type family to the filter operations it
// supports. This single table drives capabilities and request validation, so
// what a client discovers is exactly what the server accepts.
var opsByFamily = map[resource.Family][]FilterOp{
	resource.FamilyString: {FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists, FilterOpNotExists},
	resource.FamilyNumeric: {FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists},
	resource.FamilyTemporal: {FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists},
	resource.FamilyBoolean: {FilterOpEq, FilterOpNeq, FilterOpExists, FilterOpNotExists},
	resource.FamilyIP: {FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists},
	resource.FamilyFulltext: nil, // text is query-only
}

// OpsForField returns the filter operations a field supports, nil for
// query-only (text) fields. The result is a copy the caller may modify.
func OpsForField(f resource.FieldConfig) []FilterOp {
	fam, ok := f.Family()
	if !ok {
		return nil
	}
	return slices.Clone(opsByFamily[fam])
}

// withoutNegations removes negation ops, mutating and returning ops (pair it
// with the copy OpsForField returns). Reference-relation fields use it: the
// two-phase join would give a negation "some joined child differs" semantics,
// contradicting the document-level "no child matches" meaning it has on
// denormalized relations, so negation is not offered there.
func withoutNegations(ops []FilterOp) []FilterOp {
	return slices.DeleteFunc(ops, FilterOp.IsNegation)
}
