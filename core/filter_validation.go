package core

import (
	"fmt"
	"slices"
	"strings"

	"github.com/theleeeo/laika/core/resource"
)

// validateRequestFilters checks caller-supplied filters against a resource's
// read-version schema: the field must exist, the op must be allowed for the
// field's type family (negations additionally rejected on reference-relation
// fields — see withoutNegations), and the operands must match the op's shape.
//
// It runs before the middleware chain, so filters appended by consumer
// middlewares and by internal resolution (reference terms, secondary scope)
// are exempt: they are trusted code, and they may target fields that are not
// part of the advertised schema.
func validateRequestFilters(vc *resource.VersionConfig, filters []Filter) error {
	for _, f := range filters {
		if f.Field == "" {
			continue // ignored by every downstream consumer; keep ignoring it here
		}
		fieldCfg, isReference, err := resolveFilterField(vc, f.Field)
		if err != nil {
			return err
		}
		allowed := OpsForField(*fieldCfg)
		if isReference {
			allowed = withoutNegations(allowed)
		}
		if !slices.Contains(allowed, f.Op) {
			return &InvalidArgumentError{Msg: fmt.Sprintf(
				"filter op %s is not supported on field %q (type %q)",
				f.Op, f.Field, fieldCfg.ESType())}
		}
		if err := validateOperands(f); err != nil {
			return err
		}
	}
	return nil
}

// resolveFilterField resolves a filter's field path — "fields.<name>" for a
// root field, "<relation>.<name>" for a relation field — to its FieldConfig.
// isReference reports that the path names a reference relation's field.
func resolveFilterField(vc *resource.VersionConfig, path string) (*resource.FieldConfig, bool, error) {
	if name, ok := strings.CutPrefix(path, "fields."); ok {
		for i := range vc.Fields {
			if vc.Fields[i].Name == name {
				return &vc.Fields[i], false, nil
			}
		}
		return nil, false, &InvalidArgumentError{Msg: fmt.Sprintf("unknown filter field %q", path)}
	}
	if relName, fieldName, ok := splitRelationField(path); ok {
		if rel := vc.GetRelation(relName); rel != nil {
			for i := range rel.Fields {
				if rel.Fields[i].Name == fieldName {
					return &rel.Fields[i], rel.IsReference(), nil
				}
			}
		}
		if b := vc.GetNestedBlock(relName); b != nil {
			for i := range b.Fields {
				if b.Fields[i].Name == fieldName {
					return &b.Fields[i], false, nil
				}
			}
			// scope key and unknown sub-fields fall through to the error below.
		}
	}
	return nil, false, &InvalidArgumentError{Msg: fmt.Sprintf("unknown filter field %q", path)}
}

// validateOperands enforces each op's operand shape: single-value ops take
// value, set ops take values, presence ops take neither.
func validateOperands(f Filter) error {
	switch f.Op {
	case FilterOpIn, FilterOpNotIn:
		if len(f.Values) == 0 {
			return &InvalidArgumentError{Msg: fmt.Sprintf("filter on %q: op %s requires values", f.Field, f.Op)}
		}
		if f.Value != "" {
			return &InvalidArgumentError{Msg: fmt.Sprintf("filter on %q: op %s takes values, not value", f.Field, f.Op)}
		}
	case FilterOpExists, FilterOpNotExists:
		if f.Value != "" || len(f.Values) != 0 {
			return &InvalidArgumentError{Msg: fmt.Sprintf("filter on %q: op %s takes no value", f.Field, f.Op)}
		}
	default:
		if f.Value == "" {
			return &InvalidArgumentError{Msg: fmt.Sprintf("filter on %q: op %s requires value", f.Field, f.Op)}
		}
		if len(f.Values) != 0 {
			return &InvalidArgumentError{Msg: fmt.Sprintf("filter on %q: op %s takes value, not values", f.Field, f.Op)}
		}
	}
	return nil
}
