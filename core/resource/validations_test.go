package resource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// rel is a small helper for building a relation in test configs.
func rel(target string, join JoinConfig, fields ...string) RelationConfig {
	fc := make([]FieldConfig, len(fields))
	for i, f := range fields {
		fc[i] = FieldConfig{Name: f}
	}
	return RelationConfig{Resource: target, Join: join, Fields: fc}
}

func TestValidate_JoinRequiresLocalAndForeign(t *testing.T) {
	base := func(j JoinConfig) Configs {
		return Configs{
			{Resource: "a", Versions: []VersionConfig{{
				Version:   1,
				Fields:    []FieldConfig{{Name: "id"}},
				Relations: []RelationConfig{rel("b", j, "name")},
			}}},
			{Resource: "b", Versions: []VersionConfig{{
				Version: 1,
				Fields:  []FieldConfig{{Name: "name"}},
			}}},
		}
	}

	require.NoError(t, base(JoinConfig{Local: "id", Foreign: "a_id"}).Validate())

	err := base(JoinConfig{Foreign: "a_id"}).Validate()
	require.ErrorContains(t, err, "local required")

	err = base(JoinConfig{Local: "id"}).Validate()
	require.ErrorContains(t, err, "foreign required")
}

func TestValidate_JoinFromMustBeSibling(t *testing.T) {
	cfg := Configs{
		{Resource: "a", Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "id"}},
			Relations: []RelationConfig{
				rel("b", JoinConfig{Local: "id", Foreign: "ghost_id", From: "ghost"}, "name"),
			},
		}}},
		{Resource: "b", Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "name"}},
		}}},
	}

	err := cfg.Validate()
	require.ErrorContains(t, err, "join from 'ghost' is not a sibling relation")
}

func TestValidate_JoinFromCycleDetected(t *testing.T) {
	cfg := Configs{
		{Resource: "a", Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "id"}},
			Relations: []RelationConfig{
				rel("b", JoinConfig{Local: "x", Foreign: "id", From: "c"}, "name"),
				rel("c", JoinConfig{Local: "y", Foreign: "id", From: "b"}, "name"),
			},
		}}},
		{Resource: "b", Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "name"}},
		}}},
		{Resource: "c", Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "name"}},
		}}},
	}

	err := cfg.Validate()
	require.ErrorContains(t, err, "cycle")
}
