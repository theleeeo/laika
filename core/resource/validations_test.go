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

// refConfigs builds a minimal two-resource config where c references b,
// keyed by `local` sourced `from` (empty = native root field).
func refConfigs(local, from string, cFields []FieldConfig, aFields []FieldConfig) Configs {
	rels := []RelationConfig{}
	if aFields != nil {
		rels = append(rels, RelationConfig{
			Resource: "a", Join: JoinConfig{Local: "id", Foreign: "c_id"}, Fields: aFields,
		})
	}
	rels = append(rels, RelationConfig{
		Resource: "b", Strategy: StrategyReference,
		Join: JoinConfig{Local: local, Foreign: "id", From: from}, Fields: []FieldConfig{{Name: "name"}},
	})
	return Configs{
		{Resource: "c", Versions: []VersionConfig{{Version: 1, Fields: cFields, Relations: rels}}},
		{Resource: "a", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "x"}, {Name: "b_id"}}}}},
		{Resource: "b", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "name"}}}}},
	}
}

func TestReferenceKeyReachable(t *testing.T) {
	// native key present as a root field of c -> OK
	if err := refConfigs("b_id", "", []FieldConfig{{Name: "b_id"}}, nil).Validate(); err != nil {
		t.Fatalf("native root key should be reachable: %v", err)
	}
	// key denormalized via sibling a (a.fields includes b_id) -> OK
	if err := refConfigs("b_id", "a", []FieldConfig{{Name: "n"}}, []FieldConfig{{Name: "b_id"}}).Validate(); err != nil {
		t.Fatalf("key via denormalized sibling should be reachable: %v", err)
	}
}

func TestReferenceKeyUnreachable(t *testing.T) {
	// native key NOT a field of c -> error
	if err := refConfigs("b_id", "", []FieldConfig{{Name: "other"}}, nil).Validate(); err == nil {
		t.Fatal("expected error: native key not an indexed field")
	}
	// from sibling a, but a does not denormalize b_id -> error
	if err := refConfigs("b_id", "a", []FieldConfig{{Name: "n"}}, []FieldConfig{{Name: "n"}}).Validate(); err == nil {
		t.Fatal("expected error: sibling does not carry the key")
	}
}

func TestDenormalizeKeyFromReferenceSiblingRejected(t *testing.T) {
	// c has a_id as a root field, so reference relation 'a' (keyed by a_id) itself
	// passes validation. The denormalize relation 'b' then tries to source its local
	// join key From: 'a' — but 'a' is a reference relation whose data is never
	// materialized, so the key would silently resolve to empty at build time.
	cfgs := Configs{
		{Resource: "c", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "a_id"}}, Relations: []RelationConfig{
			// reference sibling 'a': uses native root field a_id -> its own validation passes
			{Resource: "a", Strategy: StrategyReference, Join: JoinConfig{Local: "a_id", Foreign: "id"}, Fields: []FieldConfig{{Name: "x"}}},
			// denormalize relation 'b': sources its local key From: 'a' (a reference sibling)
			{Resource: "b", Join: JoinConfig{Local: "x", From: "a", Foreign: "id"}, Fields: []FieldConfig{{Name: "name"}}},
		}}}},
		{Resource: "a", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "x"}}}}},
		{Resource: "b", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "name"}}}}},
	}
	err := cfgs.Validate()
	require.ErrorContains(t, err, "reference relation")
}

func TestReferenceKeyFromReferenceSibling(t *testing.T) {
	cfgs := Configs{
		{Resource: "c", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "n"}}, Relations: []RelationConfig{
			{Resource: "a", Strategy: StrategyReference, Join: JoinConfig{Local: "a_id", Foreign: "id"}, Fields: []FieldConfig{{Name: "b_id"}}},
			{Resource: "b", Strategy: StrategyReference, Join: JoinConfig{Local: "b_id", From: "a", Foreign: "id"}, Fields: []FieldConfig{{Name: "name"}}},
		}}}},
		{Resource: "a", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "b_id"}}}}},
		{Resource: "b", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{{Name: "name"}}}}},
	}
	if err := cfgs.Validate(); err == nil {
		t.Fatal("expected error: cannot source a reference key from a reference sibling")
	}
}

func TestNestedBlockValidation(t *testing.T) {
	base := Configs{{
		Resource: "a",
		Versions: []VersionConfig{{
			Version: 1,
			Fields:  []FieldConfig{{Name: "name"}},
			NestedBlocks: []NestedBlockConfig{{
				Name:     "operator_data",
				ScopeKey: "fiber_operator_id",
				Fields:   []FieldConfig{{Name: "custom_fields"}},
			}},
		}},
	}}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid scoped block rejected: %v", err)
	}

	noScope := Configs{{Resource: "a", Versions: []VersionConfig{{
		Version: 1, Fields: []FieldConfig{{Name: "name"}},
		NestedBlocks: []NestedBlockConfig{{Name: "b", Fields: []FieldConfig{{Name: "x"}}}},
	}}}}
	if err := noScope.Validate(); err == nil {
		t.Error("block without scopeKey must be rejected")
	}

	collide := Configs{{Resource: "a", Versions: []VersionConfig{{
		Version: 1, Fields: []FieldConfig{{Name: "name"}},
		NestedBlocks: []NestedBlockConfig{{
			Name: "name", ScopeKey: "s", Fields: []FieldConfig{{Name: "x"}},
		}},
	}}}}
	if err := collide.Validate(); err == nil {
		t.Error("block name colliding with a root field key must be rejected")
	}
}
