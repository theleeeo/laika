package resource

import "testing"

func TestRelationIsReference(t *testing.T) {
	if (RelationConfig{Strategy: StrategyReference}).IsReference() != true {
		t.Fatal("reference strategy should be a reference")
	}
	if (RelationConfig{Strategy: StrategyDenormalize}).IsReference() != false {
		t.Fatal("denormalize strategy should not be a reference")
	}
	if (RelationConfig{}).IsReference() != false {
		t.Fatal("empty strategy should default to denormalize (not reference)")
	}
}

func TestRelationStrategyValidation(t *testing.T) {
	base := RelationConfig{Resource: "b", Join: JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []FieldConfig{{Name: "name"}}}

	base.Strategy = "bogus"
	if err := base.Validate(); err == nil {
		t.Fatal("expected error for invalid strategy")
	}

	for _, s := range []string{"", StrategyDenormalize, StrategyReference} {
		base.Strategy = s
		if err := base.Validate(); err != nil {
			t.Fatalf("strategy %q should be valid, got %v", s, err)
		}
	}
}
