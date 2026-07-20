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

func TestNestedBlockHelpers(t *testing.T) {
	// Test IsScoped()
	scopedBlock := NestedBlockConfig{Name: "scoped", ScopeKey: "tenant_id", Fields: []FieldConfig{{Name: "data"}}}
	unscopedBlock := NestedBlockConfig{Name: "unscoped", ScopeKey: "", Fields: []FieldConfig{{Name: "data"}}}

	if !scopedBlock.IsScoped() {
		t.Fatal("scoped block with ScopeKey should report IsScoped() == true")
	}
	if unscopedBlock.IsScoped() {
		t.Fatal("unscoped block with empty ScopeKey should report IsScoped() == false")
	}

	// Test GetNestedBlock()
	vc := VersionConfig{
		Version:      1,
		NestedBlocks: []NestedBlockConfig{scopedBlock, unscopedBlock},
	}

	if block := vc.GetNestedBlock("scoped"); block == nil {
		t.Fatal("GetNestedBlock should return block pointer for 'scoped'")
	} else if block.Name != "scoped" {
		t.Fatalf("returned block should have Name='scoped', got %q", block.Name)
	}

	if block := vc.GetNestedBlock("unscoped"); block == nil {
		t.Fatal("GetNestedBlock should return block pointer for 'unscoped'")
	} else if block.Name != "unscoped" {
		t.Fatalf("returned block should have Name='unscoped', got %q", block.Name)
	}

	if block := vc.GetNestedBlock("nonexistent"); block != nil {
		t.Fatal("GetNestedBlock should return nil for nonexistent name")
	}

	// Test ScopedNestedBlocks()
	scoped := vc.ScopedNestedBlocks()
	if len(scoped) != 1 {
		t.Fatalf("ScopedNestedBlocks should return 1 block, got %d", len(scoped))
	}
	if scoped[0].Name != "scoped" {
		t.Fatalf("scoped blocks should contain 'scoped', got %q", scoped[0].Name)
	}
}
