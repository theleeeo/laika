package resource

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// HasPrimaryTierField mirrors what feeds search_primary: root fields and
// denormalized relation fields with search: primary. Reference-relation and
// nested-block fields never reach that surface.
func TestHasPrimaryTierField(t *testing.T) {
	primary := FieldConfig{Name: "p", Query: QueryConfig{Search: SearchTierPrimary}}
	secondary := FieldConfig{Name: "s", Query: QueryConfig{Search: SearchTierSecondary}}
	none := FieldConfig{Name: "n"}

	cases := []struct {
		name string
		vc   VersionConfig
		want bool
	}{
		{"root primary field", VersionConfig{Fields: []FieldConfig{none, primary}}, true},
		{"only secondary and none", VersionConfig{Fields: []FieldConfig{secondary, none}}, false},
		{"no fields at all", VersionConfig{}, false},
		{"primary on a denormalized relation", VersionConfig{
			Fields:    []FieldConfig{none},
			Relations: []RelationConfig{{Resource: "b", Fields: []FieldConfig{primary}}},
		}, true},
		{"primary on a reference relation", VersionConfig{
			Fields:    []FieldConfig{none},
			Relations: []RelationConfig{{Resource: "b", Strategy: StrategyReference, Fields: []FieldConfig{primary}}},
		}, false},
		{"primary on a nested block", VersionConfig{
			Fields:       []FieldConfig{none},
			NestedBlocks: []NestedBlockConfig{{Name: "blk", Fields: []FieldConfig{primary}}},
		}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.vc.HasPrimaryTierField(); got != tc.want {
				t.Errorf("HasPrimaryTierField() = %v, want %v", got, tc.want)
			}
		})
	}
}

// WarnMissingPrimaryTier logs one Warn per tier-less version, naming the
// resource and the version, and nothing for a version with a primary field.
func TestWarnMissingPrimaryTier(t *testing.T) {
	primary := FieldConfig{Name: "p", Query: QueryConfig{Search: SearchTierPrimary}}
	cfgs := Configs{
		{Resource: "a", Versions: []VersionConfig{
			{Version: 1, Fields: []FieldConfig{primary}},
			{Version: 2, Fields: []FieldConfig{{Name: "x"}}},
		}},
		{Resource: "b", Versions: []VersionConfig{{Version: 1, Fields: []FieldConfig{primary}}}},
	}

	var buf bytes.Buffer
	cfgs.WarnMissingPrimaryTier(slog.New(slog.NewJSONHandler(&buf, nil)))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("want exactly one log line, got %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["level"] != "WARN" || rec["resource"] != "a" || rec["version"] != float64(2) {
		t.Fatalf("unexpected record %v", rec)
	}
	if !strings.Contains(rec["msg"].(string), "no primary-tier field") {
		t.Fatalf("message should say the version has no primary-tier field, got %q", rec["msg"])
	}
}
