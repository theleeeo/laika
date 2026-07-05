package resource

import "testing"

// The search tier selector replaces the old *bool. Omitted must mean "none"
// (breaking change: previously omitted meant searchable), and only primary and
// secondary feed single-resource full-text search.
func TestSearchTier(t *testing.T) {
	cases := []struct {
		name           string
		q              QueryConfig
		wantTier       SearchTier
		wantSearchable bool
	}{
		{"omitted defaults to none", QueryConfig{}, SearchTierNone, false},
		{"explicit none", QueryConfig{Search: SearchTierNone}, SearchTierNone, false},
		{"primary is searchable", QueryConfig{Search: SearchTierPrimary}, SearchTierPrimary, true},
		{"secondary is searchable", QueryConfig{Search: SearchTierSecondary}, SearchTierSecondary, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.q.Tier(); got != tc.wantTier {
				t.Errorf("Tier() = %q, want %q", got, tc.wantTier)
			}
			if got := tc.q.IsSearchable(); got != tc.wantSearchable {
				t.Errorf("IsSearchable() = %v, want %v", got, tc.wantSearchable)
			}
		})
	}
}

// An unknown tier value is a loud config error, not a silent fallback.
func TestSearchTierValidation(t *testing.T) {
	base := FieldConfig{Name: "n"}

	for _, tier := range []SearchTier{"", SearchTierNone, SearchTierPrimary, SearchTierSecondary} {
		base.Query.Search = tier
		if err := base.Validate(); err != nil {
			t.Errorf("tier %q should be valid, got %v", tier, err)
		}
	}

	base.Query.Search = "bogus"
	if err := base.Validate(); err == nil {
		t.Error("expected error for invalid search tier")
	}
}
