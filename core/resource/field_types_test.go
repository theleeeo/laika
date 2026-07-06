package resource

import (
	"strings"
	"testing"
)

func TestFieldConfigFamily(t *testing.T) {
	cases := []struct {
		typ  string
		want Family
	}{
		{"", FamilyString}, // omitted type defaults to keyword
		{"keyword", FamilyString},
		{"text", FamilyFulltext},
		{"long", FamilyNumeric},
		{"integer", FamilyNumeric},
		{"double", FamilyNumeric},
		{"float", FamilyNumeric},
		{"date", FamilyTemporal},
		{"boolean", FamilyBoolean},
		{"ip", FamilyIP},
	}
	for _, c := range cases {
		fam, ok := (FieldConfig{Name: "f", Type: c.typ}).Family()
		if !ok {
			t.Errorf("type %q: expected supported", c.typ)
			continue
		}
		if fam != c.want {
			t.Errorf("type %q: got family %q, want %q", c.typ, fam, c.want)
		}
	}
}

func TestFieldConfigFamily_UnknownType(t *testing.T) {
	if _, ok := (FieldConfig{Name: "f", Type: "geo_point"}).Family(); ok {
		t.Error("geo_point must not be a supported type")
	}
}

func TestFieldConfigValidate_TypeWhitelist(t *testing.T) {
	if err := (FieldConfig{Name: "f", Type: "date"}).Validate(); err != nil {
		t.Errorf("date must validate, got %v", err)
	}
	if err := (FieldConfig{Name: "f"}).Validate(); err != nil {
		t.Errorf("omitted type must validate (defaults to keyword), got %v", err)
	}

	err := (FieldConfig{Name: "f", Type: "geo_point"}).Validate()
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
	if !strings.Contains(err.Error(), "geo_point") || !strings.Contains(err.Error(), "keyword") {
		t.Errorf("error should name the bad type and list supported ones, got: %v", err)
	}
}
