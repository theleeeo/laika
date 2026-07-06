package resource

import "sort"

// Family groups ES field types that share a filter-operation set. The
// type→family map lives here (next to FieldConfig, and it is the config
// whitelist); the family→ops table lives in core, which is where FilterOp is
// defined. Together they split the knowledge along the module boundary
// without duplicating the type list.
type Family string

const (
	FamilyString   Family = "string"   // keyword
	FamilyNumeric  Family = "numeric"  // long, integer, double, float
	FamilyTemporal Family = "temporal" // date
	FamilyBoolean  Family = "boolean"  // boolean
	FamilyIP       Family = "ip"       // ip
	FamilyFulltext Family = "fulltext" // text — query-only, no filter ops
)

// typeFamilies is the whitelist of supported ES field types. A FieldConfig
// whose type is not a key here fails config validation.
var typeFamilies = map[string]Family{
	"keyword": FamilyString,
	"text":    FamilyFulltext,
	"long":    FamilyNumeric,
	"integer": FamilyNumeric,
	"double":  FamilyNumeric,
	"float":   FamilyNumeric,
	"date":    FamilyTemporal,
	"boolean": FamilyBoolean,
	"ip":      FamilyIP,
}

// SupportedFieldTypes returns the whitelisted ES field types in sorted order,
// for error messages.
func SupportedFieldTypes() []string {
	types := make([]string, 0, len(typeFamilies))
	for t := range typeFamilies {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// Family returns the operator family for the field's ES type. ok is false
// when the type is not in the whitelist.
func (f FieldConfig) Family() (Family, bool) {
	fam, ok := typeFamilies[f.ESType()]
	return fam, ok
}
