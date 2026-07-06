# Filter Operations and Field Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Grow Laika's structured filtering from `EQ`/`IN` into a typed system — a whitelisted set of field types, each with a family of filter operations, advertised via capabilities, validated at request time, translated to ES queries, and usable from the demo UI.

**Architecture:** A type→family map lives in `core/resource` (next to `FieldConfig`); a family→ops table lives in `core` and drives capabilities, strict request validation (in `Indexer.Search` / `Indexer.FederatedSearch`, before the middleware chain so middleware-injected filters are exempt), and the proto surface. The ES backend translates each op to term/terms/range/prefix/wildcard/exists clauses, with negation ops wrapped in `must_not` *outside* any `nested` wrapper (document-level "no child matches" semantics). Spec: `docs/superpowers/specs/2026-07-06-filter-and-field-types-design.md`.

**Tech Stack:** Go 1.26.1 workspace (`go.work`), Elasticsearch 8 (`backend/elasticsearch` module), protobuf via `buf`, Connect RPC, testcontainers integration suite, vanilla-JS demo UI.

## Global Constraints

- Every Go command needs `GOEXPERIMENT=jsonv2` (Go 1.26.1). Run from the repo root `laika/` unless stated otherwise; the workspace covers all modules.
- Existing proto enum numbers `FILTER_OP_EQ = 1`, `FILTER_OP_IN = 2` must not change (wire compatibility).
- Existing core constants `FilterOpEq` (iota 0) and `FilterOpIn` (iota 1) must keep their values; new constants are appended after them.
- Filter values stay strings on the wire and in `core.Filter`; ES coerces them against typed mappings.
- The `demo/` directory is **not a git repo** — no commit step for Task 8.
- Unit test suite that must stay green throughout: `GOEXPERIMENT=jsonv2 go test ./core/... ./backend/elasticsearch/... ./app/server/... ./app/dsl/...`
- Commit after every task (in `laika/`); commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Field-type whitelist and families (`core/resource`)

**Files:**
- Create: `core/resource/field_types.go`
- Create: `core/resource/field_types_test.go`
- Modify: `core/resource/validations.go` (`FieldConfig.Validate`, currently at line 186)

**Interfaces:**
- Consumes: `FieldConfig.ESType()` (existing, `core/resource/config.go:120` — returns `"keyword"` when `Type` is empty).
- Produces: `type Family string` with constants `FamilyString`, `FamilyNumeric`, `FamilyTemporal`, `FamilyBoolean`, `FamilyIP`, `FamilyFulltext`; method `FieldConfig.Family() (Family, bool)`; func `SupportedFieldTypes() []string`. Task 2's ops table is keyed by these `Family` constants.

- [ ] **Step 1: Write the failing tests**

Create `core/resource/field_types_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -run 'TestFieldConfig' -v`
Expected: FAIL — compile errors `undefined: Family`, `undefined: FamilyString`, etc.

- [ ] **Step 3: Write the implementation**

Create `core/resource/field_types.go`:

```go
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
```

In `core/resource/validations.go`, extend `FieldConfig.Validate` (add the type check after the name check) and add `"strings"` to the imports:

```go
func (c FieldConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name required")
	}
	if _, ok := c.Family(); !ok {
		return fmt.Errorf("type %q is not supported; must be one of: %s",
			c.Type, strings.Join(SupportedFieldTypes(), ", "))
	}
	switch c.Query.Search {
	case "", SearchTierNone, SearchTierPrimary, SearchTierSecondary:
	default:
		return fmt.Errorf("search must be %q, %q or %q", SearchTierPrimary, SearchTierSecondary, SearchTierNone)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -v`
Expected: PASS (all package tests, including pre-existing ones — every type used in existing tests is on the whitelist).

- [ ] **Step 5: Run the wider suite to catch config fallout**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... ./backend/elasticsearch/... ./app/server/... ./app/dsl/...`
Expected: PASS. (Configs in tests and `example.resources.yml` only use `keyword`/`text`/`integer`, all whitelisted.)

- [ ] **Step 6: Commit**

```bash
git add core/resource/field_types.go core/resource/field_types_test.go core/resource/validations.go
git commit -m "feat(resource): field-type whitelist with operator families

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: New FilterOps and the family→ops table (`core`)

**Files:**
- Modify: `core/search_types.go` (FilterOp constants, lines 3–18)
- Create: `core/filter_ops.go`
- Create: `core/filter_ops_test.go`
- Modify: `core/logging.go` (`summarizeFilters`, line 63)

**Interfaces:**
- Consumes: `resource.Family` constants and `FieldConfig.Family()` from Task 1.
- Produces: constants `FilterOpNeq`, `FilterOpNotIn`, `FilterOpGt`, `FilterOpGte`, `FilterOpLt`, `FilterOpLte`, `FilterOpPrefix`, `FilterOpSuffix`, `FilterOpContains`, `FilterOpExists`, `FilterOpNotExists`; methods `FilterOp.String() string`, `FilterOp.IsNegation() bool`; funcs `OpsForField(f resource.FieldConfig) []FilterOp` and `withoutNegations(ops []FilterOp) []FilterOp`. Tasks 3–6 consume all of these.

- [ ] **Step 1: Write the failing tests**

Create `core/filter_ops_test.go`:

```go
package core

import (
	"slices"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

func TestFilterOpString(t *testing.T) {
	cases := map[FilterOp]string{
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
		FilterOp(99):      "unknown",
	}
	for op, want := range cases {
		if got := op.String(); got != want {
			t.Errorf("FilterOp(%d).String() = %q, want %q", int(op), got, want)
		}
	}
}

func TestFilterOpIsNegation(t *testing.T) {
	negations := []FilterOp{FilterOpNeq, FilterOpNotIn, FilterOpNotExists}
	positives := []FilterOp{FilterOpEq, FilterOpIn, FilterOpGt, FilterOpGte, FilterOpLt,
		FilterOpLte, FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists}
	for _, op := range negations {
		if !op.IsNegation() {
			t.Errorf("%s must be a negation", op)
		}
	}
	for _, op := range positives {
		if op.IsNegation() {
			t.Errorf("%s must not be a negation", op)
		}
	}
}

func TestOpsForField(t *testing.T) {
	cases := []struct {
		typ  string
		want []FilterOp
	}{
		{"keyword", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists, FilterOpNotExists}},
		{"long", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"date", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"boolean", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpExists, FilterOpNotExists}},
		{"ip", []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
			FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}},
		{"text", nil},
	}
	for _, c := range cases {
		got := OpsForField(resource.FieldConfig{Name: "f", Type: c.typ})
		if !slices.Equal(got, c.want) {
			t.Errorf("OpsForField(%q) = %v, want %v", c.typ, got, c.want)
		}
	}
}

func TestOpsForField_ReturnsCopy(t *testing.T) {
	a := OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"})
	a[0] = FilterOp(99)
	b := OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"})
	if b[0] != FilterOpEq {
		t.Error("OpsForField must return a fresh copy, not the shared table slice")
	}
}

func TestWithoutNegations(t *testing.T) {
	got := withoutNegations(OpsForField(resource.FieldConfig{Name: "f", Type: "keyword"}))
	want := []FilterOp{FilterOpEq, FilterOpIn, FilterOpPrefix, FilterOpSuffix,
		FilterOpContains, FilterOpExists}
	if !slices.Equal(got, want) {
		t.Errorf("withoutNegations(keyword ops) = %v, want %v", got, want)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestFilterOp|TestOpsForField|TestWithoutNegations' -v`
Expected: FAIL — compile errors `undefined: FilterOpNeq`, etc.

- [ ] **Step 3: Write the implementation**

In `core/search_types.go`, replace the constant block and the `Filter` doc comments (lines 3–18):

```go
// FilterOp is the comparison operator for a structured search filter. The
// ops a field supports are derived from its type family; see opsByFamily in
// filter_ops.go.
type FilterOp int

const (
	FilterOpEq FilterOp = iota // term
	FilterOpIn                 // terms
	// New ops are appended so FilterOpEq/FilterOpIn keep their original values.
	FilterOpNeq       // must_not(term)
	FilterOpNotIn     // must_not(terms)
	FilterOpGt        // range
	FilterOpGte       // range
	FilterOpLt        // range
	FilterOpLte       // range
	FilterOpPrefix    // prefix
	FilterOpSuffix    // wildcard *v
	FilterOpContains  // wildcard *v*
	FilterOpExists    // exists
	FilterOpNotExists // must_not(exists)
)

// Filter is a structured field-level filter for a search request.
type Filter struct {
	Field      string
	Op         FilterOp
	Value      string   // single-value ops (eq, neq, ranges, prefix, suffix, contains)
	Values     []string // set ops (in, not_in)
	NestedPath string   // non-empty when the field lives inside a nested object
}
```

Create `core/filter_ops.go`:

```go
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
```

In `core/logging.go`, update `summarizeFilters` so set ops show their values and the op renders by name:

```go
// summarizeFilters renders filters as compact "field op value" strings so they
// read cleanly in text logs.
func summarizeFilters(fs []Filter) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		val := any(f.Value)
		if f.Op == FilterOpIn || f.Op == FilterOpNotIn {
			val = f.Values
		}
		out = append(out, fmt.Sprintf("%s %s %v", f.Field, f.Op, val))
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/search_types.go core/filter_ops.go core/filter_ops_test.go core/logging.go
git commit -m "feat(core): typed filter operations with family→ops table

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Capabilities derive ops from the table

**Files:**
- Modify: `core/capabilities.go` (whole file is 55 lines)
- Modify: `core/capabilities_test.go`

**Interfaces:**
- Consumes: `OpsForField`, `withoutNegations` (Task 2).
- Produces: `GetCapabilities()` now advertises the full per-type op set; reference-relation fields advertise their type's ops minus negations. The proto layer (Task 6) and demo UI (Task 8) render exactly this.

- [ ] **Step 1: Update the tests to the new expectations (they must fail first)**

In `core/capabilities_test.go`, add package-level helper vars at the top (after the imports):

```go
// Expected op sets per family, mirroring opsByFamily. Declared once so
// assertions read as intent.
var (
	stringOps = []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpPrefix, FilterOpSuffix, FilterOpContains, FilterOpExists, FilterOpNotExists}
	numericOps = []FilterOp{FilterOpEq, FilterOpNeq, FilterOpIn, FilterOpNotIn,
		FilterOpGt, FilterOpGte, FilterOpLt, FilterOpLte, FilterOpExists, FilterOpNotExists}
	// Reference-relation fields: their type's ops minus negations.
	referenceStringOps = []FilterOp{FilterOpEq, FilterOpIn, FilterOpPrefix,
		FilterOpSuffix, FilterOpContains, FilterOpExists}
)
```

Then update every expectation that currently reads `[]FilterOp{FilterOpEq, FilterOpIn}`:

- `TestGetCapabilities_SingleResource`: `assertField(t, rc.Fields[1], "fields.status", "keyword", true, true, stringOps)`
- `TestGetCapabilities_WithRelations`: `assertField(t, rc.Fields[0], "fields.order_number", "keyword", true, true, stringOps)` and `assertField(t, rc.Fields[2], "customer.tier", "keyword", true, true, stringOps)`
- `TestGetCapabilities_SearchDisabled`: `assertField(t, f, "fields.code", "keyword", false, true, stringOps)`
- `TestGetCapabilities_MultipleResources`: `assertField(t, bField, "fields.y", "integer", true, true, numericOps)`

Replace the tail of `TestReferenceFieldsAreFilterOnly` (from the `if len(fc.FilterOps) != 2` check) with:

```go
	if len(fc.FilterOps) != len(referenceStringOps) {
		t.Fatalf("reference field ops: got %v, want %v", fc.FilterOps, referenceStringOps)
	}
	for i, op := range referenceStringOps {
		if fc.FilterOps[i] != op {
			t.Fatalf("reference field ops[%d]: got %s, want %s", i, fc.FilterOps[i], op)
		}
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestGetCapabilities|TestReferenceFields' -v`
Expected: FAIL — got `[eq in]`, want the full op lists.

- [ ] **Step 3: Update the implementation**

In `core/capabilities.go`, replace `fieldCapability` and the reference branch:

```go
// GetCapabilities returns the search capabilities for all configured resources.
func (idx *Indexer) GetCapabilities() CapabilitiesResponse {
	resp := CapabilitiesResponse{}

	for _, rc := range idx.resources {
		cap := ResourceCapability{Resource: rc.Resource}
		vc := rc.ReadVersionConfig()

		for _, f := range vc.Fields {
			cap.Fields = append(cap.Fields, fieldCapability("fields."+f.Name, f))
		}

		for _, rel := range vc.Relations {
			for _, f := range rel.Fields {
				fc := fieldCapability(fmt.Sprintf("%s.%s", rel.Resource, f.Name), f)
				if rel.IsReference() {
					// Resolved via a child search at query time: never part of the
					// parent's full-text surface or sort, and negation ops are not
					// offered because the join gives them "some joined child
					// differs" semantics instead of the document-level "no child
					// matches" (see withoutNegations).
					fc.Searchable = false
					fc.Sortable = false
					fc.FilterOps = withoutNegations(fc.FilterOps)
				}
				cap.Fields = append(cap.Fields, fc)
			}
		}

		resp.Resources = append(resp.Resources, cap)
	}

	return resp
}

func fieldCapability(path string, f resource.FieldConfig) FieldCapability {
	esType := f.ESType()
	return FieldCapability{
		Field:      path,
		Type:       esType,
		Searchable: f.Query.IsSearchable(),
		Sortable:   esType != "text",
		FilterOps:  OpsForField(f),
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/capabilities.go core/capabilities_test.go
git commit -m "feat(core): capabilities advertise type-derived filter ops

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Strict request-filter validation

**Files:**
- Create: `core/filter_validation.go`
- Create: `core/filter_validation_test.go`
- Modify: `core/search.go` (`Indexer.Search`, line 11)
- Modify: `core/federated_search.go` (resource loop in `FederatedSearch`, lines 92–100)
- Modify: `core/federated_search_test.go` (`newFederatedIndexer` — add a `region` field its own test filters on)

**Interfaces:**
- Consumes: `OpsForField`, `withoutNegations`, `FilterOp.String()` (Task 2); `splitRelationField` (existing, `core/reference_search.go:222`); `InvalidArgumentError` (existing, `core/indexer.go:22`).
- Produces: `validateRequestFilters(vc *resource.VersionConfig, filters []Filter) error` (returns `*InvalidArgumentError`). Called from `Indexer.Search` and `Indexer.FederatedSearch` **before** the middleware chain — filters appended by consumer middlewares and internal resolution are exempt by construction. Task 6 maps the error to gRPC `InvalidArgument`; Task 7 exercises it end-to-end.

- [ ] **Step 1: Write the failing tests**

Create `core/filter_validation_test.go`:

```go
package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

// newTypedIndexer builds an Indexer with one "doc" resource covering the
// validation surface: typed root fields, a denormalized many-relation and a
// reference relation.
func newTypedIndexer(backend SearchBackend) *Indexer {
	docCfg := &resource.Config{
		Resource: "doc",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{
				{Name: "name"},                  // keyword
				{Name: "size", Type: "long"},    // numeric
				{Name: "body", Type: "text"},    // fulltext: no filter ops
				{Name: "child_id"},              // join key for the reference relation
			},
			Relations: []resource.RelationConfig{
				{
					Resource:    "tag",
					Cardinality: "many",
					Join:        resource.JoinConfig{Local: "id", Foreign: "doc_id"},
					Fields:      []resource.FieldConfig{{Name: "label"}},
				},
				{
					Resource:    "child",
					Strategy:    resource.StrategyReference,
					Cardinality: "many",
					Join:        resource.JoinConfig{Local: "child_id", Foreign: "id"},
					Fields:      []resource.FieldConfig{{Name: "state"}},
				},
			},
		}},
	}
	childCfg := &resource.Config{
		Resource: "child",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields:  []resource.FieldConfig{{Name: "state"}},
		}},
	}
	docCfg.ApplyDefaults()
	childCfg.ApplyDefaults()
	return New(Config{
		Resources: resource.Configs{docCfg, childCfg},
		ES:        backend,
	})
}

func searchWithFilter(t *testing.T, f Filter) error {
	t.Helper()
	backend := &recordingBackend{}
	idx := newTypedIndexer(backend)
	_, err := idx.Search(context.Background(), SearchRequest{Resource: "doc", Filters: []Filter{f}})
	return err
}

func requireInvalidArgument(t *testing.T, err error, fragment string) {
	t.Helper()
	var inv *InvalidArgumentError
	if !errors.As(err, &inv) {
		t.Fatalf("expected InvalidArgumentError, got %v", err)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error %q should mention %q", err.Error(), fragment)
	}
}

func TestValidateFilters_ValidPass(t *testing.T) {
	valid := []Filter{
		{Field: "fields.name", Op: FilterOpEq, Value: "x"},
		{Field: "fields.name", Op: FilterOpPrefix, Value: "x"},
		{Field: "fields.size", Op: FilterOpGte, Value: "10"},
		{Field: "fields.size", Op: FilterOpLt, Value: "100"},
		{Field: "fields.name", Op: FilterOpNotIn, Values: []string{"a", "b"}},
		{Field: "fields.name", Op: FilterOpExists},
		{Field: "tag.label", Op: FilterOpNeq, Value: "x"},   // denormalized relation: negation OK
		{Field: "child.state", Op: FilterOpEq, Value: "on"}, // reference relation: positive OK
		{Field: "", Op: FilterOpEq},                         // empty field is skipped everywhere
	}
	for _, f := range valid {
		if err := searchWithFilter(t, f); err != nil {
			t.Errorf("filter %+v: unexpected error %v", f, err)
		}
	}
}

func TestValidateFilters_UnknownField(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.nope", Op: FilterOpEq, Value: "x"}), "fields.nope")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "nope.f", Op: FilterOpEq, Value: "x"}), "nope.f")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "bare", Op: FilterOpEq, Value: "x"}), "bare")
}

func TestValidateFilters_OpNotAllowedForType(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.size", Op: FilterOpPrefix, Value: "1"}), "prefix")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpGt, Value: "a"}), "gt")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.body", Op: FilterOpEq, Value: "a"}), "eq")
}

func TestValidateFilters_NegationOnReferenceField(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "child.state", Op: FilterOpNeq, Value: "on"}), "neq")
}

func TestValidateFilters_OperandShape(t *testing.T) {
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpEq}), "requires value")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpIn}), "requires values")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpEq, Value: "a", Values: []string{"b"}}), "not values")
	requireInvalidArgument(t, searchWithFilter(t,
		Filter{Field: "fields.name", Op: FilterOpExists, Value: "a"}), "takes no value")
}

// Middleware-appended filters are trusted code and bypass validation; this is
// the documented boundary that lets consumers scope by internal fields.
func TestValidateFilters_MiddlewareFiltersExempt(t *testing.T) {
	backend := &recordingBackend{}
	idx := newTypedIndexer(backend)
	mw := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.Filters = append(req.Filters, Filter{Field: "not_a_field", Op: FilterOpEq, Value: "x"})
			return next(ctx, req)
		}
	}
	idx2 := New(Config{
		Resources:         idx.resources,
		ES:                backend,
		SearchMiddlewares: []SearchMiddleware{mw},
	})
	if _, err := idx2.Search(context.Background(), SearchRequest{Resource: "doc"}); err != nil {
		t.Fatalf("middleware-added filter must bypass validation, got %v", err)
	}
	if !backend.called {
		t.Fatal("backend should have been called")
	}
}

func TestFederatedSearch_InvalidGlobalFilter(t *testing.T) {
	backend := &recordingBackend{}
	idx := newFederatedIndexer(backend, []string{"product"})

	_, err := idx.FederatedSearch(context.Background(), FederatedSearchRequest{
		Query:     "x",
		Resources: []string{"product"},
		Filters:   []Filter{{Field: "fields.unknown", Op: FilterOpEq, Value: "v"}},
	})
	requireInvalidArgument(t, err, "product")
	if backend.fedCalled {
		t.Error("backend must not be called for an invalid federated filter")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestValidateFilters|TestFederatedSearch_InvalidGlobalFilter' -v`
Expected: FAIL — the invalid-filter tests get `nil` errors (no validation exists yet). `TestValidateFilters_ValidPass` may pass already.

- [ ] **Step 3: Write the implementation**

Create `core/filter_validation.go`:

```go
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
```

In `core/search.go`, hook validation into `Indexer.Search` (between the debug log and the chain call):

```go
	// Strict validation of caller-supplied filters (spec: Request validation).
	// Runs before the middleware chain so middleware-appended filters are
	// exempt. An unknown resource falls through: searchBase reports it as
	// ErrUnknownResource, keeping that error's precedence.
	if cfg := idx.resources.Get(req.Resource); cfg != nil {
		if vc := cfg.ReadVersionConfig(); vc != nil {
			if err := validateRequestFilters(vc, req.Filters); err != nil {
				return SearchResponse{}, err
			}
		}
	}
	return idx.searchChain(ctx, req)
```

In `core/federated_search.go`, validate the global filters against every requested Type inside the existing resource loop (after the `ErrUnknownResource` check):

```go
	for _, name := range req.Resources {
		r := idx.resources.Get(name)
		if r == nil {
			return FederatedSearchResponse{}, fmt.Errorf("%q: %w", name, ErrUnknownResource)
		}
		// A global filter must be valid on every requested Type (spec:
		// Request validation): unknown-anywhere or op-mismatch-anywhere is a
		// loud InvalidArgument naming the offending Type.
		if vc := r.ReadVersionConfig(); vc != nil {
			if err := validateRequestFilters(vc, req.Filters); err != nil {
				return FederatedSearchResponse{}, &InvalidArgumentError{
					Msg: fmt.Sprintf("resource %q: %v", name, err)}
			}
		}
		for _, v := range r.SortedVersions() {
			indexToResource[IndexName(name, v)] = name
		}
	}
```

In `core/federated_search_test.go`, `newFederatedIndexer` currently gives each resource only a `text` field `name`, but `TestFederatedSearch_WiresCollectedGroupsScopeAndPaging` sends a global filter on `fields.region` — under strict validation that now fails. Add the field the test filters on:

```go
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{
					{Name: "name", Type: "text"},
					{Name: "region"}, // keyword; global-filter target in tests
				}},
			},
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... -count=1`
Expected: PASS — including the pre-existing middleware, nested-path, reference and federated tests (their caller filters all name configured fields; middleware filters are exempt).

- [ ] **Step 5: Run the app-layer suites for fallout**

Run: `GOEXPERIMENT=jsonv2 go test ./app/server/... ./app/dsl/...`
Expected: PASS (server tests filter on configured keyword fields only).

- [ ] **Step 6: Commit**

```bash
git add core/filter_validation.go core/filter_validation_test.go core/search.go core/federated_search.go core/federated_search_test.go
git commit -m "feat(core): strict request-time validation of search filters

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: ES clause building for the new ops

**Files:**
- Modify: `backend/elasticsearch/search.go` (`buildFilterClause`, line 413; add `"strings"` import)
- Modify: `backend/elasticsearch/search_test.go` (add table-driven clause tests)

**Interfaces:**
- Consumes: `core.FilterOp` constants, `FilterOp.IsNegation()`, `FilterOp.String()` (Task 2).
- Produces: `buildFilterClause(f core.Filter) (any, error)` handling all 13 ops (same signature as today — all three call sites in this file keep working); helpers `buildOpClause`, `escapeWildcard`, `opError`, `rangeKeys`.

- [ ] **Step 1: Write the failing tests**

Append to `backend/elasticsearch/search_test.go` (uses the existing `reflect` import if present; add `"reflect"` to imports if not):

```go
// ---- buildFilterClause op matrix ----

func TestBuildFilterClause_Ops(t *testing.T) {
	term := map[string]any{"term": map[string]any{"fields.a": "x"}}
	terms := map[string]any{"terms": map[string]any{"fields.a": []string{"x", "y"}}}
	exists := map[string]any{"exists": map[string]any{"field": "fields.a"}}
	mustNot := func(inner any) map[string]any {
		return map[string]any{"bool": map[string]any{"must_not": []any{inner}}}
	}
	nested := func(inner any) map[string]any {
		return map[string]any{"nested": map[string]any{"path": "rel", "query": inner}}
	}
	rangeClause := func(key string) map[string]any {
		return map[string]any{"range": map[string]any{"fields.a": map[string]any{key: "5"}}}
	}

	cases := []struct {
		name string
		f    core.Filter
		want any
	}{
		{"eq", core.Filter{Field: "fields.a", Op: core.FilterOpEq, Value: "x"}, term},
		{"neq", core.Filter{Field: "fields.a", Op: core.FilterOpNeq, Value: "x"}, mustNot(term)},
		{"in", core.Filter{Field: "fields.a", Op: core.FilterOpIn, Values: []string{"x", "y"}}, terms},
		{"not_in", core.Filter{Field: "fields.a", Op: core.FilterOpNotIn, Values: []string{"x", "y"}}, mustNot(terms)},
		{"gt", core.Filter{Field: "fields.a", Op: core.FilterOpGt, Value: "5"}, rangeClause("gt")},
		{"gte", core.Filter{Field: "fields.a", Op: core.FilterOpGte, Value: "5"}, rangeClause("gte")},
		{"lt", core.Filter{Field: "fields.a", Op: core.FilterOpLt, Value: "5"}, rangeClause("lt")},
		{"lte", core.Filter{Field: "fields.a", Op: core.FilterOpLte, Value: "5"}, rangeClause("lte")},
		{"prefix", core.Filter{Field: "fields.a", Op: core.FilterOpPrefix, Value: "vx-"},
			map[string]any{"prefix": map[string]any{"fields.a": map[string]any{"value": "vx-"}}}},
		{"suffix", core.Filter{Field: "fields.a", Op: core.FilterOpSuffix, Value: "east"},
			map[string]any{"wildcard": map[string]any{"fields.a": map[string]any{"value": "*east"}}}},
		{"contains", core.Filter{Field: "fields.a", Op: core.FilterOpContains, Value: "mid"},
			map[string]any{"wildcard": map[string]any{"fields.a": map[string]any{"value": "*mid*"}}}},
		{"suffix escapes wildcard metacharacters",
			core.Filter{Field: "fields.a", Op: core.FilterOpSuffix, Value: `a*b?c\d`},
			map[string]any{"wildcard": map[string]any{"fields.a": map[string]any{"value": `*a\*b\?c\\d`}}}},
		{"exists", core.Filter{Field: "fields.a", Op: core.FilterOpExists}, exists},
		{"not_exists", core.Filter{Field: "fields.a", Op: core.FilterOpNotExists}, mustNot(exists)},
		{"nested positive wraps in nested",
			core.Filter{Field: "rel.a", Op: core.FilterOpEq, Value: "x", NestedPath: "rel"},
			map[string]any{"nested": map[string]any{"path": "rel",
				"query": map[string]any{"term": map[string]any{"rel.a": "x"}}}}},
		{"nested negation puts must_not outside nested (no child matches)",
			core.Filter{Field: "fields.a", Op: core.FilterOpNeq, Value: "x", NestedPath: "rel"},
			mustNot(nested(term))},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildFilterClause(c.f)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got  %#v\nwant %#v", got, c.want)
			}
		})
	}
}

func TestBuildFilterClause_OperandErrors(t *testing.T) {
	cases := []core.Filter{
		{Field: "f", Op: core.FilterOpEq},                    // missing value
		{Field: "f", Op: core.FilterOpNeq},                   // missing value
		{Field: "f", Op: core.FilterOpIn},                    // missing values
		{Field: "f", Op: core.FilterOpNotIn},                 // missing values
		{Field: "f", Op: core.FilterOpGte},                   // missing value
		{Field: "f", Op: core.FilterOpPrefix},                // missing value
		{Field: "f", Op: core.FilterOpSuffix},                // missing value
		{Field: "f", Op: core.FilterOpContains},              // missing value
		{Field: "f", Op: core.FilterOp(99), Value: "x"},      // unknown op
	}
	for _, f := range cases {
		if _, err := buildFilterClause(f); err == nil {
			t.Errorf("filter %+v: expected error", f)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./backend/elasticsearch/ -run 'TestBuildFilterClause' -v`
Expected: FAIL — new ops hit the `default:` branch ("unsupported filter op").

- [ ] **Step 3: Write the implementation**

In `backend/elasticsearch/search.go`, add `"strings"` to the imports and replace `buildFilterClause` entirely:

```go
// rangeKeys maps range ops to their ES range-clause keys.
var rangeKeys = map[core.FilterOp]string{
	core.FilterOpGt:  "gt",
	core.FilterOpGte: "gte",
	core.FilterOpLt:  "lt",
	core.FilterOpLte: "lte",
}

// buildFilterClause translates one filter into an ES bool-filter clause.
// Assembly order matters for negation ops: the positive clause is wrapped in
// nested first (when NestedPath is set) and must_not second, so a negation on
// a nested field is document-level — "no child matches" — rather than "some
// child differs" (spec: Semantics / Negation on relations).
func buildFilterClause(f core.Filter) (any, error) {
	clause, err := buildOpClause(f)
	if err != nil {
		return nil, err
	}
	if f.NestedPath != "" {
		clause = map[string]any{
			"nested": map[string]any{"path": f.NestedPath, "query": clause},
		}
	}
	if f.Op.IsNegation() {
		clause = map[string]any{
			"bool": map[string]any{"must_not": []any{clause}},
		}
	}
	return clause, nil
}

// buildOpClause builds the positive form of a filter's op — a negation op
// maps to its positive counterpart's query (neq→term, not_in→terms,
// not_exists→exists); buildFilterClause adds the must_not.
func buildOpClause(f core.Filter) (any, error) {
	switch f.Op {
	case core.FilterOpEq, core.FilterOpNeq:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"term": map[string]any{f.Field: f.Value}}, nil

	case core.FilterOpIn, core.FilterOpNotIn:
		if len(f.Values) == 0 {
			return nil, opError(f, "requires values")
		}
		return map[string]any{"terms": map[string]any{f.Field: f.Values}}, nil

	case core.FilterOpGt, core.FilterOpGte, core.FilterOpLt, core.FilterOpLte:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"range": map[string]any{
			f.Field: map[string]any{rangeKeys[f.Op]: f.Value},
		}}, nil

	case core.FilterOpPrefix:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"prefix": map[string]any{
			f.Field: map[string]any{"value": f.Value},
		}}, nil

	case core.FilterOpSuffix:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"wildcard": map[string]any{
			f.Field: map[string]any{"value": "*" + escapeWildcard(f.Value)},
		}}, nil

	case core.FilterOpContains:
		if f.Value == "" {
			return nil, opError(f, "requires value")
		}
		return map[string]any{"wildcard": map[string]any{
			f.Field: map[string]any{"value": "*" + escapeWildcard(f.Value) + "*"},
		}}, nil

	case core.FilterOpExists, core.FilterOpNotExists:
		return map[string]any{"exists": map[string]any{"field": f.Field}}, nil

	default:
		return nil, fmt.Errorf("unsupported filter op for field %q", f.Field)
	}
}

func opError(f core.Filter, msg string) error {
	return fmt.Errorf("%s filter %s for field %q", f.Op, msg, f.Field)
}

// escapeWildcard backslash-escapes ES wildcard metacharacters so a
// user-supplied value always matches literally inside a wildcard query.
func escapeWildcard(s string) string {
	return strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`).Replace(s)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./backend/elasticsearch/ -count=1`
Expected: PASS — including the pre-existing `TestSearch_Filter_*` tests (EQ/IN behavior and the nested wrap are unchanged; the missing-value tests only assert that an error occurs).

- [ ] **Step 5: Commit**

```bash
git add backend/elasticsearch/search.go backend/elasticsearch/search_test.go
git commit -m "feat(es): translate the full filter-op matrix to ES clauses

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Proto enum, regeneration, and server mapping

**Files:**
- Modify: `proto/search/v1/search.proto` (`FilterOp` enum at line 89, `Filter` message comments at line 95)
- Regenerate: `app/gen/` via `buf generate` (run from repo root; do not hand-edit)
- Modify: `app/server/search.go` (`Search` error mapping at line 23, `protoFilterOp` at line 189, `capabilitiesToProto` op switch at line 225)
- Modify: `app/server/search_test.go` (new mapping + error tests)

**Interfaces:**
- Consumes: `core.FilterOp` constants (Task 2), `core.InvalidArgumentError` (validation errors from Task 4 surface through `Indexer.Search`).
- Produces: proto enum values `FILTER_OP_NEQ = 3` … `FILTER_OP_NOT_EXISTS = 13`; maps `opFromProto`/`opToProto`; `Search` returns gRPC `InvalidArgument` for validation failures. Task 8's demo UI consumes the enum names as JSON strings (e.g. `"FILTER_OP_GTE"`).

- [ ] **Step 1: Update the proto**

In `proto/search/v1/search.proto`, replace the `FilterOp` enum and the `Filter` message's value-field comments:

```proto
enum FilterOp {
  FILTER_OP_UNSPECIFIED = 0;
  FILTER_OP_EQ = 1;          // term
  FILTER_OP_IN = 2;          // terms
  FILTER_OP_NEQ = 3;         // must_not(term); on nested relations: "no child matches"
  FILTER_OP_NOT_IN = 4;      // must_not(terms); on nested relations: "no child matches"
  FILTER_OP_GT = 5;          // range — numeric, date, ip fields
  FILTER_OP_GTE = 6;         // range
  FILTER_OP_LT = 7;          // range
  FILTER_OP_LTE = 8;         // range
  FILTER_OP_PREFIX = 9;      // prefix — keyword fields
  FILTER_OP_SUFFIX = 10;     // wildcard *v — keyword fields
  FILTER_OP_CONTAINS = 11;   // wildcard *v* — keyword fields
  FILTER_OP_EXISTS = 12;     // exists — takes no value
  FILTER_OP_NOT_EXISTS = 13; // must_not(exists) — takes no value
}

message Filter {
  // Example: "b.name.keyword" or "a_status" or "c.state"
  string field = 1;

  FilterOp op = 2;

  // Single-value ops: EQ, NEQ, GT, GTE, LT, LTE, PREFIX, SUFFIX, CONTAINS.
  // Values are strings for every field type; the server coerces them against
  // the field's mapping (numbers as "42", dates as ISO-8601 / epoch millis /
  // ES date math such as "now-7d"). EXISTS / NOT_EXISTS take no value.
  string value = 3;

  // Set ops: IN, NOT_IN.
  repeated string values = 4;

  // If you need nested filtering (e.g. c is mapped as "nested"):
  // nested_path="c", field="c.state", op=EQ, value="active"
  // Usually unnecessary: the server derives it for denormalized
  // many-relations.
  string nested_path = 5;
}
```

- [ ] **Step 2: Regenerate bindings**

Run: `buf generate` (from `laika/`)
Expected: `app/gen/search/v1/search.pb.go` gains the new enum values. Then `GOEXPERIMENT=jsonv2 go build ./app/...` — expected: OK.

- [ ] **Step 3: Write the failing server tests**

Append to `app/server/search_test.go`:

```go
func TestFilterOpProtoMapping_RoundTrip(t *testing.T) {
	// Every proto op maps to a distinct core op and back.
	seen := map[core.FilterOp]bool{}
	for p, c := range opFromProto {
		require.Equal(t, c, protoFilterOp(p))
		require.False(t, seen[c], "core op %s mapped twice", c)
		seen[c] = true
		back, ok := opToProto[c]
		require.True(t, ok, "core op %s missing from opToProto", c)
		require.Equal(t, p, back)
	}
	require.Len(t, opFromProto, 13)

	// UNSPECIFIED keeps the legacy default.
	require.Equal(t, core.FilterOpEq, protoFilterOp(search.FilterOp_FILTER_OP_UNSPECIFIED))
}

func TestSearch_InvalidFilterMapsToInvalidArgument(t *testing.T) {
	// "a" has keyword fields region/name; GT is not in the string family.
	srv := federatedSearcher(&fakeBackend{})

	_, err := srv.Search(context.Background(), connect.NewRequest(&search.SearchRequest{
		Resource: "a",
		Filters:  []*search.Filter{{Field: "fields.region", Op: search.FilterOp_FILTER_OP_GT, Value: "x"}},
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./app/server/ -run 'TestFilterOpProtoMapping|TestSearch_InvalidFilter' -v`
Expected: FAIL — `undefined: opFromProto`, and the GT filter currently degrades to EQ (no error).

- [ ] **Step 5: Update the server**

In `app/server/search.go`, replace `protoFilterOp` with map-based two-way conversion:

```go
// opFromProto / opToProto convert the wire enum to core ops and back; keep
// them in sync with proto/search/v1/search.proto and core/search_types.go.
var opFromProto = map[search.FilterOp]core.FilterOp{
	search.FilterOp_FILTER_OP_EQ:         core.FilterOpEq,
	search.FilterOp_FILTER_OP_IN:         core.FilterOpIn,
	search.FilterOp_FILTER_OP_NEQ:        core.FilterOpNeq,
	search.FilterOp_FILTER_OP_NOT_IN:     core.FilterOpNotIn,
	search.FilterOp_FILTER_OP_GT:         core.FilterOpGt,
	search.FilterOp_FILTER_OP_GTE:        core.FilterOpGte,
	search.FilterOp_FILTER_OP_LT:         core.FilterOpLt,
	search.FilterOp_FILTER_OP_LTE:        core.FilterOpLte,
	search.FilterOp_FILTER_OP_PREFIX:     core.FilterOpPrefix,
	search.FilterOp_FILTER_OP_SUFFIX:     core.FilterOpSuffix,
	search.FilterOp_FILTER_OP_CONTAINS:   core.FilterOpContains,
	search.FilterOp_FILTER_OP_EXISTS:     core.FilterOpExists,
	search.FilterOp_FILTER_OP_NOT_EXISTS: core.FilterOpNotExists,
}

var opToProto = func() map[core.FilterOp]search.FilterOp {
	m := make(map[core.FilterOp]search.FilterOp, len(opFromProto))
	for p, c := range opFromProto {
		m[c] = p
	}
	return m
}()

// protoFilterOp maps a wire op to the core op. UNSPECIFIED (and any unknown
// future value) maps to EQ — the pre-extension behavior for old clients;
// core validation still rejects an EQ that doesn't fit the field.
func protoFilterOp(op search.FilterOp) core.FilterOp {
	if c, ok := opFromProto[op]; ok {
		return c
	}
	return core.FilterOpEq
}
```

Replace the op switch inside `capabilitiesToProto` (the `for _, op := range f.FilterOps` loop):

```go
			for _, op := range f.FilterOps {
				if p, ok := opToProto[op]; ok {
					pf.FilterOps = append(pf.FilterOps, p)
				}
			}
```

Replace the error handling in `Search` so validation errors surface as InvalidArgument:

```go
func (s *SearcherServer) Search(ctx context.Context, req *connect.Request[search.SearchRequest]) (*connect.Response[search.SearchResponse], error) {
	resp, err := s.idx.Search(ctx, protoToSearchRequest(req.Msg))
	if err != nil {
		var invalid *core.InvalidArgumentError
		switch {
		case errors.As(err, &invalid):
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		case errors.Is(err, core.ErrUnknownResource):
			return nil, connect.NewError(connect.CodeFailedPrecondition, core.ErrUnknownResource)
		default:
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(searchResponseToProto(resp)), nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./app/server/... -count=1`
Expected: PASS.

- [ ] **Step 7: Run the full unit suite**

Run: `GOEXPERIMENT=jsonv2 go test ./core/... ./backend/elasticsearch/... ./app/server/... ./app/dsl/...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add proto/search/v1/search.proto app/gen app/server/search.go app/server/search_test.go
git commit -m "feat(proto,server): expose the full filter-op enum over gRPC

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Integration tests against real ES

**Files:**
- Create: `app/tests/filter_ops_test.go`

**Interfaces:**
- Consumes: the whole stack (Tasks 1–6); suite plumbing from `app/tests/suite_test.go` — `t.setResourceConfig`, `t.fakeProvider.SetResource`, `t.idx.RegisterChange`, `t.worker.Drain`, `must`.
- Produces: end-to-end proof that string filter values coerce against typed ES mappings, that ranges/prefix/suffix/contains/negations/exists behave, and that validation rejects bad requests.

- [ ] **Step 1: Write the test file**

Create `app/tests/filter_ops_test.go`:

```go
package tests

import (
	"errors"

	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// TypedResourceConfig exercises the typed filter matrix: keyword, long, date
// and boolean fields on a single resource.
var TypedResourceConfig = resource.Configs{
	{
		Resource: "t",
		Versions: []resource.VersionConfig{{
			Version: 1,
			Fields: []resource.FieldConfig{
				{Name: "name"}, // keyword
				{Name: "size", Type: "long"},
				{Name: "created_at", Type: "date"},
				{Name: "active", Type: "boolean"},
			},
		}},
	},
}

func (t *TestSuite) Test_FilterOps_TypedFields() {
	for _, c := range TypedResourceConfig {
		c.ApplyDefaults()
	}
	must(t.T(), TypedResourceConfig.Validate())
	t.setResourceConfig(TypedResourceConfig)

	t.fakeProvider.SetResource("t", "1", map[string]any{
		"id": "1", "name": "vx-alpha", "size": 5,
		"created_at": "2026-01-10T00:00:00Z", "active": true,
	})
	t.fakeProvider.SetResource("t", "2", map[string]any{
		"id": "2", "name": "vx-beta", "size": 50,
		"created_at": "2026-03-10T00:00:00Z", "active": false,
	})
	t.fakeProvider.SetResource("t", "3", map[string]any{
		"id": "3", "name": "gamma", "size": 500,
		"created_at": "2026-06-10T00:00:00Z",
		// no "active" key: exercises exists / not_exists
	})

	for _, id := range []string{"1", "2", "3"} {
		err := t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "t", ResourceID: id, Kind: core.ChangeCreated,
		})
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	// search runs the filters sorted by size ascending, so expected ID slices
	// are deterministic: doc 1 (5) < doc 2 (50) < doc 3 (500).
	search := func(filters ...core.Filter) []string {
		t.T().Helper()
		resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  filters,
			Sort:     []core.SortOption{{Field: "fields.size"}},
		})
		t.Require().NoError(err)
		ids := make([]string, 0, len(resp.Hits))
		for _, h := range resp.Hits {
			ids = append(ids, h.ID)
		}
		return ids
	}

	t.Run("numeric range as two stacked AND filters", func() {
		t.Require().Equal([]string{"2"}, search(
			core.Filter{Field: "fields.size", Op: core.FilterOpGte, Value: "10"},
			core.Filter{Field: "fields.size", Op: core.FilterOpLt, Value: "100"},
		))
	})

	t.Run("date range", func() {
		t.Require().Equal([]string{"2", "3"}, search(
			core.Filter{Field: "fields.created_at", Op: core.FilterOpGt, Value: "2026-02-01T00:00:00Z"},
		))
	})

	t.Run("prefix", func() {
		t.Require().Equal([]string{"1", "2"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpPrefix, Value: "vx-"},
		))
	})

	t.Run("suffix", func() {
		t.Require().Equal([]string{"1"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpSuffix, Value: "alpha"},
		))
	})

	t.Run("contains", func() {
		t.Require().Equal([]string{"2"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpContains, Value: "bet"},
		))
	})

	t.Run("neq", func() {
		t.Require().Equal([]string{"2", "3"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpNeq, Value: "vx-alpha"},
		))
	})

	t.Run("not_in", func() {
		t.Require().Equal([]string{"3"}, search(
			core.Filter{Field: "fields.name", Op: core.FilterOpNotIn, Values: []string{"vx-alpha", "vx-beta"}},
		))
	})

	t.Run("boolean eq", func() {
		t.Require().Equal([]string{"1"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpEq, Value: "true"},
		))
	})

	t.Run("exists and not_exists", func() {
		t.Require().Equal([]string{"1", "2"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpExists},
		))
		t.Require().Equal([]string{"3"}, search(
			core.Filter{Field: "fields.active", Op: core.FilterOpNotExists},
		))
	})

	t.Run("op not allowed for type is rejected", func() {
		_, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  []core.Filter{{Field: "fields.size", Op: core.FilterOpPrefix, Value: "5"}},
		})
		var inv *core.InvalidArgumentError
		t.Require().True(errors.As(err, &inv), "expected InvalidArgumentError, got %v", err)
	})

	t.Run("unknown field is rejected", func() {
		_, err := t.idx.Search(t.T().Context(), core.SearchRequest{
			Resource: "t",
			Filters:  []core.Filter{{Field: "fields.nope", Op: core.FilterOpEq, Value: "x"}},
		})
		var inv *core.InvalidArgumentError
		t.Require().True(errors.As(err, &inv), "expected InvalidArgumentError, got %v", err)
	})
}
```

- [ ] **Step 2: Run the integration test (requires Docker)**

Run: `GOEXPERIMENT=jsonv2 go test ./app/tests/ -run 'Test_TestSuite/Test_FilterOps_TypedFields' -v -count=1`
Expected: PASS. (First run pulls testcontainer images; allow a few minutes.)

- [ ] **Step 3: Run the full integration suite**

Run: `GOEXPERIMENT=jsonv2 go test ./app/tests/... -count=1`
Expected: PASS — no existing integration test sends filters that strict validation would reject (they filter via `Query`, not `Filters`; verify any failure against that assumption before changing test data).

- [ ] **Step 4: Commit**

```bash
git add app/tests/filter_ops_test.go
git commit -m "test(app): integration coverage for the typed filter matrix

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Demo UI — typed widgets and stacked conditions

**Files:**
- Modify: `../demo/index.html` (relative to `laika/`; absolute: `laika-dev/demo/index.html`). **Not a git repo — no commit step.**

**Interfaces:**
- Consumes: capability JSON from `GetCapabilities` — each field has `type` (e.g. `"long"`, `"date"`) and `filterOps` (enum-name strings, e.g. `"FILTER_OP_GTE"`, from Task 6). Existing helpers kept as-is: `parseValues`, `classify`, `isFilterable`, `renderResource` (its `filterRow(f, c)` call keeps working — the new parameter defaults).
- Produces: per-type op dropdowns and value widgets; a per-row "+" that stacks another condition on the same field (AND — how a range is expressed); `collectFilters()` emitting `{field, op}`, `{field, op, value}` or `{field, op, values}`.

- [ ] **Step 1: Replace the op-label helper**

In the `<script>` block, replace the single line `const opLabel = (op) => op === "FILTER_OP_IN" ? "IN" : "EQ";` (near line 653; keep `isFilterable` above it) with:

```js
const OP_LABELS = {
  FILTER_OP_EQ: "=",
  FILTER_OP_NEQ: "≠",
  FILTER_OP_IN: "in",
  FILTER_OP_NOT_IN: "not in",
  FILTER_OP_GT: ">",
  FILTER_OP_GTE: "≥",
  FILTER_OP_LT: "<",
  FILTER_OP_LTE: "≤",
  FILTER_OP_PREFIX: "starts with",
  FILTER_OP_SUFFIX: "ends with",
  FILTER_OP_CONTAINS: "contains",
  FILTER_OP_EXISTS: "exists",
  FILTER_OP_NOT_EXISTS: "not exists",
};
const opLabel = (op) => OP_LABELS[op] || op;

// Op shape: set ops take a comma-separated list, presence ops take no value.
const MULTI_VALUE_OPS = new Set(["FILTER_OP_IN", "FILTER_OP_NOT_IN"]);
const NO_VALUE_OPS = new Set(["FILTER_OP_EXISTS", "FILTER_OP_NOT_EXISTS"]);
const NUMERIC_TYPES = new Set(["long", "integer", "double", "float"]);
```

- [ ] **Step 2: Replace `filterRow`**

Replace the whole `filterRow` function (currently lines 719–776) with:

```js
function filterRow(f, c, removable = false) {
  const row = document.createElement("div");
  row.className = "frow";
  row.dataset.field = f.field;
  row.dataset.type = f.type || "keyword";

  const name = document.createElement("div");
  name.className = "fname";
  name.innerHTML = `<span class="path" title="${f.field}">${removable ? "↳ " : ""}${c.label}</span>` +
                   `<span class="type">${f.type || ""}</span>`;
  row.appendChild(name);

  const controls = document.createElement("div");
  controls.className = "controls";

  const op = document.createElement("select");
  op.className = "op";
  for (const o of f.filterOps) {
    const opt = document.createElement("option");
    opt.value = o; opt.textContent = opLabel(o);
    op.appendChild(opt);
  }
  controls.appendChild(op);

  const valWrap = document.createElement("span");
  valWrap.className = "valwrap";
  controls.appendChild(valWrap);

  // "+" stacks another condition row on the same field (AND) — this is how a
  // range is expressed: one row ≥ a, a second row < b.
  const add = document.createElement("button");
  add.className = "ghost cond-btn";
  add.textContent = "+";
  add.title = "add another condition on this field (AND)";
  add.addEventListener("click", () => row.after(filterRow(f, c, true)));
  controls.appendChild(add);

  if (removable) {
    const del = document.createElement("button");
    del.className = "ghost cond-btn";
    del.textContent = "×";
    del.title = "remove this condition";
    del.addEventListener("click", () => row.remove());
    controls.appendChild(del);
  }

  const chips = document.createElement("div");
  chips.className = "chips";

  const currentValue = () => {
    const el = valWrap.querySelector(".val");
    return el ? el.value.trim() : "";
  };

  const markActive = () =>
    row.classList.toggle("active", NO_VALUE_OPS.has(op.value) || currentValue() !== "");

  const renderChips = () => {
    chips.innerHTML = "";
    if (!MULTI_VALUE_OPS.has(op.value)) return;
    for (const v of parseValues(currentValue())) {
      const chip = document.createElement("span");
      chip.className = "chip"; chip.textContent = v;
      chips.appendChild(chip);
    }
  };

  // The value control depends on op and field type: set ops take a
  // comma-separated text input regardless of type, presence ops take none,
  // booleans a —/true/false select, numbers and dates their native inputs.
  const buildValueControl = () => {
    valWrap.innerHTML = "";
    if (NO_VALUE_OPS.has(op.value)) { renderChips(); markActive(); return; }

    let el;
    const type = row.dataset.type;
    if (MULTI_VALUE_OPS.has(op.value)) {
      el = document.createElement("input");
      el.type = "text";
      el.placeholder = "comma-separated values…";
    } else if (type === "boolean") {
      el = document.createElement("select");
      for (const v of ["", "true", "false"]) {
        const o = document.createElement("option");
        o.value = v; o.textContent = v === "" ? "—" : v;
        el.appendChild(o);
      }
    } else {
      el = document.createElement("input");
      if (NUMERIC_TYPES.has(type)) el.type = "number";
      else if (type === "date") el.type = "datetime-local";
      else { el.type = "text"; el.placeholder = "value…"; }
    }
    el.className = "val";
    el.autocomplete = "off";
    el.addEventListener("input", () => { renderChips(); markActive(); });
    el.addEventListener("change", markActive);
    el.addEventListener("keydown", (e) => { if (e.key === "Enter") runSearch(); });
    valWrap.appendChild(el);
    renderChips();
    markActive();
  };

  op.addEventListener("change", buildValueControl);
  row.appendChild(controls);
  row.appendChild(chips);
  buildValueControl();
  return row;
}
```

- [ ] **Step 3: Replace `collectFilters`**

Replace the `collectFilters` function (currently lines 782–797) with:

```js
// A datetime-local value carries no timezone; convert it to the ISO-8601
// instant the backend's date fields expect. Other inputs pass through raw.
function filterValue(row) {
  const el = row.querySelector(".valwrap .val");
  if (!el) return "";
  const raw = el.value.trim();
  if (el.type === "datetime-local" && raw) {
    const d = new Date(raw);
    if (!isNaN(d)) return d.toISOString();
  }
  return raw;
}

function collectFilters() {
  const out = [];
  for (const row of document.querySelectorAll(".frow")) {
    const field = row.dataset.field;
    const op = row.querySelector("select.op").value;
    if (NO_VALUE_OPS.has(op)) {
      // Selecting exists/not exists arms the filter — there is no value.
      out.push({ field, op });
      continue;
    }
    if (MULTI_VALUE_OPS.has(op)) {
      const values = parseValues(filterValue(row));
      if (values.length) out.push({ field, op, values });
    } else {
      const value = filterValue(row);
      if (value) out.push({ field, op, value });
    }
  }
  return out;
}
```

- [ ] **Step 4: Add the supporting CSS**

In the `<style>` block, directly after the `.chips { … }` rule (near line 222), add:

```css
  .controls .valwrap { flex: 1; display: flex; min-width: 0; }
  .controls .valwrap .val { width: 100%; }
  .controls .cond-btn { padding: 2px 7px; flex: none; }
```

- [ ] **Step 5: Syntax-check the script**

Run (from `laika-dev/demo/`):
```bash
tmp=$(mktemp -d)
awk '/<script>/{flag=1;next}/<\/script>/{flag=0}flag' index.html > "$tmp/demo.js"
node --check "$tmp/demo.js"
```
Expected: no output (exit 0).

- [ ] **Step 6: Manual verification against a live backend**

Start the stack (indexer per `laika/README.md` / `make run`, or the harness), open `demo/index.html` in a browser, connect, and verify:
- a `long`/`integer` field shows a number input and the numeric op set (=, ≠, in, not in, >, ≥, <, ≤, exists, not exists);
- a `date` field shows a datetime picker; running a `≥` filter returns the expected subset;
- a keyword field offers starts with / ends with / contains and they match as expected;
- "+" adds a second row on the same field; a `≥`+`<` pair narrows results (range);
- selecting "exists" hides the value input and filters correctly;
- the raw-request pane shows `{field, op, value|values}` with the new enum names.

No commit — `demo/` is not a git repo.

---

## Final verification (after all tasks)

- [ ] Full unit suite: `GOEXPERIMENT=jsonv2 go test ./core/... ./backend/elasticsearch/... ./app/server/... ./app/dsl/... -count=1` — PASS
- [ ] Integration: `GOEXPERIMENT=jsonv2 go test ./app/tests/... -count=1` — PASS
- [ ] `GOEXPERIMENT=jsonv2 go vet ./...` — clean
- [ ] `gen-mapping` still works with typed config: `go run ./app/cmd/gen-mapping -config resources.yml` — emits mappings without error
