# Relation Strategy (denormalize vs reference) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-relation `strategy: denormalize | reference` so a low-count child can be searched from a high-count parent without denormalizing it (which would storm the parent on every child change).

**Architecture:** `reference` is subtractive at build time (no fetch, no Parent→Child edge, no reverse-parent enqueue) and additive only at search time (a two-phase application-side join: search the child index, fold matching IDs into a `terms` filter on the parent's join key). Build-side changes live entirely in `app/dsl`; search-side changes are two core middlewares wired around the existing chain.

**Tech Stack:** Go 1.26.1 (`GOEXPERIMENT=jsonv2`), Elasticsearch, Postgres, River, Go workspaces. Module root: `github.com/theleeeo/laika`.

## Global Constraints

- All Go commands require `GOEXPERIMENT=jsonv2` (e.g. `GOEXPERIMENT=jsonv2 go test ./core/...`).
- Any feature or behavior change must include tests (CLAUDE.md).
- `strategy` defaults to `denormalize`; the empty string means `denormalize`. Existing configs must keep working unchanged.
- `strategy` is orthogonal to `cardinality` (`one`|`many`); do not conflate them.
- Canonical vocabulary lives in `CONTEXT.md`; the new term is added in Task 1.

## Prerequisite (verify before starting Task 6)

The search-middleware base must be present in the branch you implement on. It is **not** in `origin/main` as of this plan — it exists as uncommitted work in the maintainer's working copy. Tasks 6–9 require these exact pre-existing symbols:

- `core/search_middleware.go`: `type SearchHandler func(ctx context.Context, req SearchRequest) (SearchResponse, error)`, `type SearchMiddleware func(next SearchHandler) SearchHandler`, `func chainSearch(base SearchHandler, mws []SearchMiddleware) SearchHandler`.
- `core/indexer.go`: `Config.SearchMiddlewares []SearchMiddleware`, `Indexer.searchChain SearchHandler` field, and `New` composing `idx.searchChain = chainSearch(idx.searchBase, cfg.SearchMiddlewares)`.
- `core/search.go`: `func (idx *Indexer) searchBase(ctx, req) (SearchResponse, error)` calling `idx.es.Search(ctx, req, AliasName(r.Resource), r.ReadVersionConfig())`.

Verify with: `GOEXPERIMENT=jsonv2 go build ./core/...` and `grep -n "searchChain" core/indexer.go`. If absent, land the middleware base first; Tasks 1–5 do not depend on it and can proceed regardless.

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `core/resource/config.go` | `RelationConfig.Strategy` field, strategy constants, `IsReference()` | 1 |
| `core/resource/validations.go` | strategy value check; reference-key reachability gate | 1, 2 |
| `app/dsl/plan.go` | skip building a fetch sub-plan for reference relations | 3 |
| `app/dsl/parent_resolver.go` | exclude reference relations from the reverse map | 3 |
| `backend/elasticsearch/mapping.go` | omit reference relation field blocks from the mapping | 4 |
| `core/capabilities.go` | advertise reference relation fields as filter-only | 5 |
| `core/search_types.go` | `ReferenceTarget`, `SearchRequest.References`, `AddFilter` | 6 |
| `core/reference_search.go` (new) | `referenceExpand` + `referenceExecute` middlewares | 7, 8 |
| `core/indexer.go` | wire the two middlewares around `cfg.SearchMiddlewares` | 8 |
| `CONTEXT.md`, `example.resources.yml` | vocabulary + DSL example | 1 |
| `app/tests/` | end-to-end C→A→B integration | 9 |

---

### Task 1: `strategy` field, constants, validation, and docs

**Files:**
- Modify: `core/resource/config.go` (add `Strategy` to `RelationConfig`, constants, `IsReference`)
- Modify: `core/resource/validations.go:182` (`RelationConfig.Validate` — reject invalid strategy)
- Test: `core/resource/config_test.go` (create or append)
- Modify: `CONTEXT.md` (vocabulary), `example.resources.yml` (example)

**Interfaces:**
- Produces: `resource.StrategyDenormalize` / `resource.StrategyReference` (string consts); `RelationConfig.Strategy string`; `func (r RelationConfig) IsReference() bool`.

- [ ] **Step 1: Write the failing test**

Append to `core/resource/config_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -run 'TestRelationIsReference|TestRelationStrategyValidation' -v`
Expected: FAIL — `undefined: StrategyReference` / `r.IsReference undefined`.

- [ ] **Step 3: Add the field, constants, and helper**

In `core/resource/config.go`, add constants above `RelationConfig` (near line 150):

```go
// Relation strategies decide how a child's data reaches the parent's
// searchable surface. denormalize copies the child's fields into the parent
// document (default). reference stores only the join key and resolves the
// child's fields at search time via a two-phase join.
const (
	StrategyDenormalize = "denormalize"
	StrategyReference   = "reference"
)
```

Add the `Strategy` field to `RelationConfig` (after `Cardinality`, line 153):

```go
type RelationConfig struct {
	Resource    string        `yaml:"resource"`
	Join        JoinConfig    `yaml:"join"`
	Cardinality string        `yaml:"cardinality"` // "one" or "many"; defaults to "many"
	Strategy    string        `yaml:"strategy"`    // "denormalize" (default) or "reference"
	Fields      []FieldConfig `yaml:"fields"`
}
```

Add the helper after `IsMany` (line 159):

```go
// IsReference reports whether this relation is resolved at search time rather
// than denormalized into the parent document.
func (r RelationConfig) IsReference() bool {
	return r.Strategy == StrategyReference
}
```

In `core/resource/validations.go`, inside `RelationConfig.Validate` after the cardinality check (line 193):

```go
	if r.Strategy != "" && r.Strategy != StrategyDenormalize && r.Strategy != StrategyReference {
		return fmt.Errorf("strategy must be %q or %q", StrategyDenormalize, StrategyReference)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -run 'TestRelationIsReference|TestRelationStrategyValidation' -v`
Expected: PASS.

- [ ] **Step 5: Update docs**

In `CONTEXT.md`, under "The graph" after the **Parent / Child** entry, add:

```markdown
**Strategy**:
How a Relation's Child reaches the Parent's searchable surface. `denormalize`
(default) copies the Child's selected fields into the Parent's Document — a
change to the Child rebuilds every Parent that contains it. `reference` copies
nothing: the Parent keeps only the join key and the Child's fields are resolved
at search time via a two-phase join. Used when a low-count Child would otherwise
be denormalized into a high-count Parent, where the rebuild fanout is ruinous.
Distinct from Cardinality (`one`/`many`), which is join multiplicity.
```

In `example.resources.yml`, on resource `c`'s relation to `b`, add `strategy: reference` and source the key from the `a` sibling to demonstrate key propagation:

```yaml
      - resource: b
        join: { local: b_id, from: a, foreign: id }
        strategy: reference
        fields:
          - name: name
```

(Ensure `c`'s relation to `a` lists `b_id` among its `fields:` so the key is denormalized onto `c`.)

- [ ] **Step 6: Commit**

```bash
git add core/resource/config.go core/resource/validations.go core/resource/config_test.go CONTEXT.md example.resources.yml
git commit -m "feat(resource): add relation strategy denormalize|reference"
```

---

### Task 2: Reference-key reachability validation (correctness gate)

**Files:**
- Modify: `core/resource/validations.go` (`verifyFieldRelations`, after the `Join.From` sibling check at line 72; add helper `verifyReferenceKeyReachable`)
- Test: `core/resource/validations_test.go` (create or append)

**Interfaces:**
- Consumes: `RelationConfig.IsReference()`, `JoinConfig{Local, From}`, `VersionConfig{Fields, Relations}` (Task 1).
- Produces: a hard config-load error when a `reference` relation's `local` key is not present on the indexed document.

- [ ] **Step 1: Write the failing test**

Append to `core/resource/validations_test.go`:

```go
package resource

import "testing"

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -run 'TestReferenceKey' -v`
Expected: FAIL — the unreachable/`from`-reference cases currently return nil.

- [ ] **Step 3: Add the reachability check**

In `core/resource/validations.go`, inside `verifyFieldRelations`, immediately after the `Join.From` sibling-existence block (after line 72, before the closing `}` of the `for _, currentRel` loop), add:

```go
				if currentRel.IsReference() {
					if err := verifyReferenceKeyReachable(rCfg.Resource, v, vc, currentRel); err != nil {
						return err
					}
				}
```

Then add the helper at the end of the file:

```go
// verifyReferenceKeyReachable ensures a reference relation's local join key is
// present on the indexed document so the two-phase search join can fold matching
// child IDs into a terms filter. The key is reachable either as a root field of
// the resource (Join.From == "") or as a denormalized field of the sibling
// relation named by Join.From (which must itself be a denormalize relation).
func verifyReferenceKeyReachable(resourceName string, version int, vc *VersionConfig, rel RelationConfig) error {
	key := rel.Join.Local

	if rel.Join.From == "" {
		for _, f := range vc.Fields {
			if f.Name == key {
				return nil
			}
		}
		return fmt.Errorf("version %d: reference relation '%s'->'%s' local key '%s' must be an indexed field on '%s'", version, resourceName, rel.Resource, key, resourceName)
	}

	for _, sib := range vc.Relations {
		if sib.Resource != rel.Join.From {
			continue
		}
		if sib.IsReference() {
			return fmt.Errorf("version %d: reference relation '%s'->'%s' sources its key from '%s', which must be a denormalize relation", version, resourceName, rel.Resource, rel.Join.From)
		}
		for _, f := range sib.Fields {
			if f.Name == key {
				return nil
			}
		}
		return fmt.Errorf("version %d: reference relation '%s'->'%s' local key '%s' must be a denormalized field of sibling '%s'", version, resourceName, rel.Resource, key, rel.Join.From)
	}

	return fmt.Errorf("version %d: reference relation '%s'->'%s' join from '%s' is not a sibling relation", version, resourceName, rel.Resource, rel.Join.From)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./core/resource/ -run 'TestReferenceKey' -v`
Expected: PASS. Also run the whole package: `GOEXPERIMENT=jsonv2 go test ./core/resource/`.

- [ ] **Step 5: Commit**

```bash
git add core/resource/validations.go core/resource/validations_test.go
git commit -m "feat(resource): validate reference relation key reachability"
```

---

### Task 3: Build-side — skip fetch, edges, and reverse parents for reference relations

**Files:**
- Modify: `app/dsl/plan.go:75-77` (`buildPlanForVersion` relation loop)
- Modify: `app/dsl/parent_resolver.go:36-39` (`buildReverseMap`)
- Test: `app/dsl/plan_test.go` and `app/dsl/parent_resolver_test.go` (append)

**Interfaces:**
- Consumes: `RelationConfig.IsReference()` (Task 1).
- Produces: a built plan that never fetches a reference child, never appends it to `BuildDoc.Relations`, and a reverse map that omits reference relations (so `BuildDoc.Parents` excludes them).

- [ ] **Step 1: Write the failing tests**

Append to `app/dsl/parent_resolver_test.go`:

```go
func TestBuildReverseMapSkipsReference(t *testing.T) {
	resources := resource.Configs{
		{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Relations: []resource.RelationConfig{
			// invertible by shape (Local==id, From=="") but reference -> must be skipped
			{Resource: "a", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "id", Foreign: "b_id"}, Fields: []resource.FieldConfig{{Name: "n"}}},
		}}}},
		{Resource: "a", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "n"}}}}},
	}
	rev := buildReverseMap(resources)
	if len(rev["a"]) != 0 {
		t.Fatalf("reference relation must not appear in reverse map, got %+v", rev["a"])
	}
}
```

Append to `app/dsl/plan_test.go` (a fake provider that records FetchRelated calls; adapt to the existing `source.Provider` interface in this package's tests):

```go
func TestReferenceRelationNotFetched(t *testing.T) {
	prov := &recordingProvider{} // implements source.Provider; records FetchRelated resource types
	resources := resource.Configs{
		{Resource: "c", Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
			}}}}},
		{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}}}},
	}
	plans := BuildPlansFromConfig(prov, resources)
	runPlan(t, plans["c"][0], "c", "c1") // execute the plan for one id; helper drains the channel

	if prov.fetchedRelated["b"] {
		t.Fatal("reference relation 'b' must not be fetched via FetchRelated")
	}
}
```

> Note: `recordingProvider`, `runPlan`, and `source.Provider` method signatures — model them on the existing fakes in `app/dsl/*_test.go`. If no fake exists, create a minimal one whose `FetchResource` returns `{Data: {"b_id":"b1"}}` and whose `FetchRelated` sets `fetchedRelated[resourceType]=true` and returns empty. The assertion that matters is `FetchRelated("b")` never runs.

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./app/dsl/ -run 'TestBuildReverseMapSkipsReference|TestReferenceRelationNotFetched' -v`
Expected: FAIL — reverse map contains `a`; `FetchRelated("b")` is called.

- [ ] **Step 3: Skip reference relations in the plan loop**

In `app/dsl/plan.go`, change the relation loop in `buildPlanForVersion` (lines 75-77):

```go
	for _, rel := range ordered {
		if rel.IsReference() {
			// reference relations are resolved at search time; never fetched
			// or denormalized into the document.
			continue
		}
		current = buildRelationSubPlan(provider, current, rel)
	}
```

- [ ] **Step 4: Skip reference relations in the reverse map**

In `app/dsl/parent_resolver.go`, change the guard in `buildReverseMap` (line 37):

```go
				if rel.IsReference() || rel.Join.From != "" || rel.Join.Local != identityField {
					continue
				}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./app/dsl/ -v`
Expected: PASS (all dsl tests).

- [ ] **Step 6: Commit**

```bash
git add app/dsl/plan.go app/dsl/parent_resolver.go app/dsl/plan_test.go app/dsl/parent_resolver_test.go
git commit -m "feat(dsl): reference relations skip fetch, edges, and reverse discovery"
```

---

### Task 4: Mapping — omit reference relation field blocks

**Files:**
- Modify: `backend/elasticsearch/mapping.go:24` (`GenerateMapping` relation loop)
- Test: `backend/elasticsearch/mapping_test.go` (create or append)

**Interfaces:**
- Consumes: `RelationConfig.IsReference()` (Task 1).
- Produces: a mapping whose top-level `properties` has no key for a reference relation's resource.

- [ ] **Step 1: Write the failing test**

Append to `backend/elasticsearch/mapping_test.go`:

```go
func TestGenerateMappingOmitsReference(t *testing.T) {
	vc := &resource.VersionConfig{
		Version: 1,
		Fields:  []resource.FieldConfig{{Name: "b_id"}},
		Relations: []resource.RelationConfig{
			{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
		},
	}
	m := GenerateMapping(vc)
	props := m["mappings"].(map[string]any)["properties"].(map[string]any)
	if _, ok := props["b"]; ok {
		t.Fatal("reference relation 'b' must not appear in the mapping")
	}
	if _, ok := props["fields"]; !ok {
		t.Fatal("root fields must still be mapped")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./backend/elasticsearch/ -run TestGenerateMappingOmitsReference -v`
Expected: FAIL — `props["b"]` exists.

- [ ] **Step 3: Skip reference relations in the mapping loop**

In `backend/elasticsearch/mapping.go`, add a guard at the top of the relations loop (line 24):

```go
	for _, rel := range vc.Relations {
		if rel.IsReference() {
			// reference relations store no child fields on the parent; only the
			// join key (a root field) is needed, and it is already mapped above.
			continue
		}

		relProps := make(map[string]any, len(rel.Fields)+1)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./backend/elasticsearch/ -run TestGenerateMappingOmitsReference -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/elasticsearch/mapping.go backend/elasticsearch/mapping_test.go
git commit -m "feat(es): omit reference relation fields from generated mapping"
```

---

### Task 5: Capabilities — advertise reference fields as filter-only

**Files:**
- Modify: `core/capabilities.go:21-25` (relation loop in `GetCapabilities`) and `fieldCapability`
- Test: `core/capabilities_test.go` (append)

**Interfaces:**
- Consumes: `RelationConfig.IsReference()` (Task 1).
- Produces: reference relation fields with `Searchable: false`, `Sortable: false`, `FilterOps: [FilterOpEq, FilterOpIn]`.

- [ ] **Step 1: Write the failing test**

Append to `core/capabilities_test.go`:

```go
func TestReferenceFieldsAreFilterOnly(t *testing.T) {
	idx := New(Config{Resources: resource.Configs{
		{Resource: "c", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
			}}}}},
	}})

	caps := idx.GetCapabilities()
	var fc *FieldCapability
	for i := range caps.Resources {
		for j := range caps.Resources[i].Fields {
			if caps.Resources[i].Fields[j].Field == "b.name" {
				fc = &caps.Resources[i].Fields[j]
			}
		}
	}
	if fc == nil {
		t.Fatal("reference field b.name must still be advertised")
	}
	if fc.Searchable || fc.Sortable {
		t.Fatalf("reference field must be filter-only, got searchable=%v sortable=%v", fc.Searchable, fc.Sortable)
	}
	if len(fc.FilterOps) != 2 {
		t.Fatalf("reference field must support Eq+In filters, got %v", fc.FilterOps)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestReferenceFieldsAreFilterOnly -v`
Expected: FAIL — `b.name` is advertised searchable/sortable (its field type is the default `keyword`, so sortable is currently true).

- [ ] **Step 3: Force filter-only for reference relation fields**

In `core/capabilities.go`, change the relation loop (lines 21-25) to pass the reference flag, and add a post-adjust:

```go
		for _, rel := range vc.Relations {
			for _, f := range rel.Fields {
				fc := fieldCapability(fmt.Sprintf("%s.%s", rel.Resource, f.Name), f)
				if rel.IsReference() {
					fc.Searchable = false
					fc.Sortable = false
					fc.FilterOps = []FilterOp{FilterOpEq, FilterOpIn}
				}
				cap.Fields = append(cap.Fields, fc)
			}
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestReferenceFieldsAreFilterOnly -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/capabilities.go core/capabilities_test.go
git commit -m "feat(core): advertise reference relation fields as filter-only"
```

---

### Task 6: Search types — `ReferenceTarget`, `SearchRequest.References`, `AddFilter`

> **Prerequisite check (run now):** `grep -n "searchChain" core/indexer.go` must succeed. If not, land the middleware base first.

**Files:**
- Modify: `core/search_types.go` (add `ReferenceTarget`, `References` field, `AddFilter` method)
- Test: `core/search_types_test.go` (create)

**Interfaces:**
- Produces:
  - `type ReferenceTarget struct { Resource string; ForeignField string; ParentKeyField string; ParentKeyNestedPath string; Filters []Filter }`
  - `SearchRequest.References []ReferenceTarget`
  - `func (r *SearchRequest) AddFilter(f Filter)` — appends `f` to `r.Filters` **and** to every `r.References[i].Filters`. This is how cross-cutting middleware (e.g. tenant scoping) applies one filter to all search targets in a single pass.

- [ ] **Step 1: Write the failing test**

Create `core/search_types_test.go`:

```go
package core

import "testing"

func TestAddFilterHitsAllTargets(t *testing.T) {
	req := SearchRequest{
		Resource:   "c",
		References: []ReferenceTarget{{Resource: "b"}, {Resource: "d"}},
	}
	req.AddFilter(Filter{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"})

	if len(req.Filters) != 1 {
		t.Fatalf("primary should have 1 filter, got %d", len(req.Filters))
	}
	for i, ref := range req.References {
		if len(ref.Filters) != 1 || ref.Filters[0].Value != "t1" {
			t.Fatalf("reference target %d should have the tenant filter, got %+v", i, ref.Filters)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestAddFilterHitsAllTargets -v`
Expected: FAIL — `ReferenceTarget` / `References` / `AddFilter` undefined.

- [ ] **Step 3: Add the types and method**

In `core/search_types.go`, add the `References` field to `SearchRequest`:

```go
type SearchRequest struct {
	Resource   string
	Query      string
	Page       int32
	PageSize   int32
	Filters    []Filter
	Sort       []SortOption
	References []ReferenceTarget
}
```

Add the new type and method at the end of the file:

```go
// ReferenceTarget is a child search the reference-join expands out of a parent
// request: search the child Resource with Filters, collect each hit's
// ForeignField value, then fold those values into a terms filter on the
// parent's ParentKeyField (wrapped in ParentKeyNestedPath when non-empty).
type ReferenceTarget struct {
	Resource            string
	ForeignField        string
	ParentKeyField      string
	ParentKeyNestedPath string
	Filters             []Filter
}

// AddFilter appends f to the primary request and to every reference target, so
// a single cross-cutting middleware pass (e.g. tenant scoping) applies the same
// filter to the parent search and all child searches.
func (r *SearchRequest) AddFilter(f Filter) {
	r.Filters = append(r.Filters, f)
	for i := range r.References {
		r.References[i].Filters = append(r.References[i].Filters, f)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestAddFilterHitsAllTargets -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/search_types.go core/search_types_test.go
git commit -m "feat(core): SearchRequest reference targets + AddFilter"
```

---

### Task 7: `referenceExpand` middleware

**Files:**
- Create: `core/reference_search.go`
- Test: `core/reference_search_test.go` (create)

**Interfaces:**
- Consumes: `resource.Configs`, `RelationConfig.IsReference()`, `RelationConfig.LocalSource`, `JoinConfig{Local, Foreign, From}`, `VersionConfig.GetRelation`, `ReferenceTarget`, `Filter` (Tasks 1, 6).
- Produces: `func referenceExpand(resources resource.Configs) SearchMiddleware` — outermost middleware. For each `req.Filters` entry whose `Field` is `"<refRel>.<field>"` where `refRel` is a reference relation on `req.Resource`'s read version: removes it from `req.Filters` and folds it into a `ReferenceTarget` (one per child resource), rewriting the field to `"fields.<field>"`.

- [ ] **Step 1: Write the failing test**

Create `core/reference_search_test.go`:

```go
package core

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/core/resource"
)

func refResources() resource.Configs {
	return resource.Configs{
		{Resource: "c", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1,
			Fields: []resource.FieldConfig{{Name: "b_id"}},
			Relations: []resource.RelationConfig{
				{Resource: "a", Join: resource.JoinConfig{Local: "id", Foreign: "c_id"}, Fields: []resource.FieldConfig{{Name: "b_id"}}},
				{Resource: "b", Strategy: resource.StrategyReference, Join: resource.JoinConfig{Local: "b_id", From: "a", Foreign: "id"}, Fields: []resource.FieldConfig{{Name: "name"}}},
			}}}}},
		{Resource: "a", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "b_id"}}}}},
		{Resource: "b", ReadVersion: 1, Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "name"}}}}},
	}
}

func TestReferenceExpandMovesFilterToTarget(t *testing.T) {
	var captured SearchRequest
	base := func(_ context.Context, req SearchRequest) (SearchResponse, error) {
		captured = req
		return SearchResponse{}, nil
	}
	mw := referenceExpand(refResources())
	_, _ = mw(base)(context.Background(), SearchRequest{
		Resource: "c",
		Filters: []Filter{
			{Field: "fields.b_id", Op: FilterOpEq, Value: "keep"}, // native field, stays
			{Field: "b.name", Op: FilterOpEq, Value: "acme"},      // reference, moves
		},
	})

	if len(captured.Filters) != 1 || captured.Filters[0].Field != "fields.b_id" {
		t.Fatalf("native filter must remain on primary, got %+v", captured.Filters)
	}
	if len(captured.References) != 1 {
		t.Fatalf("expected 1 reference target, got %d", len(captured.References))
	}
	tgt := captured.References[0]
	if tgt.Resource != "b" || tgt.ForeignField != "id" {
		t.Fatalf("bad target identity: %+v", tgt)
	}
	if tgt.ParentKeyField != "a.b_id" || tgt.ParentKeyNestedPath != "a" {
		t.Fatalf("bad parent key path: field=%q nested=%q", tgt.ParentKeyField, tgt.ParentKeyNestedPath)
	}
	if len(tgt.Filters) != 1 || tgt.Filters[0].Field != "fields.name" || tgt.Filters[0].Value != "acme" {
		t.Fatalf("child filter must be rewritten to fields.name, got %+v", tgt.Filters)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestReferenceExpandMovesFilterToTarget -v`
Expected: FAIL — `referenceExpand` undefined.

- [ ] **Step 3: Implement `referenceExpand`**

Create `core/reference_search.go`:

```go
package core

import (
	"context"
	"strings"

	"github.com/theleeeo/laika/core/resource"
)

// referenceExpand is the outermost search middleware. It rewrites filters that
// target a reference relation's fields into ReferenceTarget child searches,
// leaving the primary request with only its native filters. The child searches
// are executed later by referenceExecute (innermost), so any cross-cutting
// middleware in between applies to both primary and child targets via
// SearchRequest.AddFilter.
func referenceExpand(resources resource.Configs) SearchMiddleware {
	return func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			cfg := resources.Get(req.Resource)
			if cfg == nil {
				return next(ctx, req)
			}
			vc := cfg.ReadVersionConfig()
			if vc == nil {
				return next(ctx, req)
			}

			var primary []Filter
			targets := map[string]*ReferenceTarget{}

			for _, f := range req.Filters {
				relName, field, ok := splitRelationField(f.Field)
				if ok {
					if rel := vc.GetRelation(relName); rel != nil && rel.IsReference() {
						tgt := targets[relName]
						if tgt == nil {
							tgt = newReferenceTarget(req.Resource, *rel)
							targets[relName] = tgt
						}
						childFilter := f
						childFilter.Field = "fields." + field
						childFilter.NestedPath = ""
						tgt.Filters = append(tgt.Filters, childFilter)
						continue
					}
				}
				primary = append(primary, f)
			}

			req.Filters = primary
			for _, rel := range vc.Relations { // deterministic order
				if t, ok := targets[rel.Resource]; ok {
					req.References = append(req.References, *t)
				}
			}

			return next(ctx, req)
		}
	}
}

// splitRelationField splits "rel.field" into ("rel", "field", true). A field
// already under "fields." (a root field) or with no dot returns ok=false.
func splitRelationField(path string) (string, string, bool) {
	i := strings.IndexByte(path, '.')
	if i <= 0 {
		return "", "", false
	}
	head := path[:i]
	if head == "fields" {
		return "", "", false
	}
	return head, path[i+1:], true
}

// newReferenceTarget derives the child-search identity and the parent key path
// from a reference relation. The parent key lives at fields.<local> when read
// from the root, or <from>.<local> (nested when the sibling is a many relation)
// when sourced from a denormalized sibling.
func newReferenceTarget(root string, rel resource.RelationConfig, vc *resource.VersionConfig) *ReferenceTarget {
	tgt := &ReferenceTarget{
		Resource:     rel.Resource,
		ForeignField: rel.Join.Foreign,
	}
	if rel.Join.From == "" {
		tgt.ParentKeyField = "fields." + rel.Join.Local
		return tgt
	}
	tgt.ParentKeyField = rel.Join.From + "." + rel.Join.Local
	if sib := vc.GetRelation(rel.Join.From); sib != nil && sib.IsMany() {
		tgt.ParentKeyNestedPath = rel.Join.From
	}
	return tgt
}
```

And update the call site in `referenceExpand` to `newReferenceTarget(req.Resource, *rel, vc)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestReferenceExpandMovesFilterToTarget -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/reference_search.go core/reference_search_test.go
git commit -m "feat(core): referenceExpand middleware rewrites reference filters"
```

---

### Task 8: `referenceExecute` middleware + wiring in `New`

**Files:**
- Modify: `core/reference_search.go` (add `referenceExecute` method + `maxReferenceTerms` const)
- Modify: `core/indexer.go:65-73` (`New` — wire expand outermost, execute innermost)
- Test: `core/reference_search_test.go` (append, with a fake `SearchBackend`)

**Interfaces:**
- Consumes: `Indexer.es` (`SearchBackend`), `Indexer.resources`, `AliasName`, `SearchHit`, `ReferenceTarget`, `Filter` (Task 6).
- Produces: `func (idx *Indexer) referenceExecute(next SearchHandler) SearchHandler` — innermost middleware. For each `req.References` target: runs `idx.es.Search` on the child alias, collects `ForeignField` values, appends a `terms` filter (`ParentKeyField IN [...]`, `NestedPath: ParentKeyNestedPath`) to `req.Filters`; short-circuits to an empty response if any target matches nothing; clears `req.References`; calls `next`.

- [ ] **Step 1: Write the failing test**

Append to `core/reference_search_test.go`:

```go
type fakeBackend struct {
	childHits map[string][]SearchHit // alias -> hits
	gotPrimary SearchRequest
	calls      int
}

func (f *fakeBackend) Upsert(context.Context, string, string, any, int64) error { return nil }
func (f *fakeBackend) BulkUpsert(context.Context, []BulkItem) error             { return nil }
func (f *fakeBackend) Delete(context.Context, string, string) error            { return nil }
func (f *fakeBackend) Search(_ context.Context, req SearchRequest, alias string, _ *resource.VersionConfig) (SearchResponse, error) {
	f.calls++
	if hits, ok := f.childHits[alias]; ok {
		return SearchResponse{Total: int64(len(hits)), Hits: hits}, nil
	}
	f.gotPrimary = req // primary search (alias not in childHits)
	return SearchResponse{Total: 1, Hits: []SearchHit{{ID: "c1"}}}, nil
}

func TestReferenceExecuteFoldsTermsFilter(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{
		AliasName("b"): {{ID: "b1"}, {ID: "b2"}},
	}}
	idx := New(Config{Resources: refResources(), ES: be})

	resp, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 1 || len(resp.Hits) != 1 || resp.Hits[0].ID != "c1" {
		t.Fatalf("unexpected primary response: %+v", resp)
	}
	// primary must carry the folded terms filter on the parent key path
	var found *Filter
	for i := range be.gotPrimary.Filters {
		if be.gotPrimary.Filters[i].Field == "a.b_id" {
			found = &be.gotPrimary.Filters[i]
		}
	}
	if found == nil {
		t.Fatalf("expected folded terms filter on a.b_id, got %+v", be.gotPrimary.Filters)
	}
	if found.Op != FilterOpIn || len(found.Values) != 2 || found.NestedPath != "a" {
		t.Fatalf("bad folded filter: %+v", found)
	}
}

func TestReferenceExecuteShortCircuitsOnNoMatch(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {}}}
	idx := New(Config{Resources: refResources(), ES: be})

	resp, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c",
		Filters:  []Filter{{Field: "b.name", Op: FilterOpEq, Value: "nope"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 0 || len(resp.Hits) != 0 {
		t.Fatalf("expected empty response when reference matches nothing, got %+v", resp)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestReferenceExecute' -v`
Expected: FAIL — `referenceExecute` not wired; the `b.name` filter reaches `buildFilterClause` / no folding happens.

- [ ] **Step 3: Implement `referenceExecute`**

Append to `core/reference_search.go`:

```go
// maxReferenceTerms bounds how many child IDs a reference target may fold into
// a terms filter. Reference is chosen for low-count children, so this ceiling is
// generous; exceeding it is a misconfiguration and fails loudly rather than
// silently truncating. Stays well under Elasticsearch's 65536 terms limit.
const maxReferenceTerms = 10000

// referenceExecute is the innermost search middleware (runs just above
// searchBase). It resolves each reference target into a terms filter on the
// parent key before the primary search runs.
func (idx *Indexer) referenceExecute(next SearchHandler) SearchHandler {
	return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
		targets := req.References
		req.References = nil

		for _, tgt := range targets {
			childCfg := idx.resources.Get(tgt.Resource)
			if childCfg == nil {
				return SearchResponse{}, ErrUnknownResource
			}

			childResp, err := idx.es.Search(ctx, SearchRequest{
				Resource: tgt.Resource,
				Filters:  tgt.Filters,
				PageSize: maxReferenceTerms,
			}, AliasName(tgt.Resource), childCfg.ReadVersionConfig())
			if err != nil {
				return SearchResponse{}, err
			}
			if childResp.Total > maxReferenceTerms {
				return SearchResponse{}, &InvalidArgumentError{Msg: fmt.Sprintf(
					"reference relation to %q matched %d children, exceeding the %d-term ceiling; this child is not low-count enough for strategy: reference",
					tgt.Resource, childResp.Total, maxReferenceTerms)}
			}

			values := make([]string, 0, len(childResp.Hits))
			for _, h := range childResp.Hits {
				if v := foreignValue(h, tgt.ForeignField); v != "" {
					values = append(values, v)
				}
			}
			if len(values) == 0 {
				// No child matches -> no parent can match this reference.
				return SearchResponse{}, nil
			}

			req.Filters = append(req.Filters, Filter{
				Field:      tgt.ParentKeyField,
				Op:         FilterOpIn,
				Values:     values,
				NestedPath: tgt.ParentKeyNestedPath,
			})
		}

		return next(ctx, req)
	}
}

// foreignValue extracts the child's join value for a hit. The common case is
// foreign == "id" (the document id); otherwise it reads fields.<foreign> from
// the source.
func foreignValue(h SearchHit, foreign string) string {
	if foreign == "id" {
		return h.ID
	}
	if h.Source == nil {
		return ""
	}
	fields, _ := h.Source["fields"].(map[string]any)
	if fields == nil {
		return ""
	}
	v, _ := fields[foreign].(string)
	return v
}
```

Add `"fmt"` to the imports of `core/reference_search.go`.

- [ ] **Step 4: Wire the middlewares in `New`**

In `core/indexer.go`, change `New` (lines 65-73) so reference-expand is outermost and reference-execute is innermost, with the caller's middlewares in between:

```go
func New(cfg Config) *Indexer {
	idx := &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		river:     cfg.RiverClient,
		resources: cfg.Resources,
		plans:     cfg.Plans,
	}

	mws := make([]SearchMiddleware, 0, len(cfg.SearchMiddlewares)+2)
	mws = append(mws, referenceExpand(cfg.Resources)) // outermost: expand reference filters
	mws = append(mws, cfg.SearchMiddlewares...)        // caller middleware (e.g. tenant) sees References
	mws = append(mws, idx.referenceExecute)            // innermost: run child searches, fold terms

	idx.searchChain = chainSearch(idx.searchBase, mws)
	return idx
}
```

> If the merged middleware base already assigns `idx.searchChain` differently, replace that assignment with the three-layer composition above. The invariant: `referenceExpand` first, `cfg.SearchMiddlewares` next, `idx.referenceExecute` last.

- [ ] **Step 5: Run tests to verify they pass**

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run 'TestReferenceExecute|TestReferenceExpand|TestAddFilter' -v`
Expected: PASS. Then the whole package: `GOEXPERIMENT=jsonv2 go test ./core/`.

- [ ] **Step 6: Verify tenant composition (regression)**

Add a test proving a caller middleware that uses `AddFilter` scopes both searches. Append to `core/reference_search_test.go`:

```go
func TestTenantMiddlewareScopesBothSearches(t *testing.T) {
	be := &fakeBackend{childHits: map[string][]SearchHit{AliasName("b"): {{ID: "b1"}}}}
	var childReq SearchRequest
	be2 := &captureChild{fakeBackend: be, onChild: func(r SearchRequest) { childReq = r }}

	tenant := func(next SearchHandler) SearchHandler {
		return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
			req.AddFilter(Filter{Field: "fields.tenant_id", Op: FilterOpEq, Value: "t1"})
			return next(ctx, req)
		}
	}
	idx := New(Config{Resources: refResources(), ES: be2, SearchMiddlewares: []SearchMiddleware{tenant}})

	_, err := idx.Search(context.Background(), SearchRequest{
		Resource: "c", Filters: []Filter{{Field: "b.name", Op: FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFilter(childReq.Filters, "fields.tenant_id") {
		t.Fatalf("child search must be tenant-scoped, got %+v", childReq.Filters)
	}
	if !hasFilter(be.gotPrimary.Filters, "fields.tenant_id") {
		t.Fatalf("primary search must be tenant-scoped, got %+v", be.gotPrimary.Filters)
	}
}

func hasFilter(fs []Filter, field string) bool {
	for _, f := range fs {
		if f.Field == field {
			return true
		}
	}
	return false
}

// captureChild wraps fakeBackend to capture the child request.
type captureChild struct {
	*fakeBackend
	onChild func(SearchRequest)
}

func (c *captureChild) Search(ctx context.Context, req SearchRequest, alias string, vc *resource.VersionConfig) (SearchResponse, error) {
	if _, ok := c.childHits[alias]; ok {
		c.onChild(req)
	}
	return c.fakeBackend.Search(ctx, req, alias, vc)
}
```

Run: `GOEXPERIMENT=jsonv2 go test ./core/ -run TestTenantMiddlewareScopesBothSearches -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add core/reference_search.go core/reference_search_test.go core/indexer.go
git commit -m "feat(core): referenceExecute two-phase join + wire reference middlewares"
```

---

### Task 9: End-to-end integration (C→A→B)

**Files:**
- Create or extend: `app/tests/reference_test.go` (testcontainers: Postgres + Elasticsearch)
- Reference patterns from existing `app/tests/*_test.go` for harness setup (provider stub, indexer wiring, ES assertions).

**Interfaces:**
- Consumes: the full stack — config load + validation (Tasks 1-2), plan build (Task 3), mapping (Task 4), search middlewares (Tasks 6-8).

**Goal:** With `c` denormalizing `a` (carrying `b_id`) and referencing `b` keyed `from: a`, prove: (1) building `c` writes no `c→b` edge; (2) changing a `b` reindexes only `b` and does not enqueue `c` rebuilds; (3) "search c by b.name" returns the right `c`.

- [ ] **Step 1: Write the failing integration test**

Create `app/tests/reference_test.go`. Use the existing test harness helpers in `app/tests` (find them with `grep -rn "func.*testing.T" app/tests/`). Skeleton:

```go
//go:build integration || !unit

package tests

import (
	"context"
	"testing"
	// existing harness imports
)

func TestReferenceRelationEndToEnd(t *testing.T) {
	h := newHarness(t) // existing helper: spins ES + Postgres + Indexer from a resources config
	defer h.Close()

	// resources: c denormalizes a (fields incl b_id), references b from: a
	h.LoadConfig(t, `
resources:
  - resource: c
    versions:
      - version: 1
        fields: [{name: title}]
        relations:
          - resource: a
            join: { local: id, foreign: c_id }
            fields: [{name: b_id}]
          - resource: b
            strategy: reference
            join: { local: b_id, from: a, foreign: id }
            fields: [{name: name}]
  - resource: a
    versions: [{version: 1, fields: [{name: c_id}, {name: b_id}]}]
  - resource: b
    versions: [{version: 1, fields: [{name: name}]}]
`)

	// provider data: c1 -> a1(c_id=c1, b_id=b1); b1.name = "acme"
	h.Provider.Put("c", "c1", map[string]any{"title": "doc one"})
	h.Provider.PutRelated("a", "c_id", "c1", []map[string]any{{"id": "a1", "c_id": "c1", "b_id": "b1"}})
	h.Provider.Put("b", "b1", map[string]any{"name": "acme"})

	// build c1 and b1
	h.Build(t, "c", "c1")
	h.Build(t, "b", "b1")

	// (1) no c->b edge persisted
	parents, err := h.Store.GetParentResources(context.Background(), model.Resource{Type: "b", Id: "b1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range parents {
		if p.Type == "c" {
			t.Fatal("reference relation must not create a c->b edge")
		}
	}

	// (3) search c by b.name = acme -> returns c1
	resp, err := h.Indexer.Search(context.Background(), core.SearchRequest{
		Resource: "c",
		Filters:  []core.Filter{{Field: "b.name", Op: core.FilterOpEq, Value: "acme"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 1 || resp.Hits[0].ID != "c1" {
		t.Fatalf("expected c1 via reference join, got %+v", resp)
	}

	// (2) change b1 -> RegisterChange must not enqueue any c build
	before := h.EnqueuedBuilds("c")
	h.RegisterChange(t, "b", "b1")
	if got := h.EnqueuedBuilds("c") - before; got != 0 {
		t.Fatalf("changing referenced b must not enqueue c rebuilds, got %d", got)
	}
}
```

> Adapt method names (`newHarness`, `Put`, `PutRelated`, `Build`, `RegisterChange`, `EnqueuedBuilds`, `Store`, `Indexer`) to the actual `app/tests` harness. If the harness lacks an enqueue counter, assert via River job inspection or via the absence of a rebuilt `c` document version bump.

- [ ] **Step 2: Run it to verify it fails**

Run: `GOEXPERIMENT=jsonv2 go test ./app/tests/ -run TestReferenceRelationEndToEnd -v`
Expected: FAIL (until Tasks 1-8 are integrated; if run after them, it should pass — this is the integration guard).
Requires Docker.

- [ ] **Step 3: Make it pass**

No new product code should be needed if Tasks 1-8 are complete and correct. Fix any wiring gaps the integration test reveals (config load path passing `strategy` through YAML, alias creation for `b`, etc.). Do not weaken assertions.

- [ ] **Step 4: Run the full suite**

Run:
```bash
GOEXPERIMENT=jsonv2 go test ./core/... ./backend/... ./app/dsl/... ./core/resource/...
GOEXPERIMENT=jsonv2 go test ./app/tests/...   # Docker
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/tests/reference_test.go
git commit -m "test(integration): reference relation end-to-end C->A->B"
```

---

## Self-Review

**Spec coverage:**
- Strategy DSL field + default → Task 1. ✓
- Vocabulary in CONTEXT.md → Task 1. ✓
- Build: skip fetch / no edge / no reverse parents → Task 3 (in `app/dsl`, keeping core untouched per "Plans encapsulate data fetching"). ✓
- Mapping omits child fields → Task 4. ✓
- Search two-phase join (single forward pass, multiple targets) → Tasks 6-8. ✓
- Tenant applies to both searches via `AddFilter` → Task 6 (mechanism) + Task 8 Step 6 (regression). ✓
- Capabilities filter-only → Task 5. ✓
- Config validation: reference key reachable (root / via denormalize sibling; reject reference sibling) → Task 2. ✓
- Chained references unsupported; key propagation via `from:` → realized by Tasks 3 (skip fetch but keep sibling denormalize) + 7 (`newReferenceTarget` parent-key path). ✓
- Terms-ceiling guard → Task 8 (`maxReferenceTerms`). ✓
- Integration C→A→B → Task 9. ✓

> **Refinement vs spec:** the spec described reference-join as a single outermost middleware that also executes. Implementation splits it into `referenceExpand` (outermost) and `referenceExecute` (innermost) so caller middleware (tenant) runs *between* them and is applied to child targets. Same observable behavior; this is the only way "tenant on both searches in one pass" works without recursion. No spec rewrite required, but note the split.

**Placeholder scan:** No TBD/TODO in product code steps. The one underspecified area is the `app/tests` harness method names (Task 9) and the `app/dsl` test fakes (Task 3) — flagged explicitly with adaptation notes, because they must match pre-existing test infrastructure that varies; the assertions are concrete.

**Type consistency:** `IsReference()`, `StrategyReference`, `ReferenceTarget{Resource, ForeignField, ParentKeyField, ParentKeyNestedPath, Filters}`, `SearchRequest.References`, `AddFilter`, `referenceExpand`, `referenceExecute`, `newReferenceTarget(root, rel, vc)`, `foreignValue`, `maxReferenceTerms` are used consistently across Tasks 1, 5, 6, 7, 8. `idx.es.Search(ctx, req, AliasName(...), vc)` matches the `SearchBackend.Search` signature in `core/backend.go:16`.
