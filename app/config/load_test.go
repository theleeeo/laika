package config

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

// TestParseConfig_JoinDSL verifies the join block parses both sides of the
// relation, including the optional from for chained joins.
func TestParseConfig_JoinDSL(t *testing.T) {
	yaml := `
resources:
  - type: order
    fields:
      - name: customer_id
    relations:
      - resource: customer
        join: { local: customer_id, foreign: id }
        cardinality: one
        fields:
          - name: name
      - resource: address
        join: { local: address_id, foreign: id, from: customer }
        fields:
          - name: city
  - type: customer
    fields:
      - name: name
      - name: address_id
  - type: address
    fields:
      - name: city
`
	cfgs, err := ParseConfig([]byte(yaml))
	require.NoError(t, err)

	order := cfgs.Get("order")
	require.NotNil(t, order)
	rels := order.GetVersion(1).Relations
	require.Len(t, rels, 2)

	require.Equal(t, "customer_id", rels[0].Join.Local)
	require.Equal(t, "id", rels[0].Join.Foreign)
	require.Empty(t, rels[0].Join.From)

	require.Equal(t, "address_id", rels[1].Join.Local)
	require.Equal(t, "id", rels[1].Join.Foreign)
	require.Equal(t, "customer", rels[1].Join.From)
}

// TestParseConfig_SearchTier verifies the per-field search tier selector parses
// from YAML, that an omitted selector resolves to none, and that an unknown tier
// is rejected loudly by validation.
func TestParseConfig_SearchTier(t *testing.T) {
	yaml := `
resources:
  - type: doc
    fields:
      - name: title
        query:
          search: primary
      - name: body
        query:
          search: secondary
      - name: internal
        query:
          search: none
      - name: omitted
`
	cfgs, err := ParseConfig([]byte(yaml))
	require.NoError(t, err)
	require.NoError(t, cfgs.Validate())

	fields := cfgs.Get("doc").GetVersion(1).Fields
	require.Equal(t, resource.SearchTierPrimary, fields[0].Query.Tier())
	require.True(t, fields[0].Query.IsSearchable())
	require.Equal(t, resource.SearchTierSecondary, fields[1].Query.Tier())
	require.True(t, fields[1].Query.IsSearchable())
	require.Equal(t, resource.SearchTierNone, fields[2].Query.Tier())
	require.False(t, fields[2].Query.IsSearchable())
	// Omitted resolves to none — the breaking-change default.
	require.Equal(t, resource.SearchTierNone, fields[3].Query.Tier())
	require.False(t, fields[3].Query.IsSearchable())

	bad := `
resources:
  - type: doc
    fields:
      - name: title
        query:
          search: bogus
`
	cfgs, err = ParseConfig([]byte(bad))
	require.NoError(t, err)
	require.Error(t, cfgs.Validate())
}

// TestExampleResourcesConfig_Valid ensures the shipped example config parses
// and validates against the join DSL.
func TestExampleResourcesConfig_Valid(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "example.resources.yml")

	cfgs, err := LoadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfgs.Validate())
}

// TestParseConfig_VersionedFlatShape verifies a versioned entry parses the
// same flat shape as an unversioned one — fields as a list, relations and
// nestedBlocks as sibling keys — so adding `version:` to an entry does not
// restructure it.
func TestParseConfig_VersionedFlatShape(t *testing.T) {
	yaml := `
resources:
  - type: a
    version: 1
    fields:
      - name: searchField
        query:
          search: primary
    relations:
      - resource: c
        join: { local: id, foreign: a_id }
        fields:
          - name: number
  - type: a
    version: 2
    readVersion: 1
    fields:
      - name: searchField
    nestedBlocks:
      - name: operator_data
        scopeKey: fiber_operator_id
        fields:
          - name: custom_fields
  - type: c
    fields:
      - name: number
`
	cfgs, err := ParseConfig([]byte(yaml))
	require.NoError(t, err)
	require.NoError(t, cfgs.Validate())

	a := cfgs.Get("a")
	require.NotNil(t, a)
	require.Equal(t, 1, a.ReadVersion)
	require.Equal(t, []int{1, 2}, a.SortedVersions())

	v1 := a.GetVersion(1)
	require.Len(t, v1.Fields, 1)
	require.Equal(t, "searchField", v1.Fields[0].Name)
	require.Len(t, v1.Relations, 1)
	require.Equal(t, "c", v1.Relations[0].Resource)

	v2 := a.GetVersion(2)
	require.Len(t, v2.Fields, 1)
	require.Empty(t, v2.Relations)
	require.Len(t, v2.NestedBlocks, 1)
	require.Equal(t, "operator_data", v2.NestedBlocks[0].Name)
	require.Equal(t, "fiber_operator_id", v2.NestedBlocks[0].ScopeKey)
}

// TestParseConfig_UnversionedNestedBlocks verifies the sibling nestedBlocks
// key also works on unversioned entries.
func TestParseConfig_UnversionedNestedBlocks(t *testing.T) {
	yaml := `
resources:
  - type: a
    fields:
      - name: name
    nestedBlocks:
      - name: operator_data
        scopeKey: fiber_operator_id
        fields:
          - name: custom_fields
`
	cfgs, err := ParseConfig([]byte(yaml))
	require.NoError(t, err)

	blocks := cfgs.Get("a").GetVersion(1).NestedBlocks
	require.Len(t, blocks, 1)
	require.Equal(t, "operator_data", blocks[0].Name)
}

// TestParseConfig_VersionedNestedShape keeps the legacy nested shape working:
// a versioned entry whose fields key is a {fields, relations, nestedBlocks}
// object.
func TestParseConfig_VersionedNestedShape(t *testing.T) {
	yaml := `
resources:
  - type: a
    version: 2
    fields:
      fields:
        - name: searchField
      relations:
        - resource: c
          join: { local: id, foreign: a_id }
          fields:
            - name: number
  - type: c
    fields:
      - name: number
`
	cfgs, err := ParseConfig([]byte(yaml))
	require.NoError(t, err)

	v2 := cfgs.Get("a").GetVersion(2)
	require.Len(t, v2.Fields, 1)
	require.Len(t, v2.Relations, 1)
	require.Equal(t, "c", v2.Relations[0].Resource)
}

// TestParseConfig_NestedShapeWithSiblingKeysRejected: mixing the nested shape
// with sibling relations/nestedBlocks is ambiguous — previously the sibling
// keys were silently dropped; now it is a load error.
func TestParseConfig_NestedShapeWithSiblingKeysRejected(t *testing.T) {
	yaml := `
resources:
  - type: a
    version: 2
    fields:
      fields:
        - name: searchField
    relations:
      - resource: c
        join: { local: id, foreign: a_id }
        fields:
          - name: number
`
	_, err := ParseConfig([]byte(yaml))
	require.ErrorContains(t, err, "both")
}

// TestParseConfig_NoResourcesRejected: an indexer app with zero resources is
// a deployment mistake — the loader refuses it (the core library itself
// permits an empty set).
func TestParseConfig_NoResourcesRejected(t *testing.T) {
	_, err := ParseConfig([]byte("resources: []"))
	require.ErrorContains(t, err, "at least one resource")
}

// A schema version with no primary-tier field builds documents free-text
// queries never match; the loader warns once, naming the resource and version.
// It swaps slog.Default, so it must not run in parallel.
func TestLoadConfig_WarnsOnTierlessVersion(t *testing.T) {
	tierless := `
resources:
  - type: a
    version: 1
    fields:
      - name: title
        query:
          search: primary
  - type: a
    version: 2
    fields:
      - name: title
        query:
          search: secondary
`
	path := filepath.Join(t.TempDir(), "resources.yml")
	require.NoError(t, os.WriteFile(path, []byte(tierless), 0o600))

	logs := captureDefaultLogs(t)
	_, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, []tierlessWarning{{Resource: "a", Version: 2}}, tierlessWarnings(t, logs))

	withPrimary := `
resources:
  - type: a
    fields:
      - name: title
        query:
          search: primary
`
	logs = captureDefaultLogs(t)
	_, err = ParseConfig([]byte(withPrimary))
	require.NoError(t, err)
	require.Empty(t, tierlessWarnings(t, logs))
}

type tierlessWarning struct {
	Resource string
	Version  int
}

// captureDefaultLogs swaps slog.Default for a buffer-backed JSON logger until
// the test ends. Callers must not run in parallel.
func captureDefaultLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	// SetDefault also redirects the log package; restoring slog alone would
	// leave log output, and the built-in slog default, writing into the buffer.
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return &buf
}

// tierlessWarnings returns the tier-less-version warnings in the captured logs.
func tierlessWarnings(t *testing.T, logs *bytes.Buffer) []tierlessWarning {
	t.Helper()
	var out []tierlessWarning
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		msg, _ := rec["msg"].(string)
		if rec["level"] != "WARN" || !strings.Contains(msg, "no primary-tier field") {
			continue
		}
		res, _ := rec["resource"].(string)
		ver, _ := rec["version"].(float64)
		out = append(out, tierlessWarning{res, int(ver)})
	}
	return out
}
