package config

import (
	"path/filepath"
	"runtime"
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
