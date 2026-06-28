package config

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
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

// TestExampleResourcesConfig_Valid ensures the shipped example config parses
// and validates against the join DSL.
func TestExampleResourcesConfig_Valid(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "example.resources.yml")

	cfgs, err := LoadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfgs.Validate())
}
