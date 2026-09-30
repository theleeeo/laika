package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

// New is the validation boundary: it refuses a resource config set that
// violates the config invariants, so everything downstream may assume they
// hold (e.g. ReadVersionConfig() never returns nil).
func TestNew_RejectsInvalidResourceConfig(t *testing.T) {
	_, err := New(Config{Resources: resource.Configs{{
		Resource:    "a",
		ReadVersion: 2,
		Versions:    []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "x"}}}},
	}}})
	require.ErrorContains(t, err, "readVersion 2 is not in versions")

	_, err = New(Config{Resources: resource.Configs{{Resource: "a"}}})
	require.ErrorContains(t, err, "at least one version required")
}

// New applies config defaults itself — callers no longer need to remember
// ApplyDefaults before constructing.
func TestNew_AppliesDefaults(t *testing.T) {
	idx, err := New(Config{Resources: resource.Configs{{
		Resource: "a",
		Versions: []resource.VersionConfig{
			{Version: 2, Fields: []resource.FieldConfig{{Name: "x"}}},
			{Version: 1, Fields: []resource.FieldConfig{{Name: "x"}}},
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, 1, idx.resources.Get("a").ReadVersion)
}

// An Indexer with no resources at all is a legal (if useless) state — used by
// embedders and tests that only exercise request shaping.
func TestNew_EmptyResourcesAllowed(t *testing.T) {
	idx, err := New(Config{})
	require.NoError(t, err)
	require.NotNil(t, idx)
	require.Empty(t, idx.GetCapabilities().Resources)
}

// SetPlans is the same boundary for the late-binding path the standalone app
// uses; it must enforce the same invariants and reject without applying.
func TestSetPlans_RejectsInvalidResourceConfig(t *testing.T) {
	idx, err := New(Config{Resources: resource.Configs{{
		Resource: "a",
		Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{{Name: "x"}}}},
	}}})
	require.NoError(t, err)

	err = idx.SetPlans(nil, resource.Configs{{Resource: "b"}})
	require.ErrorContains(t, err, "at least one version required")
	// The rejected set must not have been applied.
	require.NotNil(t, idx.resources.Get("a"))
	require.Nil(t, idx.resources.Get("b"))
}

// mustNew constructs an Indexer for tests, panicking if New rejects the
// config — in tests that means the fixture itself violates the config
// invariants and must be fixed.
func mustNew(cfg Config) *Indexer {
	idx, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return idx
}

// New defaults the pool's high-water mark to 80% of the queue (at least 1)
// and refuses a mark outside [1, QueueSize]; 0 asks for the default.
func TestNew_QueueHighWater(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		queueSize, highWater int
		want                 int
	}{
		{"default is 80% of the queue", 10, 0, 8},
		{"default rounds down", 7, 0, 5},
		{"default is at least 1", 1, 0, 1},
		{"default follows the defaulted queue size", 0, 0, 80}, // 10 × default PoolSize 10
		{"explicit value kept", 10, 3, 3},
		{"the queue size itself is allowed", 10, 10, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := New(Config{QueueSize: tc.queueSize, QueueHighWater: tc.highWater})
			require.NoError(t, err)
			require.Equal(t, tc.want, idx.pool.highWater)
		})
	}

	for _, tc := range []struct {
		name                 string
		queueSize, highWater int
	}{
		{"above the queue size", 10, 11},
		{"negative", 10, -1},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			_, err := New(Config{QueueSize: tc.queueSize, QueueHighWater: tc.highWater})
			require.ErrorContains(t, err, "queue high water")
		})
	}
}
