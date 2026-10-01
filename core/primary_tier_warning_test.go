package core

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

// tierlessConfigs returns a valid set where only "a" version 2 declares no
// primary-tier field; "a" version 1 and "b" both have one.
func tierlessConfigs() resource.Configs {
	primary := resource.FieldConfig{Name: "title", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}}
	return resource.Configs{
		{Resource: "a", Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{primary}},
			{Version: 2, Fields: []resource.FieldConfig{{Name: "status"}}},
		}},
		{Resource: "b", Versions: []resource.VersionConfig{{Version: 1, Fields: []resource.FieldConfig{primary}}}},
	}
}

// A schema version with no primary-tier field builds documents without a
// search_primary surface; New warns about it once, naming the version.
func TestNew_WarnsOnTierlessVersion(t *testing.T) {
	logs := captureDefaultLogs(t)

	_, err := New(Config{Resources: tierlessConfigs()})
	require.NoError(t, err)

	require.Equal(t, []tierlessWarning{{Resource: "a", Version: 2}}, logs.tierlessWarnings(t))
}

// SetPlans is the same boundary and warns the same way.
func TestSetPlans_WarnsOnTierlessVersion(t *testing.T) {
	idx, err := New(Config{})
	require.NoError(t, err)
	logs := captureDefaultLogs(t)

	require.NoError(t, idx.SetPlans(nil, tierlessConfigs()))

	require.Equal(t, []tierlessWarning{{Resource: "a", Version: 2}}, logs.tierlessWarnings(t))
}

// An invalid set is rejected, not warned about.
func TestSetPlans_InvalidSetIsNotWarnedAbout(t *testing.T) {
	idx, err := New(Config{})
	require.NoError(t, err)
	logs := captureDefaultLogs(t)

	invalid := tierlessConfigs()
	invalid[0].ReadVersion = 9
	require.Error(t, idx.SetPlans(nil, invalid))

	require.Empty(t, logs.tierlessWarnings(t))
}

type tierlessWarning struct {
	Resource string
	Version  int
}

// capturedLogs collects the JSON lines written by the default logger. The
// mutex keeps it race-free against stray goroutines from earlier tests.
type capturedLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capturedLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// captureDefaultLogs swaps slog.Default for a buffer-backed JSON logger until
// the test ends. Callers must not run in parallel.
func captureDefaultLogs(t *testing.T) *capturedLogs {
	t.Helper()
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	c := &capturedLogs{}
	slog.SetDefault(slog.New(slog.NewJSONHandler(c, nil)))
	// SetDefault also redirects the log package; restoring slog alone would
	// leave log output, and the built-in slog default, writing into the buffer.
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return c
}

// tierlessWarnings returns the tier-less-version warnings logged so far.
func (c *capturedLogs) tierlessWarnings(t *testing.T) []tierlessWarning {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []tierlessWarning
	for _, line := range strings.Split(strings.TrimSpace(c.buf.String()), "\n") {
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
