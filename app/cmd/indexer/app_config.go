package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/spf13/viper"
)

// appConfig holds all runtime configuration for the indexer binary.
//
// Config file keys use dot-notation (e.g. es.addrs). Each key maps to an
// upper-snake-case env var by replacing '.' with '_':
//
//	grpc.public_addr       → GRPC_PUBLIC_ADDR
//	grpc.admin_addr        → GRPC_ADMIN_ADDR
//	es.addrs               → ES_ADDRS  (comma-separated when set via env)
//	es.username            → ES_USERNAME
//	es.password            → ES_PASSWORD
//	es.federated_execution → ES_FEDERATED_EXECUTION
//	pg.addr                → PG_ADDR
//	provider.addr          → PROVIDER_ADDR
//	resource_config_path   → RESOURCE_CONFIG_PATH
//	log.level              → LOG_LEVEL
//	temporal.host_port     → TEMPORAL_HOST_PORT
//	temporal.namespace     → TEMPORAL_NAMESPACE
//	temporal.task_queue    → TEMPORAL_TASK_QUEUE
//	sweep.interval         → SWEEP_INTERVAL
//	sweep.threshold        → SWEEP_THRESHOLD
//	sweep.batch_size       → SWEEP_BATCH_SIZE
//	sweep.backoff          → SWEEP_BACKOFF
//	sweep.backoff_max      → SWEEP_BACKOFF_MAX
//	pool.size              → POOL_SIZE
//	pool.queue_size        → POOL_QUEUE_SIZE
//	pool.queue_high_water  → POOL_QUEUE_HIGH_WATER
//	pool.owner_lease       → POOL_OWNER_LEASE
//
// forward_walks has no env var: it is a list, which doesn't map onto env
// vars, and it is read from the file without viper (see readForwardWalks).
type appConfig struct {
	GRPC               grpcConfig     `mapstructure:"grpc"`
	ES                 esConfig       `mapstructure:"es"`
	PG                 pgConfig       `mapstructure:"pg"`
	Provider           providerConfig `mapstructure:"provider"`
	ResourceConfigPath string         `mapstructure:"resource_config_path"`
	Log                logConfig      `mapstructure:"log"`
	Temporal           temporalConfig `mapstructure:"temporal"`
	Sweep              sweepConfig    `mapstructure:"sweep"`
	Pool               poolConfig     `mapstructure:"pool"`
	// ForwardWalks is read from the file by readForwardWalks, not by viper,
	// and has no env overrides.
	ForwardWalks []forwardWalkConfig `mapstructure:"-"`
}

// forwardWalkConfig is one forward_walks entry: the scheduled forward walk of
// one resource type (core.ForwardWalkConfig). Zero values leave core's
// defaults; a duration's zero is written 0s, since a bare 0 doesn't decode.
type forwardWalkConfig struct {
	ResourceType string        `yaml:"resource_type"`
	Enabled      bool          `yaml:"enabled"`
	Interval     time.Duration `yaml:"interval"`
	PageSize     int           `yaml:"page_size"`
	PageInterval time.Duration `yaml:"page_interval"`
	// Metadata is the static list of metadata maps a run walks with, one
	// walk per map. Empty means one walk with no metadata.
	Metadata []map[string]string `yaml:"metadata"`
}

type logConfig struct {
	Level string `mapstructure:"level"`
}

type grpcConfig struct {
	// PublicAddr is the read/search surface (SearchService). It is browser-
	// facing (CORS) and safe to expose publicly.
	PublicAddr string `mapstructure:"public_addr"`
	// AdminAddr is the write/control surface (IndexService: NotifyChange,
	// NotifyChangeBatch, Rebuild). It carries no CORS and is meant for internal
	// callers only.
	AdminAddr string `mapstructure:"admin_addr"`
}

type esConfig struct {
	Addrs    []string `mapstructure:"addrs"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
	// FederatedExecution selects how Federated Search executes: "single-dfs"
	// (default), "single", or "fanout". An experiment toggle — see
	// elasticsearch.FederatedExecution. Empty means the default.
	FederatedExecution string `mapstructure:"federated_execution"`
}

type pgConfig struct {
	Addr string `mapstructure:"addr"`
}

type providerConfig struct {
	Addr string `mapstructure:"addr"`
}

type temporalConfig struct {
	HostPort  string `mapstructure:"host_port"`
	Namespace string `mapstructure:"namespace"`
	TaskQueue string `mapstructure:"task_queue"`
}

type sweepConfig struct {
	// Interval between StaleSweep schedule firings.
	Interval time.Duration `mapstructure:"interval"`
	// Threshold: only resources stale longer than this are swept.
	Threshold time.Duration `mapstructure:"threshold"`
	BatchSize int           `mapstructure:"batch_size"`
	// Backoff is how long the sweep leaves a resource after its first failed
	// build or delete in a row, doubling with each further one
	// (core.Config.SweepBackoff); BackoffMax caps it, so a resource that never
	// builds is retried at that interval (core.Config.SweepBackoffMax).
	Backoff    time.Duration `mapstructure:"backoff"`
	BackoffMax time.Duration `mapstructure:"backoff_max"`
}

type poolConfig struct {
	// Size bounds concurrent inline builds.
	Size int `mapstructure:"size"`
	// QueueSize bounds accepted-but-not-yet-running inline builds; a full
	// queue sheds new submissions to the sweep.
	QueueSize int `mapstructure:"queue_size"`
	// QueueHighWater is the queued-build count at or above which the pool is
	// under pressure (core.Config.QueueHighWater). 0 means core's default,
	// 80% of QueueSize. Only WaitForSlot registrations pace on it, and the
	// app's RPCs never wait (ADR 0008), so it has no effect here yet.
	QueueHighWater int `mapstructure:"queue_high_water"`
	// OwnerLease is how long an inline build's ownership of its resource
	// holds without renewing (core.Config.OwnerLease): while it is live, a
	// change to the resource on any instance submits no second build, and the
	// owner runs one follow-up for it. 0 means core's default, 30s. Size it
	// above a build's or delete's queue wait plus its run; core.Config's
	// OwnerLease says what a shorter one costs.
	OwnerLease time.Duration `mapstructure:"owner_lease"`
}

// loadAppConfig reads the config file at configFilePath (if present) and
// overlays any env var overrides. Missing config file is not an error.
func loadAppConfig(configFilePath string) (appConfig, error) {
	v := viper.New()
	v.SetConfigFile(configFilePath)

	// Env vars override file values. Dots in key names become underscores,
	// so "es.addrs" → ES_ADDRS, "grpc.public_addr" → GRPC_PUBLIC_ADDR, etc.
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("grpc.public_addr", ":9000")
	v.SetDefault("grpc.admin_addr", ":9010")
	v.SetDefault("es.addrs", []string{"http://localhost:9200"})
	v.SetDefault("es.username", "")
	v.SetDefault("es.password", "")
	v.SetDefault("es.federated_execution", "")
	v.SetDefault("pg.addr", "postgres://user:pass@localhost:5432/indexer")
	v.SetDefault("provider.addr", "")
	v.SetDefault("resource_config_path", "resources.yml")
	v.SetDefault("log.level", "info")
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "laika-indexer")
	v.SetDefault("sweep.interval", "1m")
	v.SetDefault("sweep.threshold", "5m")
	v.SetDefault("sweep.batch_size", 500)
	v.SetDefault("sweep.backoff", "5m")
	v.SetDefault("sweep.backoff_max", "24h")
	v.SetDefault("pool.size", 10)
	v.SetDefault("pool.queue_size", 100)
	v.SetDefault("pool.queue_high_water", 0)
	v.SetDefault("pool.owner_lease", "0s")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := errors.AsType[viper.ConfigFileNotFoundError](err); !ok && !errors.Is(err, os.ErrNotExist) {
			return appConfig{}, fmt.Errorf("read app config: %w", err)
		}
	}

	var cfg appConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return appConfig{}, fmt.Errorf("unmarshal app config: %w", err)
	}

	if v.InConfig("forward_walks") {
		walks, err := readForwardWalks(configFilePath)
		if err != nil {
			return appConfig{}, err
		}
		cfg.ForwardWalks = walks
	}

	// ES_ADDRS may arrive as a comma-separated string when set via env var.
	// cast.ToStringSlice (used by Viper) splits on whitespace, not commas,
	// so we read the raw value and split on commas ourselves.
	cfg.ES.Addrs = getStringSlice(v, "es.addrs")

	return cfg, nil
}

// getStringSlice reads a Viper key as a string slice, handling both YAML
// list values and comma-separated env var strings.
func getStringSlice(v *viper.Viper, key string) []string {
	raw := v.Get(key)
	switch val := raw.(type) {
	case string:
		var out []string
		for _, s := range strings.Split(val, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				if s = strings.TrimSpace(s); s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	default:
		return v.GetStringSlice(key)
	}
}

// readForwardWalks decodes the forward_walks section of the YAML config file
// at path. Viper lowercases every map key it reads, those inside list items
// included, which would turn a metadata key such as tenantId into tenantid;
// decoding the section itself keeps each key as written. The section is
// decoded strictly, so a misspelled key is an error rather than a walk that
// silently isn't configured; the rest of the file is viper's.
func readForwardWalks(path string) ([]forwardWalkConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read app config forward_walks: %w", err)
	}
	var file struct {
		ForwardWalks ast.Node `yaml:"forward_walks"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		switch strings.ToLower(filepath.Ext(path)) {
		case ".yml", ".yaml", ".json":
			return nil, fmt.Errorf("decode app config forward_walks: %w", err)
		default:
			return nil, fmt.Errorf("decode app config forward_walks (the section needs a YAML config file): %w", err)
		}
	}
	if file.ForwardWalks == nil {
		// Viper found the section matching case-insensitively (InConfig), but
		// it isn't spelled forward_walks: an error, not walks left out.
		return nil, errors.New("decode app config forward_walks: write the section's key as forward_walks, in lower case")
	}
	var walks []forwardWalkConfig
	if err := yaml.NodeToValue(file.ForwardWalks, &walks, yaml.DisallowUnknownField()); err != nil {
		return nil, fmt.Errorf("decode app config forward_walks: %w", err)
	}
	return walks, nil
}
