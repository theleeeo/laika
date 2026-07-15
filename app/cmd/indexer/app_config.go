package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// appConfig holds all runtime configuration for the indexer binary.
//
// Config file keys use dot-notation (e.g. es.addrs). Each key maps to an
// upper-snake-case env var by replacing '.' with '_':
//
//	grpc.public_addr    → GRPC_PUBLIC_ADDR
//	grpc.admin_addr     → GRPC_ADMIN_ADDR
//	es.addrs            → ES_ADDRS  (comma-separated when set via env)
//	es.username         → ES_USERNAME
//	es.password         → ES_PASSWORD
//	pg.addr             → PG_ADDR
//	provider.addr       → PROVIDER_ADDR
//	resource_config_path → RESOURCE_CONFIG_PATH
//	log.level           → LOG_LEVEL
//	temporal.host_port  → TEMPORAL_HOST_PORT
//	temporal.namespace  → TEMPORAL_NAMESPACE
//	temporal.task_queue → TEMPORAL_TASK_QUEUE
//	sweep.interval      → SWEEP_INTERVAL
//	sweep.threshold     → SWEEP_THRESHOLD
//	sweep.batch_size    → SWEEP_BATCH_SIZE
//	pool.size           → POOL_SIZE
//	pool.queue_size     → POOL_QUEUE_SIZE
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
}

type poolConfig struct {
	// Size bounds concurrent inline builds.
	Size int `mapstructure:"size"`
	// QueueSize bounds accepted-but-not-yet-running inline builds; a full
	// queue sheds new submissions to the sweep.
	QueueSize int `mapstructure:"queue_size"`
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
	v.SetDefault("pool.size", 10)
	v.SetDefault("pool.queue_size", 100)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := errors.AsType[viper.ConfigFileNotFoundError](err); !ok && !errors.Is(err, os.ErrNotExist) {
			return appConfig{}, fmt.Errorf("read app config: %w", err)
		}
	}

	var cfg appConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return appConfig{}, fmt.Errorf("unmarshal app config: %w", err)
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
