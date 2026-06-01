package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// appConfig holds all runtime configuration for the indexer binary.
//
// Config file keys use dot-notation (e.g. es.addrs). Each key maps to an
// upper-snake-case env var by replacing '.' with '_':
//
//	grpc.addr           → GRPC_ADDR
//	es.addrs            → ES_ADDRS  (comma-separated when set via env)
//	es.username         → ES_USERNAME
//	es.password         → ES_PASSWORD
//	pg.addr             → PG_ADDR
//	provider.addr       → PROVIDER_ADDR
//	resource_config_path → RESOURCE_CONFIG_PATH
type appConfig struct {
	GRPC               grpcConfig     `mapstructure:"grpc"`
	ES                 esConfig       `mapstructure:"es"`
	PG                 pgConfig       `mapstructure:"pg"`
	Provider           providerConfig `mapstructure:"provider"`
	ResourceConfigPath string         `mapstructure:"resource_config_path"`
}

type grpcConfig struct {
	Addr string `mapstructure:"addr"`
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

// loadAppConfig reads the config file at configFilePath (if present) and
// overlays any env var overrides. Missing config file is not an error.
func loadAppConfig(configFilePath string) (appConfig, error) {
	v := viper.New()
	v.SetConfigFile(configFilePath)

	// Env vars override file values. Dots in key names become underscores,
	// so "es.addrs" → ES_ADDRS, "grpc.addr" → GRPC_ADDR, etc.
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("grpc.addr", ":9000")
	v.SetDefault("es.addrs", []string{"http://localhost:9200"})
	v.SetDefault("es.username", "")
	v.SetDefault("es.password", "")
	v.SetDefault("pg.addr", "postgres://user:pass@localhost:5432/indexer")
	v.SetDefault("provider.addr", "")
	v.SetDefault("resource_config_path", "resources.yml")

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
