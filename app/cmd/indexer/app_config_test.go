package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadAppConfigFromFile(t *testing.T) {
	// Ensure env vars don't bleed in from the environment.
	for _, env := range []string{"GRPC_PUBLIC_ADDR", "GRPC_ADMIN_ADDR", "ES_ADDRS", "ES_USERNAME", "ES_PASSWORD", "RESOURCE_CONFIG_PATH", "PG_ADDR", "PROVIDER_ADDR"} {
		t.Setenv(env, "")
	}

	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
grpc:
  public_addr: ":9100"
  admin_addr: ":9110"
es:
  addrs:
    - "http://es-a:9200"
    - "http://es-b:9200"
  username: "file-user"
  password: "file-pass"
pg:
  addr: "postgres://file-user:file-pass@localhost:5432/file-db"
provider:
  addr: "localhost:50051"
resource_config_path: "resources.from.file.yml"
`)

	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig error: %v", err)
	}

	if cfg.GRPC.PublicAddr != ":9100" {
		t.Fatalf("GRPC.PublicAddr mismatch: got %q", cfg.GRPC.PublicAddr)
	}
	if cfg.GRPC.AdminAddr != ":9110" {
		t.Fatalf("GRPC.AdminAddr mismatch: got %q", cfg.GRPC.AdminAddr)
	}
	if len(cfg.ES.Addrs) != 2 || cfg.ES.Addrs[0] != "http://es-a:9200" || cfg.ES.Addrs[1] != "http://es-b:9200" {
		t.Fatalf("ES.Addrs mismatch: got %#v", cfg.ES.Addrs)
	}
	if cfg.ES.Username != "file-user" {
		t.Fatalf("ES.Username mismatch: got %q", cfg.ES.Username)
	}
	if cfg.ES.Password != "file-pass" {
		t.Fatalf("ES.Password mismatch: got %q", cfg.ES.Password)
	}
	if cfg.PG.Addr != "postgres://file-user:file-pass@localhost:5432/file-db" {
		t.Fatalf("PG.Addr mismatch: got %q", cfg.PG.Addr)
	}
	if cfg.Provider.Addr != "localhost:50051" {
		t.Fatalf("Provider.Addr mismatch: got %q", cfg.Provider.Addr)
	}
	if cfg.ResourceConfigPath != "resources.from.file.yml" {
		t.Fatalf("ResourceConfigPath mismatch: got %q", cfg.ResourceConfigPath)
	}
}

func TestLoadAppConfig_Defaults(t *testing.T) {
	cfg, err := loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}

	if cfg.Temporal.HostPort != "localhost:7233" {
		t.Errorf("temporal.host_port default: %q", cfg.Temporal.HostPort)
	}
	if cfg.Temporal.Namespace != "default" {
		t.Errorf("temporal.namespace default: %q", cfg.Temporal.Namespace)
	}
	if cfg.Temporal.TaskQueue != "laika-indexer" {
		t.Errorf("temporal.task_queue default: %q", cfg.Temporal.TaskQueue)
	}
	if cfg.Sweep.Interval != time.Minute {
		t.Errorf("sweep.interval default: %v", cfg.Sweep.Interval)
	}
	if cfg.Sweep.Threshold != 5*time.Minute {
		t.Errorf("sweep.threshold default: %v", cfg.Sweep.Threshold)
	}
	if cfg.Sweep.BatchSize != 500 {
		t.Errorf("sweep.batch_size default: %d", cfg.Sweep.BatchSize)
	}
	if cfg.Pool.Size != 10 {
		t.Errorf("pool.size default: %d", cfg.Pool.Size)
	}
	if cfg.Pool.QueueSize != 100 {
		t.Errorf("pool.queue_size default: %d", cfg.Pool.QueueSize)
	}
}

func TestLoadAppConfig_LogLevelDefaultAndOverride(t *testing.T) {
	cfg, err := loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}
	if cfg.Log.Level != "info" {
		t.Fatalf("default log level = %q, want %q", cfg.Log.Level, "info")
	}

	t.Setenv("LOG_LEVEL", "debug")
	cfg, err = loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig with env: %v", err)
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("LOG_LEVEL override = %q, want %q", cfg.Log.Level, "debug")
	}
}

func TestLoadAppConfigEnvOverridesFile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
grpc:
  public_addr: ":9100"
  admin_addr: ":9110"
es:
  addrs:
    - "http://es-a:9200"
  username: "file-user"
  password: "file-pass"
pg:
  addr: "postgres://file-user:file-pass@localhost:5432/file-db"
provider:
  addr: "localhost:50051"
resource_config_path: "resources.from.file.yml"
`)

	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	t.Setenv("GRPC_PUBLIC_ADDR", ":9200")
	t.Setenv("GRPC_ADMIN_ADDR", ":9210")
	t.Setenv("ES_ADDRS", "http://env-a:9200,http://env-b:9200")
	t.Setenv("ES_USERNAME", "env-user")
	t.Setenv("ES_PASSWORD", "env-pass")
	t.Setenv("PG_ADDR", "postgres://env-user:env-pass@localhost:5432/env-db")
	t.Setenv("PROVIDER_ADDR", "localhost:50061")
	t.Setenv("RESOURCE_CONFIG_PATH", "resources.from.env.yml")

	cfg, err := loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig error: %v", err)
	}

	if cfg.GRPC.PublicAddr != ":9200" {
		t.Fatalf("GRPC.PublicAddr mismatch: got %q", cfg.GRPC.PublicAddr)
	}
	if cfg.GRPC.AdminAddr != ":9210" {
		t.Fatalf("GRPC.AdminAddr mismatch: got %q", cfg.GRPC.AdminAddr)
	}
	if len(cfg.ES.Addrs) != 2 || cfg.ES.Addrs[0] != "http://env-a:9200" || cfg.ES.Addrs[1] != "http://env-b:9200" {
		t.Fatalf("ES.Addrs mismatch: got %#v", cfg.ES.Addrs)
	}
	if cfg.ES.Username != "env-user" {
		t.Fatalf("ES.Username mismatch: got %q", cfg.ES.Username)
	}
	if cfg.ES.Password != "env-pass" {
		t.Fatalf("ES.Password mismatch: got %q", cfg.ES.Password)
	}
	if cfg.PG.Addr != "postgres://env-user:env-pass@localhost:5432/env-db" {
		t.Fatalf("PG.Addr mismatch: got %q", cfg.PG.Addr)
	}
	if cfg.Provider.Addr != "localhost:50061" {
		t.Fatalf("Provider.Addr mismatch: got %q", cfg.Provider.Addr)
	}
	if cfg.ResourceConfigPath != "resources.from.env.yml" {
		t.Fatalf("ResourceConfigPath mismatch: got %q", cfg.ResourceConfigPath)
	}
}

func TestLoadAppConfig_PoolQueueHighWater(t *testing.T) {
	t.Setenv("POOL_QUEUE_HIGH_WATER", "")
	cfg, err := loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}
	if cfg.Pool.QueueHighWater != 0 {
		t.Errorf("pool.queue_high_water default = %d, want 0 (core's default)", cfg.Pool.QueueHighWater)
	}

	t.Setenv("POOL_QUEUE_HIGH_WATER", "90")
	cfg, err = loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig with env only: %v", err)
	}
	if cfg.Pool.QueueHighWater != 90 {
		t.Errorf("POOL_QUEUE_HIGH_WATER without a file = %d, want 90", cfg.Pool.QueueHighWater)
	}
	t.Setenv("POOL_QUEUE_HIGH_WATER", "")

	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
pool:
  queue_size: 200
  queue_high_water: 150
`)
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfg, err = loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig from file: %v", err)
	}
	if cfg.Pool.QueueHighWater != 150 {
		t.Errorf("pool.queue_high_water from file = %d, want 150", cfg.Pool.QueueHighWater)
	}

	t.Setenv("POOL_QUEUE_HIGH_WATER", "120")
	cfg, err = loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig with env: %v", err)
	}
	if cfg.Pool.QueueHighWater != 120 {
		t.Errorf("POOL_QUEUE_HIGH_WATER override = %d, want 120", cfg.Pool.QueueHighWater)
	}
}

func TestLoadAppConfig_PoolOwnerLease(t *testing.T) {
	t.Setenv("POOL_OWNER_LEASE", "")
	cfg, err := loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}
	if cfg.Pool.OwnerLease != 0 {
		t.Errorf("pool.owner_lease default = %v, want 0 (core's default)", cfg.Pool.OwnerLease)
	}

	t.Setenv("POOL_OWNER_LEASE", "45s")
	cfg, err = loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig with env only: %v", err)
	}
	if cfg.Pool.OwnerLease != 45*time.Second {
		t.Errorf("POOL_OWNER_LEASE without a file = %v, want 45s", cfg.Pool.OwnerLease)
	}
	t.Setenv("POOL_OWNER_LEASE", "")

	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
pool:
  owner_lease: "2m"
`)
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfg, err = loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig from file: %v", err)
	}
	if cfg.Pool.OwnerLease != 2*time.Minute {
		t.Errorf("pool.owner_lease from file = %v, want 2m", cfg.Pool.OwnerLease)
	}

	t.Setenv("POOL_OWNER_LEASE", "90s")
	cfg, err = loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig with env: %v", err)
	}
	if cfg.Pool.OwnerLease != 90*time.Second {
		t.Errorf("POOL_OWNER_LEASE override = %v, want 90s", cfg.Pool.OwnerLease)
	}
}

func TestLoadAppConfig_ForwardWalks(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
forward_walks:
  - resource_type: Product
    enabled: true
    interval: 12h
    page_size: 50
    page_interval: 2s
    metadata:
      - tenantId: a
        regionCode: EU
      - tenantId: b
  - resource_type: order
`)
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}

	want := []forwardWalkConfig{
		{
			ResourceType: "Product",
			Enabled:      true,
			Interval:     12 * time.Hour,
			PageSize:     50,
			PageInterval: 2 * time.Second,
			Metadata: []map[string]string{
				{"tenantId": "a", "regionCode": "EU"},
				{"tenantId": "b"},
			},
		},
		{ResourceType: "order"},
	}
	if !reflect.DeepEqual(cfg.ForwardWalks, want) {
		t.Fatalf("forward_walks = %#v, want %#v (resource types and metadata keys keep their case)", cfg.ForwardWalks, want)
	}
}

func TestLoadAppConfig_ForwardWalksAbsentByDefault(t *testing.T) {
	cfg, err := loadAppConfig("does-not-exist.yml")
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}
	if len(cfg.ForwardWalks) != 0 {
		t.Fatalf("forward_walks without a file = %#v, want none", cfg.ForwardWalks)
	}

	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	if err := os.WriteFile(configPath, []byte("log:\n  level: info\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfg, err = loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig: %v", err)
	}
	if len(cfg.ForwardWalks) != 0 {
		t.Fatalf("forward_walks in a file without the section = %#v, want none", cfg.ForwardWalks)
	}
}

func TestLoadAppConfig_ForwardWalksInvalid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section string
	}{
		{name: "unknown key in an entry", section: `
forward_walks:
  - resource_type: product
    enable: true
`},
		{name: "malformed duration", section: `
forward_walks:
  - resource_type: product
    enabled: true
    interval: 1 day
`},
		{name: "mapping where the list belongs", section: `
forward_walks:
  product:
    enabled: true
`},
		{name: "section key in another case", section: `
Forward_Walks:
  - resource_type: product
    enabled: true
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "indexer.yml")
			if err := os.WriteFile(configPath, []byte(tc.section), 0o600); err != nil {
				t.Fatalf("write config file: %v", err)
			}
			cfg, err := loadAppConfig(configPath)
			if err == nil {
				t.Fatalf("loadAppConfig = %#v, want an error", cfg.ForwardWalks)
			}
			if strings.Contains(err.Error(), "needs a YAML config file") {
				t.Errorf("error %q blames the file format of a YAML file", err)
			}
		})
	}
}

func TestLoadAppConfig_ForwardWalksStrictnessScopedToSection(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
some_key_viper_ignores: true
forward_walks:
  - resource_type: product
    enabled: true
`)
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	cfg, err := loadAppConfig(configPath)
	if err != nil {
		t.Fatalf("loadAppConfig: %v (an unknown key outside forward_walks is viper's to ignore)", err)
	}
	if len(cfg.ForwardWalks) != 1 || !cfg.ForwardWalks[0].Enabled {
		t.Fatalf("forward_walks = %#v, want the one enabled entry", cfg.ForwardWalks)
	}
}
