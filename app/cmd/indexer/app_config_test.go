package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppConfigFromFile(t *testing.T) {
	// Ensure env vars don't bleed in from the environment.
	for _, env := range []string{"GRPC_ADDR", "ES_ADDRS", "ES_USERNAME", "ES_PASSWORD", "RESOURCE_CONFIG_PATH", "PG_ADDR", "PROVIDER_ADDR"} {
		t.Setenv(env, "")
	}

	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
grpc:
  addr: ":9100"
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

	if cfg.GRPC.Addr != ":9100" {
		t.Fatalf("GRPC.Addr mismatch: got %q", cfg.GRPC.Addr)
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

func TestLoadAppConfigEnvOverridesFile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "indexer.yml")
	content := []byte(`
grpc:
  addr: ":9100"
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

	t.Setenv("GRPC_ADDR", ":9200")
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

	if cfg.GRPC.Addr != ":9200" {
		t.Fatalf("GRPC.Addr mismatch: got %q", cfg.GRPC.Addr)
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
