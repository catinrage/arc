package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadGatewayDefaults(t *testing.T) {
	cfg, err := LoadGateway("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenPort != 1080 || cfg.Connections <= 0 || cfg.RelayURL == "" {
		t.Fatalf("bad defaults: %#v", cfg)
	}
}

func TestLoadGatewayJSONOverridesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	err := os.WriteFile(path, []byte(`{"listen_port":2080,"connections":3}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadGateway(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenPort != 2080 || cfg.Connections != 3 || cfg.ListenHost == "" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}

func TestLoadAgentRejectsBadDuration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.json")
	err := os.WriteFile(path, []byte(`{"target_connect_timeout":"nope"}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := LoadAgent(path); err == nil {
		t.Fatal("expected duration error")
	}
}

func TestGatewayRejectsBadLogLevel(t *testing.T) {
	cfg := DefaultGateway()
	cfg.LogLevel = "verbose"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected log level error")
	}
}
