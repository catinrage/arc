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
	if cfg.MaxStreams != 1 {
		t.Fatalf("bad max streams default: %d", cfg.MaxStreams)
	}
	if cfg.Transport != "mux" {
		t.Fatalf("bad transport default: %q", cfg.Transport)
	}
	if !cfg.UDPEnabled {
		t.Fatal("expected UDP to be enabled by default")
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

func TestGatewayRelayURLsOverrideSingleRelayURL(t *testing.T) {
	cfg := DefaultGateway()
	cfg.RelayURLs = []string{"wss://r1/client-raw", "wss://r2/client-raw"}
	got := cfg.EffectiveRelayURLs()
	if len(got) != 2 || got[0] != "wss://r1/client-raw" || got[1] != "wss://r2/client-raw" {
		t.Fatalf("unexpected relay urls: %#v", got)
	}
}

func TestGatewayAcceptsRelayURLsWithoutSingleRelayURL(t *testing.T) {
	cfg := DefaultGateway()
	cfg.RelayURL = ""
	cfg.RelayURLs = []string{"wss://r1/client-raw"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayRejectsEmptyRelayURLsEntry(t *testing.T) {
	cfg := DefaultGateway()
	cfg.RelayURLs = []string{"wss://r1/client-raw", ""}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected relay_urls error")
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

func TestGatewayRejectsBadMaxStreams(t *testing.T) {
	cfg := DefaultGateway()
	cfg.MaxStreams = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected max streams error")
	}
}

func TestGatewayRejectsBadTransport(t *testing.T) {
	cfg := DefaultGateway()
	cfg.Transport = "sse"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected transport error")
	}
}

func TestGatewayAdminRequiresCredentials(t *testing.T) {
	cfg := DefaultGateway()
	cfg.AdminListen = "127.0.0.1:8090"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing admin credentials error")
	}
	cfg.AdminUsername = "admin"
	cfg.AdminPassword = "secret"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSaveGateway(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	cfg := DefaultGateway()
	cfg.AdminListen = "127.0.0.1:8090"
	cfg.AdminUsername = "admin"
	cfg.AdminPassword = "secret"
	if err := SaveGateway(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGateway(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AdminListen != cfg.AdminListen || loaded.AdminUsername != "admin" {
		t.Fatalf("unexpected saved config: %#v", loaded)
	}
}
