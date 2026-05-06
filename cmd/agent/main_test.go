package main

import (
	"testing"
	"time"

	"arc/internal/config"
)

func TestNewAgent(t *testing.T) {
	cfg := config.DefaultAgent()
	cfg.Connections = 3

	a, err := newAgent(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.cfg.Connections != 3 || a.targetConnectTimeout != 10*time.Second {
		t.Fatalf("unexpected agent: %#v", a)
	}
}

func TestFormatPort(t *testing.T) {
	if got := formatPort(443); got != "443" {
		t.Fatalf("got %q", got)
	}
}
