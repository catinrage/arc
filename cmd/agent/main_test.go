package main

import (
	"context"
	"testing"
	"time"

	"arc/internal/config"
	"arc/internal/protocol"
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

func TestAgentRelayURLDistribution(t *testing.T) {
	cfg := config.DefaultAgent()
	cfg.RelayURLs = []string{"wss://r1/agent-raw", "wss://r2/agent-raw"}
	cfg.Connections = 4

	a, err := newAgent(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for idx, want := range []string{"wss://r1/agent-raw", "wss://r2/agent-raw", "wss://r1/agent-raw", "wss://r2/agent-raw"} {
		if got := a.relayURLForSlot(idx); got != want {
			t.Fatalf("slot %d got %q want %q", idx, got, want)
		}
	}
}

func TestFormatPort(t *testing.T) {
	if got := formatPort(443); got != "443" {
		t.Fatalf("got %q", got)
	}
}

func TestDialTargetRejectsUDPWhenDisabled(t *testing.T) {
	cfg := config.DefaultAgent()
	cfg.UDPEnabled = false
	a, err := newAgent(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.dialTarget(context.Background(), protocol.OpenRequest{Host: protocol.UDPAssociateHost, Port: 1})
	if err == nil || err.Error() != "udp is disabled" {
		t.Fatalf("expected disabled udp error, got %v", err)
	}
	if a.active.Load() != 0 {
		t.Fatalf("active target leak: %d", a.active.Load())
	}
}
