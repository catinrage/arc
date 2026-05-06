package main

import (
	"testing"
	"time"

	"arc/internal/config"
)

func TestNewGateway(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.Connections = 2

	gw, err := newGateway(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(gw.slots) != 2 || gw.openTimeout != 10*time.Second {
		t.Fatalf("unexpected gateway: %#v", gw)
	}
}

func TestGrowBackoff(t *testing.T) {
	got := growBackoff(250*time.Millisecond, time.Second)
	if got != 500*time.Millisecond {
		t.Fatalf("got %s", got)
	}
	got = growBackoff(time.Second, time.Second)
	if got != time.Second {
		t.Fatalf("got %s", got)
	}
}
