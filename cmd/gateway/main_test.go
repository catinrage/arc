package main

import (
	"testing"
	"time"

	"arc/internal/config"
	"arc/internal/mux"
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

func TestReserveSessionHonorsMaxStreams(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.Connections = 2
	cfg.MaxStreams = 1

	gw, err := newGateway(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw.setSession(0, mux.NewSession(nil, nil, 0))
	gw.setSession(1, mux.NewSession(nil, nil, 0))

	idx1, _, release1, ok := gw.reserveSession()
	if !ok {
		t.Fatal("expected first reservation")
	}
	idx2, _, release2, ok := gw.reserveSession()
	if !ok {
		t.Fatal("expected second reservation")
	}
	if idx1 == idx2 {
		t.Fatalf("expected reservations on different sessions, got %d and %d", idx1, idx2)
	}
	if _, _, _, ok := gw.reserveSession(); ok {
		t.Fatal("expected all sessions to be at capacity")
	}

	release1()
	if _, _, release3, ok := gw.reserveSession(); !ok {
		t.Fatal("expected reservation after release")
	} else {
		release3()
	}
	release2()
}

func TestReservationReleaseDoesNotAffectReconnectedSlot(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.Connections = 1
	cfg.MaxStreams = 1

	gw, err := newGateway(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw.setSession(0, mux.NewSession(nil, nil, 0))
	_, _, oldRelease, ok := gw.reserveSession()
	if !ok {
		t.Fatal("expected old reservation")
	}

	gw.clearSession(0, gw.getSession(0))
	gw.setSession(0, mux.NewSession(nil, nil, 0))
	_, _, newRelease, ok := gw.reserveSession()
	if !ok {
		t.Fatal("expected new reservation")
	}

	oldRelease()
	if _, _, _, ok := gw.reserveSession(); ok {
		t.Fatal("old release decremented the new session reservation")
	}
	newRelease()
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
