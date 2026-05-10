package main

import (
	"io"
	"net"
	"testing"
	"time"

	"arc/internal/config"
	"arc/internal/mux"
	"arc/internal/rawlane"
)

func TestNewGateway(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.Connections = 2

	gw, err := newGateway(cfg, "", nil)
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

	gw, err := newGateway(cfg, "", nil)
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

	gw, err := newGateway(cfg, "", nil)
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

func TestReserveRawLaneConsumesReadyLane(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.Transport = "raw"
	cfg.Connections = 1

	gw, err := newGateway(cfg, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	lane := rawlane.NewPumpedWire(&eofWire{}, 1)
	gw.setRawLane(0, lane)

	_, got, release, ok := gw.reserveRawLane()
	if !ok {
		t.Fatal("expected raw lane reservation")
	}
	if got != lane {
		t.Fatal("reserved wrong lane")
	}
	if _, _, _, ok := gw.reserveRawLane(); ok {
		t.Fatal("expected raw lane to be consumed")
	}
	release()
}

type eofWire struct{}

func (eofWire) ReadMessage() ([]byte, error) { return nil, io.EOF }

func (eofWire) WriteMessage([]byte) error { return nil }
func (eofWire) Close() error              { return nil }

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

func TestUDPBindAddrUsesListenHost(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.ListenHost = "127.0.0.1"
	gw, err := newGateway(cfg, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := gw.udpBindAddr().IP.String(); got != "127.0.0.1" {
		t.Fatalf("got %s", got)
	}
}

func TestUDPReplyAddrUsesControlAddressForUnspecifiedBind(t *testing.T) {
	got := udpReplyAddr(&net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 53000}, &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 1080})
	if got.IP.String() != "192.0.2.10" || got.Port != 53000 {
		t.Fatalf("unexpected reply addr: %s", got)
	}
}
