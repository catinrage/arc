package main

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"arc/internal/wsclient"
)

type fakeConn struct {
	readCh   chan []byte
	writeCh  chan []byte
	closeCh  chan struct{}
	closeNow chan struct{}
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		readCh:   make(chan []byte, 4),
		writeCh:  make(chan []byte, 4),
		closeCh:  make(chan struct{}),
		closeNow: make(chan struct{}),
	}
}

func (c *fakeConn) ReadMessage() ([]byte, error) {
	select {
	case msg := <-c.readCh:
		return msg, nil
	case <-c.closeNow:
		return nil, io.EOF
	}
}

func (c *fakeConn) WriteMessage(msg []byte) error {
	select {
	case c.writeCh <- msg:
		return nil
	case <-c.closeNow:
		return io.EOF
	}
}

func (c *fakeConn) Close() error {
	select {
	case <-c.closeNow:
	default:
		close(c.closeNow)
		close(c.closeCh)
	}
	return nil
}

func TestPoolForPaths(t *testing.T) {
	srv := newRelayServer(relayConfig{AgentQueueSize: 8, PairTimeout: time.Second})
	if srv.poolForAgentPath(muxAgentPath) != srv.muxPool {
		t.Fatal("mux agent path did not resolve to mux pool")
	}
	name, pool := srv.poolForClientPath(muxClientPath)
	if name != muxAgentPath || pool != srv.muxPool {
		t.Fatalf("unexpected mux client pool: %q %#v", name, pool)
	}
	name, pool = srv.poolForClientPath(legacyClientPath)
	if name != legacyAgentPath || pool != srv.legacyPool {
		t.Fatalf("unexpected legacy client pool: %q %#v", name, pool)
	}
}

func TestHTTPHealthResponse(t *testing.T) {
	srv := newRelayServer(relayConfig{AgentQueueSize: 8, PairTimeout: time.Second})
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "relay alive") {
		t.Fatalf("unexpected health response: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestRelayPortFallsBackToPlatformPort(t *testing.T) {
	t.Setenv("RELAY_PORT", "")
	t.Setenv("PORT", "8080")
	if got := relayPort(); got != 8080 {
		t.Fatalf("got %d", got)
	}
	t.Setenv("RELAY_PORT", "9090")
	if got := relayPort(); got != 9090 {
		t.Fatalf("got %d", got)
	}
}

func TestRelayConfigIncludesPingDefaults(t *testing.T) {
	t.Setenv("RELAY_PING_INTERVAL", "")
	t.Setenv("RELAY_PING_TIMEOUT", "")
	cfg := loadRelayConfig()
	if cfg.PingInterval != 20*time.Second || cfg.PingTimeout != 20*time.Second {
		t.Fatalf("unexpected ping defaults: interval=%s timeout=%s", cfg.PingInterval, cfg.PingTimeout)
	}
}

func TestAgentPoolSkipsClosed(t *testing.T) {
	pool := newAgentPool(4)
	closed := newFakeConn()
	live := newFakeConn()
	if !pool.put(closed, time.Second) || !pool.put(live, time.Second) {
		t.Fatal("put failed")
	}
	pool.markClosed(closed)
	got, ok := pool.get(time.Second)
	if !ok || got != live {
		t.Fatalf("expected live conn, got %#v ok=%v", got, ok)
	}
}

func TestAgentPoolCapacity(t *testing.T) {
	pool := newAgentPool(1)
	if !pool.put(newFakeConn(), time.Second) {
		t.Fatal("first put failed")
	}
	if pool.put(newFakeConn(), time.Second) {
		t.Fatal("expected full pool")
	}
}

func TestPairCopiesAndClosesBoth(t *testing.T) {
	srv := newRelayServer(relayConfig{AgentQueueSize: 8, PairTimeout: time.Second})
	client := newFakeConn()
	agent := newFakeConn()
	client.readCh <- []byte("hello")

	done := make(chan struct{})
	go func() {
		srv.pair(context.Background(), client, agent, muxAgentPath, srv.muxPool)
		close(done)
	}()

	select {
	case got := <-agent.writeCh:
		if string(got) != "hello" {
			t.Fatalf("unexpected copied message: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for copied message")
	}
	_ = client.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pair did not close")
	}
	select {
	case <-agent.closeCh:
	default:
		t.Fatal("agent was not closed")
	}
}

func TestRelayWebSocketExchange(t *testing.T) {
	srv := newRelayServer(relayConfig{AgentQueueSize: 8, PairTimeout: time.Second, OpenTimeout: time.Second})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("tcp listener unavailable: %v", err)
	}
	httpServer := httptest.NewUnstartedServer(srv)
	httpServer.Listener = listener
	httpServer.Start()
	defer httpServer.Close()

	baseURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	agent, err := wsclient.Dial(context.Background(), baseURL+muxAgentPath, wsclient.DialOptions{HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()

	client, err := wsclient.Dial(context.Background(), baseURL+muxClientPath, wsclient.DialOptions{HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.WriteMessage([]byte("client-to-agent")); err != nil {
		t.Fatal(err)
	}
	got, err := agent.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "client-to-agent" {
		t.Fatalf("unexpected agent message: %q", got)
	}

	if err := agent.WriteMessage([]byte("agent-to-client")); err != nil {
		t.Fatal(err)
	}
	got, err = client.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "agent-to-client" {
		t.Fatalf("unexpected client message: %q", got)
	}
}
