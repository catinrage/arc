package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"arc/internal/config"
	"arc/internal/mux"
	"arc/internal/protocol"
	"arc/internal/socks"
	"arc/internal/wsclient"
)

var (
	appVersion   = "dev"
	appCommit    = "none"
	appBuildDate = "unknown"
)

type sessionSlot struct {
	mu      sync.RWMutex
	session *mux.Session
}

type gateway struct {
	cfg          config.Gateway
	openTimeout  time.Duration
	reconnectMin time.Duration
	reconnectMax time.Duration

	slots []sessionSlot
	next  atomic.Uint64

	active atomic.Int64
	total  atomic.Int64
}

func main() {
	configPath := flag.String("config", "", "path to gateway JSON config")
	configPathShort := flag.String("c", "", "path to gateway JSON config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("arc-gateway version=%s commit=%s built=%s\n", appVersion, appCommit, appBuildDate)
		return
	}

	if *configPath == "" {
		*configPath = *configPathShort
	}

	cfg, err := config.LoadGateway(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	gw, err := newGateway(cfg)
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}
	if err := gw.run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func newGateway(cfg config.Gateway) (*gateway, error) {
	openTimeout, err := config.Duration(cfg.OpenTimeout)
	if err != nil {
		return nil, err
	}
	reconnectMin, err := config.Duration(cfg.ReconnectInitial)
	if err != nil {
		return nil, err
	}
	reconnectMax, err := config.Duration(cfg.ReconnectMax)
	if err != nil {
		return nil, err
	}

	return &gateway{
		cfg:          cfg,
		openTimeout:  openTimeout,
		reconnectMin: reconnectMin,
		reconnectMax: reconnectMax,
		slots:        make([]sessionSlot, cfg.Connections),
	}, nil
}

func (g *gateway) run(ctx context.Context) error {
	for i := range g.slots {
		go g.connectLoop(ctx, i)
	}
	go g.statsLoop(ctx)

	addr := fmt.Sprintf("%s:%d", g.cfg.ListenHost, g.cfg.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("gateway SOCKS5 listening on %s, relay=%s, sessions=%d", addr, g.cfg.RelayURL, g.cfg.Connections)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept: %v", err)
			continue
		}
		go g.handleConn(ctx, conn)
	}
}

func (g *gateway) connectLoop(ctx context.Context, idx int) {
	backoff := g.reconnectMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, g.openTimeout)
		wire, err := wsclient.Dial(dialCtx, g.cfg.RelayURL, wsclient.DialOptions{
			HandshakeTimeout: g.openTimeout,
			InsecureTLS:      g.cfg.InsecureTLS,
		})
		cancel()
		if err != nil {
			log.Printf("session %d relay connect failed: %v", idx, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, g.reconnectMax)
			continue
		}

		session := mux.NewSession(wire, nil, g.cfg.BufferSize)
		g.setSession(idx, session)
		log.Printf("session %d connected", idx)
		backoff = g.reconnectMin

		err = session.Serve(ctx)
		g.clearSession(idx, session)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("session %d closed: %v", idx, err)
		}
		sleepContext(ctx, backoff)
		backoff = growBackoff(backoff, g.reconnectMax)
	}
}

func (g *gateway) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	g.active.Add(1)
	connID := g.total.Add(1)
	defer g.active.Add(-1)

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	req, err := socks.ReadRequest(conn)
	if err != nil {
		_ = socks.WriteFailure(conn, 0x05)
		log.Printf("#%d SOCKS error: %v", connID, err)
		return
	}

	openCtx, cancel := context.WithTimeout(ctx, g.openTimeout)
	stream, err := g.open(openCtx, protocol.OpenRequest{Host: req.Host, Port: req.Port})
	cancel()
	if err != nil {
		_ = socks.WriteFailure(conn, 0x05)
		log.Printf("#%d open %s:%d failed: %v", connID, req.Host, req.Port, err)
		return
	}

	if err := socks.WriteSuccess(conn); err != nil {
		_ = stream.Close()
		return
	}

	log.Printf("#%d connected %s:%d active=%d", connID, req.Host, req.Port, g.active.Load())
	go copyAndClose(stream, conn, g.cfg.BufferSize)
	copyAndClose(conn, stream, g.cfg.BufferSize)
}

func (g *gateway) open(ctx context.Context, req protocol.OpenRequest) (*mux.Stream, error) {
	if len(g.slots) == 0 {
		return nil, errors.New("no relay sessions configured")
	}

	var lastErr error
	for {
		start := int(g.next.Add(1))
		for i := range g.slots {
			idx := (start + i) % len(g.slots)
			session := g.getSession(idx)
			if session == nil {
				continue
			}
			stream, err := session.Open(ctx, req)
			if err == nil {
				return stream, nil
			}
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (g *gateway) setSession(idx int, session *mux.Session) {
	g.slots[idx].mu.Lock()
	g.slots[idx].session = session
	g.slots[idx].mu.Unlock()
}

func (g *gateway) clearSession(idx int, session *mux.Session) {
	g.slots[idx].mu.Lock()
	if g.slots[idx].session == session {
		g.slots[idx].session = nil
	}
	g.slots[idx].mu.Unlock()
}

func (g *gateway) getSession(idx int) *mux.Session {
	g.slots[idx].mu.RLock()
	session := g.slots[idx].session
	g.slots[idx].mu.RUnlock()
	return session
}

func (g *gateway) statsLoop(ctx context.Context) {
	interval, err := config.Duration(g.cfg.StatsInterval)
	if err != nil || interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ready := 0
			streams := int64(0)
			for i := range g.slots {
				if session := g.getSession(i); session != nil {
					ready++
					streams += session.ActiveStreams()
				}
			}
			log.Printf("stats active=%d total=%d sessions=%d/%d streams=%d", g.active.Load(), g.total.Load(), ready, len(g.slots), streams)
		}
	}
}

func copyAndClose(dst io.WriteCloser, src io.ReadCloser, bufferSize int) {
	buf := make([]byte, bufferSize)
	_, _ = io.CopyBuffer(dst, src, buf)
	_ = dst.Close()
	_ = src.Close()
}

func growBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		current = 250 * time.Millisecond
	}
	current *= 2
	if current > max {
		return max
	}
	return current
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
