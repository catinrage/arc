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

	"arc/internal/applog"
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
	active  int
	gen     uint64
}

type gateway struct {
	cfg          config.Gateway
	openTimeout  time.Duration
	relayTimeout time.Duration
	connectRamp  time.Duration
	reconnectMin time.Duration
	reconnectMax time.Duration

	slots []sessionSlot
	next  atomic.Uint64

	active atomic.Int64
	total  atomic.Int64
	burst  atomic.Int64

	burstTokens chan struct{}

	log *applog.Logger
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

	logger, err := applog.New(cfg.LogLevel, cfg.LogFile)
	if err != nil {
		log.Fatalf("logger: %v", err)
	}
	defer logger.Close()

	gw, err := newGateway(cfg, logger)
	if err != nil {
		logger.Fatalf("gateway: %v", err)
	}
	if err := gw.run(context.Background()); err != nil {
		logger.Fatalf("%v", err)
	}
}

func newGateway(cfg config.Gateway, logger *applog.Logger) (*gateway, error) {
	openTimeout, err := config.Duration(cfg.OpenTimeout)
	if err != nil {
		return nil, err
	}
	relayTimeout, err := config.Duration(cfg.RelayHandshake)
	if err != nil {
		return nil, err
	}
	connectRamp, err := config.Duration(cfg.ConnectRamp)
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
		relayTimeout: relayTimeout,
		connectRamp:  connectRamp,
		reconnectMin: reconnectMin,
		reconnectMax: reconnectMax,
		slots:        make([]sessionSlot, cfg.Connections),
		burstTokens:  make(chan struct{}, cfg.BurstConnections),
		log:          logger,
	}, nil
}

func (g *gateway) run(ctx context.Context) error {
	for i := range g.slots {
		go g.connectLoop(ctx, i)
		sleepContext(ctx, g.connectRamp)
	}
	go g.statsLoop(ctx)

	addr := fmt.Sprintf("%s:%d", g.cfg.ListenHost, g.cfg.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	g.log.Infof("gateway SOCKS5 listening on %s, relay=%s, sessions=%d burst=%d max_streams_per_session=%d log_file=%q log_level=%s", addr, g.cfg.RelayURL, g.cfg.Connections, g.cfg.BurstConnections, g.cfg.MaxStreams, g.cfg.LogFile, g.cfg.LogLevel)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			g.log.Warnf("accept: %v", err)
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

		dialCtx, cancel := context.WithTimeout(ctx, g.relayTimeout)
		wire, err := wsclient.Dial(dialCtx, g.cfg.RelayURL, wsclient.DialOptions{
			HandshakeTimeout: g.relayTimeout,
			InsecureTLS:      g.cfg.InsecureTLS,
			Logger:           g.log,
			SessionID:        idx,
		})
		if err != nil {
			cancel()
			g.log.Warnf("session %d relay connect failed: %v", idx, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, g.reconnectMax)
			continue
		}

		if err := waitReady(dialCtx, wire); err != nil {
			cancel()
			_ = wire.Close()
			g.log.Warnf("session %d relay pair failed: %v", idx, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, g.reconnectMax)
			continue
		}
		cancel()

		session := mux.NewSessionWithLogger(wire, nil, g.cfg.BufferSize, g.log)
		g.setSession(idx, session)
		g.log.Infof("session %d connected and paired", idx)
		backoff = g.reconnectMin

		err = session.Serve(ctx)
		g.clearSession(idx, session)
		if err != nil && !errors.Is(err, context.Canceled) {
			g.log.Warnf("session %d closed: %v", idx, err)
		}
		sleepContext(ctx, backoff)
		backoff = growBackoff(backoff, g.reconnectMax)
	}
}

func waitReady(ctx context.Context, wire *wsclient.Conn) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = wire.SetDeadline(deadline)
	}
	raw, err := wire.ReadMessage()
	if err != nil {
		return fmt.Errorf("ready read: %w", err)
	}
	_ = wire.SetDeadline(time.Time{})

	msg, err := protocol.DecodeMessage(raw)
	if err != nil {
		return fmt.Errorf("ready decode: %w", err)
	}
	if msg.Type != protocol.TypeReady {
		return fmt.Errorf("unexpected first frame type %d", msg.Type)
	}
	return nil
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
		_ = socks.WriteFailure(conn, socks.ReplyCodeForError(err))
		g.log.Warnf("#%d SOCKS error from %s: %v", connID, conn.RemoteAddr(), err)
		return
	}

	g.log.Debugf("#%d SOCKS request %s:%d active=%d", connID, req.Host, req.Port, g.active.Load())
	openCtx, cancel := context.WithTimeout(ctx, g.openTimeout)
	stream, release, err := g.open(openCtx, protocol.OpenRequest{Host: req.Host, Port: req.Port})
	cancel()
	if err != nil {
		_ = socks.WriteFailure(conn, 0x05)
		g.log.Warnf("#%d open %s:%d failed: %v", connID, req.Host, req.Port, err)
		return
	}
	defer release()

	if err := socks.WriteSuccess(conn); err != nil {
		_ = stream.Close()
		return
	}

	g.log.Infof("#%d connected %s:%d active=%d", connID, req.Host, req.Port, g.active.Load())
	go copyAndClose(stream, conn, g.cfg.BufferSize)
	copyAndClose(conn, stream, g.cfg.BufferSize)
}

func (g *gateway) open(ctx context.Context, req protocol.OpenRequest) (*mux.Stream, func(), error) {
	if len(g.slots) == 0 {
		return nil, nil, errors.New("no relay sessions configured")
	}

	var lastErr error
	for {
		idx, session, release, ok := g.reserveSession()
		if ok {
			stream, err := session.Open(ctx, req)
			if err == nil {
				return stream, release, nil
			}
			release()
			g.log.Debugf("session %d open failed while reserved: %v", idx, err)
			lastErr = err
		}

		stream, release, err := g.openBurst(ctx, req)
		if err == nil {
			return stream, release, nil
		}
		if err != errNoBurstCapacity {
			lastErr = err
			g.log.Debugf("burst open failed: %v", err)
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

var errNoBurstCapacity = errors.New("no burst capacity")

func (g *gateway) openBurst(ctx context.Context, req protocol.OpenRequest) (*mux.Stream, func(), error) {
	if g.cfg.BurstConnections <= 0 {
		return nil, nil, errNoBurstCapacity
	}

	select {
	case g.burstTokens <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
		return nil, nil, errNoBurstCapacity
	}

	released := false
	releaseToken := func() {
		if released {
			return
		}
		released = true
		<-g.burstTokens
		g.burst.Add(-1)
	}
	g.burst.Add(1)

	wire, err := wsclient.Dial(ctx, g.cfg.RelayURL, wsclient.DialOptions{
		HandshakeTimeout: g.relayTimeout,
		InsecureTLS:      g.cfg.InsecureTLS,
		Logger:           g.log,
		SessionID:        -1,
	})
	if err != nil {
		releaseToken()
		return nil, nil, err
	}

	if err := waitReady(ctx, wire); err != nil {
		_ = wire.Close()
		releaseToken()
		return nil, nil, err
	}

	session := mux.NewSessionWithLogger(wire, nil, g.cfg.BufferSize, g.log)
	serveDone := make(chan struct{})
	go func() {
		_ = session.Serve(context.Background())
		close(serveDone)
	}()

	stream, err := session.Open(ctx, req)
	if err != nil {
		_ = session.Close()
		<-serveDone
		releaseToken()
		return nil, nil, err
	}

	release := func() {
		_ = stream.Close()
		_ = session.Close()
		<-serveDone
		releaseToken()
	}
	return stream, release, nil
}

func (g *gateway) reserveSession() (int, *mux.Session, func(), bool) {
	if len(g.slots) == 0 {
		return 0, nil, nil, false
	}

	start := int(g.next.Add(1))
	bestIdx := -1
	bestActive := int(^uint(0) >> 1)

	for i := range g.slots {
		idx := (start + i) % len(g.slots)
		slot := &g.slots[idx]
		slot.mu.RLock()
		session := slot.session
		active := slot.active
		slot.mu.RUnlock()
		if session == nil || active >= g.cfg.MaxStreams {
			continue
		}
		if active < bestActive {
			bestIdx = idx
			bestActive = active
			if active == 0 {
				break
			}
		}
	}

	if bestIdx < 0 {
		return 0, nil, nil, false
	}

	slot := &g.slots[bestIdx]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.session == nil || slot.active >= g.cfg.MaxStreams {
		return 0, nil, nil, false
	}
	slot.active++
	session := slot.session
	gen := slot.gen
	released := false
	release := func() {
		slot.mu.Lock()
		defer slot.mu.Unlock()
		if released {
			return
		}
		released = true
		if slot.gen == gen && slot.active > 0 {
			slot.active--
		}
	}
	return bestIdx, session, release, true
}

func (g *gateway) setSession(idx int, session *mux.Session) {
	g.slots[idx].mu.Lock()
	g.slots[idx].session = session
	g.slots[idx].active = 0
	g.slots[idx].gen++
	g.slots[idx].mu.Unlock()
}

func (g *gateway) clearSession(idx int, session *mux.Session) {
	g.slots[idx].mu.Lock()
	if g.slots[idx].session == session {
		g.slots[idx].session = nil
		g.slots[idx].active = 0
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
				g.slots[i].mu.RLock()
				session := g.slots[i].session
				active := g.slots[i].active
				g.slots[i].mu.RUnlock()
				if session != nil {
					ready++
					streams += int64(active)
				}
			}
			g.log.Infof("stats active=%d total=%d sessions=%d/%d streams=%d burst=%d/%d max_streams_per_session=%d", g.active.Load(), g.total.Load(), ready, len(g.slots), streams, g.burst.Load(), g.cfg.BurstConnections, g.cfg.MaxStreams)
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
