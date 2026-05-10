package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"arc/internal/applog"
	"arc/internal/config"
	"arc/internal/mux"
	"arc/internal/protocol"
	"arc/internal/rawlane"
	"arc/internal/udprelay"
	"arc/internal/wsclient"
)

var (
	appVersion   = "dev"
	appCommit    = "none"
	appBuildDate = "unknown"
)

type agent struct {
	cfg                  config.Agent
	targetConnectTimeout time.Duration
	relayTimeout         time.Duration
	connectRamp          time.Duration
	reconnectMin         time.Duration
	reconnectMax         time.Duration
	relayURLs            []string

	ready  atomic.Int64
	active atomic.Int64
	total  atomic.Int64

	log *applog.Logger
}

func main() {
	configPath := flag.String("config", "", "path to agent JSON config")
	configPathShort := flag.String("c", "", "path to agent JSON config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("arc-agent version=%s commit=%s built=%s\n", appVersion, appCommit, appBuildDate)
		return
	}

	if *configPath == "" {
		*configPath = *configPathShort
	}

	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger, err := applog.New(cfg.LogLevel, cfg.LogFile)
	if err != nil {
		log.Fatalf("logger: %v", err)
	}
	defer logger.Close()

	a, err := newAgent(cfg, logger)
	if err != nil {
		logger.Fatalf("agent: %v", err)
	}
	a.run(context.Background())
}

func newAgent(cfg config.Agent, logger *applog.Logger) (*agent, error) {
	targetTimeout, err := config.Duration(cfg.TargetConnectTimeout)
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

	return &agent{
		cfg:                  cfg,
		targetConnectTimeout: targetTimeout,
		relayTimeout:         relayTimeout,
		connectRamp:          connectRamp,
		reconnectMin:         reconnectMin,
		reconnectMax:         reconnectMax,
		relayURLs:            cfg.EffectiveRelayURLs(),
		log:                  logger,
	}, nil
}

func (a *agent) run(ctx context.Context) {
	a.log.Infof("agent connecting relays=%d transport=%s sessions=%d log_file=%q log_level=%s", len(a.relayURLs), a.transport(), a.cfg.Connections, a.cfg.LogFile, a.cfg.LogLevel)
	for i := 0; i < a.cfg.Connections; i++ {
		if a.rawTransport() {
			go a.connectRawLoop(ctx, i)
		} else {
			go a.connectLoop(ctx, i)
		}
		sleepContext(ctx, a.connectRamp)
	}
	a.statsLoop(ctx)
}

func (a *agent) relayURLForSlot(idx int) string {
	if len(a.relayURLs) == 0 {
		return a.cfg.RelayURL
	}
	if idx < 0 {
		idx = -idx
	}
	return a.relayURLs[idx%len(a.relayURLs)]
}

func (a *agent) transport() string {
	if a.cfg.Transport == "" {
		return "mux"
	}
	return strings.ToLower(a.cfg.Transport)
}

func (a *agent) rawTransport() bool {
	return a.transport() == "raw"
}

func (a *agent) connectLoop(ctx context.Context, idx int) {
	backoff := a.reconnectMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, a.relayTimeout)
		relayURL := a.relayURLForSlot(idx)
		wire, err := wsclient.Dial(dialCtx, relayURL, wsclient.DialOptions{
			HandshakeTimeout: a.relayTimeout,
			InsecureTLS:      a.cfg.InsecureTLS,
			Logger:           a.log,
			SessionID:        idx,
		})
		cancel()
		if err != nil {
			a.log.Warnf("session %d relay connect failed url=%s: %v", idx, relayURL, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, a.reconnectMax)
			continue
		}

		if err := sendReady(wire); err != nil {
			_ = wire.Close()
			a.log.Warnf("session %d ready send failed url=%s: %v", idx, relayURL, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, a.reconnectMax)
			continue
		}

		session := mux.NewSessionWithLogger(wire, a.dialTarget, a.cfg.BufferSize, a.log)
		a.ready.Add(1)
		a.log.Infof("session %d ready url=%s", idx, relayURL)
		backoff = a.reconnectMin

		err = session.Serve(ctx)
		a.ready.Add(-1)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.log.Warnf("session %d closed: %v", idx, err)
		}
		sleepContext(ctx, backoff)
		backoff = growBackoff(backoff, a.reconnectMax)
	}
}

func (a *agent) connectRawLoop(ctx context.Context, idx int) {
	backoff := a.reconnectMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, a.relayTimeout)
		relayURL := a.relayURLForSlot(idx)
		wire, err := wsclient.Dial(dialCtx, relayURL, wsclient.DialOptions{
			HandshakeTimeout: a.relayTimeout,
			InsecureTLS:      a.cfg.InsecureTLS,
			Logger:           a.log,
			SessionID:        idx,
		})
		cancel()
		if err != nil {
			a.log.Warnf("raw lane %d relay connect failed url=%s: %v", idx, relayURL, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, a.reconnectMax)
			continue
		}

		if err := sendReady(wire); err != nil {
			_ = wire.Close()
			a.log.Warnf("raw lane %d ready send failed url=%s: %v", idx, relayURL, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, a.reconnectMax)
			continue
		}

		a.ready.Add(1)
		a.log.Infof("raw lane %d ready url=%s", idx, relayURL)
		backoff = a.reconnectMin

		err = rawlane.Serve(ctx, wire, a.dialTarget, a.cfg.BufferSize)
		a.ready.Add(-1)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			a.log.Warnf("raw lane %d closed: %v", idx, err)
		}
		sleepContext(ctx, backoff)
		backoff = growBackoff(backoff, a.reconnectMax)
	}
}

func sendReady(wire *wsclient.Conn) error {
	raw, err := protocol.EncodeMessage(protocol.Message{Type: protocol.TypeReady})
	if err != nil {
		return err
	}
	return wire.WriteMessage(raw)
}

func (a *agent) dialTarget(ctx context.Context, req protocol.OpenRequest) (io.ReadWriteCloser, error) {
	a.active.Add(1)
	a.total.Add(1)

	if req.Host == protocol.UDPAssociateHost {
		if !a.cfg.UDPEnabled {
			a.active.Add(-1)
			return nil, errors.New("udp is disabled")
		}
		assoc, err := udprelay.NewAssociation()
		if err != nil {
			a.active.Add(-1)
			a.log.Warnf("udp associate failed: %v", err)
			return nil, err
		}
		a.log.Debugf("udp associate ready active_targets=%d", a.active.Load())
		return &countedReadWriteCloser{rw: assoc, done: func() { a.active.Add(-1) }}, nil
	}

	dialer := net.Dialer{
		Timeout:   a.targetConnectTimeout,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(req.Host, formatPort(req.Port)))
	if err != nil {
		a.active.Add(-1)
		a.log.Warnf("target dial failed %s:%d: %v", req.Host, req.Port, err)
		return nil, err
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	a.log.Debugf("target connected %s:%d active_targets=%d", req.Host, req.Port, a.active.Load())
	return &countedConn{Conn: conn, done: func() { a.active.Add(-1) }}, nil
}

func (a *agent) statsLoop(ctx context.Context) {
	interval, err := config.Duration(a.cfg.StatsInterval)
	if err != nil || interval <= 0 {
		select {}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.log.Infof("stats sessions=%d/%d active_targets=%d total_targets=%d", a.ready.Load(), a.cfg.Connections, a.active.Load(), a.total.Load())
		}
	}
}

type countedConn struct {
	net.Conn
	once atomic.Bool
	done func()
}

func (c *countedConn) Close() error {
	if c.once.CompareAndSwap(false, true) {
		c.done()
	}
	return c.Conn.Close()
}

type countedReadWriteCloser struct {
	rw   io.ReadWriteCloser
	once atomic.Bool
	done func()
}

func (c *countedReadWriteCloser) Read(p []byte) (int, error) {
	return c.rw.Read(p)
}

func (c *countedReadWriteCloser) Write(p []byte) (int, error) {
	return c.rw.Write(p)
}

func (c *countedReadWriteCloser) Close() error {
	if c.once.CompareAndSwap(false, true) {
		c.done()
	}
	return c.rw.Close()
}

func formatPort(port uint16) string {
	return fmtUint(uint64(port))
}

func fmtUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
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
