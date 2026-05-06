package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"arc/internal/config"
	"arc/internal/mux"
	"arc/internal/protocol"
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
	reconnectMin         time.Duration
	reconnectMax         time.Duration

	ready  atomic.Int64
	active atomic.Int64
	total  atomic.Int64
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

	a, err := newAgent(cfg)
	if err != nil {
		log.Fatalf("agent: %v", err)
	}
	a.run(context.Background())
}

func newAgent(cfg config.Agent) (*agent, error) {
	targetTimeout, err := config.Duration(cfg.TargetConnectTimeout)
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
		reconnectMin:         reconnectMin,
		reconnectMax:         reconnectMax,
	}, nil
}

func (a *agent) run(ctx context.Context) {
	log.Printf("agent connecting relay=%s sessions=%d", a.cfg.RelayURL, a.cfg.Connections)
	for i := 0; i < a.cfg.Connections; i++ {
		go a.connectLoop(ctx, i)
		time.Sleep(100 * time.Millisecond)
	}
	a.statsLoop(ctx)
}

func (a *agent) connectLoop(ctx context.Context, idx int) {
	backoff := a.reconnectMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, a.targetConnectTimeout)
		wire, err := wsclient.Dial(dialCtx, a.cfg.RelayURL, wsclient.DialOptions{
			HandshakeTimeout: a.targetConnectTimeout,
			InsecureTLS:      a.cfg.InsecureTLS,
		})
		cancel()
		if err != nil {
			log.Printf("session %d relay connect failed: %v", idx, err)
			sleepContext(ctx, backoff)
			backoff = growBackoff(backoff, a.reconnectMax)
			continue
		}

		session := mux.NewSession(wire, a.dialTarget, a.cfg.BufferSize)
		a.ready.Add(1)
		log.Printf("session %d ready", idx)
		backoff = a.reconnectMin

		err = session.Serve(ctx)
		a.ready.Add(-1)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("session %d closed: %v", idx, err)
		}
		sleepContext(ctx, backoff)
		backoff = growBackoff(backoff, a.reconnectMax)
	}
}

func (a *agent) dialTarget(ctx context.Context, req protocol.OpenRequest) (io.ReadWriteCloser, error) {
	a.active.Add(1)
	a.total.Add(1)

	dialer := net.Dialer{
		Timeout:   a.targetConnectTimeout,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(req.Host, formatPort(req.Port)))
	if err != nil {
		a.active.Add(-1)
		return nil, err
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

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
			log.Printf("stats sessions=%d/%d active_targets=%d total_targets=%d", a.ready.Load(), a.cfg.Connections, a.active.Load(), a.total.Load())
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
