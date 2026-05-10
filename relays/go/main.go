package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	legacyAgentPath  = "/agent"
	legacyClientPath = "/client"
	muxAgentPath     = "/agent-v2"
	muxClientPath    = "/client-v2"
	rawAgentPath     = "/agent-raw"
	rawClientPath    = "/client-raw"
)

type relayConfig struct {
	Host           string
	Port           int
	AgentQueueSize int
	PairTimeout    time.Duration
	PingInterval   time.Duration
	PingTimeout    time.Duration
	OpenTimeout    time.Duration
	CloseTimeout   time.Duration
	LogLevel       string
	LogFile        string
}

type relayServer struct {
	cfg relayConfig

	legacyPool *agentPool
	muxPool    *agentPool
	rawPool    *agentPool

	activePairs atomic.Int64
	totalPairs  atomic.Int64
}

type messageConn interface {
	ReadMessage() ([]byte, error)
	WriteMessage([]byte) error
	Close() error
}

type agentPool struct {
	maxSize int

	mu     sync.Mutex
	cond   *sync.Cond
	items  []messageConn
	closed map[messageConn]struct{}
}

func main() {
	cfg := loadRelayConfig()
	if err := configureLogging(cfg); err != nil {
		log.Fatalf("logging: %v", err)
	}

	srv := newRelayServer(cfg)
	go srv.statsLoop(context.Background())

	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	log.Printf("relay listening on %s queue=%d legacy=(%s,%s) mux=(%s,%s) raw=(%s,%s)", addr, cfg.AgentQueueSize, legacyAgentPath, legacyClientPath, muxAgentPath, muxClientPath, rawAgentPath, rawClientPath)
	server := &http.Server{
		Addr:              addr,
		Handler:           srv,
		ReadHeaderTimeout: cfg.OpenTimeout,
	}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("relay stopped: %v", err)
	}
}

func newRelayServer(cfg relayConfig) *relayServer {
	return &relayServer{
		cfg:        cfg,
		legacyPool: newAgentPool(cfg.AgentQueueSize),
		muxPool:    newAgentPool(cfg.AgentQueueSize),
		rawPool:    newAgentPool(cfg.AgentQueueSize),
	}
}

func (s *relayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isWebSocketUpgrade(r) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("relay alive\n"))
		return
	}

	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	go s.keepAlive(ws)
	s.handleWebSocket(r.Context(), r.URL.Path, ws)
}

func (s *relayServer) handleWebSocket(ctx context.Context, path string, ws *wsConn) {
	if pool := s.poolForAgentPath(path); pool != nil {
		if !pool.put(ws, s.cfg.PairTimeout) {
			log.Printf("agent queue full path=%s; rejecting", path)
			_ = ws.Close()
			return
		}
		log.Printf("agent ready path=%s queued=%d", path, pool.qsize())
		<-ws.done
		pool.remove(ws)
		return
	}

	poolName, pool := s.poolForClientPath(path)
	if pool != nil {
		agent, ok := pool.get(s.cfg.PairTimeout)
		if !ok {
			log.Printf("no agent available path=%s pool=%s", path, poolName)
			_ = ws.Close()
			return
		}
		s.pair(ctx, ws, agent, poolName, pool)
		return
	}

	_ = ws.WriteText([]byte("relay alive"))
	_ = ws.Close()
}

func (s *relayServer) poolForAgentPath(path string) *agentPool {
	switch path {
	case legacyAgentPath:
		return s.legacyPool
	case muxAgentPath:
		return s.muxPool
	case rawAgentPath:
		return s.rawPool
	default:
		return nil
	}
}

func (s *relayServer) poolForClientPath(path string) (string, *agentPool) {
	switch path {
	case legacyClientPath:
		return legacyAgentPath, s.legacyPool
	case muxClientPath:
		return muxAgentPath, s.muxPool
	case rawClientPath:
		return rawAgentPath, s.rawPool
	default:
		return "", nil
	}
}

func (s *relayServer) pair(ctx context.Context, client, agent messageConn, poolName string, pool *agentPool) {
	active := s.activePairs.Add(1)
	pairID := s.totalPairs.Add(1)
	log.Printf("paired #%d pool=%s active=%d queued_agents=%d", pairID, poolName, active, pool.qsize())

	done := make(chan string, 2)
	go pipe("client->agent", client, agent, done)
	go pipe("agent->client", agent, client, done)

	reason := "context closed"
	select {
	case <-ctx.Done():
	case reason = <-done:
	}

	_ = client.Close()
	_ = agent.Close()
	pool.markClosed(agent)
	s.activePairs.Add(-1)
	log.Printf("pair #%d closed pool=%s active=%d reason=%s", pairID, poolName, s.activePairs.Load(), reason)
}

func pipe(label string, src, dst messageConn, done chan<- string) {
	for {
		msg, err := src.ReadMessage()
		if err != nil {
			done <- label + " read: " + err.Error()
			return
		}
		if err := dst.WriteMessage(msg); err != nil {
			done <- label + " write: " + err.Error()
			return
		}
	}
}

func (s *relayServer) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf(
				"stats active_pairs=%d total_pairs=%d queued_legacy=%d queued_mux=%d queued_raw=%d",
				s.activePairs.Load(),
				s.totalPairs.Load(),
				s.legacyPool.qsize(),
				s.muxPool.qsize(),
				s.rawPool.qsize(),
			)
		}
	}
}

func (s *relayServer) keepAlive(ws *wsConn) {
	if s.cfg.PingInterval <= 0 {
		return
	}
	ticker := time.NewTicker(s.cfg.PingInterval)
	defer ticker.Stop()

	timeout := s.cfg.PingTimeout
	if timeout <= 0 {
		timeout = s.cfg.PingInterval
	}

	for {
		select {
		case <-ws.done:
			return
		case now := <-ticker.C:
			last := time.Unix(0, ws.lastRead.Load())
			if now.Sub(last) > s.cfg.PingInterval+timeout {
				_ = ws.Close()
				return
			}
			if err := ws.WritePing([]byte("arc")); err != nil {
				_ = ws.Close()
				return
			}
		}
	}
}

func newAgentPool(maxSize int) *agentPool {
	p := &agentPool{
		maxSize: maxSize,
		closed:  make(map[messageConn]struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *agentPool) put(conn messageConn, timeout time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pruneLocked()
	if len(p.items) >= p.maxSize {
		return false
	}
	p.items = append(p.items, conn)
	p.cond.Signal()
	_ = timeout
	return true
}

func (p *agentPool) get(timeout time.Duration) (messageConn, bool) {
	deadline := time.Now().Add(timeout)
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		p.pruneLocked()
		if len(p.items) > 0 {
			conn := p.items[0]
			p.items = p.items[1:]
			return conn, true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, false
		}
		timer := time.AfterFunc(remaining, func() {
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		})
		p.cond.Wait()
		timer.Stop()
	}
}

func (p *agentPool) remove(conn messageConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, item := range p.items {
		if item == conn {
			p.items = append(p.items[:i], p.items[i+1:]...)
			break
		}
	}
	p.markClosedLocked(conn)
	p.cond.Broadcast()
}

func (p *agentPool) markClosed(conn messageConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markClosedLocked(conn)
	p.cond.Broadcast()
}

func (p *agentPool) qsize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pruneLocked()
	return len(p.items)
}

func (p *agentPool) pruneLocked() {
	out := p.items[:0]
	for _, conn := range p.items {
		if _, closed := p.closed[conn]; !closed {
			out = append(out, conn)
		}
	}
	p.items = out
}

func (p *agentPool) markClosedLocked(conn messageConn) {
	p.closed[conn] = struct{}{}
}

func loadRelayConfig() relayConfig {
	return relayConfig{
		Host:           getenv("RELAY_HOST", "0.0.0.0"),
		Port:           relayPort(),
		AgentQueueSize: getenvInt("RELAY_AGENT_QUEUE_SIZE", 1024),
		PairTimeout:    getenvDurationSeconds("RELAY_PAIR_TIMEOUT", 15),
		PingInterval:   getenvDurationSeconds("RELAY_PING_INTERVAL", 20),
		PingTimeout:    getenvDurationSeconds("RELAY_PING_TIMEOUT", 20),
		OpenTimeout:    getenvDurationSeconds("RELAY_OPEN_TIMEOUT", 20),
		CloseTimeout:   getenvDurationSeconds("RELAY_CLOSE_TIMEOUT", 3),
		LogLevel:       getenv("RELAY_LOG_LEVEL", "INFO"),
		LogFile:        getenv("RELAY_LOG_FILE", ""),
	}
}

func relayPort() int {
	if value := os.Getenv("RELAY_PORT"); value != "" {
		return getenvInt("RELAY_PORT", 80)
	}
	return getenvInt("PORT", 80)
}

func configureLogging(cfg relayConfig) error {
	if cfg.LogFile == "" {
		return nil
	}
	file, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	log.SetOutput(file)
	return nil
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func getenvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvDurationSeconds(name string, fallback float64) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return time.Duration(fallback * float64(time.Second))
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return time.Duration(fallback * float64(time.Second))
	}
	return time.Duration(parsed * float64(time.Second))
}

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa
)

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
	mu   sync.Mutex

	messages chan []byte
	errMu    sync.Mutex
	err      error

	lastRead  atomic.Int64
	done      chan struct{}
	closeOnce sync.Once
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !isWebSocketUpgrade(r) {
		return nil, errors.New("missing websocket upgrade headers")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, errors.New("unsupported websocket version")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing websocket key")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("http hijacker unavailable")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	ws := &wsConn{
		conn:     conn,
		br:       rw.Reader,
		messages: make(chan []byte, 64),
		done:     make(chan struct{}),
	}
	ws.lastRead.Store(time.Now().UnixNano())
	go ws.readLoop()
	return ws, nil
}

func isWebSocketUpgrade(r *http.Request) bool {
	return headerContains(r.Header.Get("Connection"), "upgrade") && strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (c *wsConn) ReadMessage() ([]byte, error) {
	msg, ok := <-c.messages
	if ok {
		return msg, nil
	}
	if err := c.readError(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (c *wsConn) readLoop() {
	defer func() {
		close(c.messages)
		c.markDone()
	}()

	var message []byte
	var fragmented bool
	var currentOpcode byte

	for {
		opcode, fin, payload, err := c.readFrame()
		if err != nil {
			c.setReadError(err)
			return
		}
		c.lastRead.Store(time.Now().UnixNano())

		switch opcode {
		case opBinary, opText:
			if fragmented {
				c.setReadError(errors.New("new data frame before fragmented message completed"))
				return
			}
			if fin {
				if !c.enqueueMessage(payload) {
					return
				}
				continue
			}
			fragmented = true
			currentOpcode = opcode
			message = append(message[:0], payload...)
		case opContinuation:
			if !fragmented || currentOpcode == 0 {
				c.setReadError(errors.New("unexpected continuation frame"))
				return
			}
			message = append(message, payload...)
			if fin {
				if !c.enqueueMessage(message) {
					return
				}
				message = nil
				fragmented = false
				currentOpcode = 0
			}
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				c.setReadError(err)
				return
			}
		case opPong:
			continue
		case opClose:
			_ = c.writeFrame(opClose, nil)
			c.setReadError(io.EOF)
			return
		default:
			c.setReadError(fmt.Errorf("unsupported websocket opcode: %d", opcode))
			return
		}
	}
}

func (c *wsConn) WriteMessage(payload []byte) error {
	return c.writeFrame(opBinary, payload)
}

func (c *wsConn) WritePing(payload []byte) error {
	return c.writeFrame(opPing, payload)
}

func (c *wsConn) WriteText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

func (c *wsConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		_ = c.writeFrame(opClose, nil)
		err = c.conn.Close()
		c.markDone()
	})
	return err
}

func (c *wsConn) enqueueMessage(payload []byte) bool {
	msg := make([]byte, len(payload))
	copy(msg, payload)
	select {
	case c.messages <- msg:
		return true
	case <-c.done:
		return false
	}
}

func (c *wsConn) setReadError(err error) {
	c.errMu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errMu.Unlock()
}

func (c *wsConn) readError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *wsConn) markDone() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *wsConn) readFrame() (byte, bool, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		return 0, false, nil, err
	}

	fin := hdr[0]&0x80 != 0
	opcode := hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
		if length > math.MaxInt32 {
			return 0, false, nil, fmt.Errorf("websocket frame too large: %d", length)
		}
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return 0, false, nil, err
		}
	}

	payload := make([]byte, int(length))
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return 0, false, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return opcode, fin, payload, nil
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	length := len(payload)
	headerLen := 2
	switch {
	case length < 126:
		headerLen = 2
	case length <= math.MaxUint16:
		headerLen = 4
	default:
		headerLen = 10
	}

	frame := make([]byte, headerLen+length)
	frame[0] = 0x80 | opcode
	switch headerLen {
	case 2:
		frame[1] = byte(length)
	case 4:
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(length))
	case 10:
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(length))
	}
	copy(frame[headerLen:], payload)

	_, err := c.conn.Write(frame)
	return err
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func headerContains(value, token string) bool {
	for _, part := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
