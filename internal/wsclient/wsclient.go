package wsclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa
)

type DialOptions struct {
	HandshakeTimeout time.Duration
	InsecureTLS      bool
	Logger           Logger
	SessionID        int
}

type Logger interface {
	Debugf(string, ...any)
}

type Conn struct {
	conn net.Conn
	br   *bufio.Reader
	mu   sync.Mutex
}

func Dial(ctx context.Context, rawURL string, opts DialOptions) (*Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, fmt.Errorf("unsupported websocket scheme: %s", u.Scheme)
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	timeout := opts.HandshakeTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	dialer := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	debugf(opts.Logger, "session %d websocket tcp dial start host=%s timeout=%s", opts.SessionID, host, timeout)
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %s: %w", host, err)
	}
	debugf(opts.Logger, "session %d websocket tcp dial ok local=%s remote=%s", opts.SessionID, conn.LocalAddr(), conn.RemoteAddr())

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	if u.Scheme == "wss" {
		serverName := u.Hostname()
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: opts.InsecureTLS, //nolint:gosec
			MinVersion:         tls.VersionTLS12,
			NextProtos:         []string{"http/1.1"},
		})
		debugf(opts.Logger, "session %d websocket tls handshake start server_name=%s", opts.SessionID, serverName)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tls handshake %s: %w", serverName, err)
		}
		state := tlsConn.ConnectionState()
		debugf(opts.Logger, "session %d websocket tls handshake ok version=0x%x cipher=0x%x alpn=%q", opts.SessionID, state.Version, state.CipherSuite, state.NegotiatedProtocol)
		conn = tlsConn
	}

	key, err := makeKey()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nUser-Agent: arc/%s\r\n\r\n",
		path,
		u.Host,
		key,
		"1",
	)
	debugf(opts.Logger, "session %d websocket upgrade write start path=%s host=%s", opts.SessionID, path, u.Host)
	if _, err := io.WriteString(conn, req); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket upgrade write: %w", err)
	}
	debugf(opts.Logger, "session %d websocket upgrade read start", opts.SessionID)

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket upgrade read: %w", err)
	}
	defer resp.Body.Close()
	debugf(opts.Logger, "session %d websocket upgrade response status=%s", opts.SessionID, resp.Status)

	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %s", resp.Status)
	}
	if !headerContains(resp.Header.Get("Upgrade"), "websocket") {
		_ = conn.Close()
		return nil, errors.New("missing websocket upgrade header")
	}
	if !headerContains(resp.Header.Get("Connection"), "upgrade") {
		_ = conn.Close()
		return nil, errors.New("missing websocket connection header")
	}
	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), acceptKey(key); got != want {
		_ = conn.Close()
		return nil, errors.New("bad websocket accept key")
	}

	_ = conn.SetDeadline(time.Time{})
	debugf(opts.Logger, "session %d websocket connected", opts.SessionID)
	return &Conn{conn: conn, br: br}, nil
}

func (c *Conn) ReadMessage() ([]byte, error) {
	var message bytes.Buffer
	var fragmented bool
	var currentOpcode byte

	for {
		opcode, fin, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}

		switch opcode {
		case opBinary, opText:
			if fragmented {
				return nil, errors.New("new data frame before fragmented message completed")
			}
			if fin {
				return payload, nil
			}
			fragmented = true
			currentOpcode = opcode
			message.Write(payload)
		case opContinuation:
			if !fragmented || currentOpcode == 0 {
				return nil, errors.New("unexpected continuation frame")
			}
			message.Write(payload)
			if fin {
				return message.Bytes(), nil
			}
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
		case opPong:
			continue
		case opClose:
			_ = c.writeFrame(opClose, payload)
			return nil, io.EOF
		default:
			return nil, fmt.Errorf("unsupported websocket opcode: %d", opcode)
		}
	}
}

func (c *Conn) WriteMessage(payload []byte) error {
	return c.writeFrame(opBinary, payload)
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Conn) Close() error {
	_ = c.writeFrame(opClose, nil)
	return c.conn.Close()
}

func (c *Conn) readFrame() (opcode byte, fin bool, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(c.br, hdr[:]); err != nil {
		return 0, false, nil, err
	}

	fin = hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)

	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
		if length > math.MaxInt32 {
			return 0, false, nil, fmt.Errorf("websocket frame too large: %d", length)
		}
	}

	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return 0, false, nil, err
		}
	}

	payload = make([]byte, int(length))
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return 0, false, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return opcode, fin, payload, nil
}

func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hdr []byte
	length := len(payload)
	switch {
	case length < 126:
		hdr = []byte{0x80 | opcode, 0x80 | byte(length)}
	case length <= math.MaxUint16:
		hdr = make([]byte, 4)
		hdr[0] = 0x80 | opcode
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(length))
	default:
		hdr = make([]byte, 10)
		hdr[0] = 0x80 | opcode
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(length))
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}

	if _, err := c.conn.Write(hdr); err != nil {
		return err
	}
	if _, err := c.conn.Write(mask[:]); err != nil {
		return err
	}
	if len(masked) == 0 {
		return nil
	}
	_, err := c.conn.Write(masked)
	return err
}

func makeKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
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

func debugf(logger Logger, format string, args ...any) {
	if logger != nil {
		logger.Debugf(format, args...)
	}
}
