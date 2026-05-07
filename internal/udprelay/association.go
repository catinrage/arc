package udprelay

import (
	"fmt"
	"io"
	"net"
	"sync"
)

type Association struct {
	conn net.PacketConn

	readR *io.PipeReader
	readW *io.PipeWriter

	writeMu sync.Mutex
	buffer  []byte
	cache   sync.Map

	once sync.Once
}

func NewAssociation() (*Association, error) {
	conn, err := net.ListenPacket("udp", "")
	if err != nil {
		return nil, err
	}
	readR, readW := io.Pipe()
	a := &Association{
		conn:  conn,
		readR: readR,
		readW: readW,
	}
	go a.readLoop()
	return a, nil
}

func (a *Association) Read(p []byte) (int, error) {
	return a.readR.Read(p)
}

func (a *Association) Write(p []byte) (int, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	a.buffer = append(a.buffer, p...)
	if err := DecodeBuffered(&a.buffer, a.writePacket); err != nil {
		return 0, err
	}
	if len(a.buffer) == 0 && cap(a.buffer) > MaxPayload*2 {
		a.buffer = nil
	}
	return len(p), nil
}

func (a *Association) Close() error {
	var err error
	a.once.Do(func() {
		err = a.conn.Close()
		_ = a.readR.Close()
		_ = a.readW.Close()
	})
	return err
}

func (a *Association) readLoop() {
	buf := make([]byte, MaxPayload)
	for {
		n, addr, err := a.conn.ReadFrom(buf)
		if err != nil {
			_ = a.readW.CloseWithError(err)
			return
		}
		host, port, err := HostPortFromAddr(addr)
		if err != nil {
			continue
		}
		payload := make([]byte, n)
		copy(payload, buf[:n])
		if err := WritePacket(a.readW, Packet{Host: host, Port: port, Payload: payload}); err != nil {
			_ = a.readW.CloseWithError(err)
			return
		}
	}
}

func (a *Association) writePacket(pkt Packet) error {
	if pkt.Port == 0 {
		return fmt.Errorf("udp target port is zero for host %q", pkt.Host)
	}
	addr, err := a.resolve(pkt.Host, pkt.Port)
	if err != nil {
		return err
	}
	_, err = a.conn.WriteTo(pkt.Payload, addr)
	return err
}

func (a *Association) resolve(host string, port uint16) (net.Addr, error) {
	key := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	if value, ok := a.cache.Load(key); ok {
		return value.(net.Addr), nil
	}
	addr, err := net.ResolveUDPAddr("udp", key)
	if err != nil {
		return nil, err
	}
	a.cache.Store(key, addr)
	return addr, nil
}

var _ io.ReadWriteCloser = (*Association)(nil)
