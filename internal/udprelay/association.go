package udprelay

import (
	"fmt"
	"io"
	"net"
	"sync"
)

type Association struct {
	readR *io.PipeReader
	readW *io.PipeWriter

	writeMu sync.Mutex
	buffer  []byte

	flowMu sync.Mutex
	flows  map[uint64]*udpFlow

	readMu sync.Mutex
	once   sync.Once
}

type udpFlow struct {
	id    uint64
	conn  net.PacketConn
	cache sync.Map
}

func NewAssociation() (*Association, error) {
	readR, readW := io.Pipe()
	return &Association{
		readR: readR,
		readW: readW,
		flows: make(map[uint64]*udpFlow),
	}, nil
}

func (a *Association) Read(p []byte) (int, error) {
	return a.readR.Read(p)
}

func (a *Association) Write(p []byte) (int, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	a.buffer = append(a.buffer, p...)
	if err := DecodeBuffered(&a.buffer, a.handlePacket); err != nil {
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
		a.flowMu.Lock()
		for id, flow := range a.flows {
			_ = flow.conn.Close()
			delete(a.flows, id)
		}
		a.flowMu.Unlock()
		_ = a.readR.Close()
		err = a.readW.Close()
	})
	return err
}

func (a *Association) handlePacket(pkt Packet) error {
	if pkt.Close {
		a.closeFlow(pkt.AssociationID)
		return nil
	}
	if pkt.Port == 0 {
		return fmt.Errorf("udp target port is zero for host %q", pkt.Host)
	}
	flow, err := a.flow(pkt.AssociationID)
	if err != nil {
		return err
	}
	addr, err := flow.resolve(pkt.Host, pkt.Port)
	if err != nil {
		return err
	}
	_, err = flow.conn.WriteTo(pkt.Payload, addr)
	return err
}

func (a *Association) flow(id uint64) (*udpFlow, error) {
	a.flowMu.Lock()
	defer a.flowMu.Unlock()

	if flow := a.flows[id]; flow != nil {
		return flow, nil
	}
	conn, err := net.ListenPacket("udp", "")
	if err != nil {
		return nil, err
	}
	flow := &udpFlow{id: id, conn: conn}
	a.flows[id] = flow
	go a.readLoop(flow)
	return flow, nil
}

func (a *Association) closeFlow(id uint64) {
	a.flowMu.Lock()
	flow := a.flows[id]
	if flow != nil {
		delete(a.flows, id)
	}
	a.flowMu.Unlock()
	if flow != nil {
		_ = flow.conn.Close()
	}
}

func (a *Association) readLoop(flow *udpFlow) {
	buf := make([]byte, MaxPayload)
	for {
		n, addr, err := flow.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		host, port, err := HostPortFromAddr(addr)
		if err != nil {
			continue
		}
		payload := make([]byte, n)
		copy(payload, buf[:n])

		a.readMu.Lock()
		err = WritePacket(a.readW, Packet{AssociationID: flow.id, Host: host, Port: port, Payload: payload})
		a.readMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (f *udpFlow) resolve(host string, port uint16) (net.Addr, error) {
	key := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	if value, ok := f.cache.Load(key); ok {
		return value.(net.Addr), nil
	}
	addr, err := net.ResolveUDPAddr("udp", key)
	if err != nil {
		return nil, err
	}
	f.cache.Store(key, addr)
	return addr, nil
}

var _ io.ReadWriteCloser = (*Association)(nil)
