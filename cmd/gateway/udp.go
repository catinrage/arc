package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"arc/internal/mux"
	"arc/internal/protocol"
	"arc/internal/socks"
	"arc/internal/udprelay"
)

type udpRelayClient struct {
	g *gateway

	next atomic.Uint64

	mu      sync.Mutex
	stream  *mux.Stream
	release func()
	assocs  map[uint64]*udpAssociation
}

type udpAssociation struct {
	id      uint64
	connID  int64
	udpConn *net.UDPConn
	relay   *udpRelayClient

	clientMu   sync.RWMutex
	clientAddr *net.UDPAddr
}

func newUDPRelayClient(g *gateway) *udpRelayClient {
	return &udpRelayClient{
		g:      g,
		assocs: make(map[uint64]*udpAssociation),
	}
}

func (r *udpRelayClient) handleAssociate(ctx context.Context, conn net.Conn, connID int64, req socks.Request) {
	if !r.g.cfg.UDPEnabled {
		_ = socks.WriteFailure(conn, 0x07)
		r.g.log.Warnf("#%d UDP associate rejected because udp_enabled=false", connID)
		return
	}

	udpConn, err := net.ListenUDP("udp", r.g.udpBindAddr())
	if err != nil {
		_ = socks.WriteFailure(conn, 0x05)
		r.g.log.Warnf("#%d UDP associate bind failed: %v", connID, err)
		return
	}
	defer udpConn.Close()

	localAddr, ok := udpConn.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = socks.WriteFailure(conn, 0x05)
		r.g.log.Warnf("#%d UDP associate bad local addr: %v", connID, udpConn.LocalAddr())
		return
	}
	replyAddr := udpReplyAddr(localAddr, conn.LocalAddr())
	if err := socks.WriteUDPAssociateSuccess(conn, replyAddr); err != nil {
		return
	}

	assoc := &udpAssociation{
		id:      r.next.Add(1),
		connID:  connID,
		udpConn: udpConn,
		relay:   r,
	}
	r.register(assoc)
	defer r.unregister(assoc.id)

	r.g.log.Infof("#%d UDP associate ready id=%d requested=%s:%d bind=%s reply=%s active=%d", connID, assoc.id, req.Host, req.Port, localAddr, replyAddr, r.g.active.Load())

	controlDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, conn)
		close(controlDone)
		_ = udpConn.Close()
	}()

	errCh := make(chan error, 1)
	go assoc.readFromClient(ctx, errCh)

	select {
	case <-ctx.Done():
	case <-controlDone:
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
			r.g.log.Debugf("#%d UDP associate closed: %v", connID, err)
		}
	}
}

func (a *udpAssociation) readFromClient(ctx context.Context, errCh chan<- error) {
	buf := make([]byte, 65535)
	for {
		n, addr, err := a.udpConn.ReadFromUDP(buf)
		if err != nil {
			errCh <- err
			return
		}
		dgram, err := socks.ParseUDPDatagram(buf[:n])
		if err != nil {
			a.relay.g.log.Debugf("#%d bad SOCKS UDP datagram from %s: %v", a.connID, addr, err)
			continue
		}
		if dgram.Port == 0 {
			a.relay.g.log.Debugf("#%d SOCKS UDP datagram has zero target port host=%s", a.connID, dgram.Host)
			continue
		}
		a.clientMu.Lock()
		a.clientAddr = addr
		a.clientMu.Unlock()

		pkt := udprelay.Packet{AssociationID: a.id, Host: dgram.Host, Port: dgram.Port, Payload: dgram.Payload}
		if err := a.relay.writePacket(ctx, pkt); err != nil {
			errCh <- err
			return
		}
	}
}

func (a *udpAssociation) writeToClient(pkt udprelay.Packet) error {
	a.clientMu.RLock()
	addr := a.clientAddr
	a.clientMu.RUnlock()
	if addr == nil {
		return nil
	}
	raw, err := socks.EncodeUDPDatagram(socks.UDPDatagram{Host: pkt.Host, Port: pkt.Port, Payload: pkt.Payload})
	if err != nil {
		return err
	}
	_, err = a.udpConn.WriteToUDP(raw, addr)
	return err
}

func (r *udpRelayClient) register(assoc *udpAssociation) {
	r.mu.Lock()
	r.assocs[assoc.id] = assoc
	r.mu.Unlock()
}

func (r *udpRelayClient) unregister(id uint64) {
	r.mu.Lock()
	delete(r.assocs, id)
	stream := r.stream
	r.mu.Unlock()
	if stream != nil {
		if err := udprelay.WritePacket(stream, udprelay.Packet{AssociationID: id, Close: true}); err != nil {
			r.resetStream(stream)
		}
	}
}

func (r *udpRelayClient) writePacket(ctx context.Context, pkt udprelay.Packet) error {
	stream, err := r.getStream(ctx)
	if err != nil {
		return err
	}
	if err := udprelay.WritePacket(stream, pkt); err != nil {
		r.resetStream(stream)
		return err
	}
	return nil
}

func (r *udpRelayClient) getStream(ctx context.Context) (*mux.Stream, error) {
	r.mu.Lock()
	stream := r.stream
	r.mu.Unlock()
	if stream != nil {
		return stream, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stream != nil {
		return r.stream, nil
	}

	openCtx, cancel := context.WithTimeout(ctx, r.g.openTimeout)
	stream, release, err := r.g.open(openCtx, protocol.OpenRequest{Host: protocol.UDPAssociateHost, Port: 1})
	cancel()
	if err != nil {
		return nil, err
	}
	r.stream = stream
	r.release = release
	go r.readLoop(stream)
	r.g.log.Infof("UDP relay stream connected")
	return stream, nil
}

func (r *udpRelayClient) readLoop(stream *mux.Stream) {
	for {
		pkt, err := udprelay.ReadPacket(stream)
		if err != nil {
			r.resetStream(stream)
			if !errors.Is(err, io.EOF) {
				r.g.log.Debugf("UDP relay stream closed: %v", err)
			}
			return
		}
		r.mu.Lock()
		assoc := r.assocs[pkt.AssociationID]
		r.mu.Unlock()
		if assoc == nil {
			continue
		}
		if err := assoc.writeToClient(pkt); err != nil {
			r.g.log.Debugf("#%d UDP response write failed: %v", assoc.connID, err)
		}
	}
}

func (r *udpRelayClient) resetStream(stream *mux.Stream) {
	r.mu.Lock()
	if r.stream != stream {
		r.mu.Unlock()
		return
	}
	release := r.release
	r.stream = nil
	r.release = nil
	r.mu.Unlock()

	if release != nil {
		_ = stream.Close()
		release()
	}
}
