package rawlane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"arc/internal/protocol"
)

type Wire interface {
	ReadMessage() ([]byte, error)
	WriteMessage([]byte) error
	Close() error
}

type AcceptFunc func(context.Context, protocol.OpenRequest) (io.ReadWriteCloser, error)

type Stream struct {
	wire     Wire
	leftover []byte
	once     sync.Once
}

func Open(ctx context.Context, wire Wire, req protocol.OpenRequest) (*Stream, error) {
	payload, err := protocol.EncodeOpen(req)
	if err != nil {
		return nil, err
	}
	raw, err := protocol.EncodeMessage(protocol.Message{Type: protocol.TypeOpen, Payload: payload})
	if err != nil {
		return nil, err
	}
	if err := wire.WriteMessage(raw); err != nil {
		return nil, err
	}

	type result struct {
		msg protocol.Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		raw, err := wire.ReadMessage()
		if err != nil {
			ch <- result{err: err}
			return
		}
		msg, err := protocol.DecodeMessage(raw)
		ch <- result{msg: msg, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = wire.Close()
		return nil, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return nil, res.err
		}
		switch res.msg.Type {
		case protocol.TypeOpenOK:
			return &Stream{wire: wire}, nil
		case protocol.TypeError:
			if len(res.msg.Payload) == 0 {
				return nil, errors.New("open failed")
			}
			return nil, errors.New(string(res.msg.Payload))
		default:
			return nil, fmt.Errorf("unexpected raw open response type %d", res.msg.Type)
		}
	}
}

func Serve(ctx context.Context, wire Wire, accept AcceptFunc, bufferSize int) error {
	raw, err := wire.ReadMessage()
	if err != nil {
		return fmt.Errorf("open read: %w", err)
	}
	msg, err := protocol.DecodeMessage(raw)
	if err != nil {
		return fmt.Errorf("open decode: %w", err)
	}
	if msg.Type != protocol.TypeOpen {
		return fmt.Errorf("unexpected first raw frame type %d", msg.Type)
	}
	req, err := protocol.DecodeOpen(msg.Payload)
	if err != nil {
		_ = sendControl(wire, protocol.TypeError, []byte(err.Error()))
		return err
	}

	target, err := accept(ctx, req)
	if err != nil {
		_ = sendControl(wire, protocol.TypeError, []byte(err.Error()))
		return err
	}
	if err := sendControl(wire, protocol.TypeOpenOK, nil); err != nil {
		_ = target.Close()
		return err
	}

	stream := &Stream{wire: wire}
	done := make(chan struct{}, 2)
	go func() {
		copyAndClose(stream, target, bufferSize)
		done <- struct{}{}
	}()
	go func() {
		copyAndClose(target, stream, bufferSize)
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		_ = stream.Close()
		_ = target.Close()
		<-done
		return ctx.Err()
	case <-done:
		_ = stream.Close()
		_ = target.Close()
		return nil
	}
}

func sendControl(wire Wire, typ byte, payload []byte) error {
	raw, err := protocol.EncodeMessage(protocol.Message{Type: typ, Payload: payload})
	if err != nil {
		return err
	}
	return wire.WriteMessage(raw)
}

func (s *Stream) Read(p []byte) (int, error) {
	if len(s.leftover) > 0 {
		n := copy(p, s.leftover)
		s.leftover = s.leftover[n:]
		return n, nil
	}
	msg, err := s.wire.ReadMessage()
	if err != nil {
		return 0, err
	}
	n := copy(p, msg)
	if n < len(msg) {
		s.leftover = append(s.leftover[:0], msg[n:]...)
	}
	return n, nil
}

func (s *Stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := s.wire.WriteMessage(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *Stream) Close() error {
	var err error
	s.once.Do(func() {
		err = s.wire.Close()
	})
	return err
}

func (s *Stream) LocalAddr() net.Addr  { return nil }
func (s *Stream) RemoteAddr() net.Addr { return nil }

func copyAndClose(dst io.WriteCloser, src io.ReadCloser, bufferSize int) {
	if bufferSize <= 0 {
		bufferSize = 64 << 10
	}
	buf := make([]byte, bufferSize)
	_, _ = io.CopyBuffer(dst, src, buf)
	_ = dst.Close()
	_ = src.Close()
}

type PumpedWire struct {
	wire Wire

	messages chan []byte
	done     chan struct{}
	once     sync.Once

	errMu sync.Mutex
	err   error
}

func NewPumpedWire(wire Wire, depth int) *PumpedWire {
	if depth <= 0 {
		depth = 64
	}
	p := &PumpedWire{
		wire:     wire,
		messages: make(chan []byte, depth),
		done:     make(chan struct{}),
	}
	go p.readLoop()
	return p
}

func (p *PumpedWire) ReadMessage() ([]byte, error) {
	msg, ok := <-p.messages
	if ok {
		return msg, nil
	}
	if err := p.readError(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (p *PumpedWire) WriteMessage(msg []byte) error {
	return p.wire.WriteMessage(msg)
}

func (p *PumpedWire) Close() error {
	var err error
	p.once.Do(func() {
		err = p.wire.Close()
	})
	return err
}

func (p *PumpedWire) Done() <-chan struct{} {
	return p.done
}

func (p *PumpedWire) readLoop() {
	defer close(p.done)
	defer close(p.messages)
	for {
		msg, err := p.wire.ReadMessage()
		if err != nil {
			p.setReadError(err)
			return
		}
		select {
		case p.messages <- msg:
		case <-p.done:
			return
		}
	}
}

func (p *PumpedWire) setReadError(err error) {
	p.errMu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.errMu.Unlock()
}

func (p *PumpedWire) readError() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}
