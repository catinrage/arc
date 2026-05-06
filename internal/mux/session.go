package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"arc/internal/protocol"
)

type Wire interface {
	ReadMessage() ([]byte, error)
	WriteMessage([]byte) error
	Close() error
}

type AcceptFunc func(context.Context, protocol.OpenRequest) (io.ReadWriteCloser, error)

type Session struct {
	wire       Wire
	accept     AcceptFunc
	bufferSize int

	mu      sync.Mutex
	streams map[uint32]*Stream
	pending map[uint32]chan error
	nextID  uint32
	closed  chan struct{}
	once    sync.Once

	active atomic.Int64
}

func NewSession(wire Wire, accept AcceptFunc, bufferSize int) *Session {
	if bufferSize <= 0 {
		bufferSize = 64 << 10
	}
	return &Session{
		wire:       wire,
		accept:     accept,
		bufferSize: bufferSize,
		streams:    make(map[uint32]*Stream),
		pending:    make(map[uint32]chan error),
		nextID:     1,
		closed:     make(chan struct{}),
	}
}

func (s *Session) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.readLoop(ctx)
	}()

	select {
	case <-ctx.Done():
		_ = s.Close()
		return ctx.Err()
	case err := <-errCh:
		_ = s.Close()
		return err
	}
}

func (s *Session) Open(ctx context.Context, req protocol.OpenRequest) (*Stream, error) {
	payload, err := protocol.EncodeOpen(req)
	if err != nil {
		return nil, err
	}

	id, stream, done, err := s.newLocalStream()
	if err != nil {
		return nil, err
	}

	if err := s.send(protocol.Message{Type: protocol.TypeOpen, StreamID: id, Payload: payload}); err != nil {
		s.removeStream(id)
		return nil, err
	}

	select {
	case err := <-done:
		if err != nil {
			s.removeStream(id)
			return nil, err
		}
		return stream, nil
	case <-ctx.Done():
		s.removeStream(id)
		_ = s.send(protocol.Message{Type: protocol.TypeClose, StreamID: id})
		return nil, ctx.Err()
	case <-s.closed:
		return nil, errors.New("session closed")
	}
}

func (s *Session) ActiveStreams() int64 {
	return s.active.Load()
}

func (s *Session) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		err = s.wire.Close()

		s.mu.Lock()
		for _, stream := range s.streams {
			stream.closeRemote()
		}
		for _, ch := range s.pending {
			ch <- errors.New("session closed")
			close(ch)
		}
		s.streams = make(map[uint32]*Stream)
		s.pending = make(map[uint32]chan error)
		s.active.Store(0)
		s.mu.Unlock()
	})
	return err
}

func (s *Session) newLocalStream() (uint32, *Stream, chan error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closed:
		return 0, nil, nil, errors.New("session closed")
	default:
	}

	id := s.nextID
	s.nextID += 2
	stream := newStream(s, id)
	done := make(chan error, 1)
	s.streams[id] = stream
	s.pending[id] = done
	s.active.Add(1)
	return id, stream, done, nil
}

func (s *Session) addRemoteStream(id uint32) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream := newStream(s, id)
	s.streams[id] = stream
	s.active.Add(1)
	return stream
}

func (s *Session) removeStream(id uint32) {
	s.mu.Lock()
	if stream, ok := s.streams[id]; ok {
		stream.closeRemote()
		delete(s.streams, id)
		s.active.Add(-1)
	}
	delete(s.pending, id)
	s.mu.Unlock()
}

func (s *Session) getStream(id uint32) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}

func (s *Session) completeOpen(id uint32, err error) {
	s.mu.Lock()
	done := s.pending[id]
	delete(s.pending, id)
	s.mu.Unlock()

	if done != nil {
		done <- err
		close(done)
	}
}

func (s *Session) readLoop(ctx context.Context) error {
	for {
		raw, err := s.wire.ReadMessage()
		if err != nil {
			return err
		}

		msg, err := protocol.DecodeMessage(raw)
		if err != nil {
			return err
		}

		switch msg.Type {
		case protocol.TypeOpen:
			if s.accept == nil {
				_ = s.send(protocol.Message{Type: protocol.TypeError, StreamID: msg.StreamID, Payload: []byte("opens not accepted")})
				continue
			}
			req, err := protocol.DecodeOpen(msg.Payload)
			if err != nil {
				_ = s.send(protocol.Message{Type: protocol.TypeError, StreamID: msg.StreamID, Payload: []byte(err.Error())})
				continue
			}
			go s.acceptStream(ctx, msg.StreamID, req)
		case protocol.TypeOpenOK:
			s.completeOpen(msg.StreamID, nil)
		case protocol.TypeData:
			stream := s.getStream(msg.StreamID)
			if stream == nil {
				_ = s.send(protocol.Message{Type: protocol.TypeClose, StreamID: msg.StreamID})
				continue
			}
			stream.enqueue(msg.Payload)
		case protocol.TypeClose:
			s.removeStream(msg.StreamID)
		case protocol.TypeError:
			err := errors.New(string(msg.Payload))
			s.completeOpen(msg.StreamID, err)
			s.removeStream(msg.StreamID)
		default:
			return fmt.Errorf("unknown message type: %d", msg.Type)
		}
	}
}

func (s *Session) acceptStream(ctx context.Context, id uint32, req protocol.OpenRequest) {
	conn, err := s.accept(ctx, req)
	if err != nil {
		_ = s.send(protocol.Message{Type: protocol.TypeError, StreamID: id, Payload: []byte(err.Error())})
		return
	}

	stream := s.addRemoteStream(id)
	if err := s.send(protocol.Message{Type: protocol.TypeOpenOK, StreamID: id}); err != nil {
		_ = conn.Close()
		s.removeStream(id)
		return
	}

	go copyAndClose(stream, conn, s.bufferSize)
	go copyAndClose(conn, stream, s.bufferSize)
}

func (s *Session) send(msg protocol.Message) error {
	raw, err := protocol.EncodeMessage(msg)
	if err != nil {
		return err
	}
	return s.wire.WriteMessage(raw)
}

type Stream struct {
	session *Session
	id      uint32

	incoming chan []byte
	leftover []byte
	closed   chan struct{}
	once     sync.Once
}

func newStream(session *Session, id uint32) *Stream {
	return &Stream{
		session:  session,
		id:       id,
		incoming: make(chan []byte, 64),
		closed:   make(chan struct{}),
	}
}

func (s *Stream) Read(p []byte) (int, error) {
	if len(s.leftover) > 0 {
		n := copy(p, s.leftover)
		s.leftover = s.leftover[n:]
		return n, nil
	}

	data, ok := <-s.incoming
	if !ok {
		return 0, io.EOF
	}
	n := copy(p, data)
	if n < len(data) {
		s.leftover = append(s.leftover[:0], data[n:]...)
	}
	return n, nil
}

func (s *Stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	payload := make([]byte, len(p))
	copy(payload, p)
	if err := s.session.send(protocol.Message{Type: protocol.TypeData, StreamID: s.id, Payload: payload}); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *Stream) Close() error {
	s.once.Do(func() {
		close(s.closed)
		close(s.incoming)
		_ = s.session.send(protocol.Message{Type: protocol.TypeClose, StreamID: s.id})
		s.session.removeStream(s.id)
	})
	return nil
}

func (s *Stream) LocalAddr() net.Addr  { return nil }
func (s *Stream) RemoteAddr() net.Addr { return nil }

func (s *Stream) enqueue(data []byte) {
	payload := make([]byte, len(data))
	copy(payload, data)

	defer func() {
		_ = recover()
	}()

	select {
	case s.incoming <- payload:
	case <-s.closed:
	}
}

func (s *Stream) closeRemote() {
	s.once.Do(func() {
		close(s.closed)
		close(s.incoming)
	})
}

func copyAndClose(dst io.WriteCloser, src io.ReadCloser, bufferSize int) {
	buf := make([]byte, bufferSize)
	if _, err := io.CopyBuffer(dst, src, buf); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("copy closed: %v", err)
	}
	_ = dst.Close()
	_ = src.Close()
}
