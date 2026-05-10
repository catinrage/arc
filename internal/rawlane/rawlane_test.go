package rawlane

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"arc/internal/protocol"
)

type memoryWire struct {
	in     chan []byte
	out    chan []byte
	closed chan struct{}
	once   sync.Once
}

func newWirePair() (*memoryWire, *memoryWire) {
	aToB := make(chan []byte, 16)
	bToA := make(chan []byte, 16)
	return &memoryWire{in: bToA, out: aToB, closed: make(chan struct{})}, &memoryWire{in: aToB, out: bToA, closed: make(chan struct{})}
}

func (w *memoryWire) ReadMessage() ([]byte, error) {
	select {
	case msg, ok := <-w.in:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-w.closed:
		return nil, io.EOF
	}
}

func (w *memoryWire) WriteMessage(msg []byte) error {
	cp := append([]byte(nil), msg...)
	defer func() {
		_ = recover()
	}()
	select {
	case w.out <- cp:
		return nil
	case <-w.closed:
		return io.ErrClosedPipe
	}
}

func (w *memoryWire) Close() error {
	w.once.Do(func() {
		close(w.closed)
		close(w.out)
	})
	return nil
}

func TestRawLaneEcho(t *testing.T) {
	clientWire, agentWire := newWirePair()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- Serve(ctx, agentWire, func(context.Context, protocol.OpenRequest) (io.ReadWriteCloser, error) {
			serverConn, echoConn := net.Pipe()
			go func() {
				defer echoConn.Close()
				_, _ = io.Copy(echoConn, echoConn)
			}()
			return serverConn, nil
		}, 1024)
	}()

	stream, err := Open(ctx, clientWire, protocol.OpenRequest{Host: "example.com", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello" {
		t.Fatalf("got %q", buf)
	}
	_ = stream.Close()

	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("serve did not exit")
	}
}
