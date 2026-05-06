package mux

import (
	"context"
	"errors"
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
	aIn := make(chan []byte, 32)
	bIn := make(chan []byte, 32)
	return &memoryWire{in: aIn, out: bIn, closed: make(chan struct{})},
		&memoryWire{in: bIn, out: aIn, closed: make(chan struct{})}
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
	select {
	case w.out <- msg:
		return nil
	case <-w.closed:
		return io.ErrClosedPipe
	}
}

func (w *memoryWire) Close() error {
	w.once.Do(func() {
		close(w.closed)
	})
	return nil
}

func TestSessionOpenAndData(t *testing.T) {
	clientWire, serverWire := newWirePair()
	localConn, remoteConn := net.Pipe()
	defer localConn.Close()

	server := NewSession(serverWire, func(context.Context, protocol.OpenRequest) (io.ReadWriteCloser, error) {
		return remoteConn, nil
	}, 1024)
	client := NewSession(clientWire, nil, 1024)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	go client.Serve(ctx)

	stream, err := client.Open(ctx, protocol.OpenRequest{Host: "example.com", Port: 80})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_, _ = localConn.Write([]byte("from-target"))
	}()
	buf := make([]byte, 32)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "from-target" {
		t.Fatalf("unexpected stream data: %q", buf[:n])
	}

	go func() {
		_, _ = stream.Write([]byte("from-client"))
	}()
	n, err = localConn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "from-client" {
		t.Fatalf("unexpected target data: %q", buf[:n])
	}
}

func TestSessionOpenError(t *testing.T) {
	clientWire, serverWire := newWirePair()
	server := NewSession(serverWire, func(context.Context, protocol.OpenRequest) (io.ReadWriteCloser, error) {
		return nil, errors.New("dial failed")
	}, 1024)
	client := NewSession(clientWire, nil, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go server.Serve(ctx)
	go client.Serve(ctx)

	_, err := client.Open(ctx, protocol.OpenRequest{Host: "example.com", Port: 80})
	if err == nil || err.Error() != "dial failed" {
		t.Fatalf("expected dial failed, got %v", err)
	}
}
