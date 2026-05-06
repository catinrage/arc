package socks

import (
	"bytes"
	"io"
	"testing"
)

type rwBuffer struct {
	*bytes.Buffer
	writes bytes.Buffer
}

func (b *rwBuffer) Write(p []byte) (int, error) {
	return b.writes.Write(p)
}

func TestReadRequestDomain(t *testing.T) {
	input := []byte{
		0x05, 0x01, 0x00,
		0x05, 0x01, 0x00, 0x03,
		0x0b,
	}
	input = append(input, []byte("example.com")...)
	input = append(input, 0x01, 0xbb)

	rw := &rwBuffer{Buffer: bytes.NewBuffer(input)}
	req, err := ReadRequest(rw)
	if err != nil {
		t.Fatal(err)
	}
	if req.Host != "example.com" || req.Port != 443 {
		t.Fatalf("unexpected request: %#v", req)
	}
	if !bytes.Equal(rw.writes.Bytes(), []byte{0x05, 0x00}) {
		t.Fatalf("unexpected method reply: %x", rw.writes.Bytes())
	}
}

func TestWriteSuccess(t *testing.T) {
	var out bytes.Buffer
	if err := WriteSuccess(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Bytes()) != 10 || out.Bytes()[1] != 0 {
		t.Fatalf("bad success reply: %x", out.Bytes())
	}
}

func TestReadRequestRejectsUnsupportedCommand(t *testing.T) {
	input := []byte{
		0x05, 0x01, 0x00,
		0x05, 0x02, 0x00, 0x01,
		127, 0, 0, 1,
		0, 80,
	}
	_, err := ReadRequest(&rwBuffer{Buffer: bytes.NewBuffer(input)})
	if err != ErrUnsupportedCommand {
		t.Fatalf("expected unsupported command, got %v", err)
	}
}

var _ io.ReadWriter = (*rwBuffer)(nil)
