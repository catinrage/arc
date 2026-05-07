package socks

import (
	"bytes"
	"errors"
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
	var cmdErr CommandError
	if !errors.As(err, &cmdErr) || cmdErr.Command != 2 {
		t.Fatalf("expected unsupported command 2, got %v", err)
	}
	if got := ReplyCodeForError(err); got != 0x07 {
		t.Fatalf("unexpected reply code: %x", got)
	}
}

func TestReadRequestReportsUDPAssociate(t *testing.T) {
	input := []byte{
		0x05, 0x01, 0x00,
		0x05, 0x03, 0x00, 0x01,
		127, 0, 0, 1,
		0, 53,
	}
	_, err := ReadRequest(&rwBuffer{Buffer: bytes.NewBuffer(input)})
	var cmdErr CommandError
	if !errors.As(err, &cmdErr) || cmdErr.Command != 3 {
		t.Fatalf("expected udp associate command, got %v", err)
	}
	if got := cmdErr.Error(); got != "unsupported socks command udp_associate(3)" {
		t.Fatalf("unexpected error: %q", got)
	}
}

var _ io.ReadWriter = (*rwBuffer)(nil)
