package socks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
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
		0, 0, 0, 0,
		0, 0,
	}
	req, err := ReadRequest(&rwBuffer{Buffer: bytes.NewBuffer(input)})
	if err != nil {
		t.Fatal(err)
	}
	if req.Command != CmdUDPAssociate || req.Host != "0.0.0.0" || req.Port != 0 {
		t.Fatalf("unexpected udp associate request: %#v", req)
	}
}

func TestUDPDatagramRoundTrip(t *testing.T) {
	in := UDPDatagram{Host: "8.8.8.8", Port: 53, Payload: []byte("dns")}
	raw, err := EncodeUDPDatagram(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ParseUDPDatagram(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Host != in.Host || out.Port != in.Port || string(out.Payload) != string(in.Payload) {
		t.Fatalf("unexpected datagram: %#v", out)
	}
}

func TestWriteUDPAssociateSuccess(t *testing.T) {
	var out bytes.Buffer
	if err := WriteUDPAssociateSuccess(&out, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53000}); err != nil {
		t.Fatal(err)
	}
	got := out.Bytes()
	if len(got) != 10 || got[0] != 5 || got[1] != 0 || got[3] != 1 {
		t.Fatalf("bad udp associate reply: %x", got)
	}
	if port := binary.BigEndian.Uint16(got[8:]); port != 53000 {
		t.Fatalf("unexpected port: %d", port)
	}
}

var _ io.ReadWriter = (*rwBuffer)(nil)
