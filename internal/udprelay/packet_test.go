package udprelay

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

var errUnexpectedPayload = errors.New("unexpected payload")

func TestPacketRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := Packet{AssociationID: 99, Host: "example.com", Port: 53, Payload: []byte("hello")}
	if err := WritePacket(&buf, in); err != nil {
		t.Fatal(err)
	}

	out, err := ReadPacket(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.AssociationID != in.AssociationID || out.Host != in.Host || out.Port != in.Port || string(out.Payload) != string(in.Payload) {
		t.Fatalf("unexpected packet: %#v", out)
	}
}

func TestClosePacketRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePacket(&buf, Packet{AssociationID: 42, Close: true}); err != nil {
		t.Fatal(err)
	}
	out, err := ReadPacket(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Close || out.AssociationID != 42 {
		t.Fatalf("unexpected close packet: %#v", out)
	}
}

func TestPacketRejectsLongHost(t *testing.T) {
	host := make([]byte, MaxHostLength+1)
	for i := range host {
		host[i] = 'a'
	}
	if err := WritePacket(&bytes.Buffer{}, Packet{Host: string(host), Port: 53}); err == nil {
		t.Fatal("expected long host error")
	}
}

func TestDecodeBufferedHandlesPartialFrames(t *testing.T) {
	var encoded bytes.Buffer
	in := Packet{AssociationID: 7, Host: "8.8.8.8", Port: 53, Payload: []byte("dns")}
	if err := WritePacket(&encoded, in); err != nil {
		t.Fatal(err)
	}

	raw := encoded.Bytes()
	buf := append([]byte(nil), raw[:3]...)
	var got []Packet
	if err := DecodeBuffered(&buf, func(pkt Packet) error {
		got = append(got, pkt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("decoded partial frame: %#v", got)
	}

	buf = append(buf, raw[3:]...)
	if err := DecodeBuffered(&buf, func(pkt Packet) error {
		got = append(got, pkt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].AssociationID != in.AssociationID || got[0].Host != in.Host || got[0].Port != in.Port || string(got[0].Payload) != string(in.Payload) {
		t.Fatalf("unexpected decoded packets: %#v", got)
	}
}

func TestAssociationRelaysUDP(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("udp sockets unavailable: %v", err)
	}
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 128)
		n, addr, err := server.ReadFrom(buf)
		if err != nil {
			serverDone <- err
			return
		}
		if string(buf[:n]) != "ping" {
			serverDone <- errUnexpectedPayload
			return
		}
		_, err = server.WriteTo([]byte("pong"), addr)
		serverDone <- err
	}()

	assoc, err := NewAssociation()
	if err != nil {
		t.Skipf("udp sockets unavailable: %v", err)
	}
	defer assoc.Close()

	host, port, err := HostPortFromAddr(server.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	var frame bytes.Buffer
	if err := WritePacket(&frame, Packet{AssociationID: 12, Host: host, Port: port, Payload: []byte("ping")}); err != nil {
		t.Fatal(err)
	}
	if _, err := assoc.Write(frame.Bytes()); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("udp server timed out")
	}

	gotCh := make(chan Packet, 1)
	errCh := make(chan error, 1)
	go func() {
		pkt, err := ReadPacket(assoc)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- pkt
	}()

	select {
	case pkt := <-gotCh:
		if pkt.AssociationID != 12 || string(pkt.Payload) != "pong" {
			t.Fatalf("unexpected response: %#v", pkt)
		}
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("association read timed out")
	}
}
