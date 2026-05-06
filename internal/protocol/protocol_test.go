package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeMessage(t *testing.T) {
	payload := []byte("hello")
	raw, err := EncodeMessage(Message{Type: TypeData, StreamID: 42, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}

	msg, err := DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != TypeData || msg.StreamID != 42 || !bytes.Equal(msg.Payload, payload) {
		t.Fatalf("unexpected message: %#v", msg)
	}
}

func TestDecodeMessageRejectsBadLength(t *testing.T) {
	raw, err := EncodeMessage(Message{Type: TypeData, StreamID: 1, Payload: []byte("abc")})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := DecodeMessage(raw[:len(raw)-1]); err == nil {
		t.Fatal("expected length error")
	}
}

func TestEncodeDecodeOpen(t *testing.T) {
	payload, err := EncodeOpen(OpenRequest{Host: "example.com", Port: 443})
	if err != nil {
		t.Fatal(err)
	}

	req, err := DecodeOpen(payload)
	if err != nil {
		t.Fatal(err)
	}
	if req.Host != "example.com" || req.Port != 443 {
		t.Fatalf("unexpected request: %#v", req)
	}
}

func TestEncodeOpenRejectsLongHost(t *testing.T) {
	host := make([]byte, MaxHostNameLength+1)
	for i := range host {
		host[i] = 'a'
	}
	if _, err := EncodeOpen(OpenRequest{Host: string(host), Port: 80}); err == nil {
		t.Fatal("expected long host error")
	}
}
