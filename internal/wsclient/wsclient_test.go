package wsclient

import "testing"

func TestAcceptKey(t *testing.T) {
	got := acceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestHeaderContains(t *testing.T) {
	if !headerContains("keep-alive, Upgrade", "upgrade") {
		t.Fatal("expected token match")
	}
	if headerContains("keep-alive", "upgrade") {
		t.Fatal("unexpected token match")
	}
}
