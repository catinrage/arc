package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"arc/internal/config"
)

func TestAdminAuthRejectsBadCredentials(t *testing.T) {
	cfg := config.DefaultGateway()
	cfg.AdminListen = "127.0.0.1:8090"
	cfg.AdminUsername = "admin"
	cfg.AdminPassword = "secret"
	gw, err := newGateway(cfg, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	handler := gw.adminAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/state", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", rec.Code)
	}

	req.SetBasicAuth("admin", "secret")
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected success, got %d", rec.Code)
	}
}

func TestRequestTrackerLifecycle(t *testing.T) {
	cfg := config.DefaultGateway()
	gw, err := newGateway(cfg, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	gw.recordRequest(7, "CONNECT", "example.com", 443, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50000})
	gw.updateRequest(7, "connected", nil)
	items := gw.requests.snapshot()
	if len(items) != 1 || items[0].Status != "connected" || items[0].Target != "example.com:443" {
		t.Fatalf("unexpected request snapshot: %#v", items)
	}
	gw.updateRequest(7, "closed", nil)
	items = gw.requests.snapshot()
	if items[0].EndedAt == nil {
		t.Fatal("expected ended timestamp")
	}
}
