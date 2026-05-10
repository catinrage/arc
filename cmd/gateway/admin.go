package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"arc/internal/config"
)

const recentRequestLimit = 250

type requestTracker struct {
	mu    sync.Mutex
	items []requestRecord
}

type requestRecord struct {
	ID        int64      `json:"id"`
	Command   string     `json:"command"`
	Host      string     `json:"host"`
	Port      uint16     `json:"port"`
	Target    string     `json:"target"`
	Remote    string     `json:"remote"`
	Status    string     `json:"status"`
	Error     string     `json:"error,omitempty"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type adminState struct {
	Version       string          `json:"version"`
	Commit        string          `json:"commit"`
	BuildDate     string          `json:"build_date"`
	SOCKS         socksState      `json:"socks"`
	Relay         relayState      `json:"relay"`
	Requests      []requestRecord `json:"requests"`
	ServiceStatus string          `json:"service_status"`
}

type socksState struct {
	Listening bool   `json:"listening"`
	Address   string `json:"address"`
	Active    int64  `json:"active"`
	Total     int64  `json:"total"`
}

type relayState struct {
	URL             string `json:"url"`
	Transport       string `json:"transport"`
	ReadySessions   int    `json:"ready_sessions"`
	ConfigSessions  int    `json:"config_sessions"`
	ActiveStreams   int64  `json:"active_streams"`
	BurstActive     int64  `json:"burst_active"`
	BurstConfigured int    `json:"burst_configured"`
	MaxStreams      int    `json:"max_streams_per_session"`
	UDPEnabled      bool   `json:"udp_enabled"`
}

type configResponse struct {
	Config        config.Gateway `json:"config"`
	ConfigPath    string         `json:"config_path"`
	PasswordBlank bool           `json:"password_blank"`
}

type configSaveResponse struct {
	OK              bool   `json:"ok"`
	RequiresRestart bool   `json:"requires_restart"`
	Message         string `json:"message"`
}

type serviceRequest struct {
	Action string `json:"action"`
}

type serviceResponse struct {
	OK      bool   `json:"ok"`
	Action  string `json:"action"`
	Output  string `json:"output"`
	Message string `json:"message"`
}

func (g *gateway) runAdmin(ctx context.Context) {
	panelDir, err := findGatewayPanelDir()
	if err != nil {
		g.log.Warnf("admin panel disabled: %v", err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", g.adminAuth(g.handleAdminState))
	mux.HandleFunc("/api/config", g.adminAuth(g.handleAdminConfig))
	mux.HandleFunc("/api/service", g.adminAuth(g.handleAdminService))
	mux.Handle("/", g.adminAuth(spaFileHandler(panelDir)))

	server := &http.Server{
		Addr:              g.cfg.AdminListen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	g.log.Infof("gateway admin panel listening on %s panel=%s", g.cfg.AdminListen, panelDir)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		g.log.Warnf("admin panel stopped: %v", err)
	}
}

func (g *gateway) adminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(g.cfg.AdminUsername)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(g.cfg.AdminPassword)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Arc Gateway"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (g *gateway) handleAdminState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, g.adminState())
}

func (g *gateway) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfg := g.configForPanel()
		cfg.AdminPassword = ""
		writeJSON(w, configResponse{Config: cfg, ConfigPath: g.configPath, PasswordBlank: true})
	case http.MethodPost:
		var next config.Gateway
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			writeAPIError(w, http.StatusBadRequest, err)
			return
		}
		current := g.configForPanel()
		if next.AdminPassword == "" {
			next.AdminPassword = current.AdminPassword
		}
		if err := config.SaveGateway(g.configPath, next); err != nil {
			writeAPIError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, configSaveResponse{OK: true, RequiresRestart: true, Message: "Saved config. Restart gateway to apply runtime settings."})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (g *gateway) handleAdminService(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req serviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err)
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	switch action {
	case "status", "start", "stop", "restart":
	default:
		writeAPIError(w, http.StatusBadRequest, fmt.Errorf("unsupported service action %q", req.Action))
		return
	}

	if action == "stop" || action == "restart" {
		if err := g.scheduleServiceAction(action); err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, serviceResponse{OK: true, Action: action, Message: "Service action scheduled."})
		return
	}

	output, err := runServiceAction(r.Context(), action)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, fmt.Errorf("%v\n%s", err, output))
		return
	}
	writeJSON(w, serviceResponse{OK: true, Action: action, Output: output})
}

func (g *gateway) scheduleServiceAction(action string) error {
	script, err := findServiceScript()
	if err != nil {
		return err
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		cmd := exec.Command(script, "gateway", action)
		cmd.Dir = filepath.Dir(script)
		_ = cmd.Start()
	}()
	return nil
}

func (g *gateway) adminState() adminState {
	ready := 0
	streams := int64(0)
	for i := range g.slots {
		g.slots[i].mu.RLock()
		session := g.slots[i].session
		raw := g.slots[i].raw
		active := g.slots[i].active
		g.slots[i].mu.RUnlock()
		if session != nil || raw != nil {
			ready++
			streams += int64(active)
		}
	}

	return adminState{
		Version:   appVersion,
		Commit:    appCommit,
		BuildDate: appBuildDate,
		SOCKS: socksState{
			Listening: true,
			Address:   net.JoinHostPort(g.cfg.ListenHost, fmt.Sprintf("%d", g.cfg.ListenPort)),
			Active:    g.active.Load(),
			Total:     g.total.Load(),
		},
		Relay: relayState{
			URL:             g.cfg.RelayURL,
			Transport:       g.transport(),
			ReadySessions:   ready,
			ConfigSessions:  len(g.slots),
			ActiveStreams:   streams,
			BurstActive:     g.burst.Load(),
			BurstConfigured: g.cfg.BurstConnections,
			MaxStreams:      g.cfg.MaxStreams,
			UDPEnabled:      g.cfg.UDPEnabled,
		},
		Requests: g.requests.snapshot(),
	}
}

func (g *gateway) configForPanel() config.Gateway {
	if g.configPath == "" {
		return g.cfg
	}
	cfg, err := config.LoadGateway(g.configPath)
	if err != nil {
		return g.cfg
	}
	return cfg
}

func (g *gateway) recordRequest(id int64, command, host string, port uint16, remote net.Addr) {
	g.requests.add(requestRecord{
		ID:        id,
		Command:   command,
		Host:      host,
		Port:      port,
		Target:    net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		Remote:    remote.String(),
		Status:    "opening",
		StartedAt: time.Now(),
	})
}

func (g *gateway) updateRequest(id int64, status string, err error) {
	g.requests.update(id, status, err)
}

func (t *requestTracker) add(item requestRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.items = append([]requestRecord{item}, t.items...)
	if len(t.items) > recentRequestLimit {
		t.items = t.items[:recentRequestLimit]
	}
}

func (t *requestTracker) update(id int64, status string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.items {
		if t.items[i].ID != id {
			continue
		}
		t.items[i].Status = status
		if err != nil {
			t.items[i].Error = err.Error()
		}
		if status == "closed" || status == "failed" {
			now := time.Now()
			t.items[i].EndedAt = &now
		}
		return
	}
}

func (t *requestTracker) snapshot() []requestRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]requestRecord, len(t.items))
	copy(out, t.items)
	return out
}

func spaFileHandler(root string) http.HandlerFunc {
	fs := http.FileServer(http.Dir(root))
	return func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if path == "." || path == "/" {
			http.ServeFile(w, r, filepath.Join(root, "index.html"))
			return
		}
		full := filepath.Join(root, path)
		if !strings.HasPrefix(full, filepath.Clean(root)+string(os.PathSeparator)) {
			http.NotFound(w, r)
			return
		}
		if info, err := os.Stat(full); err != nil || info.IsDir() {
			http.ServeFile(w, r, filepath.Join(root, "index.html"))
			return
		}
		fs.ServeHTTP(w, r)
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func findGatewayPanelDir() (string, error) {
	for _, base := range candidateBaseDirs() {
		path := filepath.Join(base, "panel", "gateway")
		if _, err := os.Stat(filepath.Join(path, "index.html")); err == nil {
			return path, nil
		}
	}
	return "", errors.New("panel/gateway/index.html not found")
}

func findServiceScript() (string, error) {
	for _, base := range candidateBaseDirs() {
		path := filepath.Join(base, "service.sh")
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, nil
		}
	}
	return "", errors.New("service.sh not found")
}

func candidateBaseDirs() []string {
	var dirs []string
	if cwd, err := os.Getwd(); err == nil {
		dirs = append(dirs, cwd)
	}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	return dirs
}

func runServiceAction(ctx context.Context, action string) (string, error) {
	script, err := findServiceScript()
	if err != nil {
		return "", err
	}
	cmdCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, script, "gateway", action)
	cmd.Dir = filepath.Dir(script)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
