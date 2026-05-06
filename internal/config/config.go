package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Gateway struct {
	RelayURL         string `json:"relay_url"`
	ListenHost       string `json:"listen_host"`
	ListenPort       int    `json:"listen_port"`
	Connections      int    `json:"connections"`
	BufferSize       int    `json:"buffer_size"`
	OpenTimeout      string `json:"open_timeout"`
	ReconnectInitial string `json:"reconnect_initial"`
	ReconnectMax     string `json:"reconnect_max"`
	StatsInterval    string `json:"stats_interval"`
	InsecureTLS      bool   `json:"insecure_tls"`
	LogFile          string `json:"log_file"`
	LogLevel         string `json:"log_level"`
}

type Agent struct {
	RelayURL             string `json:"relay_url"`
	Connections          int    `json:"connections"`
	BufferSize           int    `json:"buffer_size"`
	TargetConnectTimeout string `json:"target_connect_timeout"`
	ReconnectInitial     string `json:"reconnect_initial"`
	ReconnectMax         string `json:"reconnect_max"`
	StatsInterval        string `json:"stats_interval"`
	InsecureTLS          bool   `json:"insecure_tls"`
	LogFile              string `json:"log_file"`
	LogLevel             string `json:"log_level"`
}

func DefaultGateway() Gateway {
	return Gateway{
		RelayURL:         "wss://ciyn-4f0b00602d-rain.apps.ir-central1.arvancaas.ir/client",
		ListenHost:       "127.0.0.1",
		ListenPort:       1080,
		Connections:      8,
		BufferSize:       64 << 10,
		OpenTimeout:      "10s",
		ReconnectInitial: "250ms",
		ReconnectMax:     "5s",
		StatsInterval:    "10s",
		LogLevel:         "info",
	}
}

func DefaultAgent() Agent {
	return Agent{
		RelayURL:             "wss://ciyn-4f0b00602d-rain.apps.ir-central1.arvancaas.ir/agent",
		Connections:          8,
		BufferSize:           64 << 10,
		TargetConnectTimeout: "10s",
		ReconnectInitial:     "250ms",
		ReconnectMax:         "5s",
		StatsInterval:        "10s",
		LogLevel:             "info",
	}
}

func LoadGateway(path string) (Gateway, error) {
	cfg := DefaultGateway()
	if path == "" {
		return cfg, cfg.Validate()
	}
	if err := loadJSON(path, &cfg); err != nil {
		return Gateway{}, err
	}
	return cfg, cfg.Validate()
}

func LoadAgent(path string) (Agent, error) {
	cfg := DefaultAgent()
	if path == "" {
		return cfg, cfg.Validate()
	}
	if err := loadJSON(path, &cfg); err != nil {
		return Agent{}, err
	}
	return cfg, cfg.Validate()
}

func loadJSON(path string, dst any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func (c Gateway) Validate() error {
	if c.RelayURL == "" {
		return errors.New("relay_url is required")
	}
	if c.ListenHost == "" {
		return errors.New("listen_host is required")
	}
	if c.ListenPort <= 0 || c.ListenPort > 65535 {
		return fmt.Errorf("listen_port out of range: %d", c.ListenPort)
	}
	if c.Connections <= 0 {
		return errors.New("connections must be positive")
	}
	if c.BufferSize <= 0 {
		return errors.New("buffer_size must be positive")
	}
	if err := validateLogLevel(c.LogLevel); err != nil {
		return err
	}
	return validateDurations(map[string]string{
		"open_timeout":      c.OpenTimeout,
		"reconnect_initial": c.ReconnectInitial,
		"reconnect_max":     c.ReconnectMax,
		"stats_interval":    c.StatsInterval,
	})
}

func (c Agent) Validate() error {
	if c.RelayURL == "" {
		return errors.New("relay_url is required")
	}
	if c.Connections <= 0 {
		return errors.New("connections must be positive")
	}
	if c.BufferSize <= 0 {
		return errors.New("buffer_size must be positive")
	}
	if err := validateLogLevel(c.LogLevel); err != nil {
		return err
	}
	return validateDurations(map[string]string{
		"target_connect_timeout": c.TargetConnectTimeout,
		"reconnect_initial":      c.ReconnectInitial,
		"reconnect_max":          c.ReconnectMax,
		"stats_interval":         c.StatsInterval,
	})
}

func validateLogLevel(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "debug", "info", "warn", "warning", "error":
		return nil
	default:
		return fmt.Errorf("invalid log_level %q", value)
	}
}

func Duration(value string) (time.Duration, error) {
	return time.ParseDuration(value)
}

func MustDuration(value string) time.Duration {
	d, err := Duration(value)
	if err != nil {
		panic(err)
	}
	return d
}

func validateDurations(values map[string]string) error {
	for name, value := range values {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
		if _, err := time.ParseDuration(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}
