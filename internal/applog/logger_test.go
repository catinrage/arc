package applog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	level, err := ParseLevel("debug")
	if err != nil {
		t.Fatal(err)
	}
	if level != Debug {
		t.Fatalf("got %v", level)
	}

	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("expected invalid level error")
	}
}

func TestLoggerWritesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arc.log")
	logger, err := New("debug", path)
	if err != nil {
		t.Fatal(err)
	}

	logger.Debugf("hello %s", "world")
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "DEBUG hello world") {
		t.Fatalf("unexpected log file: %s", data)
	}
}
