package applog

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

type Logger struct {
	min     Level
	out     io.WriteCloser
	entries chan entry
	wg      sync.WaitGroup
	closed  atomic.Bool
	dropped atomic.Uint64
}

type entry struct {
	level Level
	msg   string
	when  time.Time
}

func New(levelName, fileName string) (*Logger, error) {
	level, err := ParseLevel(levelName)
	if err != nil {
		return nil, err
	}

	var out io.WriteCloser
	if fileName == "" {
		out = nopWriteCloser{Writer: os.Stderr}
	} else {
		out, err = os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			return nil, err
		}
	}

	l := &Logger{
		min:     level,
		out:     out,
		entries: make(chan entry, 4096),
	}
	l.wg.Add(1)
	go l.run()
	return l, nil
}

func ParseLevel(value string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return Info, nil
	case "debug":
		return Debug, nil
	case "warn", "warning":
		return Warn, nil
	case "error":
		return Error, nil
	default:
		return Info, fmt.Errorf("invalid log level %q", value)
	}
}

func (l *Logger) Debugf(format string, args ...any) {
	l.logf(Debug, format, args...)
}

func (l *Logger) Infof(format string, args ...any) {
	l.logf(Info, format, args...)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.logf(Warn, format, args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.logf(Error, format, args...)
}

func (l *Logger) Fatalf(format string, args ...any) {
	l.Errorf(format, args...)
	_ = l.Close()
	log.Fatalf(format, args...)
}

func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	if l.closed.CompareAndSwap(false, true) {
		close(l.entries)
		l.wg.Wait()
		return l.out.Close()
	}
	return nil
}

func (l *Logger) logf(level Level, format string, args ...any) {
	if l == nil || level < l.min || l.closed.Load() {
		return
	}

	e := entry{
		level: level,
		msg:   fmt.Sprintf(format, args...),
		when:  time.Now(),
	}

	select {
	case l.entries <- e:
	default:
		l.dropped.Add(1)
		if level >= Error {
			_, _ = fmt.Fprintf(l.out, "%s %-5s %s\n", e.when.Format(time.RFC3339Nano), levelName(e.level), e.msg)
		}
	}
}

func (l *Logger) run() {
	defer l.wg.Done()
	for e := range l.entries {
		dropped := l.dropped.Swap(0)
		if dropped > 0 {
			_, _ = fmt.Fprintf(l.out, "%s WARN  dropped %d log messages\n", time.Now().Format(time.RFC3339Nano), dropped)
		}
		_, _ = fmt.Fprintf(l.out, "%s %-5s %s\n", e.when.Format(time.RFC3339Nano), levelName(e.level), e.msg)
	}
}

func levelName(level Level) string {
	switch level {
	case Debug:
		return "DEBUG"
	case Info:
		return "INFO"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	default:
		return "INFO"
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}
