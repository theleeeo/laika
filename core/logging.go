package core

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
)

type loggerCtxKey struct{}

// WithLogger returns a copy of ctx carrying l, retrievable via LoggerFromContext.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey{}, l)
}

// LoggerFromContext returns the logger attached by WithLogger, or slog.Default()
// when none is present. It never returns nil, so library users who never opt in
// are unaffected.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// ParseLevel maps a level name ("debug"|"info"|"warn"|"error", case-insensitive;
// empty means info) to an slog.Level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

// InitTextLogging installs a text slog handler at lvl as the default logger and
// returns it. Both app entrypoints call this once at startup.
func InitTextLogging(w io.Writer, lvl slog.Level) *slog.Logger {
	l := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(l)
	return l
}

// newSearchID returns a short hex id correlating all log lines emitted for a
// single search request (including its child reference searches). It is for
// grep-correlation, not security, so math/rand/v2 (goroutine-safe) is fine.
func newSearchID() string {
	return fmt.Sprintf("%08x", rand.Uint32())
}

// summarizeFilters renders filters as compact "field op value" strings so they
// read cleanly in text logs.
func summarizeFilters(fs []Filter) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		val := any(f.Value)
		if f.Op == FilterOpIn {
			val = f.Values
		}
		out = append(out, fmt.Sprintf("%s %v %v", f.Field, f.Op, val))
	}
	return out
}
