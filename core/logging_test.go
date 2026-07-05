package core

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func TestLoggerFromContext_FallbackToDefault(t *testing.T) {
	got := LoggerFromContext(context.Background())
	if got == nil {
		t.Fatal("expected non-nil logger")
	}
	if got != slog.Default() {
		t.Fatal("expected slog.Default() when no logger attached")
	}
}

func TestWithLogger_RoundTrip(t *testing.T) {
	want := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := WithLogger(context.Background(), want)
	if got := LoggerFromContext(ctx); got != want {
		t.Fatal("LoggerFromContext did not return the attached logger")
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug, "DEBUG": slog.LevelDebug,
		"info": slog.LevelInfo, "": slog.LevelInfo,
		"warn": slog.LevelWarn, "warning": slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil {
			t.Fatalf("ParseLevel(%q) unexpected error: %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseLevel("bogus"); err == nil {
		t.Fatal("expected error for unknown level")
	}
}
