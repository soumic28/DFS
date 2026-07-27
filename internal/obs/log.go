// Package obs holds the observability primitives every DFS binary shares:
// structured logging, request correlation, Prometheus metrics, and the HTTP
// middleware that ties them together.
package obs

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds the process logger. format is "json" (production, ships to
// Loki) or "text" (local development, human readable).
func NewLogger(service, level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(&contextHandler{Handler: h}).With(slog.String("service", service))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// contextHandler promotes correlation values from the context onto every
// record, so a log line emitted five layers deep still carries its request ID
// without anyone having to thread a logger through the call stack.
type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestIDFrom(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}
