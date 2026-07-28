// Package restapi implements the native DFS HTTP API.
//
// It shares a storage engine with the S3-compatible surface added in Phase 5;
// only the wire format and the authentication scheme differ between them.
package restapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/soumi/dfs/internal/app"
	"github.com/soumi/dfs/internal/gateway"
	"github.com/soumi/dfs/internal/obs"
)

type handlers struct {
	engine *gateway.Engine
	log    *slog.Logger
}

// Router builds the native API handler.
func Router(log *slog.Logger, engine *gateway.Engine) http.Handler {
	h := &handlers{engine: engine, log: log}
	mux := http.NewServeMux()

	route := func(pattern, label string, fn http.HandlerFunc) {
		mux.Handle(pattern, obs.Route(label, fn))
	}

	route("GET /v1/version", "/v1/version", version)

	route("PUT /v1/b/{bucket}", "/v1/b/{bucket}", h.createBucket)
	route("GET /v1/b/{bucket}/o", "/v1/b/{bucket}/o", h.listObjects)

	// {key...} is a Go 1.22+ wildcard that matches the remaining path
	// including slashes, so "photos/2026/cat.jpg" is a single key rather than
	// three path segments.
	route("PUT /v1/b/{bucket}/o/{key...}", "/v1/b/{bucket}/o/{key}", h.putObject)
	route("GET /v1/b/{bucket}/o/{key...}", "/v1/b/{bucket}/o/{key}", h.getObject)
	route("HEAD /v1/b/{bucket}/o/{key...}", "/v1/b/{bucket}/o/{key}", h.headObject)
	route("DELETE /v1/b/{bucket}/o/{key...}", "/v1/b/{bucket}/o/{key}", h.deleteObject)

	// TODO(phase-5): JWT auth middleware, presigned URLs, per-user quotas.

	return obs.Chain(mux,
		obs.RequestID,
		obs.Metrics,
		func(next http.Handler) http.Handler { return obs.AccessLog(log, next) },
		recoverPanic(log),
	)
}

func version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": app.Version,
		"commit":  app.Commit,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// recoverPanic keeps one bad request from taking down a gateway that is
// concurrently streaming other people's uploads.
func recoverPanic(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				// ErrAbortHandler is the stdlib's documented way to abandon a
				// response mid-stream. It is deliberate, not a bug, so let it
				// through to the server rather than logging it as a panic.
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.ErrorContext(r.Context(), "panic recovered",
					slog.Any("panic", v),
					slog.String("path", r.URL.Path))
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}()
			next.ServeHTTP(w, r)
		})
	}
}
