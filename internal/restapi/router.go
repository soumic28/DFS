// Package restapi implements the native DFS HTTP API.
//
// It shares a storage engine with the S3-compatible surface in internal/s3;
// only the wire format and the authentication scheme differ between them.
package restapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/soumi/dfs/internal/app"
	"github.com/soumi/dfs/internal/obs"
)

// Router builds the native API handler.
func Router(log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /v1/version", obs.Route("/v1/version", http.HandlerFunc(version)))

	// TODO(phase-2): PUT/GET/DELETE /v1/b/{bucket}/o/{key}, GET /v1/b/{bucket}/o
	// TODO(phase-5): JWT auth middleware, presigned URLs, quotas

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

// recoverPanic keeps one bad request from taking down a gateway that is
// concurrently streaming other people's uploads.
func recoverPanic(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.ErrorContext(r.Context(), "panic recovered",
						slog.Any("panic", v),
						slog.String("path", r.URL.Path),
					)
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
