// Package app wires the parts every DFS binary needs: configuration, logging,
// the admin endpoint, and an orderly shutdown.
//
// The point is that cmd/*/main.go stays declarative. If you find yourself
// writing branching logic in a main(), it belongs in internal/ instead.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/soumic28/dfs/internal/config"
	"github.com/soumic28/dfs/internal/obs"
)

// Build metadata, injected at link time by the Dockerfile:
//
//	-ldflags "-X github.com/soumic28/dfs/internal/app.Version=... -X ...Commit=..."
var (
	Version = "dev"
	Commit  = "unknown"
)

// App holds a service's shared runtime dependencies.
type App struct {
	Name string
	Log  *slog.Logger
	Cfg  *config.Loader

	adminAddr       string
	shutdownTimeout time.Duration

	ready atomic.Bool
	live  atomic.Bool

	mu       sync.Mutex
	shutdown []func(context.Context) error

	fatal chan error
}

// New reads the base configuration shared by all services and returns an App.
// Service-specific settings are read from the returned App's Cfg; call
// Cfg.Err() through Run, which reports every configuration problem at once.
//
// New does not return when the process was invoked as `<service> probe`: it
// runs the built-in healthcheck and exits with the probe's status.
func New(name string) *App {
	cfg := config.New("DFS")
	adminAddr := cfg.String("ADMIN_ADDR", ":9100")

	maybeRunProbe(adminAddr)

	a := &App{
		Name:            name,
		Cfg:             cfg,
		adminAddr:       adminAddr,
		shutdownTimeout: cfg.Duration("SHUTDOWN_TIMEOUT", 30*time.Second),
		fatal:           make(chan error, 1),
	}
	a.Log = obs.NewLogger(name,
		cfg.String("LOG_LEVEL", "info"),
		cfg.String("LOG_FORMAT", "json"),
	)
	a.live.Store(true)

	obs.SetBuildInfo(name, Version, Commit)
	return a
}

// Ready flips /readyz to 200. Call it once the service can actually serve
// traffic — connected to its database, registered with the coordinator, disk
// index recovered. Until then Docker and Caddy will keep traffic away.
func (a *App) Ready() {
	a.ready.Store(true)
	a.Log.Info("service ready", slog.String("version", Version), slog.String("commit", Commit))
}

// OnShutdown registers a cleanup function. They run in reverse registration
// order, so dependencies registered first are torn down last.
func (a *App) OnShutdown(fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.shutdown = append(a.shutdown, fn)
}

// Fatal reports an unrecoverable failure from a background goroutine and
// triggers a graceful shutdown. Safe to call more than once; the first error
// wins and the rest are dropped.
func (a *App) Fatal(err error) {
	select {
	case a.fatal <- err:
	default:
	}
}

// Run starts the admin server, invokes start, and blocks until the process is
// signalled or a goroutine calls Fatal.
//
// start must be non-blocking: launch listeners in goroutines and return. Any
// error it returns aborts startup before the service is marked ready.
func (a *App) Run(start func(ctx context.Context) error) error {
	if err := a.Cfg.Err(); err != nil {
		// The logger exists even when config is broken, so this failure is
		// visible in the same log stream as everything else.
		a.Log.Error("invalid configuration", slog.Any("error", err))
		return fmt.Errorf("configuration: %w", err)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	admin := a.startAdminServer()

	a.Log.Info("starting",
		slog.String("version", Version),
		slog.String("commit", Commit),
		slog.String("admin_addr", a.adminAddr),
	)

	if err := start(ctx); err != nil {
		a.shutdownAll(admin)
		return fmt.Errorf("start %s: %w", a.Name, err)
	}

	var runErr error
	select {
	case <-ctx.Done():
		a.Log.Info("shutdown signal received")
	case err := <-a.fatal:
		runErr = err
		a.Log.Error("fatal error, shutting down", slog.Any("error", err))
	}

	// Fail readiness first so load balancers stop sending new work while
	// in-flight requests drain.
	a.ready.Store(false)

	return errors.Join(runErr, a.shutdownAll(admin))
}

func (a *App) shutdownAll(admin *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()

	a.mu.Lock()
	fns := make([]func(context.Context) error, len(a.shutdown))
	copy(fns, a.shutdown)
	a.mu.Unlock()

	var errs []error
	for i := len(fns) - 1; i >= 0; i-- {
		if err := fns[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}

	a.live.Store(false)
	if err := admin.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("admin server: %w", err))
	}

	a.Log.Info("stopped")
	return errors.Join(errs...)
}

// startAdminServer exposes health and metrics on a port that is never
// published outside the Docker network.
func (a *App) startAdminServer() *http.Server {
	mux := http.NewServeMux()

	// Liveness: is the process functioning at all? A failure here means
	// "restart me". Keep it dependency-free — checking the database here
	// turns a brief DB blip into a restart loop across every container.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.live.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		writePlain(w, http.StatusOK, "ok")
	})

	// Readiness: can it serve traffic right now? A failure means "route
	// around me", not "restart me".
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready.Load() {
			writePlain(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writePlain(w, http.StatusOK, "ready")
	})

	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              a.adminAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Fatal(fmt.Errorf("admin server: %w", err))
		}
	}()

	return srv
}

func writePlain(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(msg))
}
