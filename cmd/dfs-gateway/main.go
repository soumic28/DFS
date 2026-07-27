// Command dfs-gateway is the client-facing tier. It terminates the native REST
// API and the S3-compatible API, runs the chunking pipeline on upload, and
// assembles objects from chunks on download.
//
// It is stateless: any request may be served by any instance, which is why
// this is the only tier you scale for throughput.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/soumi/dfs/internal/app"
	"github.com/soumi/dfs/internal/config"
	"github.com/soumi/dfs/internal/restapi"
)

func main() {
	a := app.New("dfs-gateway")

	cfg := struct {
		listenAddr     string
		metaAddr       string
		chunkSize      int64
		ecThreshold    int64
		readTimeout    time.Duration
		writeTimeout   time.Duration
		maxHeaderBytes int
	}{
		listenAddr: a.Cfg.String("GATEWAY_LISTEN_ADDR", ":8080"),
		metaAddr:   a.Cfg.Required("META_ADDR"),
		chunkSize:  a.Cfg.Bytes("CHUNK_SIZE_BYTES", 8*config.MiB),
		// Objects at or above this size are erasure coded (Phase 6); smaller
		// ones stay replicated, where the latency of gathering k shards is
		// not worth the storage saving.
		ecThreshold: a.Cfg.Bytes("EC_THRESHOLD_BYTES", 1*config.MiB),
		// No overall timeouts on the connection: uploads and downloads are
		// long-lived streams and a WriteTimeout would sever a legitimate
		// multi-gigabyte transfer mid-flight. Slow-loris protection comes
		// from ReadHeaderTimeout and from Caddy in front.
		readTimeout:    a.Cfg.Duration("READ_HEADER_TIMEOUT", 10*time.Second),
		writeTimeout:   a.Cfg.Duration("IDLE_TIMEOUT", 120*time.Second),
		maxHeaderBytes: a.Cfg.Int("MAX_HEADER_BYTES", 1<<20),
	}

	err := a.Run(func(_ context.Context) error {
		srv := &http.Server{
			Addr:              cfg.listenAddr,
			Handler:           restapi.Router(a.Log),
			ReadHeaderTimeout: cfg.readTimeout,
			IdleTimeout:       cfg.writeTimeout,
			MaxHeaderBytes:    cfg.maxHeaderBytes,
			ErrorLog:          slog.NewLogLogger(a.Log.Handler(), slog.LevelWarn),
		}

		// Bind before marking ready so a port conflict fails startup loudly
		// instead of leaving a container that is "healthy" but deaf.
		ln, err := net.Listen("tcp", cfg.listenAddr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.listenAddr, err)
		}

		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				a.Fatal(fmt.Errorf("http server: %w", err))
			}
		}()

		// Registered before Ready so a signal arriving during startup still
		// drains the listener.
		a.OnShutdown(func(ctx context.Context) error {
			a.Log.Info("draining http listener")
			return srv.Shutdown(ctx)
		})

		a.Log.Info("gateway configured",
			slog.String("listen_addr", cfg.listenAddr),
			slog.String("meta_addr", cfg.metaAddr),
			slog.String("chunk_size", config.FormatBytes(cfg.chunkSize)),
			slog.String("ec_threshold", config.FormatBytes(cfg.ecThreshold)),
		)

		// TODO(phase-2): dial dfs-meta over gRPC, wire the chunking pipeline
		// and the object handlers into restapi.Router.
		// TODO(phase-5): mount the S3 handler and SigV4 verification.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-gateway exited", slog.Any("error", err))
		os.Exit(1)
	}
}
