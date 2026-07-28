// Command dfs-gateway is the client-facing tier. It terminates the native REST
// API and (from Phase 5) the S3-compatible API, runs the chunking pipeline on
// upload, and assembles objects from chunks on download.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	metadatav1 "github.com/soumic28/dfs/api/gen/metadata/v1"
	"github.com/soumic28/dfs/internal/app"
	"github.com/soumic28/dfs/internal/config"
	"github.com/soumic28/dfs/internal/gateway"
	"github.com/soumic28/dfs/internal/restapi"
)

func main() {
	a := app.New("dfs-gateway")

	cfg := struct {
		listenAddr        string
		metaAddr          string
		chunkSize         int64
		ecThreshold       int64
		readHeaderTimeout time.Duration
		idleTimeout       time.Duration
		maxHeaderBytes    int
	}{
		listenAddr:  a.Cfg.String("GATEWAY_LISTEN_ADDR", ":8080"),
		metaAddr:    a.Cfg.Required("META_ADDR"),
		chunkSize:   a.Cfg.Bytes("CHUNK_SIZE_BYTES", 8*config.MiB),
		ecThreshold: a.Cfg.Bytes("EC_THRESHOLD_BYTES", 1*config.MiB),
		// No overall connection timeout: uploads and downloads are long-lived
		// streams, and a WriteTimeout would sever a legitimate multi-gigabyte
		// transfer mid-flight. Slow-loris protection comes from
		// ReadHeaderTimeout and from Caddy in front.
		readHeaderTimeout: a.Cfg.Duration("READ_HEADER_TIMEOUT", 10*time.Second),
		idleTimeout:       a.Cfg.Duration("IDLE_TIMEOUT", 120*time.Second),
		maxHeaderBytes:    a.Cfg.Int("MAX_HEADER_BYTES", 1<<20),
	}

	err := a.Run(func(_ context.Context) error {
		metaConn, err := grpc.NewClient(cfg.metaAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err != nil {
			return fmt.Errorf("connect to coordinator at %s: %w", cfg.metaAddr, err)
		}
		a.OnShutdown(func(context.Context) error { return metaConn.Close() })

		engine := gateway.New(gateway.Config{
			Meta:      metadatav1.NewMetadataClient(metaConn),
			Log:       a.Log,
			ChunkSize: cfg.chunkSize,
		})
		a.OnShutdown(func(context.Context) error {
			a.Log.Info("closing storage node connections")
			return engine.Close()
		})

		srv := &http.Server{
			Addr:              cfg.listenAddr,
			Handler:           restapi.Router(a.Log, engine),
			ReadHeaderTimeout: cfg.readHeaderTimeout,
			IdleTimeout:       cfg.idleTimeout,
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

		a.Log.Info("gateway ready",
			slog.String("listen_addr", cfg.listenAddr),
			slog.String("meta_addr", cfg.metaAddr),
			slog.String("chunk_size", config.FormatBytes(cfg.chunkSize)),
			slog.String("ec_threshold", config.FormatBytes(cfg.ecThreshold)),
		)

		// TODO(phase-5): mount the S3 handler and SigV4 verification.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-gateway exited", slog.Any("error", err))
		os.Exit(1)
	}
}
