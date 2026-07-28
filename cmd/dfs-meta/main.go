// Command dfs-meta is the cluster coordinator: namespace, chunk placement,
// node membership, repair scheduling and garbage collection.
//
// It is the only component holding strongly consistent state, and the only one
// whose loss is unrecoverable — chunks without metadata are undecodable bytes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	metadatav1 "github.com/soumic28/dfs/api/gen/metadata/v1"
	"github.com/soumic28/dfs/internal/app"
	"github.com/soumic28/dfs/internal/config"
	"github.com/soumic28/dfs/internal/meta"
)

func main() {
	a := app.New("dfs-meta")

	cfg := struct {
		listenAddr        string
		databaseURL       string
		replicationFactor int
		writeQuorum       int
		bootstrapNodes    string
		nodeCapacity      int64
	}{
		listenAddr: a.Cfg.String("META_LISTEN_ADDR", ":9090"),
		// Required, not defaulted: a coordinator that invents its own database
		// URL will happily start against the wrong database.
		databaseURL:       a.Cfg.Required("DATABASE_URL"),
		replicationFactor: a.Cfg.Int("REPLICATION_FACTOR", 3),
		writeQuorum:       a.Cfg.Int("WRITE_QUORUM", 2),
		// Phase 2 seeds the node table from configuration, as "id=addr" pairs.
		// Phase 3 replaces this with registration over the heartbeat stream.
		bootstrapNodes: a.Cfg.String("BOOTSTRAP_NODES", ""),
		nodeCapacity:   a.Cfg.Bytes("NODE_CAPACITY_BYTES", 13*config.GiB),
	}

	if cfg.writeQuorum > cfg.replicationFactor {
		a.Log.Warn("write quorum exceeds replication factor; clamping",
			slog.Int("write_quorum", cfg.writeQuorum),
			slog.Int("replication_factor", cfg.replicationFactor))
		cfg.writeQuorum = cfg.replicationFactor
	}

	err := a.Run(func(ctx context.Context) error {
		store, err := meta.Open(ctx, cfg.databaseURL)
		if err != nil {
			return fmt.Errorf("open metadata store: %w", err)
		}
		a.OnShutdown(func(context.Context) error {
			a.Log.Info("closing database pool")
			store.Close()
			return nil
		})

		// Migrate before serving: the process either understands the schema it
		// is about to use, or fails to start.
		migrateCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := store.Migrate(migrateCtx, a.Log); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}

		if err := seedNodes(ctx, store, a.Log, cfg.bootstrapNodes, cfg.nodeCapacity); err != nil {
			return fmt.Errorf("seed nodes: %w", err)
		}

		srv := grpc.NewServer(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    30 * time.Second,
				Timeout: 10 * time.Second,
			}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             10 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		metadatav1.RegisterMetadataServer(srv,
			meta.NewServer(store, a.Log, cfg.replicationFactor, cfg.writeQuorum))

		ln, err := net.Listen("tcp", cfg.listenAddr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.listenAddr, err)
		}

		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				a.Fatal(fmt.Errorf("grpc server: %w", err))
			}
		}()

		a.OnShutdown(func(ctx context.Context) error {
			a.Log.Info("draining grpc connections")
			done := make(chan struct{})
			go func() {
				srv.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				a.Log.Warn("grpc drain timed out; forcing stop")
				srv.Stop()
				return nil
			}
		})

		a.Log.Info("coordinator ready",
			slog.String("listen_addr", cfg.listenAddr),
			slog.Int("replication_factor", cfg.replicationFactor),
			slog.Int("write_quorum", cfg.writeQuorum),
			// Never log the DSN itself — it carries the database password.
			slog.Bool("database_configured", cfg.databaseURL != ""),
		)

		// TODO(phase-3): node heartbeat stream and failure detector.
		// TODO(phase-4): repair scheduler, rebalancer, garbage collector.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-meta exited", slog.Any("error", err))
		os.Exit(1)
	}
}

// seedNodes registers statically configured nodes, as "id=addr,id=addr".
//
// This exists only so Phase 2 has somewhere to put chunks before real node
// registration lands in Phase 3. It is idempotent, so restarts are harmless.
func seedNodes(ctx context.Context, store *meta.Store, log *slog.Logger, spec string, capacity int64) error {
	if spec == "" {
		return nil
	}

	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "=")
		if !ok {
			return fmt.Errorf("malformed bootstrap node %q, want id=addr", entry)
		}
		if _, err := store.RegisterNode(ctx, strings.TrimSpace(id), strings.TrimSpace(addr), "", capacity); err != nil {
			return fmt.Errorf("register %s: %w", id, err)
		}
		log.Info("bootstrapped storage node",
			slog.String("node_id", id), slog.String("addr", addr))
	}
	return nil
}
