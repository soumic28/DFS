// Command dfs-meta is the cluster coordinator: namespace, chunk placement,
// node membership, repair scheduling and garbage collection.
//
// Phase 0 scaffold — it starts, reports health, and exposes metrics. The gRPC
// service and PostgreSQL wiring land in Phase 2.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/soumi/dfs/internal/app"
)

func main() {
	a := app.New("dfs-meta")

	// Read service config up front so Run reports every problem at once.
	cfg := struct {
		listenAddr        string
		databaseURL       string
		replicationFactor int
		writeQuorum       int
	}{
		listenAddr: a.Cfg.String("META_LISTEN_ADDR", ":9090"),
		// Required, not defaulted: a coordinator that invents its own
		// database URL will happily start against the wrong database.
		databaseURL:       a.Cfg.Required("DATABASE_URL"),
		replicationFactor: a.Cfg.Int("REPLICATION_FACTOR", 3),
		writeQuorum:       a.Cfg.Int("WRITE_QUORUM", 2),
	}

	err := a.Run(func(_ context.Context) error {
		a.Log.Info("coordinator configured",
			slog.String("listen_addr", cfg.listenAddr),
			slog.Int("replication_factor", cfg.replicationFactor),
			slog.Int("write_quorum", cfg.writeQuorum),
			// Never log the DSN itself — it carries the database password.
			slog.Bool("database_configured", cfg.databaseURL != ""),
		)

		// TODO(phase-2): connect to PostgreSQL, run migrations, serve
		// api/proto/metadata/v1 over gRPC on cfg.listenAddr.
		// TODO(phase-3): node registry and heartbeat stream.
		// TODO(phase-4): repair scheduler, rebalancer, garbage collector.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-meta exited", slog.Any("error", err))
		os.Exit(1)
	}
}
