// Command dfs-node is a storage node: a content-addressed blob store that
// serves chunks, verifies them, and repairs them.
//
// Phase 0 scaffold — the blob store itself is Phase 1.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/soumi/dfs/internal/app"
	"github.com/soumi/dfs/internal/config"
)

func main() {
	a := app.New("dfs-node")

	cfg := struct {
		listenAddr    string
		metaAddr      string
		nodeID        string
		dataDir       string
		capacityBytes int64
		scrubInterval time.Duration
	}{
		listenAddr: a.Cfg.String("NODE_LISTEN_ADDR", ":9091"),
		// Required: a node must be told where its coordinator is and who it
		// is. Defaulting either one is how you end up with six nodes that all
		// think they are node-1.
		metaAddr: a.Cfg.Required("META_ADDR"),
		nodeID:   a.Cfg.Required("NODE_ID"),
		dataDir:  a.Cfg.String("DATA_DIR", "/data"),
		// Capacity is enforced in-process, never by letting the disk fill.
		// A full root filesystem takes PostgreSQL down with it, and the
		// metadata is the only part of this system that cannot be rebuilt.
		capacityBytes: a.Cfg.Bytes("NODE_CAPACITY_BYTES", 13*config.GiB),
		scrubInterval: a.Cfg.Duration("SCRUB_INTERVAL", 168*time.Hour),
	}

	err := a.Run(func(_ context.Context) error {
		a.Log.Info("storage node configured",
			slog.String("node_id", cfg.nodeID),
			slog.String("listen_addr", cfg.listenAddr),
			slog.String("meta_addr", cfg.metaAddr),
			slog.String("data_dir", cfg.dataDir),
			slog.String("capacity", config.FormatBytes(cfg.capacityBytes)),
			slog.Duration("scrub_interval", cfg.scrubInterval),
		)

		// TODO(phase-1): open the blob store at cfg.dataDir, purge tmp/,
		// reconcile the BoltDB index against disk, serve
		// api/proto/storage/v1 over gRPC, start the scrubber.
		// TODO(phase-3): register with the coordinator and open the
		// bidirectional heartbeat stream.
		// TODO(phase-4): handle PullChunk repair orders.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-node exited", slog.Any("error", err))
		os.Exit(1)
	}
}
