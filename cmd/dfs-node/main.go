// Command dfs-node is a storage node: a content-addressed blob store that
// serves chunks over gRPC, verifies them on every read, and scrubs them at
// rest to catch corruption before it becomes loss.
//
// It knows nothing about buckets, objects, users, or where any other chunk
// lives. That is deliberate — it keeps the component every durability
// guarantee rests on small enough to reason about.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	storagev1 "github.com/soumic28/dfs/api/gen/storage/v1"
	"github.com/soumic28/dfs/internal/app"
	"github.com/soumic28/dfs/internal/blobstore"
	"github.com/soumic28/dfs/internal/chunk"
	"github.com/soumic28/dfs/internal/config"
	"github.com/soumic28/dfs/internal/node"
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
		maxRecvBytes  int
	}{
		listenAddr: a.Cfg.String("NODE_LISTEN_ADDR", ":9091"),
		// Required: a node must be told where its coordinator is and who it
		// is. Defaulting either one is how you end up with six nodes that all
		// believe they are node-1.
		metaAddr: a.Cfg.Required("META_ADDR"),
		nodeID:   a.Cfg.Required("NODE_ID"),
		dataDir:  a.Cfg.String("DATA_DIR", "/data"),
		// Capacity is enforced in-process, never by letting the disk fill. A
		// full root filesystem takes PostgreSQL down with it, and the
		// metadata is the only part of this system that cannot be rebuilt.
		capacityBytes: a.Cfg.Bytes("NODE_CAPACITY_BYTES", 13*config.GiB),
		scrubInterval: a.Cfg.Duration("SCRUB_INTERVAL", 168*time.Hour),
		maxRecvBytes:  int(a.Cfg.Bytes("GRPC_MAX_RECV_BYTES", 8*config.MiB)),
	}

	err := a.Run(func(ctx context.Context) error {
		store, err := blobstore.Open(blobstore.Options{
			Root:     cfg.dataDir,
			Capacity: cfg.capacityBytes,
			Logger:   a.Log,
		})
		if err != nil {
			return fmt.Errorf("open blobstore: %w", err)
		}
		a.OnShutdown(func(context.Context) error {
			a.Log.Info("closing blobstore")
			return store.Close()
		})

		startScrubber(ctx, a, store, cfg.scrubInterval)

		srv := grpc.NewServer(
			grpc.MaxRecvMsgSize(cfg.maxRecvBytes),
			// Without a keepalive policy the server drops long-lived streams
			// that are legitimately idle between frames, which is exactly
			// what a slow upload looks like.
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    30 * time.Second,
				Timeout: 10 * time.Second,
			}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             10 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		storagev1.RegisterStorageNodeServer(srv, node.NewServer(store, a.Log, nil))

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
				// A client holding a stream open past the shutdown budget
				// must not stop the process from exiting.
				a.Log.Warn("grpc drain timed out; forcing stop")
				srv.Stop()
				return nil
			}
		})

		usage := store.Usage()
		a.Log.Info("storage node ready",
			slog.String("node_id", cfg.nodeID),
			slog.String("listen_addr", cfg.listenAddr),
			slog.String("meta_addr", cfg.metaAddr),
			slog.String("data_dir", cfg.dataDir),
			slog.String("capacity", config.FormatBytes(usage.CapacityBytes)),
			slog.String("used", config.FormatBytes(usage.UsedBytes)),
			slog.Int64("chunks", usage.ChunkCount),
			slog.Duration("scrub_interval", cfg.scrubInterval),
		)

		// TODO(phase-3): register with the coordinator at cfg.metaAddr and
		// open the bidirectional heartbeat stream.

		a.Ready()
		return nil
	})
	if err != nil {
		slog.Error("dfs-node exited", slog.Any("error", err))
		os.Exit(1)
	}
}

// startScrubber runs continuous at-rest verification in the background.
//
// Corrupt chunks are reported and left in place, never deleted: a corrupt
// replica still proves the chunk was placed here, and removing it before a
// replacement exists lowers durability rather than restoring it.
func startScrubber(ctx context.Context, a *app.App, store *blobstore.Store, interval time.Duration) {
	sc := blobstore.NewScrubber(store, blobstore.ScrubOptions{
		Interval: interval,
		Logger:   a.Log,
		OnCorrupt: func(id chunk.ID, err error) {
			a.Log.Error("corruption detected at rest",
				slog.String("chunk", id.String()), slog.Any("error", err))
			// TODO(phase-4): report to the coordinator so the repair
			// pipeline pulls a good copy from a peer.
		},
	})

	scrubCtx, stopScrub := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc.Run(scrubCtx)
	}()

	a.OnShutdown(func(context.Context) error {
		stopScrub()
		<-done
		return nil
	})
}
