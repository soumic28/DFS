package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/soumic28/dfs/internal/chunk"
)

var (
	scrubChunksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dfs_scrub_chunks_verified_total",
		Help: "Chunks read and re-hashed by the background scrubber.",
	})
	scrubBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dfs_scrub_bytes_total",
		Help: "Bytes read by the background scrubber.",
	})
	scrubCorruptTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dfs_scrub_corrupt_chunks_total",
		Help: "Chunks found corrupt at rest. Any non-zero value warrants investigation.",
	})
	scrubSweepDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dfs_scrub_last_sweep_duration_seconds",
		Help: "Duration of the most recently completed full scrub sweep.",
	})
	scrubProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dfs_scrub_progress_ratio",
		Help: "Fraction of the current sweep completed, 0 to 1.",
	})
)

// CorruptFunc is called when a chunk fails verification at rest.
//
// In Phase 4 this reports to the coordinator so the repair pipeline can fetch
// a good copy from a peer. The chunk is *not* deleted: a corrupt replica still
// proves the chunk was placed here, and deleting it before a replacement
// exists would reduce durability rather than restore it.
type CorruptFunc func(id chunk.ID, err error)

// ScrubOptions configures the background scrubber.
type ScrubOptions struct {
	// Interval is the target time for one full sweep of every chunk. The
	// scrubber paces itself to finish in roughly this long rather than
	// running flat out — on a 2-vCPU box, an unthrottled scrub competes
	// directly with live reads.
	Interval time.Duration

	// MinBytesPerSecond floors the pacing so a nearly-empty node still
	// completes sweeps promptly.
	MinBytesPerSecond int64

	OnCorrupt CorruptFunc
	Logger    *slog.Logger
}

// Scrubber continuously re-reads and re-hashes stored chunks to catch bitrot.
//
// This is the only defence against silent at-rest corruption. Without it, a
// flipped bit is discovered when someone reads the chunk — which, for cold
// data, may be never, or may be the moment after the last good replica died.
type Scrubber struct {
	store *Store
	opts  ScrubOptions
	log   *slog.Logger
}

// NewScrubber returns a scrubber for s.
func NewScrubber(s *Store, opts ScrubOptions) *Scrubber {
	if opts.Interval <= 0 {
		opts.Interval = 168 * time.Hour // weekly
	}
	if opts.MinBytesPerSecond <= 0 {
		opts.MinBytesPerSecond = 1 << 20 // 1 MiB/s
	}
	if opts.Logger == nil {
		opts.Logger = s.log
	}
	return &Scrubber{store: s, opts: opts, log: opts.Logger}
}

// Run sweeps until ctx is cancelled. It is intended to be called in its own
// goroutine and returns only on cancellation.
func (sc *Scrubber) Run(ctx context.Context) {
	sc.log.Info("scrubber started",
		slog.Duration("sweep_interval", sc.opts.Interval))

	for {
		start := time.Now()

		if err := sc.Sweep(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				sc.log.Info("scrubber stopped")
				return
			}
			sc.log.Error("scrub sweep failed", slog.Any("error", err))
		} else {
			scrubSweepDuration.Set(time.Since(start).Seconds())
		}

		// If a sweep finished early, idle out the remainder of the interval.
		if rest := sc.opts.Interval - time.Since(start); rest > 0 {
			select {
			case <-ctx.Done():
				sc.log.Info("scrubber stopped")
				return
			case <-time.After(rest):
			}
		}
	}
}

// Sweep verifies every chunk once, pacing itself to take about Interval.
func (sc *Scrubber) Sweep(ctx context.Context) error {
	chunks, err := sc.store.idx.ScrubOrder()
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}
	if len(chunks) == 0 {
		scrubProgress.Set(1)
		return nil
	}

	var totalBytes int64
	for _, c := range chunks {
		totalBytes += c.Size
	}

	// Pace by bytes rather than by chunk count: chunks vary in size, and it
	// is bytes read that competes with live traffic for disk and CPU.
	rate := float64(totalBytes) / sc.opts.Interval.Seconds()
	if min := float64(sc.opts.MinBytesPerSecond); rate < min {
		rate = min
	}

	sc.log.Debug("scrub sweep starting",
		slog.Int("chunks", len(chunks)),
		slog.Int64("bytes", totalBytes),
		slog.Float64("bytes_per_second", rate))

	var done int64
	for n, info := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}

		started := time.Now()
		if err := sc.verify(info.ID); err != nil {
			switch {
			case errors.Is(err, ErrNotFound):
				// Deleted between listing and verifying. Normal.
			case errors.Is(err, chunk.ErrChecksumMismatch):
				scrubCorruptTotal.Inc()
				sc.log.Error("chunk is corrupt at rest",
					slog.String("chunk", info.ID.String()),
					slog.Int64("size", info.Size),
					slog.Any("error", err))
				if sc.opts.OnCorrupt != nil {
					sc.opts.OnCorrupt(info.ID, err)
				}
			default:
				sc.log.Warn("could not scrub chunk",
					slog.String("chunk", info.ID.Short()), slog.Any("error", err))
			}
		} else {
			if err := sc.store.idx.MarkScrubbed(info.ID, time.Now()); err != nil {
				sc.log.Warn("could not record scrub time",
					slog.String("chunk", info.ID.Short()), slog.Any("error", err))
			}
		}

		scrubChunksTotal.Inc()
		scrubBytesTotal.Add(float64(info.Size))
		done += info.Size
		scrubProgress.Set(float64(n+1) / float64(len(chunks)))

		// Sleep off the difference between how long reading this chunk
		// should have taken at the target rate and how long it did take.
		budget := time.Duration(float64(info.Size) / rate * float64(time.Second))
		if pause := budget - time.Since(started); pause > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}

	return nil
}

// verify reads a chunk end to end. Get's verifying reader does the checking;
// draining it to EOF is what triggers the comparison.
func (sc *Scrubber) verify(id chunk.ID) error {
	rc, _, err := sc.store.Get(id, 0, 0)
	if err != nil {
		return err
	}
	defer rc.Close()

	if _, err := io.Copy(io.Discard, rc); err != nil {
		return err
	}
	return nil
}

func sortByLastScrubbed(in []Info) {
	sort.Slice(in, func(a, b int) bool {
		return in[a].LastScrubbedAt.Before(in[b].LastScrubbedAt)
	})
}
