package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/soumic28/dfs/internal/chunk"
)

func openAt(t *testing.T, root string) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:   root,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Open(%s): %v", root, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// A crash mid-write leaves a partial temp file. It must never be visible as a
// chunk, and must not survive the restart.
func TestRecoveryDiscardsInterruptedWrites(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)

	good := put(t, s, []byte("committed before the crash"))

	// Simulate a process killed partway through streaming a chunk to disk:
	// bytes in tmp/, nothing renamed, no index entry.
	partial := filepath.Join(s.tmp, "abcdef01-partial")
	if err := os.WriteFile(partial, []byte("half a chun"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openAt(t, root)

	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Error("interrupted write survived restart; tmp/ was not purged")
	}
	if got := readAll(t, s2, good); string(got) != "committed before the crash" {
		t.Error("committed chunk did not survive restart")
	}
	if u := s2.Usage(); u.ChunkCount != 1 {
		t.Errorf("chunk count after recovery = %d, want 1", u.ChunkCount)
	}
}

// A crash between rename(2) and the index write leaves a valid chunk file with
// no index entry. The file was hash-verified before the rename, so it is real
// data and must be adopted rather than discarded — throwing it away would be
// silent data loss during a routine restart.
func TestRecoveryAdoptsOrphanedChunkFiles(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)

	data := []byte("renamed into place, never indexed")
	id := chunk.Sum(data)

	// Write the file exactly where a successful rename would have put it,
	// then close without ever recording it.
	dst := s.Path(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openAt(t, root)

	info, ok, err := s2.Stat(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("orphaned chunk file was not adopted; this is silent data loss")
	}
	if info.Size != int64(len(data)) {
		t.Errorf("adopted size = %d, want %d", info.Size, len(data))
	}
	if got := readAll(t, s2, id); !bytes.Equal(got, data) {
		t.Error("adopted chunk does not read back correctly")
	}
	if u := s2.Usage(); u.UsedBytes != int64(len(data)) {
		t.Errorf("used bytes after adoption = %d, want %d", u.UsedBytes, len(data))
	}
}

// An index entry whose file has vanished is a lie. Reads must report a miss so
// the caller fails over to another replica, rather than returning an error
// that looks like the whole node is broken.
func TestRecoveryDropsIndexEntriesWithNoFile(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)

	id := put(t, s, []byte("about to lose its file"))
	survivor := put(t, s, []byte("still here"))

	if err := os.Remove(s.Path(id)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openAt(t, root)

	if _, ok, _ := s2.Stat(id); ok {
		t.Error("index still claims to hold a chunk whose file is gone")
	}
	if _, _, err := s2.Get(id, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get: err = %v, want ErrNotFound so the reader fails over", err)
	}
	if _, ok, _ := s2.Stat(survivor); !ok {
		t.Error("recovery dropped a healthy chunk")
	}
	if u := s2.Usage(); u.ChunkCount != 1 {
		t.Errorf("chunk count = %d, want 1", u.ChunkCount)
	}
}

// Losing index.db entirely must cost a rescan, not the data. The chunk files
// carry their own identity in their names, so the index is fully rebuildable.
func TestRecoveryRebuildsAfterIndexLoss(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)

	want := map[chunk.ID][]byte{}
	for _, text := range []string{"alpha", "beta", "gamma", "delta"} {
		data := bytes.Repeat([]byte(text), 100)
		want[put(t, s, data)] = data
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(root, indexFile)); err != nil {
		t.Fatal(err)
	}

	s2 := openAt(t, root)

	if u := s2.Usage(); u.ChunkCount != int64(len(want)) {
		t.Fatalf("recovered %d chunks, want %d", u.ChunkCount, len(want))
	}
	for id, data := range want {
		if got := readAll(t, s2, id); !bytes.Equal(got, data) {
			t.Errorf("chunk %s did not survive index loss", id.Short())
		}
	}
	if n := chunkFileCount(t, root); n != len(want) {
		t.Errorf("%d chunk files on disk, want %d", n, len(want))
	}
}

// Usage must be reconstructed from what is actually on disk, not carried over
// from a stale counter, or a node slowly lies about how full it is.
func TestRecoveryRecomputesUsage(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)

	var total int64
	for i := range 5 {
		data := bytes.Repeat([]byte{byte(i)}, 1000+i)
		put(t, s, data)
		total += int64(len(data))
	}
	before := s.Usage()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	after := openAt(t, root).Usage()

	if after.UsedBytes != total || after.UsedBytes != before.UsedBytes {
		t.Errorf("used bytes: before %d, after %d, actual %d", before.UsedBytes, after.UsedBytes, total)
	}
	if after.ChunkCount != 5 {
		t.Errorf("chunk count after restart = %d, want 5", after.ChunkCount)
	}
	if after.PendingBytes != 0 {
		t.Errorf("pending bytes after restart = %d, want 0", after.PendingBytes)
	}
}

// Files that are not chunks must be ignored rather than crashing the node at
// boot — an operator poking around in the data directory should not be able to
// prevent it from starting.
func TestRecoveryIgnoresForeignFiles(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)
	id := put(t, s, []byte("real chunk"))

	junk := filepath.Join(root, chunksDir, "ab", "notes.txt")
	if err := os.MkdirAll(filepath.Dir(junk), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(junk, []byte("operator was here"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openAt(t, root)

	if u := s2.Usage(); u.ChunkCount != 1 {
		t.Errorf("chunk count = %d, want 1 (foreign file was counted)", u.ChunkCount)
	}
	if _, ok, _ := s2.Stat(id); !ok {
		t.Error("real chunk was lost")
	}
}

// Repeated restarts must converge, not accumulate drift.
func TestRecoveryIsIdempotent(t *testing.T) {
	root := t.TempDir()
	s := openAt(t, root)
	id := put(t, s, bytes.Repeat([]byte("stable"), 200))
	first := s.Usage()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		st, err := Open(Options{Root: root, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		u := st.Usage()
		if u.UsedBytes != first.UsedBytes || u.ChunkCount != first.ChunkCount {
			t.Errorf("restart %d: usage drifted to %d bytes / %d chunks, want %d / %d",
				i, u.UsedBytes, u.ChunkCount, first.UsedBytes, first.ChunkCount)
		}
		if _, ok, _ := st.Stat(id); !ok {
			t.Errorf("restart %d: chunk disappeared", i)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A cancelled upload must leave nothing behind — not a chunk, not a temp file,
// not a leaked reservation.
func TestCancelledPutLeavesNothing(t *testing.T) {
	s := newStore(t, 1<<20)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	data := bytes.Repeat([]byte("cancelled"), 1000)
	id := chunk.Sum(data)
	if _, err := s.Put(ctx, id, int64(len(data)), bytes.NewReader(data)); err == nil {
		t.Fatal("Put with a cancelled context succeeded, want an error")
	}

	if _, ok, _ := s.Stat(id); ok {
		t.Error("cancelled write was committed")
	}
	if u := s.Usage(); u.PendingBytes != 0 || u.UsedBytes != 0 {
		t.Errorf("usage after cancellation = %d used / %d pending, want 0 / 0", u.UsedBytes, u.PendingBytes)
	}
	assertNoTempFilesLeft(t, s)
}
