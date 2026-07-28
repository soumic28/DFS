package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/soumic28/dfs/internal/chunk"
)

func newStore(t *testing.T, capacity int64) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:     t.TempDir(),
		Capacity: capacity,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func put(t *testing.T, s *Store, data []byte) chunk.ID {
	t.Helper()
	id := chunk.Sum(data)
	if _, err := s.Put(context.Background(), id, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return id
}

func readAll(t *testing.T, s *Store, id chunk.ID) []byte {
	t.Helper()
	rc, _, err := s.Get(id, 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return got
}

// The fundamental contract: whatever bytes go in come back out, unchanged, for
// any input at all.
func TestPutGetRoundTripsAnyBytes(t *testing.T) {
	s := newStore(t, 0)

	f := func(data []byte) bool {
		id := put(t, s, data)
		return bytes.Equal(readAll(t, s, id), data)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}

func TestPutGetEmptyAndLargeChunks(t *testing.T) {
	s := newStore(t, 0)

	for _, size := range []int{0, 1, 4095, 4096, 4097, 1 << 20, 8 << 20} {
		data := make([]byte, size)
		if _, err := rand.New(rand.NewSource(int64(size))).Read(data); err != nil {
			t.Fatal(err)
		}
		id := put(t, s, data)
		if got := readAll(t, s, id); !bytes.Equal(got, data) {
			t.Fatalf("size %d: round trip mismatch (got %d bytes)", size, len(got))
		}
	}
}

// The content-addressing invariant. A chunk whose bytes do not hash to its
// declared name must never reach the store — otherwise every guarantee built
// on "the name is the hash" quietly stops being true.
func TestPutRejectsMisdeclaredContent(t *testing.T) {
	s := newStore(t, 0)

	honest := []byte("the real contents")
	lie := []byte("something else entirely")
	id := chunk.Sum(honest)

	_, err := s.Put(context.Background(), id, int64(len(lie)), bytes.NewReader(lie))
	if !errors.Is(err, chunk.ErrChecksumMismatch) && !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("Put with mismatched content: err = %v, want a checksum or size error", err)
	}
	if _, ok, _ := s.Stat(id); ok {
		t.Error("a chunk that failed verification was stored anyway")
	}
	assertNoTempFilesLeft(t, s)
}

func TestPutRejectsWrongSize(t *testing.T) {
	s := newStore(t, 0)
	data := []byte("exactly twenty chars")
	id := chunk.Sum(data)

	t.Run("declared too small", func(t *testing.T) {
		_, err := s.Put(context.Background(), id, 5, bytes.NewReader(data))
		if !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("err = %v, want ErrSizeMismatch", err)
		}
	})

	t.Run("declared too large", func(t *testing.T) {
		_, err := s.Put(context.Background(), id, 500, bytes.NewReader(data))
		if !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("err = %v, want ErrSizeMismatch", err)
		}
	})

	assertNoTempFilesLeft(t, s)
}

// Ten writers racing on the same chunk must produce one file and no
// corruption. This is the normal case under replication, not an edge case:
// several gateways can fan the same chunk at the same node simultaneously.
func TestConcurrentPutOfSameChunk(t *testing.T) {
	s := newStore(t, 0)

	data := bytes.Repeat([]byte("concurrent"), 100_000) // ~1 MB
	id := chunk.Sum(data)

	const writers = 10
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.Put(context.Background(), id, int64(len(data)), bytes.NewReader(data))
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d: %v", i, err)
		}
	}
	if got := readAll(t, s, id); !bytes.Equal(got, data) {
		t.Error("chunk is corrupt after concurrent writes")
	}

	// Accounting must count the chunk once, however many writers raced.
	if u := s.Usage(); u.ChunkCount != 1 || u.UsedBytes != int64(len(data)) {
		t.Errorf("usage = %d chunks / %d bytes, want 1 / %d", u.ChunkCount, u.UsedBytes, len(data))
	}
	assertNoTempFilesLeft(t, s)
}

func TestPutDeduplicates(t *testing.T) {
	s := newStore(t, 0)
	data := []byte("stored once, referenced many times")

	first, err := s.Put(context.Background(), chunk.Sum(data), int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if first.AlreadyPresent {
		t.Error("first write reported as a duplicate")
	}

	second, err := s.Put(context.Background(), chunk.Sum(data), int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !second.AlreadyPresent {
		t.Error("second write of identical data was not deduplicated")
	}
	if u := s.Usage(); u.ChunkCount != 1 {
		t.Errorf("chunk count = %d, want 1", u.ChunkCount)
	}
}

// Bitrot detection. A chunk altered on disk must not be served as if it were
// good — silent corruption propagating into a repair would poison the healthy
// replicas too.
func TestGetDetectsCorruptionOnDisk(t *testing.T) {
	s := newStore(t, 0)
	data := bytes.Repeat([]byte("intact"), 1000)
	id := put(t, s, data)

	flipOneBit(t, s.Path(id))

	rc, _, err := s.Get(id, 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	if _, err := io.ReadAll(rc); !errors.Is(err, chunk.ErrChecksumMismatch) {
		t.Fatalf("reading a corrupted chunk: err = %v, want ErrChecksumMismatch", err)
	}
}

func TestScrubberFindsCorruption(t *testing.T) {
	s := newStore(t, 0)

	good := put(t, s, bytes.Repeat([]byte("healthy"), 500))
	bad := put(t, s, bytes.Repeat([]byte("rotten"), 500))
	flipOneBit(t, s.Path(bad))

	var reported []chunk.ID
	sc := NewScrubber(s, ScrubOptions{
		Interval:          time.Nanosecond, // no pacing delay in tests
		MinBytesPerSecond: 1 << 40,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnCorrupt:         func(id chunk.ID, _ error) { reported = append(reported, id) },
	})

	if err := sc.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if len(reported) != 1 || reported[0] != bad {
		t.Fatalf("scrubber reported %v, want exactly the corrupt chunk %s", reported, bad.Short())
	}

	// A corrupt chunk is reported, not deleted: it still proves placement,
	// and removing it before a replacement exists lowers durability.
	if _, ok, _ := s.Stat(bad); !ok {
		t.Error("scrubber deleted the corrupt chunk instead of reporting it")
	}
	if _, ok, _ := s.Stat(good); !ok {
		t.Error("scrubber lost a healthy chunk")
	}
}

func TestCapacityIsEnforced(t *testing.T) {
	s := newStore(t, 1000)

	if _, err := s.Put(context.Background(), chunk.Sum(make([]byte, 800)), 800,
		bytes.NewReader(make([]byte, 800))); err != nil {
		t.Fatalf("write within capacity: %v", err)
	}

	over := bytes.Repeat([]byte{7}, 500)
	_, err := s.Put(context.Background(), chunk.Sum(over), int64(len(over)), bytes.NewReader(over))
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("write past capacity: err = %v, want ErrNoCapacity", err)
	}

	if free := s.Usage().Free(); free != 200 {
		t.Errorf("free = %d, want 200", free)
	}
}

// Reservations must be released when a write fails, or a node bleeds capacity
// until it refuses everything.
func TestFailedWriteReleasesReservation(t *testing.T) {
	s := newStore(t, 1000)

	for range 20 {
		data := []byte("wrong size on purpose")
		_, _ = s.Put(context.Background(), chunk.Sum(data), 900, bytes.NewReader(data))
	}

	if p := s.Usage().PendingBytes; p != 0 {
		t.Fatalf("pending bytes = %d after failed writes, want 0 (reservations leaked)", p)
	}
	if _, err := s.Put(context.Background(), chunk.Sum(make([]byte, 900)), 900,
		bytes.NewReader(make([]byte, 900))); err != nil {
		t.Fatalf("store refused a write that should fit: %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := newStore(t, 0)
	data := []byte("temporary")
	id := put(t, s, data)

	existed, err := s.Delete(id)
	if err != nil || !existed {
		t.Fatalf("Delete: existed = %v, err = %v", existed, err)
	}
	if _, _, err := s.Get(id, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete: err = %v, want ErrNotFound", err)
	}
	if u := s.Usage(); u.UsedBytes != 0 || u.ChunkCount != 0 {
		t.Errorf("usage after delete = %d bytes / %d chunks, want 0 / 0", u.UsedBytes, u.ChunkCount)
	}
	if _, err := os.Stat(s.Path(id)); !os.IsNotExist(err) {
		t.Error("chunk file survived deletion")
	}

	existed, err = s.Delete(id)
	if err != nil || existed {
		t.Errorf("second Delete: existed = %v, err = %v, want false, nil", existed, err)
	}
}

func TestRangedReads(t *testing.T) {
	s := newStore(t, 0)
	data := []byte("0123456789abcdefghij")
	id := put(t, s, data)

	for _, tc := range []struct {
		offset, length int64
		want           string
	}{
		{0, 5, "01234"},
		{10, 5, "abcde"},
		{15, 100, "fghij"}, // clamped to the chunk end
		{0, 0, string(data)},
	} {
		rc, _, err := s.Get(id, tc.offset, tc.length)
		if err != nil {
			t.Fatalf("Get(%d,%d): %v", tc.offset, tc.length, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read(%d,%d): %v", tc.offset, tc.length, err)
		}
		if string(got) != tc.want {
			t.Errorf("Get(%d,%d) = %q, want %q", tc.offset, tc.length, got, tc.want)
		}
	}
}

func TestGetMissingChunk(t *testing.T) {
	s := newStore(t, 0)
	if _, _, err := s.Get(chunk.Sum([]byte("never stored")), 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func assertNoTempFilesLeft(t *testing.T, s *Store) {
	t.Helper()
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		t.Fatalf("read tmp dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("temp files leaked: %v", names)
	}
}

func flipOneBit(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(data) == 0 {
		t.Fatal("cannot corrupt an empty file")
	}
	data[len(data)/2] ^= 0x01
	// The file is mode 0600 from CreateTemp; rewrite it in place.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func chunkFileCount(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(root, chunksDir), func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return n
}
