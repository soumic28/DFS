// Package blobstore implements a storage node's local, content-addressed chunk
// store.
//
// Every durability property of the cluster ultimately rests on this package
// being correct, so it is deliberately small and knows nothing about buckets,
// objects, users, replication or peers. It stores bytes under the hash of those
// bytes, and refuses to hand back anything that does not match its name.
//
// # Crash safety
//
// A write goes: temp file -> fsync(file) -> verify hash -> fsync(dir) ->
// rename(2). Rename within a filesystem is atomic, so a chunk file either
// exists complete and verified, or does not exist. There is no third state,
// which makes crash recovery merely "delete the temp directory".
//
// # On-disk layout
//
//	<root>/chunks/ab/cd/abcd...ef.chunk   the bytes; the name is the hash
//	<root>/tmp/                            in-flight writes, purged at boot
//	<root>/index.db                        BoltDB: id -> size, timestamps
package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/soumic28/dfs/internal/chunk"
)

// Errors a caller is expected to distinguish.
var (
	// ErrNotFound means this node holds no such chunk. Callers read from
	// another replica.
	ErrNotFound = errors.New("chunk not found")

	// ErrNoCapacity means the node is at its configured limit. Capacity is
	// enforced here rather than by letting the disk fill, because a full root
	// filesystem takes PostgreSQL down with it and the metadata is the one
	// part of this system that cannot be rebuilt.
	ErrNoCapacity = errors.New("node at capacity")

	// ErrSizeMismatch means the stream length disagreed with the declared
	// size.
	ErrSizeMismatch = errors.New("chunk size mismatch")

	// ErrClosed means the store has been shut down.
	ErrClosed = errors.New("blobstore is closed")
)

const (
	chunksDir = "chunks"
	tmpDir    = "tmp"
	indexFile = "index.db"
	chunkExt  = ".chunk"

	// Two levels of 1-byte hex fanout: 256 dirs, each with up to 256 subdirs.
	// Keeps any single directory small enough that lookups stay cheap.
	fanoutDepth = 2
)

// Options configures a Store.
type Options struct {
	// Root is the data directory. Created if absent.
	Root string

	// Capacity is the maximum committed bytes. Zero means unlimited, which is
	// only appropriate in tests.
	Capacity int64

	Logger *slog.Logger
}

// Info describes a stored chunk.
type Info struct {
	ID             chunk.ID
	Size           int64
	CreatedAt      time.Time
	LastScrubbedAt time.Time
}

// Usage reports how full the node is.
type Usage struct {
	CapacityBytes int64
	UsedBytes     int64
	PendingBytes  int64 // reserved by in-flight writes
	ChunkCount    int64
}

// Free returns the bytes still accepting writes.
func (u Usage) Free() int64 {
	if u.CapacityBytes == 0 {
		return -1 // unlimited
	}
	free := u.CapacityBytes - u.UsedBytes - u.PendingBytes
	if free < 0 {
		return 0
	}
	return free
}

// PutResult reports the outcome of a write.
type PutResult struct {
	// AlreadyPresent is true when the chunk was already stored and no bytes
	// were written. This is deduplication doing its job.
	AlreadyPresent bool
	BytesWritten   int64
}

// Store is a node's local chunk store. It is safe for concurrent use.
type Store struct {
	root     string
	chunks   string
	tmp      string
	capacity int64
	log      *slog.Logger

	idx *index

	// acct guards capacity accounting. Reservations are taken before a write
	// starts and released when it finishes, so N concurrent writers cannot
	// each pass a check that only one of them could satisfy.
	acct    sync.Mutex
	used    int64
	pending int64
	count   int64

	closeOnce sync.Once
	closed    chan struct{}
}

// Open prepares the store at opts.Root: it creates the layout, opens the
// index, purges any temp files left by a crash, and reconciles the index
// against what is actually on disk.
func Open(opts Options) (*Store, error) {
	if opts.Root == "" {
		return nil, errors.New("blobstore: Root is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	s := &Store{
		root:     opts.Root,
		chunks:   filepath.Join(opts.Root, chunksDir),
		tmp:      filepath.Join(opts.Root, tmpDir),
		capacity: opts.Capacity,
		log:      opts.Logger,
		closed:   make(chan struct{}),
	}

	for _, dir := range []string{s.root, s.chunks, s.tmp} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("blobstore: create %s: %w", dir, err)
		}
	}

	idx, err := openIndex(filepath.Join(opts.Root, indexFile))
	if err != nil {
		return nil, fmt.Errorf("blobstore: open index: %w", err)
	}
	s.idx = idx

	if err := s.purgeTmp(); err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("blobstore: purge tmp: %w", err)
	}

	if err := s.recover(); err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("blobstore: recover: %w", err)
	}

	s.log.Info("blobstore opened",
		slog.String("root", s.root),
		slog.Int64("chunks", s.count),
		slog.Int64("used_bytes", s.used),
		slog.Int64("capacity_bytes", s.capacity),
	)
	return s, nil
}

// Close releases the index. In-flight reads keep working until their readers
// are closed; the underlying files are independent of the index.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = s.idx.Close()
	})
	return err
}

func (s *Store) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// Path returns the on-disk location of a chunk, fanned out by hash prefix.
// Exported so operators and tests can find a chunk file directly.
func (s *Store) Path(id chunk.ID) string {
	hex := id.String()
	parts := make([]string, 0, fanoutDepth+2)
	parts = append(parts, s.chunks)
	for i := range fanoutDepth {
		parts = append(parts, hex[i*2:i*2+2])
	}
	return filepath.Join(append(parts, hex+chunkExt)...)
}

// Put stores a chunk. The reader is streamed to disk and hashed as it goes; if
// the resulting digest is not id, nothing is committed.
//
// size must be the exact byte count. Declaring it up front lets the store
// reserve capacity before spending I/O on a write it would have to reject.
func (s *Store) Put(ctx context.Context, id chunk.ID, size int64, r io.Reader) (PutResult, error) {
	if s.isClosed() {
		return PutResult{}, ErrClosed
	}
	if size < 0 {
		return PutResult{}, fmt.Errorf("%w: negative size %d", ErrSizeMismatch, size)
	}

	// Deduplication: an existing chunk with this name necessarily has these
	// contents, because the name is the hash. Nothing to do.
	if _, ok, err := s.idx.Get(id); err != nil {
		return PutResult{}, fmt.Errorf("index lookup: %w", err)
	} else if ok {
		return PutResult{AlreadyPresent: true}, nil
	}

	if err := s.reserve(size); err != nil {
		return PutResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			s.release(size, 0)
		}
	}()

	tmpPath, written, err := s.writeTemp(ctx, id, size, r)
	if err != nil {
		return PutResult{}, err
	}
	defer func() {
		// Best effort: on any failure after this point the temp file is
		// removed here, and anything that survives a crash is purged at boot.
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	finalPath := s.Path(id)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return PutResult{}, fmt.Errorf("create chunk dir: %w", err)
	}

	// The atomic step. Before this the chunk does not exist; after it, it
	// exists complete and hash-verified.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		// A concurrent writer of the same chunk may have won the race. That
		// is harmless — identical content by definition — so treat an
		// existing target as success rather than an error.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			committed = true
			s.release(size, 0)
			return PutResult{AlreadyPresent: true}, nil
		}
		return PutResult{}, fmt.Errorf("commit chunk %s: %w", id.Short(), err)
	}

	// fsync the directory so the rename itself survives power loss. Without
	// this the file's bytes are durable but the name pointing at them may not
	// be, and a chunk you cannot find is a chunk you have lost.
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		s.log.Warn("could not fsync chunk directory",
			slog.String("chunk", id.Short()), slog.Any("error", err))
	}

	inserted, err := s.idx.Put(id, Info{ID: id, Size: written, CreatedAt: time.Now()})
	if err != nil {
		return PutResult{}, fmt.Errorf("index put: %w", err)
	}

	committed = true
	if inserted {
		s.release(size, written)
	} else {
		s.release(size, 0)
	}

	return PutResult{BytesWritten: written}, nil
}

// writeTemp streams r into the temp directory, hashing as it goes, and returns
// the temp path only if the bytes hashed to id and matched size.
func (s *Store) writeTemp(ctx context.Context, id chunk.ID, size int64, r io.Reader) (string, int64, error) {
	f, err := os.CreateTemp(s.tmp, id.Short()+"-*")
	if err != nil {
		return "", 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := f.Name()

	cleanup := func(cause error) (string, int64, error) {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", 0, cause
	}

	hw := chunk.NewHashingWriter(f)
	// Cap the copy one byte above the declared size so an over-long stream is
	// detected rather than silently written.
	written, err := io.Copy(hw, io.LimitReader(r, size+1))
	if err != nil {
		return cleanup(fmt.Errorf("write chunk %s: %w", id.Short(), err))
	}
	if err := ctx.Err(); err != nil {
		return cleanup(err)
	}
	if written != size {
		return cleanup(fmt.Errorf("%w: declared %d bytes, received %d", ErrSizeMismatch, size, written))
	}

	// fsync before verifying: the bytes must be on the platter before we
	// promise anything about them.
	if err := f.Sync(); err != nil {
		return cleanup(fmt.Errorf("fsync chunk %s: %w", id.Short(), err))
	}
	if err := f.Close(); err != nil {
		return cleanup(fmt.Errorf("close chunk %s: %w", id.Short(), err))
	}

	// The content-addressing invariant, enforced at the only point where it
	// could be violated. A chunk whose bytes do not hash to its name never
	// reaches the chunks directory.
	if got := hw.ID(); got != id {
		_ = os.Remove(tmpPath)
		return "", 0, fmt.Errorf("%w: stream hashed to %s, declared %s",
			chunk.ErrChecksumMismatch, got.Short(), id.Short())
	}

	return tmpPath, written, nil
}

// Get opens a chunk for reading.
//
// A full read is hash-verified: the returned reader fails with
// chunk.ErrChecksumMismatch at EOF if the bytes on disk no longer match the
// name. A ranged read cannot be verified without reading the whole chunk, so
// it is not — at-rest corruption in that path is caught by the scrubber
// instead. The caller must Close the reader.
func (s *Store) Get(id chunk.ID, offset, length int64) (io.ReadCloser, Info, error) {
	if s.isClosed() {
		return nil, Info{}, ErrClosed
	}

	info, ok, err := s.idx.Get(id)
	if err != nil {
		return nil, Info{}, fmt.Errorf("index lookup: %w", err)
	}
	if !ok {
		return nil, Info{}, fmt.Errorf("%w: %s", ErrNotFound, id.Short())
	}

	f, err := os.Open(s.Path(id))
	if err != nil {
		if os.IsNotExist(err) {
			// Index and disk disagree. The index is the liar here — the file
			// is the data — so drop the entry and report a miss so the reader
			// fails over to another replica.
			s.log.Error("index references a missing chunk file; dropping entry",
				slog.String("chunk", id.String()))
			s.forget(id, info.Size)
			return nil, Info{}, fmt.Errorf("%w: %s", ErrNotFound, id.Short())
		}
		return nil, Info{}, fmt.Errorf("open chunk %s: %w", id.Short(), err)
	}

	full := offset == 0 && (length <= 0 || length >= info.Size)
	if full {
		return &verifiedReader{f: f, r: chunk.NewVerifyingReader(f, id)}, info, nil
	}

	if offset < 0 || offset > info.Size {
		_ = f.Close()
		return nil, Info{}, fmt.Errorf("offset %d out of range for %d-byte chunk", offset, info.Size)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, Info{}, fmt.Errorf("seek chunk %s: %w", id.Short(), err)
	}
	if length <= 0 || offset+length > info.Size {
		length = info.Size - offset
	}
	return &verifiedReader{f: f, r: io.LimitReader(f, length)}, info, nil
}

// verifiedReader ties a reader's lifetime to the file underneath it.
type verifiedReader struct {
	f *os.File
	r io.Reader
}

func (v *verifiedReader) Read(p []byte) (int, error) { return v.r.Read(p) }
func (v *verifiedReader) Close() error               { return v.f.Close() }

// Stat reports what the store knows about a chunk.
func (s *Store) Stat(id chunk.ID) (Info, bool, error) {
	if s.isClosed() {
		return Info{}, false, ErrClosed
	}
	return s.idx.Get(id)
}

// Delete removes a chunk. It reports whether the chunk existed.
func (s *Store) Delete(id chunk.ID) (bool, error) {
	if s.isClosed() {
		return false, ErrClosed
	}

	info, ok, err := s.idx.Get(id)
	if err != nil {
		return false, fmt.Errorf("index lookup: %w", err)
	}
	if !ok {
		return false, nil
	}

	// Index entry first: an entry with no file is a hard error on read, while
	// a file with no entry is a harmless orphan that boot recovery adopts or
	// the operator reclaims. Order the operations so a crash lands in the
	// benign state.
	if _, err := s.idx.Delete(id); err != nil {
		return false, fmt.Errorf("index delete: %w", err)
	}
	s.releaseCommitted(info.Size)

	if err := os.Remove(s.Path(id)); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("remove chunk %s: %w", id.Short(), err)
	}
	return true, nil
}

// Usage reports current capacity consumption.
func (s *Store) Usage() Usage {
	s.acct.Lock()
	defer s.acct.Unlock()
	return Usage{
		CapacityBytes: s.capacity,
		UsedBytes:     s.used,
		PendingBytes:  s.pending,
		ChunkCount:    s.count,
	}
}

// --- capacity accounting -------------------------------------------------

// reserve claims space for an in-flight write. Reserving before writing is
// what stops N concurrent uploads from each passing a check that only one of
// them could actually satisfy.
func (s *Store) reserve(size int64) error {
	s.acct.Lock()
	defer s.acct.Unlock()

	if s.capacity > 0 && s.used+s.pending+size > s.capacity {
		return fmt.Errorf("%w: %d used + %d pending + %d requested exceeds %d",
			ErrNoCapacity, s.used, s.pending, size, s.capacity)
	}
	s.pending += size
	return nil
}

// release ends a reservation, committing committedBytes of it (0 if the write
// failed or deduplicated).
func (s *Store) release(reserved, committedBytes int64) {
	s.acct.Lock()
	defer s.acct.Unlock()

	s.pending -= reserved
	if s.pending < 0 {
		s.pending = 0
	}
	if committedBytes > 0 {
		s.used += committedBytes
		s.count++
	}
}

func (s *Store) releaseCommitted(size int64) {
	s.acct.Lock()
	defer s.acct.Unlock()

	s.used -= size
	if s.used < 0 {
		s.used = 0
	}
	s.count--
	if s.count < 0 {
		s.count = 0
	}
}

// forget drops an index entry whose file has vanished.
func (s *Store) forget(id chunk.ID, size int64) {
	if _, err := s.idx.Delete(id); err != nil {
		s.log.Error("could not drop stale index entry",
			slog.String("chunk", id.Short()), slog.Any("error", err))
		return
	}
	s.releaseCommitted(size)
}

// --- boot recovery -------------------------------------------------------

// purgeTmp removes in-flight writes abandoned by a crash. They are always
// discardable: a temp file by definition never completed verification.
func (s *Store) purgeTmp() error {
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		return err
	}
	for _, e := range entries {
		p := filepath.Join(s.tmp, e.Name())
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}
	if len(entries) > 0 {
		s.log.Warn("discarded incomplete writes from a previous run",
			slog.Int("count", len(entries)))
	}
	return nil
}

// recover reconciles the index against the filesystem in both directions,
// because a crash can land between the rename and the index write.
//
//   - index entry with no file: the entry is a lie. Drop it, so reads fail
//     over to another replica instead of returning an error.
//   - file with no index entry: the file was renamed into place, which means
//     it was already hash-verified, so it is real data. Adopt it.
func (s *Store) recover() error {
	onDisk := make(map[chunk.ID]int64)

	err := filepath.WalkDir(s.chunks, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != chunkExt {
			return nil
		}
		name := filepath.Base(path)
		id, parseErr := chunk.ParseID(name[:len(name)-len(chunkExt)])
		if parseErr != nil {
			s.log.Warn("ignoring unrecognised file in chunk directory",
				slog.String("path", path))
			return nil
		}
		fi, statErr := d.Info()
		if statErr != nil {
			return statErr
		}
		onDisk[id] = fi.Size()
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk chunks: %w", err)
	}

	var stale, adopted int
	var used, count int64

	indexed, err := s.idx.All()
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}

	for id, info := range indexed {
		if _, ok := onDisk[id]; !ok {
			if _, err := s.idx.Delete(id); err != nil {
				return fmt.Errorf("drop stale entry: %w", err)
			}
			stale++
			continue
		}
		used += info.Size
		count++
		delete(onDisk, id) // leaves only unindexed files behind
	}

	for id, size := range onDisk {
		if _, err := s.idx.Put(id, Info{ID: id, Size: size, CreatedAt: time.Now()}); err != nil {
			return fmt.Errorf("adopt orphan chunk: %w", err)
		}
		used += size
		count++
		adopted++
	}

	s.acct.Lock()
	s.used, s.count = used, count
	s.acct.Unlock()

	if stale > 0 {
		s.log.Warn("dropped index entries with no chunk file", slog.Int("count", stale))
	}
	if adopted > 0 {
		s.log.Warn("adopted chunk files missing from the index", slog.Int("count", adopted))
	}
	return nil
}

// syncDir fsyncs a directory so a rename within it is durable.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		// Windows cannot open a directory as a file. Node deployments are
		// Linux containers; this branch exists so tests run natively too.
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
