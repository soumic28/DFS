package gateway

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility only, never for integrity
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "github.com/soumi/dfs/api/gen/metadata/v1"
	storagev1 "github.com/soumi/dfs/api/gen/storage/v1"
	"github.com/soumi/dfs/internal/chunk"
)

// frameSize is the payload of one streamed gRPC data frame.
const frameSize = 256 << 10

// Errors the HTTP layer maps to status codes.
var (
	ErrNotFound     = errors.New("object not found")
	ErrNoCapacity   = errors.New("cluster is out of capacity")
	ErrQuorum       = errors.New("write quorum not met")
	ErrBucketExists = errors.New("bucket already exists")
	ErrInvalidRange = errors.New("invalid range")
)

// Engine is the storage engine shared by the native REST API and (from Phase 5)
// the S3-compatible API. Only the wire format differs between them; the
// chunking, placement and assembly below is identical.
type Engine struct {
	meta      metadatav1.MetadataClient
	pool      *nodePool
	log       *slog.Logger
	chunkSize int64
}

// Config configures an Engine.
type Config struct {
	Meta        metadatav1.MetadataClient
	Log         *slog.Logger
	ChunkSize   int64
	MaxMsgBytes int
}

// New returns a storage engine.
func New(cfg Config) *Engine {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = chunk.DefaultSize
	}
	if cfg.MaxMsgBytes <= 0 {
		cfg.MaxMsgBytes = int(cfg.ChunkSize) + (1 << 20)
	}
	return &Engine{
		meta:      cfg.Meta,
		pool:      newNodePool(cfg.MaxMsgBytes),
		log:       cfg.Log,
		chunkSize: cfg.ChunkSize,
	}
}

// Close releases pooled node connections.
func (e *Engine) Close() error { return e.pool.Close() }

// PutResult describes a completed upload.
type PutResult struct {
	VersionID       string
	Size            int64
	ETag            string
	Chunks          int32
	DeduplicatedNum int32
	BytesUploaded   int64
}

// Put streams an object into the cluster.
//
// The body is never buffered whole: it is split into chunks, each chunk is
// hashed, offered to the coordinator (which may say "already have it"), fanned
// out to the nodes that should hold it, and only once every chunk is durable
// is a single transaction asked to make the object visible.
func (e *Engine) Put(ctx context.Context, bucket, key, contentType string, body io.Reader) (PutResult, error) {
	splitter, err := chunk.NewSplitter(body, e.chunkSize)
	if err != nil {
		return PutResult{}, err
	}

	// MD5 exists purely so ETags match what S3 clients assert on. Integrity is
	// BLAKE3's job everywhere else in this system.
	etagHash := md5.New() //nolint:gosec // not used for integrity

	var (
		commitChunks []*metadatav1.ObjectChunk
		deduped      int32
		uploaded     int64
	)

	for {
		piece, err := splitter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return PutResult{}, err
		}

		etagHash.Write(piece.Data)

		alloc, err := e.meta.AllocateChunk(ctx, &metadatav1.AllocateChunkRequest{
			ChunkId: piece.ID.Bytes(),
			Size:    piece.Size,
		})
		if err != nil {
			return PutResult{}, e.metaError("allocate chunk", err)
		}

		var addrs []string
		if alloc.GetAlreadyExists() {
			// Deduplication saves the network round trip, not just the disk.
			// The bytes are already in the cluster, so we never send them.
			deduped++
			placed, err := e.placementsFor(ctx, piece.ID)
			if err != nil {
				return PutResult{}, err
			}
			addrs = placed
		} else {
			addrs, err = e.fanOut(ctx, piece, alloc.GetTargetNodeAddrs(), int(alloc.GetWriteQuorum()))
			if err != nil {
				return PutResult{}, err
			}
			uploaded += piece.Size
		}

		commitChunks = append(commitChunks, &metadatav1.ObjectChunk{
			Seq:        piece.Seq,
			Ref:        &storagev1.ChunkRef{Id: piece.ID.Bytes(), ShardIndex: -1},
			ByteOffset: piece.ByteOffset,
			Size:       piece.Size,
			NodeAddrs:  addrs,
		})
	}

	etag := `"` + hex.EncodeToString(etagHash.Sum(nil)) + `"`

	commit, err := e.meta.CommitObject(ctx, &metadatav1.CommitObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Size:        splitter.Total(),
		ContentType: contentType,
		Etag:        etag,
		Chunks:      commitChunks,
	})
	if err != nil {
		return PutResult{}, e.metaError("commit object", err)
	}

	e.log.InfoContext(ctx, "object stored",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int64("size", splitter.Total()),
		slog.Int("chunks", len(commitChunks)),
		slog.Int64("deduplicated", int64(deduped)),
	)

	return PutResult{
		VersionID:       commit.GetVersionId(),
		Size:            splitter.Total(),
		ETag:            etag,
		Chunks:          splitter.Count(),
		DeduplicatedNum: deduped,
		BytesUploaded:   uploaded,
	}, nil
}

// fanOut writes one chunk to every target in parallel and returns the
// addresses that acknowledged, once quorum is reached.
//
// Acking at W of R rather than all R is deliberate: waiting for every replica
// makes the p99 of every write the p99 of the slowest node in the cluster, and
// one sick node stalls everything. The remaining replicas are filled in by the
// repair pipeline.
func (e *Engine) fanOut(ctx context.Context, piece chunk.Piece, targets []string, quorum int) ([]string, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: coordinator returned no targets", ErrNoCapacity)
	}
	if quorum <= 0 || quorum > len(targets) {
		quorum = len(targets)
	}

	// The buffer aliases the splitter's scratch space, which is reused on the
	// next iteration. The fan-out below reads it concurrently, so it must be
	// copied — this is the one place the pipeline pays for a copy, and
	// skipping it corrupts data under concurrency in a way that only shows up
	// under load.
	data := make([]byte, len(piece.Data))
	copy(data, piece.Data)

	type ack struct {
		addr string
		err  error
	}
	results := make(chan ack, len(targets))

	var wg sync.WaitGroup
	for _, addr := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- ack{addr: addr, err: e.putChunk(ctx, addr, piece.ID, data)}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	var (
		ok     []string
		failed []error
	)
	for r := range results {
		if r.err != nil {
			failed = append(failed, fmt.Errorf("%s: %w", r.addr, r.err))
			continue
		}
		ok = append(ok, r.addr)
	}

	if len(ok) < quorum {
		return nil, fmt.Errorf("%w: chunk %s got %d/%d acks: %w",
			ErrQuorum, piece.ID.Short(), len(ok), quorum, errors.Join(failed...))
	}
	if len(failed) > 0 {
		e.log.WarnContext(ctx, "chunk written below full replication",
			slog.String("chunk", piece.ID.Short()),
			slog.Int("acked", len(ok)),
			slog.Int("targets", len(targets)),
			slog.Any("errors", errors.Join(failed...)))
	}
	return ok, nil
}

func (e *Engine) putChunk(ctx context.Context, addr string, id chunk.ID, data []byte) error {
	client, err := e.pool.client(addr)
	if err != nil {
		return err
	}

	stream, err := client.PutChunk(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&storagev1.PutChunkRequest{
		Payload: &storagev1.PutChunkRequest_Header{
			Header: &storagev1.PutChunkHeader{
				Ref:  &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1},
				Size: int64(len(data)),
			},
		},
	}); err != nil {
		return fmt.Errorf("send header: %w", err)
	}

	for off := 0; off < len(data); off += frameSize {
		end := min(off+frameSize, len(data))
		if err := stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: data[off:end]},
		}); err != nil {
			if errors.Is(err, io.EOF) {
				break // server closed early; CloseAndRecv carries the status
			}
			return fmt.Errorf("send data: %w", err)
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		if status.Code(err) == codes.ResourceExhausted {
			return fmt.Errorf("%w: %s", ErrNoCapacity, addr)
		}
		return err
	}
	return nil
}

// placementsFor asks where a deduplicated chunk already lives.
func (e *Engine) placementsFor(ctx context.Context, id chunk.ID) ([]string, error) {
	// The coordinator returns placements as part of allocation for new chunks;
	// for an existing one we ask again through a zero-size allocate, which is
	// idempotent and cheap.
	alloc, err := e.meta.AllocateChunk(ctx, &metadatav1.AllocateChunkRequest{ChunkId: id.Bytes()})
	if err != nil {
		return nil, e.metaError("locate chunk", err)
	}
	return alloc.GetTargetNodeAddrs(), nil
}

// ObjectInfo describes an object being read.
type ObjectInfo struct {
	VersionID   string
	Size        int64
	ContentType string
	ETag        string
	Metadata    map[string]string
}

// Get resolves an object and streams the requested byte range to w.
//
// offset and length address the logical object; the engine works out which
// chunks overlap and pulls only those, so a range request over a large object
// touches a bounded amount of data.
func (e *Engine) Get(ctx context.Context, bucket, key string, offset, length int64, w io.Writer) (ObjectInfo, error) {
	lookup, err := e.meta.LookupObject(ctx, &metadatav1.LookupObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return ObjectInfo{}, e.metaError("lookup object", err)
	}
	if !lookup.GetFound() {
		return ObjectInfo{}, fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)
	}

	info := ObjectInfo{
		VersionID:   lookup.GetVersionId(),
		Size:        lookup.GetSize(),
		ContentType: lookup.GetContentType(),
		ETag:        lookup.GetEtag(),
		Metadata:    lookup.GetMetadata(),
	}
	if w == nil {
		return info, nil // HEAD: metadata only
	}

	if length <= 0 || offset+length > info.Size {
		length = info.Size - offset
	}
	if offset < 0 || offset > info.Size {
		return info, fmt.Errorf("%w: offset %d for %d-byte object", ErrInvalidRange, offset, info.Size)
	}

	remaining := length
	for _, c := range lookup.GetChunks() {
		if remaining <= 0 {
			break
		}
		chunkStart, chunkEnd := c.GetByteOffset(), c.GetByteOffset()+c.GetSize()
		if chunkEnd <= offset {
			continue // entirely before the requested range
		}

		// Where inside this chunk the requested range begins.
		within := max(offset-chunkStart, 0)
		want := min(c.GetSize()-within, remaining)

		n, err := e.readChunk(ctx, c, within, want, w)
		if err != nil {
			return info, err
		}
		remaining -= n
	}

	return info, nil
}

// readChunk streams one chunk to w, trying each replica in turn.
//
// A replica that returns corrupt data is reported to the coordinator before we
// move on, because a reader detecting corruption is the fastest signal the
// repair pipeline can get — much faster than waiting for the scrubber.
func (e *Engine) readChunk(ctx context.Context, c *metadatav1.ObjectChunk, offset, length int64, w io.Writer) (int64, error) {
	addrs := c.GetNodeAddrs()
	if len(addrs) == 0 {
		return 0, fmt.Errorf("chunk %d has no live replicas", c.GetSeq())
	}

	var attempts []error
	for _, addr := range addrs {
		n, err := e.streamChunk(ctx, addr, c, offset, length, w)
		if err == nil {
			return n, nil
		}

		// Only retry elsewhere if nothing was written yet. Once bytes are on
		// the wire to the client, switching replicas would splice two
		// responses together and silently corrupt the download.
		if n > 0 {
			return n, fmt.Errorf("chunk %d truncated after %d bytes from %s: %w", c.GetSeq(), n, addr, err)
		}

		attempts = append(attempts, fmt.Errorf("%s: %w", addr, err))

		if status.Code(err) == codes.DataLoss {
			e.reportBadReplica(ctx, c.GetRef(), addr, err.Error())
		}
		e.log.WarnContext(ctx, "replica read failed, trying another",
			slog.Int("chunk_seq", int(c.GetSeq())),
			slog.String("node", addr),
			slog.Any("error", err))
	}

	return 0, fmt.Errorf("chunk %d unreadable from every replica: %w",
		c.GetSeq(), errors.Join(attempts...))
}

func (e *Engine) streamChunk(ctx context.Context, addr string, c *metadatav1.ObjectChunk, offset, length int64, w io.Writer) (int64, error) {
	client, err := e.pool.client(addr)
	if err != nil {
		return 0, err
	}

	stream, err := client.GetChunk(ctx, &storagev1.GetChunkRequest{
		Ref:    c.GetRef(),
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return 0, err
	}

	var written int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return written, nil
		}
		if err != nil {
			return written, err
		}
		n, err := w.Write(msg.GetData())
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("write to client: %w", err)
		}
		if written >= length {
			return written, nil
		}
	}
}

func (e *Engine) reportBadReplica(ctx context.Context, ref *storagev1.ChunkRef, addr, reason string) {
	// Best effort and deliberately not fatal: failing a client's read because
	// we could not file a repair report would be the wrong trade.
	if _, err := e.meta.ReportBadReplica(ctx, &metadatav1.ReportBadReplicaRequest{
		Ref:      ref,
		NodeAddr: addr,
		Reason:   reason,
	}); err != nil {
		e.log.ErrorContext(ctx, "could not report a bad replica",
			slog.String("node", addr), slog.Any("error", err))
	}
}

// --- namespace passthroughs ---------------------------------------------

// CreateBucket creates a bucket.
func (e *Engine) CreateBucket(ctx context.Context, name string) error {
	_, err := e.meta.CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: name})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return fmt.Errorf("%w: %s", ErrBucketExists, name)
		}
		return e.metaError("create bucket", err)
	}
	return nil
}

// ListResult is a page of object listings.
type ListResult struct {
	Objects        []ObjectSummary
	CommonPrefixes []string
	NextToken      string
	IsTruncated    bool
}

// ObjectSummary is one listing entry.
type ObjectSummary struct {
	Key        string
	Size       int64
	ETag       string
	ModifiedAt int64
}

// List returns a page of a bucket's objects.
func (e *Engine) List(ctx context.Context, bucket, prefix, delimiter, token string, maxKeys int32) (ListResult, error) {
	resp, err := e.meta.ListObjects(ctx, &metadatav1.ListObjectsRequest{
		Bucket:            bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		ContinuationToken: token,
		MaxKeys:           maxKeys,
	})
	if err != nil {
		return ListResult{}, e.metaError("list objects", err)
	}

	out := ListResult{
		Objects:        make([]ObjectSummary, len(resp.GetObjects())),
		CommonPrefixes: resp.GetCommonPrefixes(),
		NextToken:      resp.GetNextContinuationToken(),
		IsTruncated:    resp.GetIsTruncated(),
	}
	for i, o := range resp.GetObjects() {
		out.Objects[i] = ObjectSummary{
			Key:        o.GetKey(),
			Size:       o.GetSize(),
			ETag:       o.GetEtag(),
			ModifiedAt: o.GetModifiedAtUnix(),
		}
	}
	return out, nil
}

// Delete tombstones an object.
func (e *Engine) Delete(ctx context.Context, bucket, key string) (bool, error) {
	resp, err := e.meta.DeleteObject(ctx, &metadatav1.DeleteObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return false, e.metaError("delete object", err)
	}
	return resp.GetDeleted(), nil
}

// metaError translates a coordinator error into an engine-level one the HTTP
// layer can map to a status code.
func (e *Engine) metaError(op string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, status.Convert(err).Message())
	case codes.AlreadyExists:
		return fmt.Errorf("%w: %s", ErrBucketExists, status.Convert(err).Message())
	case codes.ResourceExhausted, codes.Unavailable:
		return fmt.Errorf("%w: %s", ErrNoCapacity, status.Convert(err).Message())
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
