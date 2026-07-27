// Package node exposes a storage node's blob store over gRPC.
//
// It is a thin translation layer: wire types in, blobstore calls out, and
// errors mapped to gRPC codes that tell a caller what to do next — retry here,
// try another replica, or stop.
package node

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/soumi/dfs/api/gen/storage/v1"
	"github.com/soumi/dfs/internal/blobstore"
	"github.com/soumi/dfs/internal/chunk"
)

// frameSize is the payload size of a streamed data frame. gRPC's default
// message limit is 4 MiB; 256 KiB keeps per-message overhead low without
// making a single lost frame expensive to resend.
const frameSize = 256 << 10

// Server implements storage.v1.StorageNode.
type Server struct {
	storagev1.UnimplementedStorageNodeServer

	store *blobstore.Store
	log   *slog.Logger
	// dial opens a connection to a peer node, for repair pulls. Injectable so
	// tests do not need real networking.
	dial DialFunc
}

// NewServer returns a gRPC service backed by store.
func NewServer(store *blobstore.Store, log *slog.Logger, dial DialFunc) *Server {
	if dial == nil {
		dial = DialPeer
	}
	return &Server{store: store, log: log, dial: dial}
}

// PutChunk accepts a streamed chunk: one header frame naming the chunk, then
// data frames. The store verifies the bytes hash to the declared name before
// anything is committed, so a lying or corrupted upload cannot land.
func (s *Server) PutChunk(stream storagev1.StorageNode_PutChunkServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "empty stream: expected a header frame")
		}
		return status.Errorf(codes.Internal, "receive header: %v", err)
	}

	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first frame must be a header")
	}
	id, err := chunk.IDFromBytes(header.GetRef().GetId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if header.GetSize() < 0 {
		return status.Error(codes.InvalidArgument, "size must not be negative")
	}

	body := &streamReader{recv: func() ([]byte, error) {
		msg, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if h := msg.GetHeader(); h != nil {
			return nil, status.Error(codes.InvalidArgument, "unexpected second header frame")
		}
		return msg.GetData(), nil
	}}

	res, err := s.store.Put(stream.Context(), id, header.GetSize(), body)
	if err != nil {
		return s.storeError("put", id, err)
	}

	if res.AlreadyPresent {
		s.log.DebugContext(stream.Context(), "chunk deduplicated",
			slog.String("chunk", id.Short()))
	}

	return stream.SendAndClose(&storagev1.PutChunkResponse{
		AlreadyPresent: res.AlreadyPresent,
		BytesWritten:   res.BytesWritten,
	})
}

// GetChunk streams a chunk out. A full read is checksum-verified as it is
// sent; if the bytes on disk have rotted, the stream fails with DataLoss part
// way through rather than delivering corruption.
func (s *Server) GetChunk(req *storagev1.GetChunkRequest, stream storagev1.StorageNode_GetChunkServer) error {
	id, err := chunk.IDFromBytes(req.GetRef().GetId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	rc, _, err := s.store.Get(id, req.GetOffset(), req.GetLength())
	if err != nil {
		return s.storeError("get", id, err)
	}
	defer rc.Close()

	buf := make([]byte, frameSize)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			if err := stream.Send(&storagev1.ChunkData{Data: buf[:n]}); err != nil {
				return status.Errorf(codes.Unavailable, "send chunk %s: %v", id.Short(), err)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return s.storeError("read", id, readErr)
		}
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
	}
}

// StatChunk reports whether this node holds a chunk.
func (s *Server) StatChunk(_ context.Context, req *storagev1.StatChunkRequest) (*storagev1.StatChunkResponse, error) {
	id, err := chunk.IDFromBytes(req.GetRef().GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	info, ok, err := s.store.Stat(id)
	if err != nil {
		return nil, s.storeError("stat", id, err)
	}
	if !ok {
		return &storagev1.StatChunkResponse{Exists: false}, nil
	}

	resp := &storagev1.StatChunkResponse{
		Exists:        true,
		Size:          info.Size,
		CreatedAtUnix: info.CreatedAt.Unix(),
	}
	if !info.LastScrubbedAt.IsZero() {
		resp.LastScrubbedAtUnix = info.LastScrubbedAt.Unix()
	}
	return resp, nil
}

// DeleteChunk removes a chunk. Deleting something absent is not an error —
// the coordinator may legitimately retry a delete it is unsure landed.
func (s *Server) DeleteChunk(_ context.Context, req *storagev1.DeleteChunkRequest) (*storagev1.DeleteChunkResponse, error) {
	id, err := chunk.IDFromBytes(req.GetRef().GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	existed, err := s.store.Delete(id)
	if err != nil {
		return nil, s.storeError("delete", id, err)
	}
	return &storagev1.DeleteChunkResponse{Existed: existed}, nil
}

// PullChunk fetches a chunk directly from a peer.
//
// Repair traffic flows node to node: routing it through the coordinator or a
// gateway would put the busiest component in the path of every rebuild, which
// is how a recoverable node failure turns into a cluster-wide outage.
func (s *Server) PullChunk(ctx context.Context, req *storagev1.PullChunkRequest) (*storagev1.PullChunkResponse, error) {
	id, err := chunk.IDFromBytes(req.GetRef().GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetSourceAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_addr is required")
	}

	if _, ok, err := s.store.Stat(id); err != nil {
		return nil, s.storeError("stat", id, err)
	} else if ok {
		return &storagev1.PullChunkResponse{BytesTransferred: 0}, nil
	}

	peer, err := s.dial(ctx, req.GetSourceAddr())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial source %s: %v", req.GetSourceAddr(), err)
	}
	defer peer.Close()

	src, err := peer.Open(ctx, id, 0, 0)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "fetch %s from %s: %v",
			id.Short(), req.GetSourceAddr(), err)
	}
	defer src.Close()

	res, err := s.store.Put(ctx, id, req.GetSize(), src)
	if err != nil {
		return nil, s.storeError("pull", id, err)
	}

	s.log.InfoContext(ctx, "repaired chunk from peer",
		slog.String("chunk", id.Short()),
		slog.String("source", req.GetSourceAddr()),
		slog.Int64("bytes", res.BytesWritten),
	)
	return &storagev1.PullChunkResponse{BytesTransferred: res.BytesWritten}, nil
}

// storeError maps a blobstore error to a gRPC code that tells the caller what
// to do: NotFound and DataLoss both mean "try another replica", but only
// DataLoss also means "somebody's copy needs repairing".
func (s *Server) storeError(op string, id chunk.ID, err error) error {
	switch {
	case errors.Is(err, blobstore.ErrNotFound):
		return status.Errorf(codes.NotFound, "chunk %s not on this node", id.Short())

	case errors.Is(err, chunk.ErrChecksumMismatch):
		// Which side is at fault depends entirely on the operation, and
		// getting this wrong has real consequences: reporting DataLoss for a
		// bad upload would make the coordinator suspect this node's disk and
		// schedule repairs, when the actual problem was the uploader sending
		// bytes that do not match the name it declared.
		if op == "put" {
			return status.Errorf(codes.InvalidArgument,
				"uploaded bytes do not match declared chunk id %s: %v", id.Short(), err)
		}
		s.log.Error("serving a corrupt chunk was prevented",
			slog.String("op", op), slog.String("chunk", id.String()), slog.Any("error", err))
		return status.Errorf(codes.DataLoss, "chunk %s failed verification: %v", id.Short(), err)

	case errors.Is(err, blobstore.ErrNoCapacity):
		return status.Errorf(codes.ResourceExhausted, "node at capacity: %v", err)

	case errors.Is(err, blobstore.ErrSizeMismatch):
		return status.Errorf(codes.InvalidArgument, "%v", err)

	case errors.Is(err, blobstore.ErrClosed):
		return status.Error(codes.Unavailable, "node is shutting down")

	case errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()

	default:
		s.log.Error("blobstore operation failed",
			slog.String("op", op), slog.String("chunk", id.Short()), slog.Any("error", err))
		return status.Errorf(codes.Internal, "%s chunk %s: %v", op, id.Short(), err)
	}
}

// streamReader adapts a sequence of gRPC data frames to an io.Reader, so the
// blob store sees an ordinary stream and never has to know it came off a wire.
type streamReader struct {
	recv func() ([]byte, error)
	buf  []byte
	err  error
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		b, err := r.recv()
		if err != nil {
			r.err = err
			if len(b) == 0 {
				return 0, err
			}
		}
		r.buf = b
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

var _ io.Reader = (*streamReader)(nil)
