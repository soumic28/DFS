package node

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagev1 "github.com/soumic28/dfs/api/gen/storage/v1"
	"github.com/soumic28/dfs/internal/chunk"
)

// DialFunc opens a connection to a peer storage node. It is a parameter rather
// than a hard-coded dial so tests can wire nodes together over an in-memory
// listener.
type DialFunc func(ctx context.Context, addr string) (Peer, error)

// Peer is the subset of a storage node another node needs during repair.
type Peer interface {
	// Open streams a chunk from the peer. The returned reader must be closed.
	Open(ctx context.Context, id chunk.ID, offset, length int64) (io.ReadCloser, error)
	Close() error
}

// DialPeer connects to a storage node over plaintext gRPC.
//
// Plaintext is correct here: node-to-node traffic never leaves the internal
// Docker network, which is declared `internal: true` and has no route to the
// internet. When the cluster spans hosts (Phase 9) the transport becomes a
// WireGuard mesh, which encrypts at the link layer rather than per connection.
func DialPeer(_ context.Context, addr string) (Peer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &grpcPeer{conn: conn, client: storagev1.NewStorageNodeClient(conn)}, nil
}

type grpcPeer struct {
	conn   *grpc.ClientConn
	client storagev1.StorageNodeClient
}

func (p *grpcPeer) Open(ctx context.Context, id chunk.ID, offset, length int64) (io.ReadCloser, error) {
	stream, err := p.client.GetChunk(ctx, &storagev1.GetChunkRequest{
		Ref:    &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1},
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return nil, err
	}
	return &streamReadCloser{
		streamReader: streamReader{recv: func() ([]byte, error) {
			msg, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return msg.GetData(), nil
		}},
	}, nil
}

func (p *grpcPeer) Close() error { return p.conn.Close() }

// streamReadCloser adds a no-op Close to a stream-backed reader. The stream's
// lifetime is bounded by its context, which the caller already owns.
type streamReadCloser struct {
	streamReader
}

func (s *streamReadCloser) Close() error { return nil }

// NewClient returns a StorageNode client over an existing connection, for
// callers (the gateway, from Phase 3) that manage their own connection pool.
func NewClient(conn grpc.ClientConnInterface) storagev1.StorageNodeClient {
	return storagev1.NewStorageNodeClient(conn)
}
