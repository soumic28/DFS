package node

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev1 "github.com/soumi/dfs/api/gen/storage/v1"
	"github.com/soumi/dfs/internal/blobstore"
	"github.com/soumi/dfs/internal/chunk"
)

// newTestNode starts a real gRPC server over an in-memory listener, so these
// tests exercise actual streaming and status codes rather than direct method
// calls that would skip the wire entirely.
func newTestNode(t *testing.T, capacity int64) (storagev1.StorageNodeClient, *blobstore.Store) {
	t.Helper()

	store, err := blobstore.Open(blobstore.Options{
		Root:     t.TempDir(),
		Capacity: capacity,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	storagev1.RegisterStorageNodeServer(srv, NewServer(store, slog.New(slog.NewTextHandler(io.Discard, nil)), nil))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return storagev1.NewStorageNodeClient(conn), store
}

func ref(id chunk.ID) *storagev1.ChunkRef {
	return &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1}
}

func putChunk(t *testing.T, c storagev1.StorageNodeClient, data []byte) (chunk.ID, *storagev1.PutChunkResponse) {
	t.Helper()
	id := chunk.Sum(data)

	stream, err := c.PutChunk(context.Background())
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := stream.Send(&storagev1.PutChunkRequest{
		Payload: &storagev1.PutChunkRequest_Header{
			Header: &storagev1.PutChunkHeader{Ref: ref(id), Size: int64(len(data))},
		},
	}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	for off := 0; off < len(data); off += frameSize {
		end := min(off+frameSize, len(data))
		if err := stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: data[off:end]},
		}); err != nil {
			t.Fatalf("send data: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	return id, resp
}

func getChunk(t *testing.T, c storagev1.StorageNodeClient, id chunk.ID, offset, length int64) ([]byte, error) {
	t.Helper()
	stream, err := c.GetChunk(context.Background(), &storagev1.GetChunkRequest{
		Ref: ref(id), Offset: offset, Length: length,
	})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return buf.Bytes(), err
		}
		buf.Write(msg.GetData())
	}
}

func TestGRPCRoundTrip(t *testing.T) {
	c, _ := newTestNode(t, 0)

	// Deliberately spans several frames so the streaming path is exercised.
	data := bytes.Repeat([]byte("distributed"), 200_000) // ~2.2 MB
	id, resp := putChunk(t, c, data)

	if resp.GetAlreadyPresent() {
		t.Error("first write reported as a duplicate")
	}
	if resp.GetBytesWritten() != int64(len(data)) {
		t.Errorf("bytes written = %d, want %d", resp.GetBytesWritten(), len(data))
	}

	got, err := getChunk(t, c, id, 0, 0)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("round trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestGRPCDeduplication(t *testing.T) {
	c, _ := newTestNode(t, 0)
	data := []byte("write me twice")

	if _, resp := putChunk(t, c, data); resp.GetAlreadyPresent() {
		t.Error("first write was reported as a duplicate")
	}
	if _, resp := putChunk(t, c, data); !resp.GetAlreadyPresent() {
		t.Error("second identical write was not deduplicated")
	}
}

// Error codes are a contract: the gateway decides whether to retry here, fail
// over to another replica, or report corruption based purely on these.
func TestGRPCErrorCodes(t *testing.T) {
	c, store := newTestNode(t, 500)

	t.Run("missing chunk is NotFound", func(t *testing.T) {
		_, err := getChunk(t, c, chunk.Sum([]byte("never stored")), 0, 0)
		if status.Code(err) != codes.NotFound {
			t.Errorf("code = %s, want NotFound", status.Code(err))
		}
	})

	t.Run("over capacity is ResourceExhausted", func(t *testing.T) {
		data := make([]byte, 900)
		id := chunk.Sum(data)
		stream, err := c.PutChunk(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Header{
				Header: &storagev1.PutChunkHeader{Ref: ref(id), Size: int64(len(data))},
			},
		})
		_ = stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: data},
		})
		_, err = stream.CloseAndRecv()
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("code = %s, want ResourceExhausted", status.Code(err))
		}
	})

	t.Run("malformed id is InvalidArgument", func(t *testing.T) {
		_, err := c.StatChunk(context.Background(), &storagev1.StatChunkRequest{
			Ref: &storagev1.ChunkRef{Id: []byte("too short")},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("code = %s, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("corruption at rest is DataLoss", func(t *testing.T) {
		data := bytes.Repeat([]byte("rot"), 100)
		id, _ := putChunk(t, c, data)

		// Corrupt the bytes underneath the node, exactly as bitrot would.
		path := store.Path(id)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)/2] ^= 0xFF
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}

		_, err = getChunk(t, c, id, 0, 0)
		if status.Code(err) != codes.DataLoss {
			t.Errorf("code = %s, want DataLoss (corruption must never be served silently)", status.Code(err))
		}
	})

	// Fault attribution matters more than it looks. A bad upload must not be
	// reported the same way as a rotted disk, or the coordinator will suspect
	// a healthy node and schedule repairs it does not need.
	t.Run("misdeclared upload is InvalidArgument, not DataLoss", func(t *testing.T) {
		honest := []byte("the bytes this id names")
		lie := []byte("completely different bytes!")

		stream, err := c.PutChunk(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Header{
				Header: &storagev1.PutChunkHeader{
					Ref:  ref(chunk.Sum(honest)),
					Size: int64(len(lie)),
				},
			},
		})
		_ = stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: lie},
		})

		_, err = stream.CloseAndRecv()
		if code := status.Code(err); code != codes.InvalidArgument {
			t.Errorf("code = %s, want InvalidArgument (the uploader is at fault, not this node)", code)
		}
	})

	t.Run("first frame must be a header", func(t *testing.T) {
		stream, err := c.PutChunk(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: []byte("no header")},
		})
		if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("code = %s, want InvalidArgument", status.Code(err))
		}
	})
}

func TestGRPCStatAndDelete(t *testing.T) {
	c, _ := newTestNode(t, 0)
	data := []byte("stat me")
	id, _ := putChunk(t, c, data)

	st, err := c.StatChunk(context.Background(), &storagev1.StatChunkRequest{Ref: ref(id)})
	if err != nil {
		t.Fatal(err)
	}
	if !st.GetExists() || st.GetSize() != int64(len(data)) {
		t.Errorf("stat = exists:%v size:%d, want exists:true size:%d",
			st.GetExists(), st.GetSize(), len(data))
	}

	del, err := c.DeleteChunk(context.Background(), &storagev1.DeleteChunkRequest{Ref: ref(id)})
	if err != nil || !del.GetExisted() {
		t.Fatalf("delete: existed = %v, err = %v", del.GetExisted(), err)
	}

	// Deleting again must succeed: the coordinator retries deletes it is
	// unsure landed, and those retries must not look like failures.
	del, err = c.DeleteChunk(context.Background(), &storagev1.DeleteChunkRequest{Ref: ref(id)})
	if err != nil {
		t.Fatalf("second delete returned an error: %v", err)
	}
	if del.GetExisted() {
		t.Error("second delete claimed the chunk still existed")
	}
}

func TestGRPCRangedRead(t *testing.T) {
	c, _ := newTestNode(t, 0)
	data := []byte("0123456789abcdefghij")
	id, _ := putChunk(t, c, data)

	got, err := getChunk(t, c, id, 5, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "56789" {
		t.Errorf("ranged read = %q, want %q", got, "56789")
	}
}

// Repair moves bytes directly between nodes. This wires two real nodes
// together and pulls a chunk across.
func TestPullChunkFetchesFromPeer(t *testing.T) {
	source, _ := newTestNode(t, 0)
	data := bytes.Repeat([]byte("repair me"), 50_000) // ~450 KB, multi-frame
	id, _ := putChunk(t, source, data)

	// A destination node whose peer dialer resolves to the source client.
	destStore, err := blobstore.Open(blobstore.Options{
		Root:   t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destStore.Close() })

	dest := NewServer(destStore, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(context.Context, string) (Peer, error) { return clientPeer{source}, nil })

	resp, err := dest.PullChunk(context.Background(), &storagev1.PullChunkRequest{
		Ref:        ref(id),
		SourceAddr: "source-node:9091",
		Size:       int64(len(data)),
	})
	if err != nil {
		t.Fatalf("PullChunk: %v", err)
	}
	if resp.GetBytesTransferred() != int64(len(data)) {
		t.Errorf("transferred %d bytes, want %d", resp.GetBytesTransferred(), len(data))
	}

	rc, _, err := destStore.Get(id, 0, 0)
	if err != nil {
		t.Fatalf("chunk missing after pull: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read pulled chunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("pulled chunk does not match the source")
	}
}

func TestPullChunkIsIdempotent(t *testing.T) {
	source, _ := newTestNode(t, 0)
	data := []byte("already here")
	id, _ := putChunk(t, source, data)

	destStore, err := blobstore.Open(blobstore.Options{
		Root:   t.TempDir(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destStore.Close() })
	if _, err := destStore.Put(context.Background(), id, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	dest := NewServer(destStore, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(context.Context, string) (Peer, error) {
			t.Error("dialled a peer for a chunk already held locally")
			return clientPeer{source}, nil
		})

	resp, err := dest.PullChunk(context.Background(), &storagev1.PullChunkRequest{
		Ref: ref(id), SourceAddr: "source:9091", Size: int64(len(data)),
	})
	if err != nil {
		t.Fatalf("PullChunk: %v", err)
	}
	if resp.GetBytesTransferred() != 0 {
		t.Errorf("transferred %d bytes for a chunk already held, want 0", resp.GetBytesTransferred())
	}
}

// clientPeer adapts a gRPC client to the Peer interface for tests.
type clientPeer struct{ c storagev1.StorageNodeClient }

func (p clientPeer) Open(ctx context.Context, id chunk.ID, offset, length int64) (io.ReadCloser, error) {
	stream, err := p.c.GetChunk(ctx, &storagev1.GetChunkRequest{
		Ref: ref(id), Offset: offset, Length: length,
	})
	if err != nil {
		return nil, err
	}
	return &streamReadCloser{streamReader: streamReader{recv: func() ([]byte, error) {
		msg, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return msg.GetData(), nil
	}}}, nil
}

func (p clientPeer) Close() error { return nil }
