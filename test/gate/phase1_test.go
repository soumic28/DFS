//go:build gate

// Package gate holds acceptance tests that run against a live cluster rather
// than an in-process fake. They are behind a build tag so `go test ./...`
// stays fast; run them with `make phase1-gate`.
package gate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	storagev1 "github.com/soumic28/dfs/api/gen/storage/v1"
	"github.com/soumic28/dfs/internal/chunk"
)

const chunkSize = 8 << 20 // the production chunk size

func nodeClient(t *testing.T) storagev1.StorageNodeClient {
	t.Helper()

	addr := os.Getenv("NODE_ADDR")
	if addr == "" {
		t.Skip("NODE_ADDR is not set; run via `make phase1-gate`")
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(chunkSize+1<<20)),
	)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return storagev1.NewStorageNodeClient(conn)
}

// fillChunk writes deterministic pseudo-random bytes for chunk n, so a
// gigabyte of test data costs 8 MiB of memory instead of a gigabyte.
// Incompressible by design — real chunk data will not compress either.
func fillChunk(buf []byte, n int) {
	r := rand.New(rand.NewSource(int64(0xDF5000 + n)))
	for i := 0; i < len(buf); i += 8 {
		v := r.Uint64()
		for j := 0; j < 8 && i+j < len(buf); j++ {
			buf[i+j] = byte(v >> (8 * j))
		}
	}
}

// The Phase 1 gate: a gigabyte, chunked exactly as the gateway will chunk it,
// streamed in and back out through gRPC, verified end to end.
func TestGigabyteRoundTrip(t *testing.T) {
	c := nodeClient(t)

	sizeMB := 1024
	if v := os.Getenv("SIZE_MB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("SIZE_MB=%q: %v", v, err)
		}
		sizeMB = n
	}
	chunks := sizeMB * (1 << 20) / chunkSize
	totalBytes := int64(chunks) * chunkSize

	t.Logf("streaming %d MiB as %d x 8 MiB chunks", sizeMB, chunks)

	ids := make([]chunk.ID, chunks)
	buf := make([]byte, chunkSize)
	sent := chunk.NewHasher() // digest of the whole logical stream

	// --- upload ---
	upStart := time.Now()
	var deduped int
	for i := range chunks {
		fillChunk(buf, i)
		ids[i] = chunk.Sum(buf)
		_, _ = sent.Write(buf)

		res, err := putChunk(context.Background(), c, ids[i], buf)
		if err != nil {
			t.Fatalf("upload chunk %d/%d: %v", i+1, chunks, err)
		}
		if res.GetAlreadyPresent() {
			deduped++
		}
	}
	upElapsed := time.Since(upStart)

	t.Logf("upload:   %s in %s (%.1f MB/s), %d deduplicated",
		byteSize(totalBytes), upElapsed.Round(time.Millisecond),
		float64(totalBytes)/upElapsed.Seconds()/1e6, deduped)

	// --- download ---
	downStart := time.Now()
	received := chunk.NewHasher()
	for i, id := range ids {
		data, err := getChunk(context.Background(), c, id)
		if err != nil {
			t.Fatalf("download chunk %d/%d: %v", i+1, chunks, err)
		}
		if len(data) != chunkSize {
			t.Fatalf("chunk %d: got %d bytes, want %d", i, len(data), chunkSize)
		}
		// Per-chunk: the bytes must still hash to the name they were stored
		// under. This is the content-addressing invariant holding across a
		// full write-read cycle.
		if got := chunk.Sum(data); got != id {
			t.Fatalf("chunk %d hashed to %s on the way out, stored as %s",
				i, got.Short(), id.Short())
		}
		_, _ = received.Write(data)
	}
	downElapsed := time.Since(downStart)

	t.Logf("download: %s in %s (%.1f MB/s)",
		byteSize(totalBytes), downElapsed.Round(time.Millisecond),
		float64(totalBytes)/downElapsed.Seconds()/1e6)

	if sent.ID() != received.ID() {
		t.Fatalf("stream digest mismatch: sent %s, received %s",
			sent.ID().Short(), received.ID().Short())
	}
	if received.Size() != totalBytes {
		t.Fatalf("received %d bytes, want %d", received.Size(), totalBytes)
	}

	t.Logf("OK: %s round-tripped byte-identical (digest %s)",
		byteSize(totalBytes), sent.ID().Short())
}

// Uploading the same bytes twice must transfer nothing the second time.
func TestDeduplicationOverTheWire(t *testing.T) {
	c := nodeClient(t)

	data := make([]byte, chunkSize)
	fillChunk(data, 999_001)
	id := chunk.Sum(data)

	first, err := putChunk(context.Background(), c, id, data)
	if err != nil {
		t.Fatalf("first upload: %v", err)
	}
	second, err := putChunk(context.Background(), c, id, data)
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}

	if second.GetAlreadyPresent() == first.GetAlreadyPresent() && !second.GetAlreadyPresent() {
		t.Error("second upload of identical data was not deduplicated")
	}
	if !second.GetAlreadyPresent() {
		t.Error("second upload reported as new")
	}
	if second.GetBytesWritten() != 0 {
		t.Errorf("deduplicated upload wrote %d bytes, want 0", second.GetBytesWritten())
	}
}

// A chunk uploaded under the wrong name must be rejected — otherwise the
// invariant that a chunk's name is its hash quietly stops being true.
func TestNodeRejectsMisdeclaredChunk(t *testing.T) {
	c := nodeClient(t)

	honest := []byte("these are the real bytes")
	lie := []byte("these are NOT the real bytes at all")

	_, err := putChunk(context.Background(), c, chunk.Sum(honest), lie)
	if err == nil {
		t.Fatal("node accepted a chunk whose contents did not match its name")
	}
	// InvalidArgument specifically: the uploader is at fault. Reporting
	// DataLoss here would make the coordinator suspect this node's disk and
	// schedule repairs for a chunk that was never validly stored.
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("code = %s, want InvalidArgument", code)
	}
}

func TestNodeReportsCapacity(t *testing.T) {
	c := nodeClient(t)

	data := make([]byte, 1024)
	fillChunk(data, 999_002)
	id := chunk.Sum(data)
	if _, err := putChunk(context.Background(), c, id, data); err != nil {
		t.Fatalf("upload: %v", err)
	}

	st, err := c.StatChunk(context.Background(), &storagev1.StatChunkRequest{
		Ref: &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1},
	})
	if err != nil {
		t.Fatalf("StatChunk: %v", err)
	}
	if !st.GetExists() || st.GetSize() != int64(len(data)) {
		t.Errorf("stat = exists:%v size:%d, want exists:true size:%d",
			st.GetExists(), st.GetSize(), len(data))
	}
}

// Bitrot, end to end against the live containerised node: corrupt the bytes in
// the node's real volume, underneath it, and confirm it refuses to serve them.
//
// The unit tests cover this too, but only this version proves it survives the
// journey through gRPC framing, a container boundary and a Docker volume.
func TestLiveCorruptionIsDetected(t *testing.T) {
	dataDir := os.Getenv("NODE_DATA_DIR")
	if dataDir == "" {
		t.Skip("NODE_DATA_DIR is not set; the node volume is not mounted here")
	}
	c := nodeClient(t)

	data := make([]byte, 64<<10)
	fillChunk(data, 999_100)
	id := chunk.Sum(data)
	if _, err := putChunk(context.Background(), c, id, data); err != nil {
		t.Fatalf("upload: %v", err)
	}

	// Reads must work before we touch anything, or the test proves nothing.
	if got, err := getChunk(context.Background(), c, id); err != nil {
		t.Fatalf("read before corruption: %v", err)
	} else if !bytes.Equal(got, data) {
		t.Fatal("chunk did not round trip before corruption")
	}

	hex := id.String()
	path := filepath.Join(dataDir, "chunks", hex[0:2], hex[2:4], hex+".chunk")

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read chunk file %s: %v", path, err)
	}
	raw[len(raw)/2] ^= 0x40 // a single flipped bit, exactly like real bitrot
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("corrupt chunk file: %v", err)
	}

	_, err = getChunk(context.Background(), c, id)
	if err == nil {
		t.Fatal("node served a corrupted chunk as if it were good")
	}
	if code := status.Code(err); code != codes.DataLoss {
		t.Errorf("code = %s, want DataLoss", code)
	}
	t.Logf("corruption correctly refused: %v", err)
}

// --- helpers -------------------------------------------------------------

func putChunk(ctx context.Context, c storagev1.StorageNodeClient, id chunk.ID, data []byte) (*storagev1.PutChunkResponse, error) {
	stream, err := c.PutChunk(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&storagev1.PutChunkRequest{
		Payload: &storagev1.PutChunkRequest_Header{
			Header: &storagev1.PutChunkHeader{
				Ref:  &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1},
				Size: int64(len(data)),
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("send header: %w", err)
	}

	const frame = 256 << 10
	for off := 0; off < len(data); off += frame {
		end := min(off+frame, len(data))
		if err := stream.Send(&storagev1.PutChunkRequest{
			Payload: &storagev1.PutChunkRequest_Data{Data: data[off:end]},
		}); err != nil {
			if errors.Is(err, io.EOF) {
				// The server closed early, which means it rejected the
				// upload; CloseAndRecv carries the real status.
				break
			}
			return nil, fmt.Errorf("send data: %w", err)
		}
	}
	return stream.CloseAndRecv()
}

func getChunk(ctx context.Context, c storagev1.StorageNodeClient, id chunk.ID) ([]byte, error) {
	stream, err := c.GetChunk(ctx, &storagev1.GetChunkRequest{
		Ref: &storagev1.ChunkRef{Id: id.Bytes(), ShardIndex: -1},
	})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(chunkSize)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return buf.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		buf.Write(msg.GetData())
	}
}

func byteSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
