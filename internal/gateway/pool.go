// Package gateway implements the client-facing storage engine: the chunking
// pipeline on upload and the assembly pipeline on download.
//
// It is stateless. Every request carries everything needed to serve it, which
// is why this is the only tier that scales horizontally.
package gateway

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"time"

	storagev1 "github.com/soumi/dfs/api/gen/storage/v1"
)

// nodePool keeps one long-lived gRPC connection per storage node.
//
// Dialing per request would be catastrophic here: a 1 GiB upload is ~128
// chunks, and a TCP plus HTTP/2 handshake per chunk per replica would dominate
// the transfer. gRPC connections are safe for concurrent use and reconnect on
// their own, so holding them open is both faster and simpler.
type nodePool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn

	maxMsgBytes int
}

func newNodePool(maxMsgBytes int) *nodePool {
	return &nodePool{
		conns:       make(map[string]*grpc.ClientConn),
		maxMsgBytes: maxMsgBytes,
	}
}

// client returns a storage client for addr, dialing on first use.
func (p *nodePool) client(addr string) (storagev1.StorageNodeClient, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		return storagev1.NewStorageNodeClient(conn), nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Re-check: another goroutine may have dialed while we waited for the lock.
	if conn, ok := p.conns[addr]; ok {
		return storagev1.NewStorageNodeClient(conn), nil
	}

	// Plaintext is correct here: this traffic never leaves the internal Docker
	// network, which has no route to the internet. Across hosts (Phase 9) the
	// transport becomes a WireGuard mesh, encrypted at the link layer.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.maxMsgBytes),
			grpc.MaxCallSendMsgSize(p.maxMsgBytes),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", addr, err)
	}

	p.conns[addr] = conn
	return storagev1.NewStorageNodeClient(conn), nil
}

// Close shuts every connection down.
func (p *nodePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("close connection to %s: %w", addr, err)
		}
		delete(p.conns, addr)
	}
	return nil
}
