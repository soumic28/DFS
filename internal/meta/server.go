package meta

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "github.com/soumic28/dfs/api/gen/metadata/v1"
	storagev1 "github.com/soumic28/dfs/api/gen/storage/v1"
	"github.com/soumic28/dfs/internal/chunk"
)

// Server implements metadata.v1.Metadata.
type Server struct {
	metadatav1.UnimplementedMetadataServer

	store             *Store
	log               *slog.Logger
	replicationFactor int
	writeQuorum       int
}

// NewServer returns the coordinator's gRPC service.
func NewServer(store *Store, log *slog.Logger, replicationFactor, writeQuorum int) *Server {
	return &Server{
		store:             store,
		log:               log,
		replicationFactor: replicationFactor,
		writeQuorum:       writeQuorum,
	}
}

func (s *Server) CreateBucket(ctx context.Context, req *metadatav1.CreateBucketRequest) (*metadatav1.CreateBucketResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is required")
	}

	b, err := s.store.CreateBucket(ctx, req.GetName(), req.GetOwnerId(),
		req.GetVersioningEnabled(), req.GetQuotaBytes())
	if err != nil {
		return nil, s.mapError("create bucket", err)
	}
	return &metadatav1.CreateBucketResponse{BucketId: b.ID.String()}, nil
}

func (s *Server) AllocateChunk(ctx context.Context, req *metadatav1.AllocateChunkRequest) (*metadatav1.AllocateChunkResponse, error) {
	id, err := chunk.IDFromBytes(req.GetChunkId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	alloc, err := s.store.AllocateChunk(ctx, id, req.GetSize(), s.replicationFactor, s.writeQuorum)
	if err != nil {
		return nil, s.mapError("allocate chunk", err)
	}

	return &metadatav1.AllocateChunkResponse{
		AlreadyExists:   alloc.AlreadyExists,
		TargetNodeAddrs: alloc.TargetAddrs,
		WriteQuorum:     int32(alloc.WriteQuorum),
	}, nil
}

func (s *Server) CommitObject(ctx context.Context, req *metadatav1.CommitObjectRequest) (*metadatav1.CommitObjectResponse, error) {
	if req.GetBucket() == "" || req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket and key are required")
	}

	chunks := make([]CommitChunk, 0, len(req.GetChunks()))
	for _, c := range req.GetChunks() {
		id, err := chunk.IDFromBytes(c.GetRef().GetId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "chunk %d: %v", c.GetSeq(), err)
		}
		// The gateway reports the addresses that acknowledged; the commit
		// records node ids, so translate here where the node table is at hand.
		nodeIDs := make([]string, 0, len(c.GetNodeAddrs()))
		for _, addr := range c.GetNodeAddrs() {
			nodeID, err := s.store.NodeIDForAddr(ctx, addr)
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "chunk %d: %v", c.GetSeq(), err)
			}
			nodeIDs = append(nodeIDs, nodeID)
		}
		chunks = append(chunks, CommitChunk{
			Seq:        c.GetSeq(),
			ChunkID:    id,
			ByteOffset: c.GetByteOffset(),
			Size:       c.GetSize(),
			NodeIDs:    nodeIDs,
		})
	}

	obj, err := s.store.CommitObject(ctx, CommitRequest{
		Bucket:      req.GetBucket(),
		Key:         req.GetKey(),
		Size:        req.GetSize(),
		ContentType: req.GetContentType(),
		ETag:        req.GetEtag(),
		Metadata:    req.GetMetadata(),
		Chunks:      chunks,
	})
	if err != nil {
		return nil, s.mapError("commit object", err)
	}

	return &metadatav1.CommitObjectResponse{VersionId: obj.VersionID.String()}, nil
}

func (s *Server) LookupObject(ctx context.Context, req *metadatav1.LookupObjectRequest) (*metadatav1.LookupObjectResponse, error) {
	var version *uuid.UUID
	if v := req.GetVersionId(); v != "" {
		parsed, err := uuid.Parse(v)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid version id %q", v)
		}
		version = &parsed
	}

	obj, err := s.store.LookupObject(ctx, req.GetBucket(), req.GetKey(), version)
	if err != nil {
		// A miss is a normal answer, not a failure: report found=false so the
		// gateway can return 404 without treating it as an RPC error.
		if errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound) {
			return &metadatav1.LookupObjectResponse{Found: false}, nil
		}
		return nil, s.mapError("lookup object", err)
	}

	chunks := make([]*metadatav1.ObjectChunk, len(obj.Chunks))
	for i, c := range obj.Chunks {
		chunks[i] = &metadatav1.ObjectChunk{
			Seq:        c.Seq,
			Ref:        &storagev1.ChunkRef{Id: c.ChunkID.Bytes(), ShardIndex: -1},
			ByteOffset: c.ByteOffset,
			Size:       c.Size,
			NodeAddrs:  c.NodeAddrs,
		}
	}

	return &metadatav1.LookupObjectResponse{
		Found:       true,
		VersionId:   obj.VersionID.String(),
		Size:        obj.Size,
		ContentType: obj.ContentType,
		Etag:        obj.ETag,
		Chunks:      chunks,
		Metadata:    obj.Metadata,
	}, nil
}

func (s *Server) ListObjects(ctx context.Context, req *metadatav1.ListObjectsRequest) (*metadatav1.ListObjectsResponse, error) {
	res, err := s.store.ListObjects(ctx, req.GetBucket(), req.GetPrefix(),
		req.GetDelimiter(), req.GetContinuationToken(), req.GetMaxKeys())
	if err != nil {
		return nil, s.mapError("list objects", err)
	}

	objects := make([]*metadatav1.ObjectSummary, len(res.Objects))
	for i, o := range res.Objects {
		objects[i] = &metadatav1.ObjectSummary{
			Key:            o.Key,
			Size:           o.Size,
			Etag:           o.ETag,
			ModifiedAtUnix: o.ModifiedAt.Unix(),
		}
	}

	return &metadatav1.ListObjectsResponse{
		Objects:              objects,
		CommonPrefixes:       res.CommonPrefixes,
		NextContinuationToken: res.NextToken,
		IsTruncated:          res.IsTruncated,
	}, nil
}

func (s *Server) DeleteObject(ctx context.Context, req *metadatav1.DeleteObjectRequest) (*metadatav1.DeleteObjectResponse, error) {
	deleted, err := s.store.DeleteObject(ctx, req.GetBucket(), req.GetKey())
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			return &metadatav1.DeleteObjectResponse{Deleted: false}, nil
		}
		return nil, s.mapError("delete object", err)
	}
	return &metadatav1.DeleteObjectResponse{Deleted: deleted}, nil
}

func (s *Server) RegisterNode(ctx context.Context, req *metadatav1.RegisterNodeRequest) (*metadatav1.RegisterNodeResponse, error) {
	if req.GetNodeId() == "" || req.GetAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and addr are required")
	}
	if _, err := s.store.RegisterNode(ctx, req.GetNodeId(), req.GetAddr(),
		req.GetZone(), req.GetCapacityBytes()); err != nil {
		return nil, s.mapError("register node", err)
	}
	return &metadatav1.RegisterNodeResponse{HeartbeatIntervalMs: 3000}, nil
}

func (s *Server) ClusterStatus(ctx context.Context, _ *metadatav1.ClusterStatusRequest) (*metadatav1.ClusterStatusResponse, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, s.mapError("cluster status", err)
	}

	out := &metadatav1.ClusterStatusResponse{Nodes: make([]*metadatav1.NodeInfo, len(nodes))}
	for i, n := range nodes {
		out.Nodes[i] = &metadatav1.NodeInfo{
			NodeId:        n.ID,
			Addr:          n.Addr,
			Zone:          n.Zone,
			State:         nodeState(n.State),
			CapacityBytes: n.CapacityBytes,
			UsedBytes:     n.UsedBytes,
		}
		out.TotalCapacityBytes += n.CapacityBytes
		out.TotalUsedBytes += n.UsedBytes
	}
	return out, nil
}

// ReportBadReplica records a checksum failure seen by a reader. A read that
// detects corruption is the fastest corruption signal available — faster than
// waiting for the scrubber's next sweep.
func (s *Server) ReportBadReplica(ctx context.Context, req *metadatav1.ReportBadReplicaRequest) (*metadatav1.ReportBadReplicaResponse, error) {
	id, err := chunk.IDFromBytes(req.GetRef().GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	nodeID, err := s.store.NodeIDForAddr(ctx, req.GetNodeAddr())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	s.log.Error("replica reported bad by a reader",
		slog.String("chunk", id.String()),
		slog.String("node", nodeID),
		slog.String("reason", req.GetReason()))

	if err := s.store.MarkPlacementBad(ctx, id, nodeID, "corrupt"); err != nil {
		return nil, s.mapError("mark placement bad", err)
	}
	// TODO(phase-4): enqueue a repair job for this chunk.
	return &metadatav1.ReportBadReplicaResponse{}, nil
}

func (s *Server) mapError(op string, err error) error {
	switch {
	case errors.Is(err, ErrBucketNotFound), errors.Is(err, ErrObjectNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrBucketExists):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, ErrQuotaExceeded):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	case errors.Is(err, ErrNoNodes):
		// Retryable: nodes may come back. Distinct from a permanent failure so
		// the gateway can decide whether to retry or fail the client request.
		return status.Errorf(codes.Unavailable, "%v", err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		s.log.Error("coordinator operation failed",
			slog.String("op", op), slog.Any("error", err))
		return status.Errorf(codes.Internal, "%s: %v", op, err)
	}
}

func nodeState(s string) metadatav1.NodeState {
	switch s {
	case "joining":
		return metadatav1.NodeState_NODE_STATE_JOINING
	case "live":
		return metadatav1.NodeState_NODE_STATE_LIVE
	case "suspect":
		return metadatav1.NodeState_NODE_STATE_SUSPECT
	case "dead":
		return metadatav1.NodeState_NODE_STATE_DEAD
	case "draining":
		return metadatav1.NodeState_NODE_STATE_DRAINING
	default:
		return metadatav1.NodeState_NODE_STATE_UNSPECIFIED
	}
}
