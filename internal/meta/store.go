// Package meta is the coordinator's data layer: the namespace, chunk registry
// and placement map, on top of PostgreSQL.
//
// Everything the cluster needs to be strongly consistent lives here, and it is
// consistent for one reason — a single relational database with real
// transactions. The distributed parts of this system (immutable chunks,
// content addressing, quorum writes) are deliberately arranged so that all the
// ordering problems collapse into transactions in this package.
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/soumi/dfs/internal/chunk"
	"github.com/soumi/dfs/internal/meta/dbgen"
)

// Errors callers are expected to distinguish.
var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrObjectNotFound = errors.New("object not found")
	ErrBucketExists   = errors.New("bucket already exists")
	ErrQuotaExceeded  = errors.New("bucket quota exceeded")
	ErrNoNodes        = errors.New("no live storage nodes")
)

// Store is the coordinator's view of the cluster.
type Store struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
}

// Open connects to PostgreSQL and verifies the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	// The coordinator is a single process serving a handful of gateways; a
	// large pool would just hold idle connections that PostgreSQL pays for.
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Store{pool: pool, q: dbgen.New(pool)}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Ping reports whether the database is reachable. Used by /readyz.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// inTx runs fn inside a transaction, rolling back on error or panic.
func (s *Store) inTx(ctx context.Context, fn func(*dbgen.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// Rollback after a successful Commit is a no-op, so this is safe
	// unconditionally and survives a panic in fn.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(s.q.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- buckets -------------------------------------------------------------

// Bucket describes a namespace.
type Bucket struct {
	ID                uuid.UUID
	Name              string
	OwnerID           string
	VersioningEnabled bool
	QuotaBytes        int64
	CreatedAt         time.Time
}

// CreateBucket creates a bucket, reporting ErrBucketExists if the name is taken.
func (s *Store) CreateBucket(ctx context.Context, name, ownerID string, versioning bool, quota int64) (Bucket, error) {
	b, err := s.q.CreateBucket(ctx, dbgen.CreateBucketParams{
		Name:              name,
		OwnerID:           ownerID,
		VersioningEnabled: versioning,
		QuotaBytes:        quota,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return Bucket{}, fmt.Errorf("%w: %s", ErrBucketExists, name)
		}
		return Bucket{}, fmt.Errorf("create bucket: %w", err)
	}
	return toBucket(b), nil
}

// GetBucket looks a bucket up by name.
func (s *Store) GetBucket(ctx context.Context, name string) (Bucket, error) {
	b, err := s.q.GetBucketByName(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Bucket{}, fmt.Errorf("%w: %s", ErrBucketNotFound, name)
		}
		return Bucket{}, fmt.Errorf("get bucket: %w", err)
	}
	return toBucket(b), nil
}

// ListBuckets returns every bucket, alphabetically.
func (s *Store) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.q.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	out := make([]Bucket, len(rows))
	for i, b := range rows {
		out[i] = toBucket(b)
	}
	return out, nil
}

// DeleteBucket removes an empty bucket.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	n, err := s.q.DeleteBucket(ctx, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s", ErrBucketNotFound, name)
	}
	return nil
}

// --- chunk allocation ----------------------------------------------------

// Allocation tells a gateway what to do with a chunk it is about to upload.
type Allocation struct {
	// AlreadyExists means the cluster already holds these bytes. The gateway
	// skips the upload entirely — this is where deduplication saves the
	// network round trip, not just the disk space.
	AlreadyExists bool

	// TargetAddrs are the nodes to write to when the chunk is new.
	TargetAddrs []string

	// WriteQuorum is how many must acknowledge before the write counts.
	WriteQuorum int
}

// AllocateChunk registers a chunk and returns where to put it.
//
// Placement in Phase 2 is "every live node", because there is one. Phase 3
// replaces the node selection with weighted rendezvous hashing; nothing else
// in the call has to change.
func (s *Store) AllocateChunk(ctx context.Context, id chunk.ID, size int64, replicationFactor, writeQuorum int) (Allocation, error) {
	// An existing chunk with this id necessarily has these bytes, because the
	// id is the hash of the bytes. No verification needed, no upload needed.
	if placements, err := s.q.GetPlacements(ctx, id.Bytes()); err != nil {
		return Allocation{}, fmt.Errorf("get placements: %w", err)
	} else if len(placements) > 0 {
		return Allocation{AlreadyExists: true}, nil
	}

	nodes, err := s.q.ListLiveNodes(ctx)
	if err != nil {
		return Allocation{}, fmt.Errorf("list nodes: %w", err)
	}
	if len(nodes) == 0 {
		return Allocation{}, ErrNoNodes
	}

	targets := selectTargets(id, nodes, replicationFactor)
	addrs := make([]string, len(targets))
	for i, n := range targets {
		addrs[i] = n.Addr
	}

	if _, err := s.q.UpsertChunk(ctx, dbgen.UpsertChunkParams{ID: id.Bytes(), Size: size}); err != nil {
		return Allocation{}, fmt.Errorf("upsert chunk: %w", err)
	}

	return Allocation{
		TargetAddrs: addrs,
		WriteQuorum: min(writeQuorum, len(addrs)),
	}, nil
}

// ConfirmPlacement records that a node accepted a chunk.
func (s *Store) ConfirmPlacement(ctx context.Context, id chunk.ID, nodeID string, shardIndex int32) error {
	return s.q.UpsertPlacement(ctx, dbgen.UpsertPlacementParams{
		ChunkID:    id.Bytes(),
		NodeID:     nodeID,
		ShardIndex: shardIndex,
	})
}

// --- objects -------------------------------------------------------------

// ObjectChunk is one entry in an object's ordered chunk list.
type ObjectChunk struct {
	Seq        int32
	ChunkID    chunk.ID
	ByteOffset int64
	Size       int64
	NodeAddrs  []string
}

// Object is a committed object version.
type Object struct {
	ID          uuid.UUID
	Bucket      string
	Key         string
	VersionID   uuid.UUID
	Size        int64
	ContentType string
	ETag        string
	Metadata    map[string]string
	CreatedAt   time.Time
	Chunks      []ObjectChunk
}

// CommitRequest is everything needed to make an upload visible.
type CommitRequest struct {
	Bucket      string
	Key         string
	Size        int64
	ContentType string
	ETag        string
	Metadata    map[string]string
	Chunks      []CommitChunk
}

// CommitChunk is one uploaded chunk with the nodes that acknowledged it.
type CommitChunk struct {
	Seq        int32
	ChunkID    chunk.ID
	ByteOffset int64
	Size       int64
	NodeIDs    []string
}

// CommitObject makes an upload visible, atomically.
//
// This single transaction is the whole consistency story for the object
// namespace. It demotes the previous version, inserts the new one with its
// ordered chunk list, records placements and bumps refcounts. Either a reader
// sees the complete new version or it sees the complete old one; there is no
// interval in which a key has no current version, or two.
func (s *Store) CommitObject(ctx context.Context, req CommitRequest) (Object, error) {
	var out Object

	err := s.inTx(ctx, func(q *dbgen.Queries) error {
		bucket, err := q.GetBucketByName(ctx, req.Bucket)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, req.Bucket)
			}
			return fmt.Errorf("get bucket: %w", err)
		}

		if bucket.QuotaBytes > 0 {
			used, err := q.BucketUsedBytes(ctx, bucket.ID)
			if err != nil {
				return fmt.Errorf("bucket usage: %w", err)
			}
			if used+req.Size > bucket.QuotaBytes {
				return fmt.Errorf("%w: %d + %d > %d", ErrQuotaExceeded, used, req.Size, bucket.QuotaBytes)
			}
		}

		// Demote the current version first: the unique partial index on
		// (bucket_id, key) WHERE is_latest would otherwise reject the insert.
		if err := q.ClearLatestVersion(ctx, dbgen.ClearLatestVersionParams{
			BucketID: bucket.ID,
			Key:      req.Key,
		}); err != nil {
			return fmt.Errorf("clear previous version: %w", err)
		}

		metadata, err := json.Marshal(orEmptyMap(req.Metadata))
		if err != nil {
			return fmt.Errorf("encode metadata: %w", err)
		}

		obj, err := q.InsertObject(ctx, dbgen.InsertObjectParams{
			BucketID:     bucket.ID,
			Key:          req.Key,
			Size:         req.Size,
			ContentType:  orDefault(req.ContentType, "application/octet-stream"),
			Etag:         req.ETag,
			StorageClass: "replicated",
			Metadata:     metadata,
		})
		if err != nil {
			return fmt.Errorf("insert object: %w", err)
		}

		for _, c := range req.Chunks {
			if err := q.InsertObjectChunk(ctx, dbgen.InsertObjectChunkParams{
				ObjectID:   obj.ID,
				Seq:        c.Seq,
				ChunkID:    c.ChunkID.Bytes(),
				ByteOffset: c.ByteOffset,
				Size:       c.Size,
			}); err != nil {
				return fmt.Errorf("insert chunk %d: %w", c.Seq, err)
			}

			for _, nodeID := range c.NodeIDs {
				if err := q.UpsertPlacement(ctx, dbgen.UpsertPlacementParams{
					ChunkID:    c.ChunkID.Bytes(),
					NodeID:     nodeID,
					ShardIndex: -1,
				}); err != nil {
					return fmt.Errorf("record placement: %w", err)
				}
			}

			// Refcount is per object version, so an object that repeats the
			// same chunk holds one reference per occurrence and GC cannot
			// remove it while any of them remain.
			if err := q.IncrementChunkRefcount(ctx, c.ChunkID.Bytes()); err != nil {
				return fmt.Errorf("increment refcount: %w", err)
			}
		}

		out = Object{
			ID:          obj.ID,
			Bucket:      req.Bucket,
			Key:         obj.Key,
			VersionID:   obj.VersionID,
			Size:        obj.Size,
			ContentType: obj.ContentType,
			ETag:        obj.Etag,
			Metadata:    orEmptyMap(req.Metadata),
			CreatedAt:   obj.CreatedAt,
		}
		return nil
	})

	return out, err
}

// LookupObject resolves an object to its ordered chunk list and the live nodes
// holding each chunk. Pass an empty versionID for the current version.
func (s *Store) LookupObject(ctx context.Context, bucket, key string, versionID *uuid.UUID) (Object, error) {
	b, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return Object{}, err
	}

	var obj dbgen.Object
	if versionID == nil {
		obj, err = s.q.GetLatestObject(ctx, dbgen.GetLatestObjectParams{BucketID: b.ID, Key: key})
	} else {
		obj, err = s.q.GetObjectVersion(ctx, dbgen.GetObjectVersionParams{
			BucketID: b.ID, Key: key, VersionID: *versionID,
		})
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Object{}, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return Object{}, fmt.Errorf("get object: %w", err)
	}

	rows, err := s.q.GetObjectChunkPlacements(ctx, obj.ID)
	if err != nil {
		return Object{}, fmt.Errorf("get chunk placements: %w", err)
	}

	chunks := make([]ObjectChunk, len(rows))
	for i, r := range rows {
		id, err := chunk.IDFromBytes(r.ChunkID)
		if err != nil {
			return Object{}, fmt.Errorf("chunk %d of %s/%s: %w", r.Seq, bucket, key, err)
		}
		chunks[i] = ObjectChunk{
			Seq:        r.Seq,
			ChunkID:    id,
			ByteOffset: r.ByteOffset,
			Size:       r.Size,
			NodeAddrs:  r.NodeAddrs,
		}
	}

	return Object{
		ID:          obj.ID,
		Bucket:      bucket,
		Key:         obj.Key,
		VersionID:   obj.VersionID,
		Size:        obj.Size,
		ContentType: obj.ContentType,
		ETag:        obj.Etag,
		Metadata:    decodeMetadata(obj.Metadata),
		CreatedAt:   obj.CreatedAt,
		Chunks:      chunks,
	}, nil
}

// ObjectSummary is a listing entry.
type ObjectSummary struct {
	Key        string
	Size       int64
	ETag       string
	ModifiedAt time.Time
}

// ListResult is a page of a listing.
type ListResult struct {
	Objects        []ObjectSummary
	CommonPrefixes []string
	NextToken      string
	IsTruncated    bool
}

// ListObjects returns a page of a bucket's current objects.
//
// When delimiter is set, keys sharing a prefix up to the next delimiter are
// rolled up into CommonPrefixes, which is what makes a flat key space look
// like folders to both S3 clients and the dashboard.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, delimiter, after string, maxKeys int32) (ListResult, error) {
	b, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return ListResult{}, err
	}
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	// Fetch one extra row to learn whether another page exists without a
	// second count query.
	rows, err := s.q.ListObjects(ctx, dbgen.ListObjectsParams{
		BucketID: b.ID,
		Column2:  &prefix,
		Key:      after,
		Limit:    maxKeys + 1,
	})
	if err != nil {
		return ListResult{}, fmt.Errorf("list objects: %w", err)
	}

	res := ListResult{Objects: []ObjectSummary{}, CommonPrefixes: []string{}}
	seenPrefix := map[string]bool{}

	for i, o := range rows {
		if int32(i) >= maxKeys {
			res.IsTruncated = true
			break
		}
		if delimiter != "" {
			if cp, ok := commonPrefix(o.Key, prefix, delimiter); ok {
				if !seenPrefix[cp] {
					seenPrefix[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
				}
				res.NextToken = o.Key
				continue
			}
		}
		res.Objects = append(res.Objects, ObjectSummary{
			Key:        o.Key,
			Size:       o.Size,
			ETag:       o.Etag,
			ModifiedAt: o.CreatedAt,
		})
		res.NextToken = o.Key
	}

	if !res.IsTruncated {
		res.NextToken = ""
	}
	return res, nil
}

// DeleteObject tombstones the current version and releases its chunk
// references. The chunks themselves are removed later by GC, once nothing
// references them and an upload in flight can no longer claim them.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) (bool, error) {
	var deleted bool

	err := s.inTx(ctx, func(q *dbgen.Queries) error {
		b, err := q.GetBucketByName(ctx, bucket)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
			}
			return fmt.Errorf("get bucket: %w", err)
		}

		obj, err := q.GetLatestObject(ctx, dbgen.GetLatestObjectParams{BucketID: b.ID, Key: key})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // deleting something absent is not an error
			}
			return fmt.Errorf("get object: %w", err)
		}

		if err := q.DecrementChunkRefcounts(ctx, obj.ID); err != nil {
			return fmt.Errorf("decrement refcounts: %w", err)
		}

		n, err := q.SoftDeleteObject(ctx, dbgen.SoftDeleteObjectParams{BucketID: b.ID, Key: key})
		if err != nil {
			return fmt.Errorf("soft delete: %w", err)
		}
		deleted = n > 0
		return nil
	})

	return deleted, err
}

// --- nodes ---------------------------------------------------------------

// Node is a storage node as the coordinator sees it.
type Node struct {
	ID            string
	Addr          string
	Zone          string
	CapacityBytes int64
	UsedBytes     int64
	State         string
}

// RegisterNode records a node as live.
func (s *Store) RegisterNode(ctx context.Context, id, addr, zone string, capacity int64) (Node, error) {
	n, err := s.q.UpsertNode(ctx, dbgen.UpsertNodeParams{
		ID: id, Addr: addr, Zone: zone, CapacityBytes: capacity,
	})
	if err != nil {
		return Node{}, fmt.Errorf("register node: %w", err)
	}
	return toNode(n), nil
}

// ListNodes returns every known node.
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.q.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out := make([]Node, len(rows))
	for i, n := range rows {
		out[i] = toNode(n)
	}
	return out, nil
}

// NodeIDForAddr maps a node address back to its id, which the gateway needs
// because it talks to nodes by address but commits placements by id.
func (s *Store) NodeIDForAddr(ctx context.Context, addr string) (string, error) {
	nodes, err := s.q.ListLiveNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	for _, n := range nodes {
		if n.Addr == addr {
			return n.ID, nil
		}
	}
	return "", fmt.Errorf("no live node at %s", addr)
}

// MarkPlacementBad flags a replica as corrupt or missing so the repair
// pipeline (Phase 4) will rebuild it.
func (s *Store) MarkPlacementBad(ctx context.Context, id chunk.ID, nodeID, state string) error {
	return s.q.MarkPlacementBad(ctx, dbgen.MarkPlacementBadParams{
		ChunkID: id.Bytes(), NodeID: nodeID, State: state,
	})
}

// --- helpers -------------------------------------------------------------

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func toBucket(b dbgen.Bucket) Bucket {
	return Bucket{
		ID: b.ID, Name: b.Name, OwnerID: b.OwnerID,
		VersioningEnabled: b.VersioningEnabled,
		QuotaBytes:        b.QuotaBytes, CreatedAt: b.CreatedAt,
	}
}

func toNode(n dbgen.Node) Node {
	return Node{
		ID: n.ID, Addr: n.Addr, Zone: n.Zone,
		CapacityBytes: n.CapacityBytes, UsedBytes: n.UsedBytes, State: n.State,
	}
}

func decodeMetadata(raw []byte) map[string]string {
	out := map[string]string{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}

func orEmptyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// commonPrefix rolls a key up to the next delimiter after prefix, the way S3
// list-with-delimiter presents a flat key space as folders.
func commonPrefix(key, prefix, delimiter string) (string, bool) {
	rest := key[len(prefix):]
	idx := indexOf(rest, delimiter)
	if idx < 0 {
		return "", false
	}
	return prefix + rest[:idx+len(delimiter)], true
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
