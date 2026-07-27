-- name: UpsertChunk :one
-- Returns xmax = 0 when the row was inserted rather than updated, which is how
-- we tell a genuinely new chunk from a deduplication hit in a single round trip.
INSERT INTO chunks (id, size)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET size = chunks.size
RETURNING id, size, refcount, ec_scheme, created_at, (xmax = 0) AS inserted;

-- name: GetChunk :one
SELECT * FROM chunks WHERE id = $1;

-- name: IncrementChunkRefcount :exec
UPDATE chunks SET refcount = refcount + 1 WHERE id = $1;

-- name: DecrementChunkRefcounts :exec
-- Decrements every chunk referenced by an object version. Clamped at zero so a
-- double delete cannot drive the count negative and trip the CHECK constraint.
UPDATE chunks
SET refcount = GREATEST(refcount - 1, 0)
WHERE id IN (SELECT chunk_id FROM object_chunks WHERE object_id = $1);

-- name: ListUnreferencedChunks :many
-- GC candidates: no references left, and old enough that an upload in flight
-- cannot still be about to reference them.
SELECT * FROM chunks
WHERE refcount = 0 AND created_at < $1
ORDER BY created_at
LIMIT $2;

-- name: DeleteChunk :execrows
DELETE FROM chunks WHERE id = $1 AND refcount = 0;

-- name: UpsertPlacement :exec
INSERT INTO chunk_placements (chunk_id, node_id, shard_index, state, last_verified_at)
VALUES ($1, $2, $3, 'ok', now())
ON CONFLICT (chunk_id, node_id, shard_index)
DO UPDATE SET state = 'ok', last_verified_at = now();

-- name: GetPlacements :many
SELECT p.chunk_id, p.node_id, p.shard_index, n.addr
FROM chunk_placements p
JOIN nodes n ON n.id = p.node_id
WHERE p.chunk_id = $1 AND p.state = 'ok'
ORDER BY p.shard_index;

-- name: MarkPlacementBad :exec
UPDATE chunk_placements
SET state = $3
WHERE chunk_id = $1 AND node_id = $2;

-- name: GetObjectChunkPlacements :many
-- One query returns an object's whole chunk list with every live location,
-- so reading an object costs a single round trip to the coordinator rather
-- than one per chunk.
SELECT
    oc.seq,
    oc.chunk_id,
    oc.byte_offset,
    oc.size,
    COALESCE(
        ARRAY_AGG(n.addr ORDER BY n.addr) FILTER (WHERE n.addr IS NOT NULL),
        ARRAY[]::TEXT[]
    )::TEXT[] AS node_addrs
FROM object_chunks oc
LEFT JOIN chunk_placements p ON p.chunk_id = oc.chunk_id AND p.state = 'ok'
LEFT JOIN nodes n ON n.id = p.node_id
WHERE oc.object_id = $1
GROUP BY oc.seq, oc.chunk_id, oc.byte_offset, oc.size
ORDER BY oc.seq;
