-- name: ClearLatestVersion :exec
-- Demotes the current version of a key. Run inside CommitObject's transaction
-- immediately before inserting the new one, so the unique partial index on
-- (bucket_id, key) WHERE is_latest is never violated and no reader ever sees
-- zero or two current versions.
UPDATE objects
SET is_latest = false
WHERE bucket_id = $1 AND key = $2 AND is_latest;

-- name: InsertObject :one
INSERT INTO objects (
    bucket_id, key, size, content_type, etag, storage_class, metadata
) VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: InsertObjectChunk :exec
INSERT INTO object_chunks (object_id, seq, chunk_id, byte_offset, size)
VALUES ($1, $2, $3, $4, $5);

-- name: GetLatestObject :one
SELECT * FROM objects
WHERE bucket_id = $1 AND key = $2 AND is_latest AND deleted_at IS NULL;

-- name: GetObjectVersion :one
SELECT * FROM objects
WHERE bucket_id = $1 AND key = $2 AND version_id = $3 AND deleted_at IS NULL;

-- name: ListObjectVersions :many
SELECT * FROM objects
WHERE bucket_id = $1 AND key = $2
ORDER BY created_at DESC;

-- name: ListObjects :many
-- Keyset pagination on (key): stable under concurrent writes, unlike OFFSET,
-- and the continuation token is just the last key returned.
SELECT * FROM objects
WHERE bucket_id = $1
  AND is_latest
  AND deleted_at IS NULL
  AND key LIKE $2 || '%'
  AND key > $3
ORDER BY key
LIMIT $4;

-- name: SoftDeleteObject :execrows
-- A tombstone rather than a row removal: the chunk list has to survive until
-- refcounts have been decremented and GC has run.
UPDATE objects
SET deleted_at = now(), is_latest = false
WHERE bucket_id = $1 AND key = $2 AND is_latest AND deleted_at IS NULL;

-- name: ListDeletableObjects :many
-- Tombstoned versions whose chunks are ready to be dereferenced.
SELECT * FROM objects
WHERE deleted_at IS NOT NULL AND deleted_at < $1
ORDER BY deleted_at
LIMIT $2;

-- name: PurgeObject :exec
DELETE FROM objects WHERE id = $1;
