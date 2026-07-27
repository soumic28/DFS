-- name: CreateBucket :one
INSERT INTO buckets (name, owner_id, versioning_enabled, quota_bytes)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetBucketByName :one
SELECT * FROM buckets WHERE name = $1;

-- name: ListBuckets :many
SELECT * FROM buckets ORDER BY name;

-- name: DeleteBucket :execrows
DELETE FROM buckets WHERE name = $1;

-- name: BucketUsedBytes :one
SELECT COALESCE(SUM(size), 0)::BIGINT AS used
FROM objects
WHERE bucket_id = $1 AND is_latest AND deleted_at IS NULL;
