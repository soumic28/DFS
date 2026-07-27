-- name: UpsertNode :one
INSERT INTO nodes (id, addr, zone, capacity_bytes, state, last_heartbeat_at)
VALUES ($1, $2, $3, $4, 'live', now())
ON CONFLICT (id) DO UPDATE
SET addr = EXCLUDED.addr,
    zone = EXCLUDED.zone,
    capacity_bytes = EXCLUDED.capacity_bytes,
    state = 'live',
    last_heartbeat_at = now()
RETURNING *;

-- name: ListLiveNodes :many
SELECT * FROM nodes WHERE state = 'live' ORDER BY id;

-- name: ListNodes :many
SELECT * FROM nodes ORDER BY id;

-- name: GetNode :one
SELECT * FROM nodes WHERE id = $1;

-- name: UpdateNodeUsage :exec
UPDATE nodes
SET used_bytes = $2, last_heartbeat_at = now()
WHERE id = $1;

-- name: SetNodeState :exec
UPDATE nodes SET state = $2 WHERE id = $1;
