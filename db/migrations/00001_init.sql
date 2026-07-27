-- +goose Up
-- +goose StatementBegin

-- Storage nodes. Phase 2 registers a single node by configuration; Phase 3
-- replaces that with real registration and heartbeats.
CREATE TABLE nodes (
    id                TEXT PRIMARY KEY,
    addr              TEXT        NOT NULL,
    zone              TEXT        NOT NULL DEFAULT '',
    capacity_bytes    BIGINT      NOT NULL DEFAULT 0,
    used_bytes        BIGINT      NOT NULL DEFAULT 0,
    state             TEXT        NOT NULL DEFAULT 'joining',
    last_heartbeat_at TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE buckets (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT        NOT NULL UNIQUE,
    owner_id           TEXT        NOT NULL DEFAULT '',
    versioning_enabled BOOLEAN     NOT NULL DEFAULT false,
    -- 0 means unlimited. Phase 5 enforces per-user quotas on top of this.
    quota_bytes        BIGINT      NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per distinct chunk in the cluster. The primary key is the BLAKE3
-- digest of the contents, which is what makes deduplication automatic: a
-- second upload of identical bytes collides here and only bumps refcount.
CREATE TABLE chunks (
    id         BYTEA PRIMARY KEY,
    size       BIGINT      NOT NULL CHECK (size >= 0),
    -- Number of object versions referencing this chunk. GC deletes at zero.
    refcount   BIGINT      NOT NULL DEFAULT 0 CHECK (refcount >= 0),
    ec_scheme  TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Objects are immutable and versioned. Overwriting a key inserts a new row and
-- clears is_latest on the old one, in one transaction, so a reader never
-- observes a half-written object.
CREATE TABLE objects (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_id     UUID        NOT NULL REFERENCES buckets (id) ON DELETE CASCADE,
    key           TEXT        NOT NULL,
    version_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
    size          BIGINT      NOT NULL CHECK (size >= 0),
    content_type  TEXT        NOT NULL DEFAULT 'application/octet-stream',
    etag          TEXT        NOT NULL,
    storage_class TEXT        NOT NULL DEFAULT 'replicated',
    metadata      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    is_latest     BOOLEAN     NOT NULL DEFAULT true,
    deleted_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket_id, key, version_id)
);

-- At most one current version per key. This is the invariant that makes
-- "latest" well defined; the database enforces it rather than the application
-- promising to.
CREATE UNIQUE INDEX objects_one_latest_idx
    ON objects (bucket_id, key) WHERE is_latest;

-- Prefix listing. text_pattern_ops makes LIKE 'prefix%' index-usable
-- regardless of the database's collation.
CREATE INDEX objects_prefix_idx
    ON objects (bucket_id, key text_pattern_ops) WHERE is_latest AND deleted_at IS NULL;

-- The ordered chunk list that reconstitutes an object.
CREATE TABLE object_chunks (
    object_id   UUID   NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    chunk_id    BYTEA  NOT NULL REFERENCES chunks (id),
    byte_offset BIGINT NOT NULL,
    size        BIGINT NOT NULL,
    PRIMARY KEY (object_id, seq)
);

CREATE INDEX object_chunks_chunk_idx ON object_chunks (chunk_id);

-- Which nodes hold which chunks. shard_index is -1 for a whole replica and
-- 0..(k+m-1) for an erasure-coded shard (Phase 6).
CREATE TABLE chunk_placements (
    chunk_id         BYTEA   NOT NULL REFERENCES chunks (id) ON DELETE CASCADE,
    node_id          TEXT    NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    shard_index      INTEGER NOT NULL DEFAULT -1,
    state            TEXT    NOT NULL DEFAULT 'ok',
    last_verified_at TIMESTAMPTZ,
    PRIMARY KEY (chunk_id, node_id, shard_index)
);

-- Finding a chunk's live locations is the hottest read in the system.
CREATE INDEX chunk_placements_lookup_idx
    ON chunk_placements (chunk_id) WHERE state = 'ok';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS chunk_placements;
DROP TABLE IF EXISTS object_chunks;
DROP TABLE IF EXISTS objects;
DROP TABLE IF EXISTS chunks;
DROP TABLE IF EXISTS buckets;
DROP TABLE IF EXISTS nodes;
-- +goose StatementEnd
