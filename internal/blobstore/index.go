package blobstore

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/soumic28/dfs/internal/chunk"
)

// The index is node-local metadata only: chunk sizes and timestamps. It is
// never the source of truth for what the cluster contains — that is the
// coordinator's PostgreSQL. Losing index.db costs a rescan at boot, nothing
// more, because the chunk files carry their own identity in their names.
var chunkBucket = []byte("chunks")

// Record layout, fixed 24 bytes:
//
//	[0:8]   size            int64 big endian
//	[8:16]  created_at      unix nanoseconds
//	[16:24] last_scrubbed   unix nanoseconds, 0 if never
const recordSize = 24

type index struct {
	db *bolt.DB
}

func openIndex(path string) (*index, error) {
	db, err := bolt.Open(path, 0o644, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(chunkBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &index{db: db}, nil
}

func (i *index) Close() error { return i.db.Close() }

// Get returns a chunk's metadata.
func (i *index) Get(id chunk.ID) (Info, bool, error) {
	var (
		info  Info
		found bool
	)
	err := i.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(chunkBucket).Get(id[:])
		if v == nil {
			return nil
		}
		var err error
		info, err = decodeRecord(id, v)
		found = err == nil
		return err
	})
	return info, found, err
}

// Put stores metadata, reporting whether this created a new entry. The caller
// uses that to keep capacity accounting correct when two writers race on the
// same chunk.
func (i *index) Put(id chunk.ID, info Info) (bool, error) {
	inserted := false
	err := i.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(chunkBucket)
		if b.Get(id[:]) != nil {
			return nil // already known; leave the original timestamps alone
		}
		inserted = true
		return b.Put(id[:], encodeRecord(info))
	})
	return inserted, err
}

// Delete removes an entry, reporting whether it existed.
func (i *index) Delete(id chunk.ID) (bool, error) {
	existed := false
	err := i.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(chunkBucket)
		if b.Get(id[:]) == nil {
			return nil
		}
		existed = true
		return b.Delete(id[:])
	})
	return existed, err
}

// MarkScrubbed records that a chunk was verified at t.
func (i *index) MarkScrubbed(id chunk.ID, t time.Time) error {
	return i.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(chunkBucket)
		v := b.Get(id[:])
		if v == nil {
			return nil // deleted while being scrubbed; nothing to record
		}
		info, err := decodeRecord(id, v)
		if err != nil {
			return err
		}
		info.LastScrubbedAt = t
		return b.Put(id[:], encodeRecord(info))
	})
}

// All loads the whole index. Safe because it is bounded by a node's capacity:
// 13 GiB of 8 MiB chunks is under 2000 entries.
func (i *index) All() (map[chunk.ID]Info, error) {
	out := make(map[chunk.ID]Info)
	err := i.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(chunkBucket).ForEach(func(k, v []byte) error {
			id, err := chunk.IDFromBytes(k)
			if err != nil {
				return fmt.Errorf("corrupt index key: %w", err)
			}
			info, err := decodeRecord(id, v)
			if err != nil {
				return err
			}
			out[id] = info
			return nil
		})
	})
	return out, err
}

// ScrubOrder returns every chunk ID, least recently scrubbed first, so the
// scrubber always works on the chunk that has gone longest without
// verification.
func (i *index) ScrubOrder() ([]Info, error) {
	all, err := i.All()
	if err != nil {
		return nil, err
	}
	out := make([]Info, 0, len(all))
	for _, info := range all {
		out = append(out, info)
	}
	sortByLastScrubbed(out)
	return out, nil
}

func encodeRecord(info Info) []byte {
	var buf [recordSize]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(info.Size))
	binary.BigEndian.PutUint64(buf[8:16], uint64(info.CreatedAt.UnixNano()))
	if !info.LastScrubbedAt.IsZero() {
		binary.BigEndian.PutUint64(buf[16:24], uint64(info.LastScrubbedAt.UnixNano()))
	}
	return buf[:]
}

func decodeRecord(id chunk.ID, v []byte) (Info, error) {
	if len(v) != recordSize {
		return Info{}, fmt.Errorf("corrupt index record for %s: %d bytes, want %d",
			id.Short(), len(v), recordSize)
	}
	info := Info{
		ID:        id,
		Size:      int64(binary.BigEndian.Uint64(v[0:8])),
		CreatedAt: time.Unix(0, int64(binary.BigEndian.Uint64(v[8:16]))),
	}
	if ns := int64(binary.BigEndian.Uint64(v[16:24])); ns != 0 {
		info.LastScrubbedAt = time.Unix(0, ns)
	}
	return info, nil
}
