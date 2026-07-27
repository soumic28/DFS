// Package chunk defines chunk identity: the BLAKE3 hash that names a chunk's
// contents.
//
// The identity rule that the whole system rests on: a chunk's ID *is* the hash
// of its bytes. That makes chunks immutable by construction — you cannot change
// a chunk's contents without changing its name — which in turn means replicas
// never need write coordination, caches never go stale, and identical data is
// stored once cluster-wide.
package chunk

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
)

// IDSize is the length of a chunk ID in bytes. BLAKE3's default 256-bit output
// is far beyond any realistic collision risk: storing 2^64 distinct chunks
// leaves collision probability below 2^-128.
const IDSize = 32

// ID names a chunk by the BLAKE3-256 digest of its contents.
type ID [IDSize]byte

// ErrInvalidID is returned when bytes cannot be interpreted as a chunk ID.
var ErrInvalidID = errors.New("invalid chunk id")

// Sum computes the ID of a complete in-memory buffer. For streams, use Hasher.
func Sum(data []byte) ID {
	return ID(blake3.Sum256(data))
}

// IDFromBytes converts a wire-format ID, rejecting anything of the wrong length.
func IDFromBytes(b []byte) (ID, error) {
	if len(b) != IDSize {
		return ID{}, fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidID, len(b), IDSize)
	}
	return ID(b), nil
}

// ParseID converts a hex-encoded ID, as used in file names and log lines.
func ParseID(s string) (ID, error) {
	if len(s) != hex.EncodedLen(IDSize) {
		return ID{}, fmt.Errorf("%w: got %d hex chars, want %d", ErrInvalidID, len(s), hex.EncodedLen(IDSize))
	}
	var id ID
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return ID{}, fmt.Errorf("%w: %s", ErrInvalidID, err)
	}
	return id, nil
}

// Bytes returns the ID in wire format.
func (id ID) Bytes() []byte { return id[:] }

// String returns the full hex encoding. This is the on-disk file name.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the first 12 hex characters, for log lines where the full
// digest is noise.
func (id ID) Short() string { return hex.EncodeToString(id[:6]) }

// IsZero reports whether the ID is unset.
func (id ID) IsZero() bool { return id == ID{} }

// LogValue keeps chunk IDs readable in structured logs without every call site
// remembering to stringify them.
func (id ID) LogValue() string { return id.Short() }
