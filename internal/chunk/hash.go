package chunk

import (
	"errors"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

// ErrChecksumMismatch means bytes did not hash to the ID that named them. It
// is always either corruption or a lie, never a benign condition.
var ErrChecksumMismatch = errors.New("chunk checksum mismatch")

// Hasher computes a chunk ID incrementally over a stream, so a multi-megabyte
// chunk is never held in memory just to be hashed.
type Hasher struct {
	h    *blake3.Hasher
	size int64
}

// NewHasher returns a Hasher ready to accept data.
func NewHasher() *Hasher {
	return &Hasher{h: blake3.New()}
}

// Write feeds bytes into the digest. It never returns an error.
func (h *Hasher) Write(p []byte) (int, error) {
	h.size += int64(len(p))
	return h.h.Write(p)
}

// ID returns the digest of everything written so far.
func (h *Hasher) ID() ID {
	var id ID
	h.h.Digest().Read(id[:])
	return id
}

// Size returns the number of bytes written so far.
func (h *Hasher) Size() int64 { return h.size }

// Reset returns the Hasher to its initial state for reuse.
func (h *Hasher) Reset() {
	h.h.Reset()
	h.size = 0
}

// VerifyingReader wraps r and checks, at EOF, that the stream hashed to want.
//
// The verification happens on the final Read — the one returning io.EOF — so a
// caller that reads to completion cannot skip it. A caller that stops early
// gets no verification, which is correct: it has not seen the whole chunk.
type VerifyingReader struct {
	r      io.Reader
	hasher *Hasher
	want   ID
	done   bool
}

// NewVerifyingReader returns a reader that fails with ErrChecksumMismatch if
// the bytes of r do not hash to want.
func NewVerifyingReader(r io.Reader, want ID) *VerifyingReader {
	return &VerifyingReader{r: r, hasher: NewHasher(), want: want}
}

func (v *VerifyingReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		_, _ = v.hasher.Write(p[:n])
	}
	if errors.Is(err, io.EOF) && !v.done {
		v.done = true
		if got := v.hasher.ID(); got != v.want {
			return n, fmt.Errorf("%w: read %s, expected %s (%d bytes)",
				ErrChecksumMismatch, got.Short(), v.want.Short(), v.hasher.Size())
		}
	}
	return n, err
}

// Size returns the number of bytes read so far.
func (v *VerifyingReader) Size() int64 { return v.hasher.Size() }

// HashingWriter wraps w and accumulates the digest of everything written
// through it, so a chunk can be hashed and stored in a single pass.
type HashingWriter struct {
	w      io.Writer
	hasher *Hasher
}

// NewHashingWriter returns a writer that tees into a BLAKE3 digest.
func NewHashingWriter(w io.Writer) *HashingWriter {
	return &HashingWriter{w: w, hasher: NewHasher()}
}

func (h *HashingWriter) Write(p []byte) (int, error) {
	n, err := h.w.Write(p)
	if n > 0 {
		_, _ = h.hasher.Write(p[:n])
	}
	return n, err
}

// ID returns the digest of everything written so far.
func (h *HashingWriter) ID() ID { return h.hasher.ID() }

// Size returns the number of bytes written so far.
func (h *HashingWriter) Size() int64 { return h.hasher.Size() }
