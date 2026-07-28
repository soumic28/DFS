package chunk

import (
	"errors"
	"fmt"
	"io"
)

// DefaultSize is the production chunk size.
//
// 8 MiB is a compromise: large enough that per-chunk metadata and round trips
// stay negligible against the payload, small enough that a failed chunk is
// cheap to retry and a range request need not pull tens of megabytes to serve
// a few kilobytes.
const DefaultSize = 8 << 20

// MaxSize bounds what a caller may configure. Beyond this a single chunk stops
// fitting comfortably in a gRPC message window and in a node's write buffer.
const MaxSize = 64 << 20

// Piece is one chunk produced by a Splitter.
type Piece struct {
	Seq        int32
	ID         ID
	ByteOffset int64
	Size       int64

	// Data aliases the Splitter's internal buffer and is only valid until the
	// next call to Next. Copy it if you need to keep it — the whole point of
	// streaming is that we never hold the entire object in memory.
	Data []byte
}

// Splitter cuts a stream into fixed-size chunks, hashing each one as it goes.
//
// Fixed-size splitting gives whole-file and identical-prefix deduplication.
// It does not survive byte insertion: prepend one byte to a file and every
// boundary shifts, so nothing deduplicates. Content-defined chunking (FastCDC)
// fixes that and is a Phase 9 upgrade — only this type changes, because the
// rest of the system only ever sees (id, offset, size) triples.
type Splitter struct {
	r         io.Reader
	buf       []byte
	seq       int32
	offset    int64
	total     int64
	done      bool
	wholeHash *Hasher
}

// NewSplitter returns a Splitter reading from r. A size of zero means
// DefaultSize.
func NewSplitter(r io.Reader, size int64) (*Splitter, error) {
	if size == 0 {
		size = DefaultSize
	}
	if size < 0 || size > MaxSize {
		return nil, fmt.Errorf("chunk size %d out of range (1..%d)", size, MaxSize)
	}
	return &Splitter{
		r:         r,
		buf:       make([]byte, size),
		wholeHash: NewHasher(),
	}, nil
}

// Next returns the next chunk, or io.EOF when the stream is exhausted.
//
// A zero-length stream yields exactly one empty chunk, so that an empty object
// still has a chunk list and reads back through the same code path as any
// other object rather than needing a special case.
func (s *Splitter) Next() (Piece, error) {
	if s.done {
		return Piece{}, io.EOF
	}

	// ReadFull rather than Read: a short Read is normal on a network stream
	// and would otherwise produce undersized chunks whose boundaries depend on
	// packet timing — which would wreck deduplication non-deterministically.
	n, err := io.ReadFull(s.r, s.buf)
	switch {
	case err == nil:
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		s.done = true
		if n == 0 && s.seq > 0 {
			return Piece{}, io.EOF
		}
	default:
		return Piece{}, fmt.Errorf("read chunk %d: %w", s.seq, err)
	}

	data := s.buf[:n]
	_, _ = s.wholeHash.Write(data)

	p := Piece{
		Seq:        s.seq,
		ID:         Sum(data),
		ByteOffset: s.offset,
		Size:       int64(n),
		Data:       data,
	}

	s.seq++
	s.offset += int64(n)
	s.total += int64(n)
	return p, nil
}

// Total returns the bytes read so far.
func (s *Splitter) Total() int64 { return s.total }

// Count returns the number of chunks produced so far.
func (s *Splitter) Count() int32 { return s.seq }

// WholeID returns the digest of the entire stream, which is only meaningful
// once Next has returned io.EOF.
func (s *Splitter) WholeID() ID { return s.wholeHash.ID() }
