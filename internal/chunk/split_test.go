package chunk

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func collect(t *testing.T, r io.Reader, size int64) ([]Piece, []byte) {
	t.Helper()

	s, err := NewSplitter(r, size)
	if err != nil {
		t.Fatalf("NewSplitter: %v", err)
	}

	var (
		pieces []Piece
		joined []byte
	)
	for {
		p, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		// Data aliases the splitter's buffer and is invalid after the next
		// call, so anything kept has to be copied.
		joined = append(joined, p.Data...)
		p.Data = nil
		pieces = append(pieces, p)
	}
	return pieces, joined
}

func TestSplitterReassembles(t *testing.T) {
	for _, n := range []int{0, 1, 999, 1000, 1001, 4096, 100_000} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i * 31)
		}

		pieces, joined := collect(t, bytes.NewReader(data), 1000)
		if !bytes.Equal(joined, data) {
			t.Errorf("size %d: reassembly mismatch", n)
		}

		// Offsets must be contiguous, or a ranged read lands on wrong bytes.
		var offset int64
		for i, p := range pieces {
			if p.Seq != int32(i) {
				t.Errorf("size %d: piece %d has seq %d", n, i, p.Seq)
			}
			if p.ByteOffset != offset {
				t.Errorf("size %d: piece %d offset = %d, want %d", n, i, p.ByteOffset, offset)
			}
			offset += p.Size
		}
		if offset != int64(n) {
			t.Errorf("size %d: offsets covered %d bytes", n, offset)
		}
	}
}

// An empty object still gets one chunk, so it reads back through exactly the
// same path as any other object rather than needing a special case.
func TestSplitterEmitsOneChunkForEmptyInput(t *testing.T) {
	pieces, joined := collect(t, bytes.NewReader(nil), 1000)

	if len(pieces) != 1 {
		t.Fatalf("got %d pieces for empty input, want 1", len(pieces))
	}
	if pieces[0].Size != 0 || len(joined) != 0 {
		t.Errorf("piece size = %d, want 0", pieces[0].Size)
	}
	if pieces[0].ID != Sum(nil) {
		t.Error("empty chunk has the wrong id")
	}
}

// Boundaries must depend only on the data, never on how the bytes arrived.
// If a slow network could shift boundaries, identical files uploaded over
// different connections would fail to deduplicate — silently and unreproducibly.
func TestSplitterBoundariesAreIndependentOfReadSizes(t *testing.T) {
	data := make([]byte, 50_000)
	for i := range data {
		data[i] = byte(i * 7)
	}

	want, _ := collect(t, bytes.NewReader(data), 4096)

	// iotest.OneByteReader delivers one byte per Read, the pathological case.
	for name, r := range map[string]io.Reader{
		"one byte at a time": iotest.OneByteReader(bytes.NewReader(data)),
		"half reads":         iotest.HalfReader(bytes.NewReader(data)),
		"data-err reader":    iotest.DataErrReader(bytes.NewReader(data)),
	} {
		got, joined := collect(t, r, 4096)

		if !bytes.Equal(joined, data) {
			t.Errorf("%s: reassembly mismatch", name)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d chunks, want %d", name, len(got), len(want))
		}
		for i := range got {
			if got[i].ID != want[i].ID {
				t.Errorf("%s: chunk %d id = %s, want %s — boundaries shifted with read size",
					name, i, got[i].ID.Short(), want[i].ID.Short())
			}
			if got[i].Size != want[i].Size {
				t.Errorf("%s: chunk %d size = %d, want %d", name, i, got[i].Size, want[i].Size)
			}
		}
	}
}

func TestSplitterIdenticalDataProducesIdenticalChunks(t *testing.T) {
	data := bytes.Repeat([]byte("dedupe me"), 5000)

	a, _ := collect(t, bytes.NewReader(data), 4096)
	b, _ := collect(t, bytes.NewReader(data), 4096)

	if len(a) != len(b) {
		t.Fatalf("chunk counts differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("chunk %d differs between identical inputs", i)
		}
	}
}

func TestSplitterWholeIDMatchesSum(t *testing.T) {
	data := bytes.Repeat([]byte("whole stream digest"), 3000)

	s, err := NewSplitter(bytes.NewReader(data), 4096)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := s.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}

	if s.WholeID() != Sum(data) {
		t.Errorf("WholeID = %s, want %s", s.WholeID().Short(), Sum(data).Short())
	}
	if s.Total() != int64(len(data)) {
		t.Errorf("Total = %d, want %d", s.Total(), len(data))
	}
}

func TestSplitterRejectsBadSizes(t *testing.T) {
	for _, size := range []int64{-1, MaxSize + 1} {
		if _, err := NewSplitter(bytes.NewReader(nil), size); err == nil {
			t.Errorf("NewSplitter(size=%d) succeeded, want an error", size)
		}
	}
	if _, err := NewSplitter(bytes.NewReader(nil), 0); err != nil {
		t.Errorf("size 0 should mean DefaultSize, got %v", err)
	}
}

func TestSplitterPropagatesReadErrors(t *testing.T) {
	wantErr := errors.New("network died")
	r := io.MultiReader(bytes.NewReader(make([]byte, 100)), iotest.ErrReader(wantErr))

	s, err := NewSplitter(r, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want it to wrap %v", err, wantErr)
	}
}
