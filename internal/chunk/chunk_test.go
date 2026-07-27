package chunk

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/quick"
)

// The identity rule the whole system rests on: the same bytes always produce
// the same name, and different bytes never do.
func TestSumIsDeterministic(t *testing.T) {
	f := func(data []byte) bool {
		return Sum(data) == Sum(data)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

func TestSumDistinguishesContent(t *testing.T) {
	a := Sum([]byte("chunk contents"))
	b := Sum([]byte("chunk contentt")) // one byte different
	if a == b {
		t.Fatal("two different inputs produced the same chunk ID")
	}
}

// Streaming must agree with the one-shot digest, whatever the write sizes —
// otherwise a chunk hashed during upload gets a different name than the same
// chunk hashed during verification.
func TestHasherMatchesSumAcrossArbitraryWrites(t *testing.T) {
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	want := Sum(data)

	for _, writeSize := range []int{1, 7, 512, 4096, 65536, len(data)} {
		h := NewHasher()
		for off := 0; off < len(data); off += writeSize {
			end := min(off+writeSize, len(data))
			if _, err := h.Write(data[off:end]); err != nil {
				t.Fatalf("write size %d: %v", writeSize, err)
			}
		}
		if got := h.ID(); got != want {
			t.Errorf("write size %d: ID = %s, want %s", writeSize, got.Short(), want.Short())
		}
		if h.Size() != int64(len(data)) {
			t.Errorf("write size %d: Size = %d, want %d", writeSize, h.Size(), len(data))
		}
	}
}

func TestHasherReset(t *testing.T) {
	h := NewHasher()
	_, _ = h.Write([]byte("discard this"))
	h.Reset()
	_, _ = h.Write([]byte("keep this"))

	if got, want := h.ID(), Sum([]byte("keep this")); got != want {
		t.Errorf("after Reset: ID = %s, want %s", got.Short(), want.Short())
	}
	if h.Size() != int64(len("keep this")) {
		t.Errorf("after Reset: Size = %d, want %d", h.Size(), len("keep this"))
	}
}

func TestIDEncodingRoundTrips(t *testing.T) {
	id := Sum([]byte("encode me"))

	parsed, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if parsed != id {
		t.Error("hex round trip changed the ID")
	}

	fromBytes, err := IDFromBytes(id.Bytes())
	if err != nil {
		t.Fatalf("IDFromBytes: %v", err)
	}
	if fromBytes != id {
		t.Error("byte round trip changed the ID")
	}
	if len(id.String()) != 64 {
		t.Errorf("hex length = %d, want 64", len(id.String()))
	}
	if len(id.Short()) != 12 {
		t.Errorf("short length = %d, want 12", len(id.Short()))
	}
}

// Malformed IDs arrive from the network. They must be rejected at the edge,
// not turned into a lookup for a chunk that cannot exist.
func TestIDParsingRejectsMalformed(t *testing.T) {
	t.Run("wrong byte length", func(t *testing.T) {
		for _, n := range []int{0, 1, 31, 33, 64} {
			if _, err := IDFromBytes(make([]byte, n)); !errors.Is(err, ErrInvalidID) {
				t.Errorf("IDFromBytes(%d bytes): err = %v, want ErrInvalidID", n, err)
			}
		}
	})

	t.Run("bad hex", func(t *testing.T) {
		for _, s := range []string{
			"",
			"abcd",
			"zz" + "00000000000000000000000000000000000000000000000000000000000000"[:62],
		} {
			if _, err := ParseID(s); !errors.Is(err, ErrInvalidID) {
				t.Errorf("ParseID(%q): err = %v, want ErrInvalidID", s, err)
			}
		}
	})
}

// The verifying reader is the last line of defence before corrupt bytes reach
// a client. It must fail at EOF, after the whole stream has been seen.
func TestVerifyingReaderAcceptsGoodData(t *testing.T) {
	data := bytes.Repeat([]byte("verified"), 1000)
	vr := NewVerifyingReader(bytes.NewReader(data), Sum(data))

	got, err := io.ReadAll(vr)
	if err != nil {
		t.Fatalf("reading valid data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("verifying reader altered the data")
	}
	if vr.Size() != int64(len(data)) {
		t.Errorf("Size = %d, want %d", vr.Size(), len(data))
	}
}

func TestVerifyingReaderRejectsAlteredData(t *testing.T) {
	original := bytes.Repeat([]byte("verified"), 1000)
	want := Sum(original)

	altered := bytes.Clone(original)
	altered[len(altered)/2] ^= 0x01

	_, err := io.ReadAll(NewVerifyingReader(bytes.NewReader(altered), want))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("err = %v, want ErrChecksumMismatch", err)
	}
}

func TestVerifyingReaderRejectsTruncatedData(t *testing.T) {
	original := bytes.Repeat([]byte("verified"), 1000)
	truncated := original[:len(original)-1]

	_, err := io.ReadAll(NewVerifyingReader(bytes.NewReader(truncated), Sum(original)))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("err = %v, want ErrChecksumMismatch", err)
	}
}

func TestHashingWriterMatchesSum(t *testing.T) {
	data := bytes.Repeat([]byte("teed off"), 5000)

	var sink bytes.Buffer
	hw := NewHashingWriter(&sink)
	if _, err := io.Copy(hw, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if got := hw.ID(); got != Sum(data) {
		t.Errorf("ID = %s, want %s", got.Short(), Sum(data).Short())
	}
	if !bytes.Equal(sink.Bytes(), data) {
		t.Error("hashing writer altered the bytes passing through it")
	}
	if hw.Size() != int64(len(data)) {
		t.Errorf("Size = %d, want %d", hw.Size(), len(data))
	}
}

func TestZeroValueID(t *testing.T) {
	var id ID
	if !id.IsZero() {
		t.Error("zero value reported as set")
	}
	if Sum([]byte("anything")).IsZero() {
		t.Error("a real digest reported as zero")
	}
	// The empty input still has a well-defined, non-zero digest.
	if Sum(nil).IsZero() {
		t.Error("digest of empty input reported as zero")
	}
}

func BenchmarkHashThroughput(b *testing.B) {
	data := make([]byte, 8<<20) // one 8 MiB chunk
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		h := NewHasher()
		_, _ = h.Write(data)
		_ = h.ID()
	}
}
