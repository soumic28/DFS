package config

import "testing"

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"512", 512},
		{"8MiB", 8 * MiB},
		{"8 MiB", 8 * MiB},
		{"13GiB", 13 * GiB},
		{"13958643712", 13 * GiB}, // the literal value used in compose
		{"20MB", 20 * MB},
		{"1.5GiB", 1610612736},
		{"4k", 4 * KiB},
		{"1024b", 1024},
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		if err != nil {
			t.Errorf("ParseBytes(%q) returned error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseBytesRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "  ", "abc", "-5MiB", "MiB", "8XiB"} {
		if _, err := ParseBytes(in); err == nil {
			t.Errorf("ParseBytes(%q) succeeded, want error", in)
		}
	}
}

func TestFormatBytesRoundTrips(t *testing.T) {
	for _, n := range []int64{0, 512, 8 * MiB, 13 * GiB} {
		got, err := ParseBytes(FormatBytes(n))
		if err != nil {
			t.Fatalf("FormatBytes(%d) produced unparseable %q: %v", n, FormatBytes(n), err)
		}
		if got != n {
			t.Errorf("round trip of %d gave %d (via %q)", n, got, FormatBytes(n))
		}
	}
}
