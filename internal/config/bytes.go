package config

import (
	"fmt"
	"strconv"
	"strings"
)

// Byte size units. Binary (KiB) and decimal (KB) are both accepted because
// disk vendors and RFCs disagree and operators type whichever they know.
const (
	KB = 1000
	MB = 1000 * KB
	GB = 1000 * MB
	TB = 1000 * GB

	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

// Longest suffixes first so "MiB" is matched before "B".
var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"KIB", KiB}, {"MIB", MiB}, {"GIB", GiB}, {"TIB", TiB},
	{"KB", KB}, {"MB", MB}, {"GB", GB}, {"TB", TB},
	{"K", KiB}, {"M", MiB}, {"G", GiB}, {"T", TiB},
	{"B", 1},
}

// ParseBytes converts a human byte size to a count of bytes. A bare number is
// interpreted as bytes, so "13958643712" and "13GiB" are equivalent.
func ParseBytes(s string) (int64, error) {
	raw := strings.ToUpper(strings.TrimSpace(s))
	if raw == "" {
		return 0, fmt.Errorf("empty byte size")
	}

	num, scale := raw, int64(1)
	for _, u := range byteUnits {
		if rest, ok := strings.CutSuffix(raw, u.suffix); ok {
			num, scale = strings.TrimSpace(rest), u.scale
			break
		}
	}

	n, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size %q must not be negative", s)
	}
	return int64(n * float64(scale)), nil
}

// FormatBytes renders a byte count using binary units, for logs and the CLI.
func FormatBytes(n int64) string {
	switch {
	case n >= TiB:
		return fmt.Sprintf("%.1fTiB", float64(n)/TiB)
	case n >= GiB:
		return fmt.Sprintf("%.1fGiB", float64(n)/GiB)
	case n >= MiB:
		return fmt.Sprintf("%.1fMiB", float64(n)/MiB)
	case n >= KiB:
		return fmt.Sprintf("%.1fKiB", float64(n)/KiB)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
