package restapi

import "testing"

// Range handling is easy to get subtly wrong and the failure mode is silent:
// the client gets a 206 with the wrong bytes and has no way to tell.
func TestParseRange(t *testing.T) {
	const size = 1000

	cases := []struct {
		name       string
		header     string
		wantOffset int64
		wantLength int64
		wantPartial bool
	}{
		{"absent header means the whole object", "", 0, 1000, false},
		{"explicit range", "bytes=0-99", 0, 100, true},
		{"mid-object range", "bytes=500-599", 500, 100, true},
		{"open-ended range", "bytes=900-", 900, 100, true},
		{"suffix range", "bytes=-100", 900, 100, true},
		{"suffix longer than the object clamps", "bytes=-5000", 0, 1000, true},
		// RFC 9110 requires clamping an over-long end rather than rejecting it.
		{"end past the object clamps", "bytes=990-2000", 990, 10, true},
		{"single byte", "bytes=0-0", 0, 1, true},
		{"last byte", "bytes=999-999", 999, 1, true},
		{"whole object explicitly", "bytes=0-999", 0, 1000, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			offset, length, partial, err := parseRange(c.header, size)
			if err != nil {
				t.Fatalf("parseRange(%q): %v", c.header, err)
			}
			if offset != c.wantOffset || length != c.wantLength || partial != c.wantPartial {
				t.Errorf("parseRange(%q) = (%d, %d, %v), want (%d, %d, %v)",
					c.header, offset, length, partial,
					c.wantOffset, c.wantLength, c.wantPartial)
			}
			if offset+length > size {
				t.Errorf("parseRange(%q) reads past the end of the object", c.header)
			}
		})
	}
}

func TestParseRangeRejectsUnsatisfiable(t *testing.T) {
	const size = 1000

	for _, header := range []string{
		"bytes=1000-",     // starts at the end
		"bytes=5000-6000", // entirely past the end
		"bytes=100-50",    // inverted
		"bytes=abc-def",   // not numbers
		"bytes=-",         // no bounds
		"bytes=-0",        // zero-length suffix
		"items=0-99",      // wrong unit
		"bytes=0-10,20-30", // multi-range needs multipart encoding
	} {
		if _, _, _, err := parseRange(header, size); err == nil {
			t.Errorf("parseRange(%q) succeeded, want an error", header)
		}
	}
}

// Bucket names follow S3's rules from the start, so anything created through
// the native API in Phase 2 is still addressable through the S3 API in Phase 5.
func TestValidBucketName(t *testing.T) {
	valid := []string{"photos", "my-bucket", "a.b.c", "bucket123", "abc"}
	for _, name := range valid {
		if !validBucketName(name) {
			t.Errorf("validBucketName(%q) = false, want true", name)
		}
	}

	invalid := []string{
		"",                    // empty
		"ab",                  // too short
		"UPPERCASE",           // uppercase is not addressable as a subdomain
		"has space",           //
		"-leading",            // hyphen at the edge
		"trailing-",           //
		"under_score",         //
		"bucket/with/slashes", //
		"this-name-is-far-too-long-to-be-a-valid-bucket-name-because-it-exceeds-sixty-three",
	}
	for _, name := range invalid {
		if validBucketName(name) {
			t.Errorf("validBucketName(%q) = true, want false", name)
		}
	}
}
