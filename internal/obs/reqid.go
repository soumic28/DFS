package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// HeaderRequestID is the header carrying a request's correlation ID. It is
// echoed back to the client so a user reporting "my upload failed" can hand
// you the exact ID to grep for.
const HeaderRequestID = "X-Request-Id"

type requestIDKey struct{}

// NewRequestID returns a fresh 128-bit correlation ID.
func NewRequestID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error as of Go 1.24; it panics on
	// failure of the system source, which is the correct behaviour here.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// WithRequestID returns a context carrying id.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the correlation ID in ctx, or "" if there is none.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// RequestID middleware attaches a correlation ID to every request, reusing an
// inbound one when present so a trace survives the hop through Caddy and the
// gateway into the storage tier.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if !validRequestID(id) {
			id = NewRequestID()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
}

// validRequestID rejects inbound IDs that are absent, oversized, or contain
// anything but hex. Without this a client controls a string that lands in
// every log line, which is a log-injection vector.
func validRequestID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F', c == '-':
		default:
			return false
		}
	}
	return true
}
