package obs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The route label is set by an inner handler but read by an outer middleware.
// Getting this wrong is silent: every metric lands under "unmatched" and the
// RED dashboards become useless without anything failing.
func TestMetricsSeesRouteLabelSetDownstream(t *testing.T) {
	var observed string

	mux := http.NewServeMux()
	mux.Handle("GET /v1/thing/{id}", Route("/v1/thing/{id}", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))

	// Stand-in for the metrics layer: outermost, reads the label afterwards.
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r, _ = ensureRouteSlot(r)
			next.ServeHTTP(w, r)
			observed = routeFrom(r.Context())
		})
	}

	h := Chain(mux, outer)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/thing/42", nil))

	if observed != "/v1/thing/{id}" {
		t.Fatalf("route label = %q, want the template (got the raw path or a miss)", observed)
	}
}

func TestUnmatchedRouteFallsBack(t *testing.T) {
	var observed string
	h := Chain(http.NotFoundHandler(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r, _ = ensureRouteSlot(r)
			next.ServeHTTP(w, r)
			observed = routeFrom(r.Context())
		})
	})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/nope", nil))

	if observed != "unmatched" {
		t.Fatalf("route label = %q, want %q", observed, "unmatched")
	}
}

func TestRequestIDIsReusedAndSanitised(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))

	t.Run("valid inbound id is reused", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(HeaderRequestID, "deadbeefcafe1234")
		h.ServeHTTP(rec, req)

		if seen != "deadbeefcafe1234" {
			t.Errorf("context id = %q, want the inbound id", seen)
		}
		if got := rec.Header().Get(HeaderRequestID); got != "deadbeefcafe1234" {
			t.Errorf("echoed id = %q, want the inbound id", got)
		}
	})

	t.Run("hostile inbound id is replaced", func(t *testing.T) {
		// A client-controlled string reaching every log line is a log
		// injection vector, so anything non-hex is discarded.
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(HeaderRequestID, "evil\ninjected=1")
		h.ServeHTTP(httptest.NewRecorder(), req)

		if strings.Contains(seen, "\n") || seen == "evil\ninjected=1" {
			t.Errorf("context id = %q, want a freshly minted id", seen)
		}
		if len(seen) != 32 {
			t.Errorf("minted id = %q (len %d), want 32 hex chars", seen, len(seen))
		}
	})

	t.Run("absent id is minted", func(t *testing.T) {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		if len(seen) != 32 {
			t.Errorf("minted id = %q (len %d), want 32 hex chars", seen, len(seen))
		}
	})
}

// The gateway streams multi-gigabyte downloads, so the response wrapper must
// not hide the Flusher underneath it.
func TestRecorderPreservesFlushing(t *testing.T) {
	h := Metrics(Route("/stream", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "part-1"); err != nil {
			t.Errorf("write: %v", err)
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush through the recorder failed: %v", err)
		}
	})))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if !rec.Flushed {
		t.Error("response was never flushed; streaming would buffer")
	}
	if got := rec.Body.String(); got != "part-1" {
		t.Errorf("body = %q, want %q", got, "part-1")
	}
}
