package obs

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type routeKey struct{}

// routeSlot is a mutable cell shared down the handler chain.
//
// It has to be mutable. The route is only known once the mux has matched,
// which happens *inside* the middleware — and a context value written by an
// inner handler is invisible to the outer one, because r.WithContext derives a
// new request rather than mutating the one the middleware is holding. Writing
// through a pointer is what lets the metrics layer see a label chosen after it
// already called next.ServeHTTP. (Go 1.22's r.Pattern has the same problem:
// ServeMux sets it on the request it passes down, not on ours.)
//
// Only ever written before the response completes, and read after, so there is
// no concurrent access to guard.
type routeSlot struct{ name string }

// ensureRouteSlot installs a slot if the chain does not already have one, so
// Metrics and AccessLog each work whether or not the other is present.
func ensureRouteSlot(r *http.Request) (*http.Request, *routeSlot) {
	if slot, ok := r.Context().Value(routeKey{}).(*routeSlot); ok {
		return r, slot
	}
	slot := &routeSlot{}
	return r.WithContext(context.WithValue(r.Context(), routeKey{}, slot)), slot
}

// Route tags a handler with the metric label to record it under. Always pass a
// template ("/v1/b/{bucket}/o/{key}"), never an interpolated path — labelling
// by raw path mints a time series per object key and kills Prometheus.
func Route(name string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slot, ok := r.Context().Value(routeKey{}).(*routeSlot); ok {
			slot.name = name
		}
		h.ServeHTTP(w, r)
	})
}

func routeFrom(ctx context.Context) string {
	if slot, ok := ctx.Value(routeKey{}).(*routeSlot); ok && slot.name != "" {
		return slot.name
	}
	return "unmatched"
}

// recorder captures the status and byte count of a response.
//
// It deliberately implements Unwrap rather than forwarding Flush/Hijack by
// hand: http.ResponseController walks the Unwrap chain, so streaming a chunked
// download through this wrapper keeps working without us re-declaring every
// optional interface the stdlib might add.
type recorder struct {
	http.ResponseWriter
	status  int
	written int64
}

func (rec *recorder) WriteHeader(code int) {
	if rec.status == 0 {
		rec.status = code
		rec.ResponseWriter.WriteHeader(code)
	}
}

func (rec *recorder) Write(b []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	n, err := rec.ResponseWriter.Write(b)
	rec.written += int64(n)
	return n, err
}

func (rec *recorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

// countingBody tracks how many bytes a handler actually read from the request,
// which for uploads is the number that matters — Content-Length is a claim,
// this is the measurement.
type countingBody struct {
	rc   interface{ Read([]byte) (int, error) }
	read int64
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.read += int64(n)
	return n, err
}

// Metrics records RED metrics and byte counters for every request.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpInFlight.Inc()
		defer httpInFlight.Dec()

		r, _ = ensureRouteSlot(r)

		body := &countingBody{rc: r.Body}
		r.Body = readCloser{Reader: body, Closer: r.Body}

		rec := &recorder{ResponseWriter: w}
		start := time.Now()

		next.ServeHTTP(rec, r)

		route := routeFrom(r.Context())
		if rec.status == 0 {
			rec.status = http.StatusOK
		}

		httpDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
		httpRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		httpRequestBytes.WithLabelValues(route).Add(float64(body.read))
		httpResponseBytes.WithLabelValues(route).Add(float64(rec.written))
	})
}

type readCloser struct {
	Reader interface{ Read([]byte) (int, error) }
	Closer interface{ Close() error }
}

func (rc readCloser) Read(p []byte) (int, error) { return rc.Reader.Read(p) }
func (rc readCloser) Close() error               { return rc.Closer.Close() }

// AccessLog logs one line per completed request at the end of the handler
// chain. Health and metrics endpoints are excluded — at a 10s scrape interval
// they would otherwise be 90% of your log volume.
func AccessLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, _ = ensureRouteSlot(r)

		rec := &recorder{ResponseWriter: w}
		start := time.Now()

		next.ServeHTTP(rec, r)

		if isNoise(r.URL.Path) {
			return
		}
		if rec.status == 0 {
			rec.status = http.StatusOK
		}

		level := slog.LevelInfo
		switch {
		case rec.status >= 500:
			level = slog.LevelError
		case rec.status >= 400:
			level = slog.LevelWarn
		}

		log.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("route", routeFrom(r.Context())),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Int64("response_bytes", rec.written),
			slog.Duration("duration", time.Since(start)),
			slog.String("remote", r.RemoteAddr),
		)
	})
}

func isNoise(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/metrics":
		return true
	default:
		return false
	}
}

// Chain applies middleware so that the first argument is the outermost layer,
// matching the order you would read them in a request's life.
func Chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
