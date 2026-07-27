package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RED metrics for the HTTP surface: Rate, Errors, Duration.
//
// The route label is always a template ("/v1/b/{bucket}/o/{key}"), never the
// raw path. Labelling by raw path would mint a new time series per object key
// and blow up Prometheus within a day.
var (
	httpRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_http_requests_total",
			Help: "Total HTTP requests handled, by route, method and status class.",
		},
		[]string{"route", "method", "status"},
	)

	httpDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "dfs_http_request_duration_seconds",
			Help: "HTTP request latency in seconds.",
			// Wide spread: health checks land near 1ms, multi-gigabyte
			// uploads take minutes. The default buckets top out at 10s.
			Buckets: []float64{.005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
		},
		[]string{"route", "method"},
	)

	httpInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dfs_http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		},
	)

	httpRequestBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_http_request_bytes_total",
			Help: "Total bytes read from request bodies.",
		},
		[]string{"route"},
	)

	httpResponseBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dfs_http_response_bytes_total",
			Help: "Total bytes written to response bodies.",
		},
		[]string{"route"},
	)

	// BuildInfo lets you confirm which commit is actually running in
	// production, which matters the first time a deploy half-succeeds.
	buildInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dfs_build_info",
			Help: "Build metadata; the value is always 1.",
		},
		[]string{"service", "version", "commit"},
	)
)

// SetBuildInfo publishes the running build's identity.
func SetBuildInfo(service, version, commit string) {
	buildInfo.WithLabelValues(service, version, commit).Set(1)
}
