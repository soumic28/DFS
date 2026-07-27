package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// ProbeArg is the argument that switches a binary into health-probe mode.
const ProbeArg = "probe"

// maybeRunProbe implements `<service> probe`: a one-shot HTTP request against
// this process's own admin endpoint, exiting 0 if healthy and 1 otherwise.
//
// The distroless runtime image has no shell, no curl and no wget, so the
// binary is its own healthcheck. Compose invokes it as:
//
//	healthcheck:
//	  test: ["CMD", "/service", "probe"]
//
// It never returns when a probe was requested — it exits the process.
func maybeRunProbe(adminAddr string) {
	if len(os.Args) < 2 || os.Args[1] != ProbeArg {
		return
	}

	path := "/healthz"
	if len(os.Args) > 2 && os.Args[2] == "ready" {
		path = "/readyz"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url := "http://" + probeHost(adminAddr) + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "probe: %s returned %s\n", url, resp.Status)
		os.Exit(1)
	}
	os.Exit(0)
}

// probeHost turns a listen address into a dialable one. ":9100" means "every
// interface" when listening, but is not a valid destination.
func probeHost(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1:9100"
	}
	if host == "" || host == "0.0.0.0" || host == "::" || strings.EqualFold(host, "[::]") {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
