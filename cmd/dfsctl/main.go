// Command dfsctl is the DFS command line client and operator tool.
//
// It ships as a static binary so the same tool works on your laptop and on the
// VPS without a runtime to install.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

const usage = `dfsctl - DFS command line client

Usage:
  dfsctl [flags] <command> [args]

Commands:
  version            Print client and server versions
  cluster status     Show node health and capacity        (phase 3)
  cp <src> <dst>     Copy to or from dfs://bucket/key      (phase 2)
  ls <bucket>[/pfx]  List objects                          (phase 2)
  rm <dfs-uri>       Delete an object                      (phase 2)

Flags:
`

func main() {
	fs := flag.NewFlagSet("dfsctl", flag.ExitOnError)
	endpoint := fs.String("endpoint", envOr("DFS_ENDPOINT", ""), "DFS gateway base URL (env: DFS_ENDPOINT)")
	timeout := fs.Duration("timeout", 30*time.Second, "request timeout")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	if fs.NArg() == 0 {
		fs.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, fs.Arg(0), fs.Args()[1:], *endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "dfsctl: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, _ []string, endpoint string) error {
	switch cmd {
	case "version":
		return printVersion(ctx, endpoint)
	case "cluster", "cp", "ls", "rm":
		return fmt.Errorf("%q is not implemented yet; see docs/ROADMAP.md", cmd)
	default:
		return fmt.Errorf("unknown command %q (try: dfsctl -h)", cmd)
	}
}

func printVersion(ctx context.Context, endpoint string) error {
	if endpoint == "" {
		return errors.New("no endpoint configured: pass -endpoint or set DFS_ENDPOINT")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v1/version", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("contacting %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s", resp.Status)
	}

	var v map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	fmt.Printf("server version %s (commit %s)\n", v["version"], v["commit"])
	return nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
