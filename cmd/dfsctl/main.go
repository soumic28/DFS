// Command dfsctl is the DFS command line client and operator tool.
//
// It ships as a static binary so the same tool works on a laptop and on the
// VPS without a runtime to install.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

const usage = `dfsctl - DFS command line client

Usage:
  dfsctl [flags] <command> [args]

Commands:
  version                       Print client and server versions
  mb <bucket>                   Make a bucket
  cp <src> <dst>                Copy to or from dfs://bucket/key
  cat <dfs-uri>                 Write an object to stdout
  ls <bucket>[/prefix]          List objects
  rm <dfs-uri>                  Delete an object
  stat <dfs-uri>                Show an object's metadata

Paths beginning dfs:// refer to the cluster; anything else is a local file.
Use - as a path to read stdin or write stdout.

Examples:
  dfsctl mb photos
  dfsctl cp ./holiday.jpg dfs://photos/2026/holiday.jpg
  dfsctl cp dfs://photos/2026/holiday.jpg ./restored.jpg
  dfsctl ls photos/2026/

Flags:
`

func main() {
	fs := flag.NewFlagSet("dfsctl", flag.ExitOnError)
	endpoint := fs.String("endpoint", envOr("DFS_ENDPOINT", "http://localhost:8080"),
		"gateway base URL (env: DFS_ENDPOINT)")
	timeout := fs.Duration("timeout", 30*time.Minute, "overall request timeout")
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

	c := &client{base: strings.TrimSuffix(*endpoint, "/"), http: &http.Client{}}

	if err := run(ctx, c, fs.Arg(0), fs.Args()[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "dfsctl: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *client, cmd string, args []string) error {
	switch cmd {
	case "version":
		return c.version(ctx)
	case "mb":
		if len(args) != 1 {
			return errors.New("usage: dfsctl mb <bucket>")
		}
		return c.makeBucket(ctx, args[0])
	case "cp":
		if len(args) != 2 {
			return errors.New("usage: dfsctl cp <src> <dst>")
		}
		return c.copy(ctx, args[0], args[1])
	case "cat":
		if len(args) != 1 {
			return errors.New("usage: dfsctl cat <dfs-uri>")
		}
		return c.copy(ctx, args[0], "-")
	case "ls":
		if len(args) != 1 {
			return errors.New("usage: dfsctl ls <bucket>[/prefix]")
		}
		return c.list(ctx, args[0])
	case "rm":
		if len(args) != 1 {
			return errors.New("usage: dfsctl rm <dfs-uri>")
		}
		return c.remove(ctx, args[0])
	case "stat":
		if len(args) != 1 {
			return errors.New("usage: dfsctl stat <dfs-uri>")
		}
		return c.stat(ctx, args[0])
	case "cluster":
		return errors.New("cluster status arrives in phase 3; see docs/ROADMAP.md")
	default:
		return fmt.Errorf("unknown command %q (try: dfsctl -h)", cmd)
	}
}

// --- client --------------------------------------------------------------

type client struct {
	base string
	http *http.Client
}

// ref is a parsed dfs://bucket/key reference.
type ref struct {
	bucket string
	key    string
}

func parseRef(s string) (ref, bool) {
	if !strings.HasPrefix(s, "dfs://") {
		return ref{}, false
	}
	rest := strings.TrimPrefix(s, "dfs://")
	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return ref{}, false
	}
	return ref{bucket: bucket, key: key}, true
}

func (c *client) objectURL(r ref) string {
	// Escape each path segment, not the whole key: slashes are structural and
	// must survive, but spaces and '#' in a segment must not.
	segments := strings.Split(r.key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return fmt.Sprintf("%s/v1/b/%s/o/%s", c.base, url.PathEscape(r.bucket), strings.Join(segments, "/"))
}

func (c *client) version(ctx context.Context) error {
	var v map[string]string
	if err := c.getJSON(ctx, c.base+"/v1/version", &v); err != nil {
		return err
	}
	fmt.Printf("server version %s (commit %s)\n", v["version"], v["commit"])
	return nil
}

func (c *client) makeBucket(ctx context.Context, bucket string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/v1/b/%s", c.base, url.PathEscape(bucket)), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		fmt.Printf("bucket %s already exists\n", bucket)
		return nil
	}
	if err := checkStatus(resp); err != nil {
		return err
	}
	fmt.Printf("created bucket %s\n", bucket)
	return nil
}

// copy moves bytes between the local filesystem and the cluster. Exactly one
// side must be a dfs:// reference.
func (c *client) copy(ctx context.Context, src, dst string) error {
	srcRef, srcRemote := parseRef(src)
	dstRef, dstRemote := parseRef(dst)

	switch {
	case srcRemote && dstRemote:
		return errors.New("server-side copy arrives with the S3 API in phase 5")
	case !srcRemote && !dstRemote:
		return errors.New("at least one path must be a dfs:// reference")
	case dstRemote:
		return c.upload(ctx, src, dstRef)
	default:
		return c.download(ctx, srcRef, dst)
	}
}

func (c *client) upload(ctx context.Context, localPath string, r ref) error {
	if r.key == "" {
		// "cp ./a.txt dfs://bucket" keeps the local file's name, matching cp.
		r.key = path.Base(localPath)
	}

	var (
		body io.Reader
		size int64 = -1
	)
	if localPath == "-" {
		body = os.Stdin
	} else {
		f, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fmt.Errorf("%s is a directory; recursive copy arrives in phase 5", localPath)
		}
		body, size = f, info.Size()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(r), body)
	if err != nil {
		return err
	}
	// Setting ContentLength lets Go send a sized body instead of chunked
	// transfer encoding, which the gateway can then account for exactly.
	req.ContentLength = size
	req.Header.Set("Content-Type", contentTypeFor(r.key))

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}

	var res struct {
		Size               int64  `json:"size"`
		ETag               string `json:"etag"`
		Chunks             int32  `json:"chunks"`
		DeduplicatedChunks int32  `json:"deduplicated_chunks"`
		BytesUploaded      int64  `json:"bytes_uploaded"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	elapsed := time.Since(start)
	line := fmt.Sprintf("uploaded %s -> dfs://%s/%s  %s in %s (%s/s)",
		localPath, r.bucket, r.key, humanBytes(res.Size),
		elapsed.Round(time.Millisecond), humanBytes(rate(res.Size, elapsed)))
	if res.DeduplicatedChunks > 0 {
		line += fmt.Sprintf("\n  %d/%d chunks deduplicated, %s actually transferred",
			res.DeduplicatedChunks, res.Chunks, humanBytes(res.BytesUploaded))
	}
	fmt.Println(line)
	return nil
}

func (c *client) download(ctx context.Context, r ref, localPath string) error {
	if r.key == "" {
		return errors.New("source must include an object key")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(r), nil)
	if err != nil {
		return err
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}

	out := io.Writer(os.Stdout)
	if localPath != "-" {
		if strings.HasSuffix(localPath, "/") || isDir(localPath) {
			localPath = path.Join(localPath, path.Base(r.key))
		}
		// Write to a temp file and rename, so an interrupted download never
		// leaves a truncated file sitting at the destination path.
		tmp, err := os.CreateTemp(path.Dir(localPath), ".dfsctl-*")
		if err != nil {
			return err
		}
		defer func() {
			tmp.Close()
			_ = os.Remove(tmp.Name())
		}()

		n, err := io.Copy(tmp, resp.Body)
		if err != nil {
			return fmt.Errorf("download: %w", err)
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmp.Name(), localPath); err != nil {
			return err
		}

		elapsed := time.Since(start)
		fmt.Printf("downloaded dfs://%s/%s -> %s  %s in %s (%s/s)\n",
			r.bucket, r.key, localPath, humanBytes(n),
			elapsed.Round(time.Millisecond), humanBytes(rate(n, elapsed)))
		return nil
	}

	_, err = io.Copy(out, resp.Body)
	return err
}

func (c *client) list(ctx context.Context, spec string) error {
	bucket, prefix, _ := strings.Cut(spec, "/")

	u := fmt.Sprintf("%s/v1/b/%s/o?delimiter=%%2F", c.base, url.PathEscape(bucket))
	if prefix != "" {
		u += "&prefix=" + url.QueryEscape(prefix)
	}

	var res struct {
		Objects []struct {
			Key        string `json:"key"`
			Size       int64  `json:"size"`
			ModifiedAt int64  `json:"modified_at"`
		} `json:"objects"`
		CommonPrefixes []string `json:"common_prefixes"`
		IsTruncated    bool     `json:"is_truncated"`
	}
	if err := c.getJSON(ctx, u, &res); err != nil {
		return err
	}

	for _, p := range res.CommonPrefixes {
		fmt.Printf("%-12s  %s\n", "DIR", p)
	}
	for _, o := range res.Objects {
		fmt.Printf("%-12s  %s  %s\n",
			humanBytes(o.Size),
			time.Unix(o.ModifiedAt, 0).Format("2006-01-02 15:04"),
			o.Key)
	}
	if len(res.Objects) == 0 && len(res.CommonPrefixes) == 0 {
		fmt.Println("(empty)")
	}
	if res.IsTruncated {
		fmt.Println("... more results available")
	}
	return nil
}

func (c *client) remove(ctx context.Context, spec string) error {
	r, ok := parseRef(spec)
	if !ok || r.key == "" {
		return errors.New("usage: dfsctl rm dfs://bucket/key")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(r), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	fmt.Printf("deleted dfs://%s/%s\n", r.bucket, r.key)
	return nil
}

func (c *client) stat(ctx context.Context, spec string) error {
	r, ok := parseRef(spec)
	if !ok || r.key == "" {
		return errors.New("usage: dfsctl stat dfs://bucket/key")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(r), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}

	fmt.Printf("dfs://%s/%s\n", r.bucket, r.key)
	fmt.Printf("  size:         %s\n", resp.Header.Get("Content-Length"))
	fmt.Printf("  content-type: %s\n", resp.Header.Get("Content-Type"))
	fmt.Printf("  etag:         %s\n", resp.Header.Get("ETag"))
	fmt.Printf("  version:      %s\n", resp.Header.Get("X-Dfs-Version-Id"))
	return nil
}

// --- helpers -------------------------------------------------------------

func (c *client) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// checkStatus turns a non-2xx response into an error carrying the server's
// message, so failures say what went wrong rather than just the status code.
func checkStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	var body struct {
		Error string `json:"error"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if json.Unmarshal(raw, &body) == nil && body.Error != "" {
		return fmt.Errorf("%s: %s", resp.Status, body.Error)
	}
	if len(raw) > 0 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return errors.New(resp.Status)
}

func contentTypeFor(key string) string {
	switch strings.ToLower(path.Ext(key)) {
	case ".txt", ".md":
		return "text/plain; charset=utf-8"
	case ".json":
		return "application/json"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".iso":
		return "application/x-iso9660-image"
	default:
		return "application/octet-stream"
	}
}

func isDir(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

func rate(n int64, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(float64(n) / d.Seconds())
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
