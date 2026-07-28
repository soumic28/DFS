package blobstore

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/soumic28/dfs/internal/chunk"
)

// Environment contract between the parent test and the child process it kills.
const (
	envCrashChild = "DFS_CRASH_CHILD"
	envCrashDir   = "DFS_CRASH_DIR"
	markerFile    = "child-is-writing"
)

var (
	seedData = bytes.Repeat([]byte("committed before the kill"), 500)
	seedID   = chunk.Sum(seedData)
)

// TestSurvivesSIGKILLDuringWrite is the crash-safety gate.
//
// It re-executes the test binary as a child, lets it commit one chunk and
// begin a second, then SIGKILLs it — no signal handler, no deferred cleanup,
// no graceful anything, exactly like a power cut or an OOM kill. The parent
// then reopens the same directory and asserts the store came back consistent.
//
// This is the test that justifies the temp-file-then-rename design. Simulating
// a crash by calling Close() would prove nothing, because Close is precisely
// what a real crash never gets to run.
func TestSurvivesSIGKILLDuringWrite(t *testing.T) {
	if os.Getenv(envCrashChild) == "1" {
		runCrashChild()
		return
	}

	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=^TestSurvivesSIGKILLDuringWrite$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), envCrashChild+"=1", envCrashDir+"="+dir)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard

	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// Wait for the child to signal that it has committed the seed chunk and is
	// now mid-write on a second one.
	marker := filepath.Join(dir, markerFile)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("child never reached the mid-write state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond) // let bytes accumulate in tmp/

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()

	// Everything below is the state a node finds itself in on restart.
	s, err := Open(Options{Root: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("store did not reopen after a hard kill: %v", err)
	}
	defer func() { _ = s.Close() }()

	t.Run("committed chunk survived", func(t *testing.T) {
		rc, _, err := s.Get(seedID, 0, 0)
		if err != nil {
			t.Fatalf("seed chunk lost: %v", err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("seed chunk unreadable: %v", err)
		}
		if !bytes.Equal(got, seedData) {
			t.Error("seed chunk came back corrupted")
		}
	})

	t.Run("interrupted write left no partial chunk", func(t *testing.T) {
		entries, err := os.ReadDir(s.tmp)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("tmp/ still holds %d file(s) after recovery", len(entries))
		}
		if n := chunkFileCount(t, dir); n != 1 {
			t.Errorf("%d chunk files on disk, want exactly 1 (the committed one)", n)
		}
	})

	t.Run("accounting reflects reality", func(t *testing.T) {
		u := s.Usage()
		if u.ChunkCount != 1 {
			t.Errorf("chunk count = %d, want 1", u.ChunkCount)
		}
		if u.UsedBytes != int64(len(seedData)) {
			t.Errorf("used bytes = %d, want %d", u.UsedBytes, len(seedData))
		}
		if u.PendingBytes != 0 {
			t.Errorf("pending bytes = %d, want 0 (a killed write leaked a reservation)", u.PendingBytes)
		}
	})

	t.Run("store still accepts writes", func(t *testing.T) {
		data := []byte("life after the crash")
		id := chunk.Sum(data)
		if _, err := s.Put(context.Background(), id, int64(len(data)), bytes.NewReader(data)); err != nil {
			t.Fatalf("write after recovery: %v", err)
		}
		if got := readAll(t, s, id); !bytes.Equal(got, data) {
			t.Error("post-recovery write did not read back")
		}
	})
}

// runCrashChild commits one chunk, announces itself, then blocks forever
// partway through a second write, waiting to be killed.
func runCrashChild() {
	dir := os.Getenv(envCrashDir)

	s, err := Open(Options{Root: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		os.Exit(2)
	}

	if _, err := s.Put(context.Background(), seedID, int64(len(seedData)), bytes.NewReader(seedData)); err != nil {
		os.Exit(3)
	}

	// A reader that delivers a little and then stalls, so the process is
	// provably inside writeTemp with bytes on disk when the kill lands.
	big := bytes.Repeat([]byte("interrupted"), 1<<20)
	stalling := io.MultiReader(
		bytes.NewReader(big[:1<<16]),
		readerFunc(func([]byte) (int, error) {
			if f, err := os.Create(filepath.Join(dir, markerFile)); err == nil {
				_ = f.Close()
			}
			select {} // block until SIGKILL
		}),
	)

	_, _ = s.Put(context.Background(), chunk.Sum(big), int64(len(big)), stalling)
	os.Exit(4) // unreachable: the parent kills us first
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
