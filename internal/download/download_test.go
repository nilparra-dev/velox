package download

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nilparra-dev/velox/internal/manifest"
	"github.com/nilparra-dev/velox/internal/probe"
	"github.com/nilparra-dev/velox/internal/writer"
)

func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

func rangedServer(data []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
}

func noRangeServer(data []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK) // ignores Range
		_, _ = w.Write(data)
	}))
}

func TestRunRangedParallel(t *testing.T) {
	data := makeData(1 << 20) // 1 MiB
	srv := rangedServer(data)
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "result.bin")
	res, err := Run(context.Background(), Options{
		URL:         srv.URL + "/file.bin",
		Output:      out,
		Connections: 4,
		ChunkSize:   64 * 1024, // 64 KiB -> 1 MiB / 64 KiB = 16 chunks
		Client:      srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Ranged || res.Connections != 4 {
		t.Errorf("Ranged=%v Connections=%d, want true/4", res.Ranged, res.Connections)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Errorf("output bytes differ from source")
	}
	if _, err := os.Stat(out + ".part"); !os.IsNotExist(err) {
		t.Errorf(".part file was not removed after finalize")
	}
}

func TestRunSingleStreamFallback(t *testing.T) {
	data := makeData(64 * 1024)
	srv := noRangeServer(data)
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "result.bin")
	res, err := Run(context.Background(), Options{
		URL:         srv.URL + "/blob",
		Output:      out,
		Connections: 4, // requested, but server has no ranges
		Client:      srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Ranged || res.Connections != 1 {
		t.Errorf("Ranged=%v Connections=%d, want false/1", res.Ranged, res.Connections)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Errorf("fallback output bytes differ from source")
	}
	if _, err := os.Stat(out + ".part"); !os.IsNotExist(err) {
		t.Errorf(".part file was not removed after finalize")
	}
}

func TestRunSingleStreamTruncatedFails(t *testing.T) {
	full := makeData(64 * 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Advertise the full length but send only half, then return (truncated).
		w.Header().Set("Content-Length", strconv.Itoa(len(full)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(full[:len(full)/2])
	}))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "trunc.bin")
	if _, err := Run(context.Background(), Options{
		URL: srv.URL + "/blob", Output: out, Connections: 4, Client: srv.Client(),
	}); err == nil {
		t.Fatal("expected error for truncated single-stream download, got nil")
	}
}

func TestRunRejectsWrongContentRange(t *testing.T) {
	data := makeData(4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always claim the range starts at 0, no matter what was requested.
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", "bytes 0-4095/4096")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:1])
	}))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "wrongrange.bin")
	if _, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 2, ChunkSize: 2048, Retries: 2, Client: srv.Client(),
	}); err == nil {
		t.Fatal("expected error when server returns the wrong Content-Range, got nil")
	}
}

func TestRunResumesFromManifest(t *testing.T) {
	data := makeData(40 * 1024) // 40 KiB
	const cs = 10 * 1024        // 4 chunks
	var served int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rng := r.Header.Get("Range"); rng != "" && rng != "bytes=0-0" {
			atomic.AddInt32(&served, 1)
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "result.bin")
	part := out + ".part"
	mpath := out + ".dm"

	// Seed a pre-allocated .part with chunks 0 and 1 written, and a manifest
	// marking them done (size-only so Validate passes on size alone).
	w, err := writer.New(part, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	w.WriteAt(data[0:cs], 0)
	w.WriteAt(data[cs:2*cs], cs)
	w.Close()

	mf := manifest.New(mpath, &probe.RemoteInfo{URL: srv.URL + "/file.bin", Size: int64(len(data))}, cs)
	mf.MarkDone(0)
	mf.MarkDone(1)
	if err := mf.Save(); err != nil {
		t.Fatal(err)
	}

	res, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 4, ChunkSize: cs, Client: srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Resumed {
		t.Error("expected Resumed=true")
	}
	if got := atomic.LoadInt32(&served); got != 2 {
		t.Errorf("served %d ranged requests, want 2 (only chunks 2 and 3)", got)
	}
	final, _ := os.ReadFile(out)
	if !bytes.Equal(final, data) {
		t.Error("resumed file is not byte-exact")
	}
	if _, err := os.Stat(mpath); !os.IsNotExist(err) {
		t.Error(".dm manifest not removed after success")
	}
}

func TestRunRestartIgnoresStaleManifest(t *testing.T) {
	data := makeData(40 * 1024)
	const cs = 10 * 1024
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "result.bin")
	// Stale manifest claiming a different size -> Validate fails -> fresh start.
	mf := manifest.New(out+".dm", &probe.RemoteInfo{URL: srv.URL + "/file.bin", Size: 999999}, cs)
	mf.MarkDone(0)
	mf.Save()

	res, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 4, ChunkSize: cs, Client: srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Resumed {
		t.Error("stale manifest (wrong size) should force a fresh start, Resumed=false")
	}
	final, _ := os.ReadFile(out)
	if !bytes.Equal(final, data) {
		t.Error("restarted file is not byte-exact")
	}
}

func TestRunNilClientUsesDefault(t *testing.T) {
	data := makeData(20 * 1024)
	srv := rangedServer(data)
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "r.bin")
	// Client deliberately omitted (nil) — Run must fall back to http.DefaultClient.
	if _, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 2, ChunkSize: 8 * 1024,
	}); err != nil {
		t.Fatalf("Run with nil client: %v", err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Error("nil-client download bytes mismatch")
	}
}
