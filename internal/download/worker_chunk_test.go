package download

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nilparra-dev/velox/internal/chunk"
	"github.com/nilparra-dev/velox/internal/retry"
	"github.com/nilparra-dev/velox/internal/writer"
)

func fastPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 5, Base: time.Millisecond, Max: 5 * time.Millisecond}
}

// flakyRangedServer serves ranged content but kills the connection on the first
// `fails` requests (counted globally) to force retries.
func flakyRangedServer(data []byte, fails int) *httptest.Server {
	var seen int
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen++
		n := seen
		mu.Unlock()
		if n <= fails {
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
				return
			}
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
}

func TestDownloadChunkRetriesThenSucceeds(t *testing.T) {
	data := makeData(8192)
	srv := flakyRangedServer(data, 2)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, err := writer.New(path, int64(len(data)))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}
	defer w.Close()

	c := chunk.Chunk{Index: 0, Start: 0, End: int64(len(data)) - 1}
	if err := downloadChunk(context.Background(), srv.Client(), srv.URL+"/file.bin", "", c, w, fastPolicy(), 2*time.Second); err != nil {
		t.Fatalf("downloadChunk: %v", err)
	}
	w.Close()
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, data) {
		t.Error("chunk bytes mismatch after retries")
	}
}

func TestDownloadChunkRejectsRemoteChange(t *testing.T) {
	data := makeData(4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Range") != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, _ := writer.New(path, int64(len(data)))
	defer w.Close()

	c := chunk.Chunk{Index: 0, Start: 0, End: int64(len(data)) - 1}
	if err := downloadChunk(context.Background(), srv.Client(), srv.URL+"/file.bin", "\"etag\"", c, w, fastPolicy(), 2*time.Second); err == nil {
		t.Fatal("expected errRemoteChanged, got nil")
	}
}
