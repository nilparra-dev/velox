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

	"github.com/nilparra-dev/velox/internal/segment"
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

func TestDownloadSegmentWritesAtOffset(t *testing.T) {
	data := makeData(8192)
	srv := rangedServer(data)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, err := writer.New(path, int64(len(data)))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}

	seg := segment.Segment{Index: 0, Start: 100, End: 199} // 100 bytes
	var progressed int64
	prog := func(n int64) { atomic.AddInt64(&progressed, n) }

	if err := downloadSegment(context.Background(), srv.Client(), srv.URL+"/file.bin", seg, w, prog); err != nil {
		t.Fatalf("downloadSegment: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}

	if progressed != 100 {
		t.Errorf("progress = %d, want 100", progressed)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got[100:200], data[100:200]) {
		t.Errorf("bytes [100:200] mismatch")
	}
	if !bytes.Equal(got[:100], make([]byte, 100)) {
		t.Error("bytes before the segment are non-zero (wrote at wrong offset)")
	}
	if !bytes.Equal(got[200:], make([]byte, len(got)-200)) {
		t.Error("bytes after the segment are non-zero (wrote past the range)")
	}
}

func TestDownloadSegmentNilProgress(t *testing.T) {
	data := makeData(1024)
	srv := rangedServer(data)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, err := writer.New(path, int64(len(data)))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}
	defer w.Close()

	seg := segment.Segment{Index: 0, Start: 0, End: int64(len(data)) - 1}
	if err := downloadSegment(context.Background(), srv.Client(), srv.URL+"/file.bin", seg, w, nil); err != nil {
		t.Fatalf("downloadSegment with nil prog: %v", err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, data) {
		t.Error("nil-progress download produced wrong bytes")
	}
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
}
