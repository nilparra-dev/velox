package download

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	w.Close()

	if progressed != 100 {
		t.Errorf("progress = %d, want 100", progressed)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got[100:200], data[100:200]) {
		t.Errorf("bytes [100:200] mismatch")
	}
}
