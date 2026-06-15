package probe

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestProbeRangedServer(t *testing.T) {
	data := makeData(4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	info, err := Probe(context.Background(), srv.Client(), srv.URL+"/file.bin")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !info.AcceptRanges {
		t.Error("AcceptRanges = false, want true")
	}
	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}
	if info.Filename != "file.bin" {
		t.Errorf("Filename = %q, want file.bin", info.Filename)
	}
}

func TestProbeNoRangeServer(t *testing.T) {
	data := makeData(2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK) // ignores Range entirely
		w.Write(data)
	}))
	defer srv.Close()

	info, err := Probe(context.Background(), srv.Client(), srv.URL+"/blob")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.AcceptRanges {
		t.Error("AcceptRanges = true, want false")
	}
	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}
}
