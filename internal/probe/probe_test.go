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
		_, _ = w.Write(data)
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
	if info.Filename != "blob" {
		t.Errorf("Filename = %q, want blob", info.Filename)
	}
}

func TestProbeErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	info, err := Probe(context.Background(), srv.Client(), srv.URL+"/x")
	if err == nil {
		t.Fatal("expected error for 403, got nil")
	}
	if info != nil {
		t.Errorf("expected nil RemoteInfo on error, got %+v", info)
	}
}

func TestProbeFollowsRedirectToFinalURL(t *testing.T) {
	data := makeData(1024)
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "real.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer final.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+"/real.bin", http.StatusFound)
	}))
	defer redir.Close()

	info, err := Probe(context.Background(), redir.Client(), redir.URL+"/start")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if info.URL != final.URL+"/real.bin" {
		t.Errorf("final URL = %q, want %q", info.URL, final.URL+"/real.bin")
	}
	if info.Filename != "real.bin" {
		t.Errorf("Filename = %q, want real.bin", info.Filename)
	}
}
