package manifest

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nilparra-dev/velox/internal/probe"
)

func TestRoundTripAndPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.dm")
	info := &probe.RemoteInfo{URL: "http://x/f", Size: 1000, ETag: "\"v1\"", LastModified: "lm"}

	m := New(path, info, 100)
	m.MarkDone(0)
	m.MarkDone(2)
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp manifest file left behind after Save")
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.IsDone(0) || !loaded.IsDone(2) || loaded.IsDone(1) {
		t.Errorf("done set not preserved: %+v", loaded.Completed)
	}
	if loaded.ChunkSize != 100 || loaded.Size != 1000 || loaded.ETag != "\"v1\"" {
		t.Errorf("header fields not preserved: %+v", loaded)
	}
	pending := loaded.Pending(10)
	if len(pending) != 8 || pending[0] != 1 || pending[1] != 3 {
		t.Errorf("Pending = %v, want [1 3 4 5 6 7 8 9]", pending)
	}
}

func TestValidate(t *testing.T) {
	m := New("p", &probe.RemoteInfo{Size: 1000, ETag: "\"v1\"", LastModified: "lmA"}, 100)
	if !m.Validate(&probe.RemoteInfo{Size: 1000, ETag: "\"v1\""}) {
		t.Error("matching ETag+size should validate")
	}
	if m.Validate(&probe.RemoteInfo{Size: 1000, ETag: "\"v2\""}) {
		t.Error("different ETag should NOT validate")
	}
	if m.Validate(&probe.RemoteInfo{Size: 999, ETag: "\"v1\""}) {
		t.Error("different size should NOT validate")
	}
	m2 := New("p", &probe.RemoteInfo{Size: 1000, LastModified: "lmA"}, 100)
	if !m2.Validate(&probe.RemoteInfo{Size: 1000, LastModified: "lmA"}) {
		t.Error("matching Last-Modified+size should validate")
	}
	if m2.Validate(&probe.RemoteInfo{Size: 1000, LastModified: "lmB"}) {
		t.Error("different Last-Modified should NOT validate")
	}
	m3 := New("p", &probe.RemoteInfo{Size: 1000}, 100)
	if !m3.Validate(&probe.RemoteInfo{Size: 1000}) {
		t.Error("size-only match should validate when no validators exist")
	}
}

func TestConcurrentMarkDone(t *testing.T) {
	m := New("p", &probe.RemoteInfo{Size: 1000}, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); m.MarkDone(i) }(i)
	}
	wg.Wait()
	if got := len(m.Pending(100)); got != 0 {
		t.Errorf("after marking all 100 done, Pending = %d, want 0", got)
	}
}

func TestLoadCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.dm")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("Load of corrupt manifest should return an error")
	}
}

func TestConcurrentSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.dm")
	m := New(path, &probe.RemoteInfo{URL: "http://x/f", Size: 1000}, 100)
	m.MarkDone(1)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- m.Save() }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Save failed: %v", err)
		}
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file left behind")
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load after concurrent saves: %v", err)
	}
	if !loaded.IsDone(1) {
		t.Error("manifest content lost after concurrent saves")
	}
}

func TestLoadWrongVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v999.dm")
	if err := os.WriteFile(path, []byte(`{"version":999,"size":10}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("Load of a future-version manifest should return an error")
	}
}
