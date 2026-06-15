package writer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNewPreallocatesAndConcurrentWriteAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	const size = 1 << 16

	w, err := New(path, size)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Two goroutines writing non-overlapping halves concurrently.
	half := make([]byte, size/2)
	for i := range half {
		half[i] = byte(i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.WriteAt(half, 0) }()
	go func() { defer wg.Done(); w.WriteAt(half, size/2) }()
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(got) != size {
		t.Fatalf("file size %d, want %d", len(got), size)
	}
	for off := 0; off < size; off += size / 2 {
		for i := 0; i < size/2; i++ {
			if got[off+i] != byte(i) {
				t.Fatalf("byte at %d = %d, want %d", off+i, got[off+i], byte(i))
			}
		}
	}
}
