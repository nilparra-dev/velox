package writer

import "os"

// Writer owns a file pre-allocated to its final size so that concurrent
// WriteAt calls to non-overlapping regions land at fixed offsets.
type Writer struct {
	f *os.File
}

// New creates (or truncates) path and pre-allocates it to size bytes.
func New(path string, size int64) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{f: f}, nil
}

// WriteAt writes p at off. Safe for concurrent use on non-overlapping regions.
func (w *Writer) WriteAt(p []byte, off int64) (int, error) {
	return w.f.WriteAt(p, off)
}

// Sync flushes to stable storage.
func (w *Writer) Sync() error { return w.f.Sync() }

// Close closes the underlying file.
func (w *Writer) Close() error { return w.f.Close() }
