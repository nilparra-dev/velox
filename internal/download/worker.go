package download

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/nilparra-dev/velox/internal/segment"
	"github.com/nilparra-dev/velox/internal/writer"
)

// ProgressFunc is called with the number of bytes just written to disk.
type ProgressFunc func(n int64)

const bufSize = 256 * 1024

// downloadSegment fetches seg with a ranged GET and writes the body at its
// absolute offset in w, reporting progress as it goes.
func downloadSegment(ctx context.Context, client *http.Client, rawURL string, seg segment.Segment, w *writer.Writer, prog ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", seg.Start, seg.End))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("segment %d: expected 206, got %s", seg.Index, resp.Status)
	}

	buf := make([]byte, bufSize)
	off := seg.Start
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.WriteAt(buf[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
			if prog != nil {
				prog(int64(n))
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	if got := off - seg.Start; got != seg.Length() {
		return fmt.Errorf("segment %d: wrote %d bytes, want %d", seg.Index, got, seg.Length())
	}
	return nil
}
