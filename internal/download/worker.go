package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/nilparra-dev/velox/internal/segment"
	"github.com/nilparra-dev/velox/internal/writer"
)

// ProgressFunc is called with the number of bytes just written to disk.
type ProgressFunc func(n int64)

const bufSize = 256 * 1024

// copyAt streams r into w starting at byte offset off, reporting progress per
// chunk, and returns the total number of bytes copied. It is the single read
// loop shared by every download path (ranged segments, single stream, and
// unknown-size stream).
func copyAt(r io.Reader, w io.WriterAt, off int64, prog ProgressFunc) (int64, error) {
	buf := make([]byte, bufSize)
	start := off
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if _, werr := w.WriteAt(buf[:n], off); werr != nil {
				return off - start, werr
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
			return off - start, readErr
		}
	}
	return off - start, nil
}

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

	// Guard against a server that returns 206 with a right-sized body but the
	// WRONG byte range (e.g. a broken CDN cache): the response must claim it
	// starts at the offset we requested.
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		want := fmt.Sprintf("bytes %d-", seg.Start)
		if !strings.HasPrefix(cr, want) {
			return fmt.Errorf("segment %d: unexpected Content-Range %q (want prefix %q)", seg.Index, cr, want)
		}
	}

	got, err := copyAt(resp.Body, w, seg.Start, prog)
	if err != nil {
		return err
	}
	if got != seg.Length() {
		return fmt.Errorf("segment %d: wrote %d bytes, want %d", seg.Index, got, seg.Length())
	}
	return nil
}
