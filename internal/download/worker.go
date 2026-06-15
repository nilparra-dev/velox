package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/nilparra-dev/velox/internal/chunk"
	"github.com/nilparra-dev/velox/internal/retry"
	"github.com/nilparra-dev/velox/internal/writer"
)

// ProgressFunc is called with the number of bytes just written to disk.
type ProgressFunc func(n int64)

const bufSize = 256 * 1024

// copyAt streams r into w starting at off, reporting progress per read and
// invoking tick after each successful read (for stall detection). prog and tick
// are both nil-safe. Returns the total bytes copied.
func copyAt(r io.Reader, w io.WriterAt, off int64, prog ProgressFunc, tick func()) (int64, error) {
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
			if tick != nil {
				tick()
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

// errRemoteChanged signals the server answered an If-Range request with 200,
// meaning the resource changed mid-download; the run must abort.
var errRemoteChanged = errors.New("remote resource changed during download")

// httpError carries an HTTP status for retry classification.
type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("http status %d", e.status) }

// downloadChunk fetches one chunk with retries, optional If-Range validation,
// and stall detection, writing the body at its absolute offset in w.
func downloadChunk(ctx context.Context, client *http.Client, rawURL, ifRange string, c chunk.Chunk, w *writer.Writer, pol retry.Policy, stall time.Duration) error {
	var lastErr error
	var raDelay time.Duration
	for attempt := 0; attempt < pol.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := pol.Backoff(attempt-1, rand.Float64())
			if raDelay > delay {
				delay = raDelay
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		ra, err := attemptChunk(ctx, client, rawURL, ifRange, c, w, stall)
		if err == nil {
			return nil
		}
		if errors.Is(err, errRemoteChanged) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err() // parent cancelled (Ctrl-C): stop, don't retry
		}
		status := 0
		var he *httpError
		if errors.As(err, &he) {
			status = he.status
		}
		if retry.Classify(err, status) == retry.Fatal {
			return err
		}
		lastErr, raDelay = err, ra
	}
	return fmt.Errorf("chunk %d: failed after %d attempts: %w", c.Index, pol.MaxAttempts, lastErr)
}

// attemptChunk performs one chunk download attempt. It returns any Retry-After
// delay parsed from the response (for the caller's next backoff) and an error.
func attemptChunk(parent context.Context, client *http.Client, rawURL, ifRange string, c chunk.Chunk, w *writer.Writer, stall time.Duration) (time.Duration, error) {
	actx, acancel := context.WithCancel(parent)
	defer acancel()

	req, err := http.NewRequestWithContext(actx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c.Start, c.End))
	if ifRange != "" {
		req.Header.Set("If-Range", ifRange)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if want := fmt.Sprintf("bytes %d-", c.Start); !strings.HasPrefix(cr, want) {
				return 0, fmt.Errorf("chunk %d: unexpected Content-Range %q (want prefix %q)", c.Index, cr, want)
			}
		}
	case http.StatusOK:
		return 0, errRemoteChanged
	default:
		ra, _ := retry.RetryAfter(resp.Header, time.Now())
		// Drain a bounded amount so the connection can be reused on retry.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return ra, &httpError{status: resp.StatusCode}
	}

	// Stall watchdog: cancel this attempt if no bytes arrive for `stall`.
	ticks := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(stall)
		defer timer.Stop()
		for {
			select {
			case <-actx.Done():
				return
			case <-ticks:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(stall)
			case <-timer.C:
				acancel()
				return
			}
		}
	}()
	tick := func() {
		select {
		case ticks <- struct{}{}:
		default:
		}
	}

	got, err := copyAt(resp.Body, w, c.Start, nil, tick)
	if err != nil {
		return 0, err
	}
	if got != c.Length() {
		return 0, fmt.Errorf("chunk %d: wrote %d bytes, want %d", c.Index, got, c.Length())
	}
	return 0, nil
}
