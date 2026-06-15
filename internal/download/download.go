package download

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/nilparra-dev/velox/internal/probe"
	"github.com/nilparra-dev/velox/internal/segment"
	"github.com/nilparra-dev/velox/internal/writer"
)

// Options configures a single download.
type Options struct {
	URL         string
	Output      string                        // final path; empty -> derived from URL
	Connections int                           // desired parallel connections (>=1)
	Client      *http.Client                  // nil -> http.DefaultClient
	Progress    ProgressFunc                  // optional, called per chunk written
	OnInfo      func(size int64, ranged bool) // optional, called once after probe
}

// Result reports the outcome of a download.
type Result struct {
	Output      string
	Size        int64 // total bytes; -1 if the server did not report a size
	Connections int
	Ranged      bool
}

// Run probes the URL, downloads it (ranged-parallel or single-stream), verifies
// the size, and atomically renames the .part file to the final output.
func Run(ctx context.Context, opts Options) (*Result, error) {
	client := opts.Client
	if client == nil {
		client = http.DefaultClient
	}
	if opts.Connections < 1 {
		opts.Connections = 1
	}

	info, err := probe.Probe(ctx, client, opts.URL)
	if err != nil {
		return nil, err
	}

	out := opts.Output
	if out == "" {
		out = info.Filename
	}
	part := out + ".part"

	ranged := info.AcceptRanges && info.Size > 0 && opts.Connections > 1
	conns := 1
	if ranged {
		conns = opts.Connections
	}
	if opts.OnInfo != nil {
		opts.OnInfo(info.Size, ranged)
	}

	if info.Size > 0 {
		w, werr := writer.New(part, info.Size)
		if werr != nil {
			return nil, werr
		}
		if ranged {
			err = runRanged(ctx, client, info.URL, info.Size, conns, w, opts.Progress)
		} else {
			err = runSingle(ctx, client, info.URL, info.Size, w, opts.Progress)
		}
		if cerr := w.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, err
		}
		if verr := verifySize(part, info.Size); verr != nil {
			return nil, verr
		}
	} else {
		if serr := streamUnknown(ctx, client, info.URL, part, opts.Progress); serr != nil {
			return nil, serr
		}
	}

	if rerr := os.Rename(part, out); rerr != nil {
		return nil, rerr
	}
	return &Result{Output: out, Size: info.Size, Connections: conns, Ranged: ranged}, nil
}

func runRanged(ctx context.Context, client *http.Client, rawURL string, size int64, conns int, w *writer.Writer, prog ProgressFunc) error {
	segs := segment.Split(size, conns)
	g, gctx := errgroup.WithContext(ctx)
	for _, seg := range segs {
		seg := seg
		g.Go(func() error {
			return downloadSegment(gctx, client, rawURL, seg, w, prog)
		})
	}
	return g.Wait()
}

func runSingle(ctx context.Context, client *http.Client, rawURL string, size int64, w *writer.Writer, prog ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("single: expected 200, got %s", resp.Status)
	}
	// off starts at 0: the single-stream path fills a file already
	// pre-allocated to the full size, so byte i of the body is file byte i.
	got, err := copyAt(resp.Body, w, 0, prog, nil)
	if err != nil {
		return err
	}
	if got != size {
		return fmt.Errorf("single: wrote %d bytes, want %d", got, size)
	}
	return nil
}

func streamUnknown(ctx context.Context, client *http.Client, rawURL, part string, prog ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream: expected 200, got %s", resp.Status)
	}
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = copyAt(resp.Body, f, 0, prog, nil)
	return err
}

func verifySize(path string, want int64) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Size() != want {
		return fmt.Errorf("size mismatch: got %d, want %d", fi.Size(), want)
	}
	return nil
}
