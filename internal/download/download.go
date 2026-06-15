// internal/download/download.go
package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nilparra-dev/velox/internal/probe"
	"github.com/nilparra-dev/velox/internal/retry"
	"github.com/nilparra-dev/velox/internal/writer"
)

const (
	defaultChunkSize      = 16 << 20 // 16 MiB
	defaultStall          = 30 * time.Second
	manifestFlushInterval = time.Second
)

// Options configures a single download.
type Options struct {
	URL          string
	Output       string                                       // final path; empty -> derived from URL
	Connections  int                                          // worker count (>=1)
	ChunkSize    int64                                        // bytes per chunk (<=0 -> default)
	Retries      int                                          // max attempts per chunk (<=0 -> default)
	Restart      bool                                         // ignore/delete any existing .part/.dm and start fresh
	StallTimeout time.Duration                                // no-progress timeout per attempt (<=0 -> default)
	Client       *http.Client                                 // nil -> http.DefaultClient
	Progress     ProgressFunc                                 // optional, called per completed chunk (or per read on single-stream)
	OnInfo       func(size int64, ranged bool, resumed int64) // optional, called once after probe
}

// Result reports the outcome of a download.
type Result struct {
	Output      string
	Size        int64
	Connections int
	Ranged      bool
	Resumed     bool
}

// Run downloads opts.URL: chunked + resumable when the server supports ranges,
// or a single stream otherwise. It verifies size, fsyncs, atomically renames
// the .part to the final output, and removes the manifest on success.
func Run(ctx context.Context, opts Options) (*Result, error) {
	client := opts.Client
	if client == nil {
		client = http.DefaultClient
	}
	opts.Client = client // ensure runChunked/workers use the normalized client
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
	mpath := out + ".dm"

	ranged := info.AcceptRanges && info.Size > 0
	if !ranged {
		if opts.OnInfo != nil {
			opts.OnInfo(info.Size, false, 0)
		}
		return runSingleStream(ctx, client, info, out, part, opts.Progress)
	}

	pol := retry.Default()
	if opts.Retries > 0 {
		pol.MaxAttempts = opts.Retries
	}

	p := prepare(opts, info, part, mpath)
	if opts.OnInfo != nil {
		opts.OnInfo(info.Size, true, p.resumed)
	}

	var w *writer.Writer
	if p.fresh {
		w, err = writer.New(part, info.Size)
	} else {
		w, err = writer.Open(part, info.Size)
	}
	if err != nil {
		return nil, err
	}

	workers := opts.Connections
	if workers > len(p.pending) {
		workers = len(p.pending)
	}
	if workers < 1 {
		workers = 1
	}

	runErr := runChunked(ctx, opts, info, w, p, pol, workers)
	if runErr == nil {
		runErr = w.Sync()
	}
	if cerr := w.Close(); runErr == nil {
		runErr = cerr
	}
	if runErr != nil {
		if errors.Is(runErr, errRemoteChanged) {
			os.Remove(part)
			os.Remove(mpath)
		}
		// Otherwise keep .part + .dm so a re-run resumes.
		return nil, runErr
	}

	if err := verifySize(part, info.Size); err != nil {
		return nil, err
	}
	if err := os.Rename(part, out); err != nil {
		return nil, err
	}
	os.Remove(mpath)

	return &Result{Output: out, Size: info.Size, Connections: workers, Ranged: true, Resumed: !p.fresh}, nil
}

// runChunked runs the worker pool over the pending chunks, flushing the
// manifest periodically and once more at the end.
func runChunked(ctx context.Context, opts Options, info *probe.RemoteInfo, w *writer.Writer, p *plan, pol retry.Policy, workers int) error {
	ifRange := info.ETag
	if ifRange == "" {
		ifRange = info.LastModified
	}
	stall := opts.StallTimeout
	if stall <= 0 {
		stall = defaultStall
	}

	queue := make(chan int, len(p.pending))
	for _, idx := range p.pending {
		queue <- idx
	}
	close(queue)

	flushDone := make(chan struct{})
	flushStopped := make(chan struct{})
	go func() {
		defer close(flushStopped)
		t := time.NewTicker(manifestFlushInterval)
		defer t.Stop()
		for {
			select {
			case <-flushDone:
				return
			case <-t.C:
				_ = p.manifest.Save()
			}
		}
	}()

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for idx := range queue {
				if gctx.Err() != nil {
					return gctx.Err()
				}
				c := p.chunks[idx]
				if err := downloadChunk(gctx, opts.Client, info.URL, ifRange, c, w, pol, stall); err != nil {
					return err
				}
				p.manifest.MarkDone(idx)
				if opts.Progress != nil {
					opts.Progress(c.Length())
				}
			}
			return nil
		})
	}
	err := g.Wait()
	close(flushDone)
	<-flushStopped        // ensure the flusher has stopped before the final Save
	_ = p.manifest.Save() // persist final progress
	return err
}

// runSingleStream handles servers without range support: one GET, no resume.
func runSingleStream(ctx context.Context, client *http.Client, info *probe.RemoteInfo, out, part string, prog ProgressFunc) (*Result, error) {
	if info.Size > 0 {
		w, err := writer.New(part, info.Size)
		if err != nil {
			return nil, err
		}
		err = streamFull(ctx, client, info.URL, w, prog, info.Size)
		if cerr := w.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, err
		}
		if err := verifySize(part, info.Size); err != nil {
			return nil, err
		}
	} else {
		if err := streamUnknown(ctx, client, info.URL, part, prog); err != nil {
			return nil, err
		}
	}
	if err := os.Rename(part, out); err != nil {
		return nil, err
	}
	return &Result{Output: out, Size: info.Size, Connections: 1, Ranged: false, Resumed: false}, nil
}

func streamFull(ctx context.Context, client *http.Client, rawURL string, w *writer.Writer, prog ProgressFunc, size int64) error {
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
