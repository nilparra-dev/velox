// main.go
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/nilparra-dev/velox/internal/download"
)

func main() {
	conns := flag.Int("n", 8, "number of parallel connections (1-16)")
	out := flag.String("o", "", "output file path (default: derived from URL)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: velox [-n N] [-o FILE] URL\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	url := flag.Arg(0)

	if *conns < 1 {
		*conns = 1
	}
	if *conns > 16 {
		*conns = 16
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     *conns + 2,
			MaxIdleConnsPerHost: *conns + 2,
			// Disable HTTP/2: an empty (non-nil) TLSNextProto removes the "h2"
			// ALPN upgrade, so each of the N range requests uses its own TCP
			// connection. (ForceAttemptHTTP2:false alone does NOT disable h2
			// when no custom dialer/TLSConfig is set.)
			TLSNextProto:    make(map[string]func(string, *tls.Conn) http.RoundTripper),
			IdleConnTimeout: 90 * time.Second,
		},
	}

	p := mpb.NewWithContext(ctx)
	// bar is assigned once in onInfo, which download.Run calls before it
	// launches the worker goroutines (goroutine creation is a happens-before
	// edge, so the write is visible to them). bar.IncrInt64 is itself safe for
	// concurrent use, so the workers can report progress in parallel.
	var bar *mpb.Bar
	onInfo := func(size int64, ranged bool, resumed int64) {
		// Unknown size (server gave no Content-Length): total stays 0, so the bar
		// shows transferred bytes/speed without a percentage. Acceptable for the MVP.
		total := size
		if total < 0 {
			total = 0
		}
		bar = p.AddBar(total,
			mpb.PrependDecorators(
				decor.CountersKibiByte("% .2f / % .2f"),
			),
			mpb.AppendDecorators(
				decor.AverageSpeed(decor.SizeB1024(0), " % .2f"),
				decor.Percentage(decor.WCSyncSpace),
			),
		)
	}
	prog := func(n int64) {
		if bar != nil {
			bar.IncrInt64(n)
		}
	}

	res, err := download.Run(ctx, download.Options{
		URL:         url,
		Output:      *out,
		Connections: *conns,
		Client:      client,
		Progress:    prog,
		OnInfo:      onInfo,
	})
	p.Wait()

	if err != nil {
		fmt.Fprintf(os.Stderr, "velox: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Saved %s (%d bytes, %d connections, ranged=%v)\n",
		res.Output, res.Size, res.Connections, res.Ranged)
}
