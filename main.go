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
	chunkSize := flag.Int64("chunk-size", 16<<20, "bytes per chunk")
	retries := flag.Int("retries", 6, "max attempts per chunk")
	restart := flag.Bool("restart", false, "ignore any existing .part/.dm and start fresh")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: velox [-n N] [-o FILE] [--chunk-size BYTES] [--retries N] [--restart] URL\n\n")
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
			// Disable HTTP/2 so each range request uses its own TCP connection.
			TLSNextProto:          make(map[string]func(string, *tls.Conn) http.RoundTripper),
			ResponseHeaderTimeout: 30 * time.Second, // bounds time-to-headers, not the body
			IdleConnTimeout:       90 * time.Second,
		},
	}

	p := mpb.NewWithContext(ctx)
	var bar *mpb.Bar
	onInfo := func(size int64, ranged bool, resumed int64) {
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
		if resumed > 0 {
			bar.SetCurrent(resumed) // reflect bytes already on disk from a prior run
		}
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
		ChunkSize:   *chunkSize,
		Retries:     *retries,
		Restart:     *restart,
		Client:      client,
		Progress:    prog,
		OnInfo:      onInfo,
	})
	p.Wait()

	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "velox: interrupted — re-run the same command to resume")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "velox: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Saved %s (%d bytes, %d connections, ranged=%v, resumed=%v)\n",
		res.Output, res.Size, res.Connections, res.Ranged, res.Resumed)
}
