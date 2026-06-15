// main.go
package main

import (
	"context"
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
			ForceAttemptHTTP2:   false, // multiple HTTP/1.1 conns bypass per-flow throttling
			IdleConnTimeout:     90 * time.Second,
		},
	}

	p := mpb.New()
	var bar *mpb.Bar
	onInfo := func(size int64, ranged bool) {
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
