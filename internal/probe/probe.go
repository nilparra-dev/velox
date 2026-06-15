package probe

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// RemoteInfo describes the target as discovered by Probe.
type RemoteInfo struct {
	URL          string // final URL after redirects
	Size         int64  // -1 when unknown
	AcceptRanges bool
	ETag         string
	LastModified string
	Filename     string
}

// Probe issues a single GET with Range: bytes=0-0 to learn the size, range
// support, validators and the final URL after redirects.
func Probe(ctx context.Context, client *http.Client, rawURL string) (*RemoteInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", "bytes=0-0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	info := &RemoteInfo{
		URL:          resp.Request.URL.String(),
		Size:         -1,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent: // 206 -> ranges supported
		info.AcceptRanges = true
		if total := totalFromContentRange(resp.Header.Get("Content-Range")); total >= 0 {
			info.Size = total
		}
	case http.StatusOK: // 200 -> server ignored the range
		info.AcceptRanges = false
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, perr := strconv.ParseInt(cl, 10, 64); perr == nil {
				info.Size = n
			}
		}
	default:
		return nil, fmt.Errorf("probe: unexpected status %s", resp.Status)
	}

	info.Filename = filenameFromURL(info.URL)
	return info, nil
}

// totalFromContentRange parses N from "bytes 0-0/N". Returns -1 if unknown.
func totalFromContentRange(v string) int64 {
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return -1
	}
	total := strings.TrimSpace(v[i+1:])
	if total == "" || total == "*" {
		return -1
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func filenameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "download"
	}
	base := path.Base(u.Path)
	if base == "." || base == "/" || base == "" {
		return "download"
	}
	return base
}
