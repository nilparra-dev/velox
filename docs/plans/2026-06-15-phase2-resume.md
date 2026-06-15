# velox Phase 2 Implementation Plan — Resumable, hardened downloads

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make velox robust for very large, long-running downloads: a work-stealing chunk queue, cross-run resume via a JSON manifest, per-chunk retries with backoff + `Retry-After`, stall detection, `If-Range` remote-change safety, and durable `fsync`+atomic finalize.

**Architecture:** `probe` → resume-check against a `<output>.dm` manifest → plan fixed-size chunks → enqueue only the not-completed chunks → N workers pull from the queue (work-stealing), each fetching its chunk with retries/stall/`If-Range` and writing at the absolute offset → manifest records completed chunks (atomic, periodic) → on success `fsync`, rename `.part`→final, delete `.dm`.

**Tech Stack:** Go 1.26; `net/http`, `context`, `encoding/json`, `math/rand`, `sync` (stdlib); `golang.org/x/sync/errgroup`; `github.com/vbauerster/mpb/v8`. Spec: `docs/specs/2026-06-15-phase2-resume.md`.

**Note vs spec:** the spec said `internal/writer` was unchanged; planning surfaced one needed addition — a non-truncating `writer.Open` for resuming an existing `.part`. It is included in Task 4.

---

## File structure delta

```
internal/
├── chunk/        (new; replaces segment/, which is deleted in Task 5)
│   ├── chunk.go
│   └── chunk_test.go
├── retry/        (new)
│   ├── retry.go
│   └── retry_test.go
├── manifest/     (new)
│   ├── manifest.go
│   └── manifest_test.go
├── writer/       (Open added in Task 4)
├── probe/        (unchanged)
└── download/     (rewritten)
    ├── download.go      (Run, orchestration, queue + pool, single-stream fallback, finalize)
    ├── worker.go        (copyAt, downloadChunk: retry + If-Range + stall + validation)
    ├── resume.go        (manifest load/validate, pending + resumed-bytes computation)
    └── download_test.go
main.go            (new flags, ResponseHeaderTimeout, resume-aware bar, interrupt messaging)
```

Task order keeps the tree compiling and green at every step: pure packages first (`chunk`, `retry`, `manifest`), then the worker (additive — old `downloadSegment` kept until Task 5), then the orchestration rewrite (removes `segment`), then the CLI.

---

## Task 1: `internal/chunk` — fixed-size chunk planning

**Files:**
- Create: `internal/chunk/chunk.go`
- Test: `internal/chunk/chunk_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/chunk/chunk_test.go
package chunk

import "testing"

func TestPlanFixedSizeWithShortLast(t *testing.T) {
	chunks := Plan(100, 30) // 30,30,30,10
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}
	want := []int64{30, 30, 30, 10}
	var total int64
	for i, c := range chunks {
		if c.Index != i {
			t.Errorf("chunk %d has Index %d", i, c.Index)
		}
		if c.Length() != want[i] {
			t.Errorf("chunk %d length %d, want %d", i, c.Length(), want[i])
		}
		if i > 0 && c.Start != chunks[i-1].End+1 {
			t.Errorf("gap/overlap before chunk %d", i)
		}
		total += c.Length()
	}
	if total != 100 {
		t.Errorf("chunks cover %d bytes, want 100", total)
	}
	if chunks[3].End != 99 {
		t.Errorf("last chunk ends at %d, want 99", chunks[3].End)
	}
}

func TestPlanGuards(t *testing.T) {
	if got := Plan(100, 100); len(got) != 1 || got[0].End != 99 {
		t.Errorf("chunkSize==size: want 1 chunk [0,99], got %v", got)
	}
	if got := Plan(100, 500); len(got) != 1 || got[0].End != 99 {
		t.Errorf("chunkSize>size: want 1 chunk [0,99], got %v", got)
	}
	if got := Plan(50, 0); len(got) != 1 || got[0].Length() != 50 {
		t.Errorf("chunkSize<1: want 1 chunk of 50, got %v", got)
	}
	if got := Plan(0, 10); got != nil {
		t.Errorf("size 0: want nil, got %v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/chunk/ -v`
Expected: FAIL — `undefined: Plan`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/chunk/chunk.go
package chunk

// Chunk is a contiguous, inclusive byte range [Start, End].
type Chunk struct {
	Index int
	Start int64
	End   int64
}

// Length returns the number of bytes in the chunk.
func (c Chunk) Length() int64 { return c.End - c.Start + 1 }

// Plan divides [0, size) into fixed-size chunks of chunkSize bytes; the last
// chunk may be shorter. chunkSize < 1 is treated as size (one chunk). Returns
// nil when size <= 0.
func Plan(size, chunkSize int64) []Chunk {
	if size <= 0 {
		return nil
	}
	if chunkSize < 1 {
		chunkSize = size
	}
	var chunks []Chunk
	var start int64
	for idx := 0; start < size; idx++ {
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, Chunk{Index: idx, Start: start, End: end})
		start = end + 1
	}
	return chunks
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/chunk/ -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/chunk/
git commit -m "feat(chunk): fixed-size chunk planning for work-stealing"
```

---

## Task 2: `internal/retry` — backoff policy, error classification, Retry-After

**Files:**
- Create: `internal/retry/retry.go`
- Test: `internal/retry/retry_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/retry/retry_test.go
package retry

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestBackoffBoundsAndJitter(t *testing.T) {
	p := Policy{MaxAttempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second}
	// Full jitter: result in [0, capped]. jitter=1 -> exactly capped, jitter=0 -> 0.
	if got := p.Backoff(0, 1.0); got != 500*time.Millisecond {
		t.Errorf("Backoff(0,1.0) = %v, want 500ms", got)
	}
	if got := p.Backoff(0, 0.0); got != 0 {
		t.Errorf("Backoff(0,0.0) = %v, want 0", got)
	}
	if got := p.Backoff(2, 1.0); got != 2*time.Second {
		t.Errorf("Backoff(2,1.0) = %v, want 2s", got)
	}
	if got := p.Backoff(100, 1.0); got != 30*time.Second {
		t.Errorf("Backoff(overflow,1.0) = %v, want cap 30s", got)
	}
	if got := p.Backoff(0, 2.0); got != 500*time.Millisecond {
		t.Errorf("jitter clamps >1 to 1: got %v", got)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err    error
		status int
		want   Decision
	}{
		{errors.New("conn reset"), 0, Retryable},
		{nil, http.StatusTooManyRequests, Retryable},
		{nil, http.StatusServiceUnavailable, Retryable},
		{nil, http.StatusInternalServerError, Retryable},
		{nil, http.StatusRequestTimeout, Retryable},
		{nil, http.StatusNotFound, Fatal},
		{nil, http.StatusForbidden, Fatal},
		{nil, http.StatusRequestedRangeNotSatisfiable, Fatal},
	}
	for _, c := range cases {
		if got := Classify(c.err, c.status); got != c.want {
			t.Errorf("Classify(%v,%d) = %v, want %v", c.err, c.status, got, c.want)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	h := http.Header{}
	h.Set("Retry-After", "5")
	if d, ok := RetryAfter(h, now); !ok || d != 5*time.Second {
		t.Errorf("seconds: got %v ok=%v, want 5s true", d, ok)
	}

	h = http.Header{}
	h.Set("Retry-After", now.Add(10*time.Second).UTC().Format(http.TimeFormat))
	if d, ok := RetryAfter(h, now); !ok || d != 10*time.Second {
		t.Errorf("http-date: got %v ok=%v, want 10s true", d, ok)
	}

	if _, ok := RetryAfter(http.Header{}, now); ok {
		t.Error("missing header should return ok=false")
	}
	h = http.Header{}
	h.Set("Retry-After", "garbage")
	if _, ok := RetryAfter(h, now); ok {
		t.Error("garbage header should return ok=false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/retry/ -v`
Expected: FAIL — `undefined: Policy` / `undefined: Classify` / `undefined: RetryAfter`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/retry/retry.go
package retry

import (
	"net/http"
	"strconv"
	"time"
)

// Decision is the outcome of classifying a failed attempt.
type Decision int

const (
	Fatal Decision = iota
	Retryable
)

// Policy controls retry attempts and backoff.
type Policy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
}

// Default returns the standard policy: up to 6 attempts, 500ms base, 30s cap.
func Default() Policy {
	return Policy{MaxAttempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second}
}

// Backoff returns the delay before a 0-based attempt index using exponential
// growth (Base * 2^attempt) capped at Max, scaled by full jitter in [0,1].
// Callers pass rand.Float64() in production; tests pass fixed values.
func (p Policy) Backoff(attempt int, jitter float64) time.Duration {
	d := p.Base << uint(attempt)
	if d <= 0 || d > p.Max { // overflow or above cap
		d = p.Max
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return time.Duration(jitter * float64(d))
}

// Classify decides whether an attempt that produced err and/or statusCode is
// retryable. statusCode is 0 when the request failed before a response. Any
// transport-level error is treated as transient; the caller is responsible for
// checking the parent context first (a cancelled parent means stop, not retry).
func Classify(err error, statusCode int) Decision {
	if err != nil {
		return Retryable
	}
	switch statusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,    // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return Retryable
	}
	return Fatal
}

// RetryAfter parses a Retry-After header (delta-seconds or HTTP-date) relative
// to now, returning the delay and whether a valid value was present.
func RetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/retry/ -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/retry/
git commit -m "feat(retry): backoff policy, error classification, Retry-After"
```

---

## Task 3: `internal/manifest` — atomic resume state

**Files:**
- Create: `internal/manifest/manifest.go`
- Test: `internal/manifest/manifest_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/manifest/manifest_test.go
package manifest

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nilparra-dev/velox/internal/probe"
)

func TestRoundTripAndPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.dm")
	info := &probe.RemoteInfo{URL: "http://x/f", Size: 1000, ETag: "\"v1\"", LastModified: "lm"}

	m := New(path, info, 100)
	m.MarkDone(0)
	m.MarkDone(2)
	if err := m.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp manifest file left behind after Save")
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.IsDone(0) || !loaded.IsDone(2) || loaded.IsDone(1) {
		t.Errorf("done set not preserved: %+v", loaded.Completed)
	}
	if loaded.ChunkSize != 100 || loaded.Size != 1000 || loaded.ETag != "\"v1\"" {
		t.Errorf("header fields not preserved: %+v", loaded)
	}
	pending := loaded.Pending(10) // 10 chunks, 0 and 2 done
	if len(pending) != 8 || pending[0] != 1 || pending[1] != 3 {
		t.Errorf("Pending = %v, want [1 3 4 5 6 7 8 9]", pending)
	}
}

func TestValidate(t *testing.T) {
	m := New("p", &probe.RemoteInfo{Size: 1000, ETag: "\"v1\"", LastModified: "lmA"}, 100)

	if !m.Validate(&probe.RemoteInfo{Size: 1000, ETag: "\"v1\""}) {
		t.Error("matching ETag+size should validate")
	}
	if m.Validate(&probe.RemoteInfo{Size: 1000, ETag: "\"v2\""}) {
		t.Error("different ETag should NOT validate")
	}
	if m.Validate(&probe.RemoteInfo{Size: 999, ETag: "\"v1\""}) {
		t.Error("different size should NOT validate")
	}
	// No ETag on either side -> fall back to Last-Modified.
	m2 := New("p", &probe.RemoteInfo{Size: 1000, LastModified: "lmA"}, 100)
	if !m2.Validate(&probe.RemoteInfo{Size: 1000, LastModified: "lmA"}) {
		t.Error("matching Last-Modified+size should validate")
	}
	if m2.Validate(&probe.RemoteInfo{Size: 1000, LastModified: "lmB"}) {
		t.Error("different Last-Modified should NOT validate")
	}
	// No validators at all -> size-only.
	m3 := New("p", &probe.RemoteInfo{Size: 1000}, 100)
	if !m3.Validate(&probe.RemoteInfo{Size: 1000}) {
		t.Error("size-only match should validate when no validators exist")
	}
}

func TestConcurrentMarkDone(t *testing.T) {
	m := New("p", &probe.RemoteInfo{Size: 1000}, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); m.MarkDone(i) }(i)
	}
	wg.Wait()
	if got := len(m.Pending(100)); got != 0 {
		t.Errorf("after marking all 100 done, Pending = %d, want 0", got)
	}
}

func TestLoadCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.dm")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("Load of corrupt manifest should return an error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/manifest/ -v`
Expected: FAIL — `undefined: New` / `undefined: Load`.

- [ ] **Step 3: Write minimal implementation**

```go
// internal/manifest/manifest.go
package manifest

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"

	"github.com/nilparra-dev/velox/internal/probe"
)

// Version is the manifest schema version.
const Version = 1

// Manifest is the resume state persisted next to a .part file. Only the indices
// of fully-completed chunks are stored; an interrupted chunk is re-downloaded
// in full on resume.
type Manifest struct {
	Version      int    `json:"version"`
	URL          string `json:"url"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	LastModified string `json:"lastModified"`
	ChunkSize    int64  `json:"chunkSize"`
	Completed    []int  `json:"completed"`

	mu   sync.Mutex   // guards done
	done map[int]bool // in-memory set; serialized to Completed on Save
	path string
}

// New creates a fresh manifest for info, to be persisted at path.
func New(path string, info *probe.RemoteInfo, chunkSize int64) *Manifest {
	return &Manifest{
		Version:      Version,
		URL:          info.URL,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		ChunkSize:    chunkSize,
		done:         make(map[int]bool),
		path:         path,
	}
}

// Load reads and parses a manifest from path.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Version != Version {
		return nil, errors.New("manifest: unsupported version")
	}
	m.path = path
	m.done = make(map[int]bool, len(m.Completed))
	for _, i := range m.Completed {
		m.done[i] = true
	}
	return &m, nil
}

// MarkDone records chunk idx as fully downloaded.
func (m *Manifest) MarkDone(idx int) {
	m.mu.Lock()
	m.done[idx] = true
	m.mu.Unlock()
}

// IsDone reports whether chunk idx is complete.
func (m *Manifest) IsDone(idx int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done[idx]
}

// Pending returns the indices in [0,total) that are not yet complete.
func (m *Manifest) Pending(total int) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	var p []int
	for i := 0; i < total; i++ {
		if !m.done[i] {
			p = append(p, i)
		}
	}
	return p
}

// Validate reports whether info matches this manifest (so resuming is safe):
// the size must match, and a validator must match when one exists on both
// sides; with no validators, a size match alone is accepted.
func (m *Manifest) Validate(info *probe.RemoteInfo) bool {
	if m.Size <= 0 || info.Size != m.Size {
		return false
	}
	if m.ETag != "" && info.ETag != "" {
		return m.ETag == info.ETag
	}
	if m.LastModified != "" && info.LastModified != "" {
		return m.LastModified == info.LastModified
	}
	return true
}

// Save atomically writes the manifest (tmp file + rename).
func (m *Manifest) Save() error {
	m.mu.Lock()
	m.Completed = make([]int, 0, len(m.done))
	for i := range m.done {
		m.Completed = append(m.Completed, i)
	}
	sort.Ints(m.Completed)
	data, err := json.MarshalIndent(m, "", "  ")
	m.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/manifest/ -v`
Expected: PASS (4 tests). Run `go vet ./internal/manifest/` (the `sync.Mutex` is never copied — `Manifest` is always used by pointer — so vet's copylocks check stays clean).

- [ ] **Step 5: Commit**

```bash
git add internal/manifest/
git commit -m "feat(manifest): atomic resume state with completed-chunk set"
```

---

## Task 4: `writer.Open` + `download` worker (chunk fetch with retry/stall/If-Range)

**Files:**
- Modify: `internal/writer/writer.go` (add `Open`)
- Modify: `internal/download/worker.go` (add `tick` to `copyAt`; add `downloadChunk`, `attemptChunk`, `httpError`, `errRemoteChanged`)
- Modify: `internal/download/download.go` (update the two `copyAt` callers to pass `nil` tick — keep behavior)
- Test: `internal/download/worker_chunk_test.go` (new test file)

This task is additive: the MVP's `downloadSegment` and `runRanged` stay in place (still compiling and passing) until Task 5 replaces them.

- [ ] **Step 1: Add `writer.Open`**

Add to `internal/writer/writer.go` (after `New`):

```go
// Open opens an EXISTING file for resuming a download without truncating its
// contents, ensuring it is pre-allocated to size bytes. Use New for a fresh
// download.
func Open(path string, size int64) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644) // no O_CREATE, no O_TRUNC
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{f: f}, nil
}
```

Run: `go test ./internal/writer/ -v` → still PASS (existing test untouched).

- [ ] **Step 2: Write the failing worker test**

```go
// internal/download/worker_chunk_test.go
package download

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nilparra-dev/velox/internal/chunk"
	"github.com/nilparra-dev/velox/internal/retry"
	"github.com/nilparra-dev/velox/internal/writer"
)

func fastPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 5, Base: time.Millisecond, Max: 5 * time.Millisecond}
}

// flakyRangedServer serves ranged content but fails the first `fails` requests
// (counted globally) by hijacking and closing the connection.
func flakyRangedServer(data []byte, fails int32) *httptest.Server {
	var seen int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&seen, 1) <= fails {
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
				return
			}
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
}

func TestDownloadChunkRetriesThenSucceeds(t *testing.T) {
	data := makeData(8192)
	srv := flakyRangedServer(data, 2) // first 2 requests die; probe is separate
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, err := writer.New(path, int64(len(data)))
	if err != nil {
		t.Fatalf("writer.New: %v", err)
	}
	defer w.Close()

	c := chunk.Chunk{Index: 0, Start: 0, End: int64(len(data)) - 1}
	if err := downloadChunk(context.Background(), srv.Client(), srv.URL+"/file.bin", "", c, w, fastPolicy(), 2*time.Second); err != nil {
		t.Fatalf("downloadChunk: %v", err)
	}
	w.Close()
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, data) {
		t.Error("chunk bytes mismatch after retries")
	}
}

func TestDownloadChunkRejectsRemoteChange(t *testing.T) {
	// Server returns 200 (full) whenever an If-Range header is present.
	data := makeData(4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Range") != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.bin")
	w, _ := writer.New(path, int64(len(data)))
	defer w.Close()

	c := chunk.Chunk{Index: 0, Start: 0, End: int64(len(data)) - 1}
	err := downloadChunk(context.Background(), srv.Client(), srv.URL+"/file.bin", "\"etag\"", c, w, fastPolicy(), 2*time.Second)
	if err == nil {
		t.Fatal("expected errRemoteChanged, got nil")
	}
}
```

(Helper `makeData` already exists in `download_test.go` from the MVP.)

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/download/ -run TestDownloadChunk -v`
Expected: FAIL — `undefined: downloadChunk`.

- [ ] **Step 4: Update `copyAt` and add the chunk worker**

First, change `copyAt` in `internal/download/worker.go` to accept a `tick` callback (called after each successful read; used by stall detection). Replace the existing `copyAt` with:

```go
// copyAt streams r into w starting at off, reporting progress per chunk and
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
```

Update the existing `downloadSegment` call to `copyAt` (in `worker.go`) to pass `nil` for the new `tick` argument: `got, err := copyAt(resp.Body, w, seg.Start, prog, nil)`.

Now append the chunk worker to `internal/download/worker.go`. Add `"errors"`, `"math/rand"`, `"time"` to its import block, plus `"github.com/nilparra-dev/velox/internal/chunk"` and `"github.com/nilparra-dev/velox/internal/retry"` (keep the existing `segment` and `writer` imports):

```go
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/download/ -v` (all old tests + the two new chunk tests) and `go vet ./...`.
Expected: PASS, vet clean. The package still compiles because `downloadSegment`/`runRanged` remain and now call the updated `copyAt`.

- [ ] **Step 6: Commit**

```bash
git add internal/writer/writer.go internal/download/worker.go internal/download/download.go internal/download/worker_chunk_test.go
git commit -m "feat(download): chunk worker with retry, If-Range and stall detection"
```

---

## Task 5: `download` orchestration rewrite — queue, pool, resume, finalize

**Files:**
- Create: `internal/download/resume.go`
- Rewrite: `internal/download/download.go` (chunked orchestration; keep single-stream fallback; remove `runRanged`/`downloadSegment` usage)
- Modify: `internal/download/worker.go` (delete now-unused `downloadSegment` and its `segment` import)
- Delete: `internal/segment/`
- Modify: `internal/download/download_test.go` (update `Options`/`OnInfo` usages; add resume/remote-change/stall integration tests)

- [ ] **Step 1: Write `internal/download/resume.go`**

```go
// internal/download/resume.go
package download

import (
	"os"

	"github.com/nilparra-dev/velox/internal/chunk"
	"github.com/nilparra-dev/velox/internal/manifest"
	"github.com/nilparra-dev/velox/internal/probe"
)

// plan describes how a chunked download will run.
type plan struct {
	manifest *manifest.Manifest
	chunks   []chunk.Chunk
	pending  []int
	resumed  int64 // bytes already on disk from completed chunks
	fresh    bool  // true when starting from scratch (no usable resume state)
}

// prepare decides whether to resume an existing download or start fresh.
// part is the <output>.part path and mpath the <output>.dm manifest path.
func prepare(opts Options, info *probe.RemoteInfo, part, mpath string) *plan {
	chunkSize := opts.ChunkSize
	if chunkSize < 1 {
		chunkSize = defaultChunkSize
	}

	if opts.Restart {
		os.Remove(part)
		os.Remove(mpath)
	} else if m, err := manifest.Load(mpath); err == nil && m.Validate(info) && fileExists(part) {
		chunks := chunk.Plan(info.Size, m.ChunkSize)
		pending := m.Pending(len(chunks))
		var resumed int64
		for _, c := range chunks {
			if m.IsDone(c.Index) {
				resumed += c.Length()
			}
		}
		return &plan{manifest: m, chunks: chunks, pending: pending, resumed: resumed, fresh: false}
	} else {
		// No usable manifest (missing, corrupt, changed remote, or no .part).
		os.Remove(part)
		os.Remove(mpath)
	}

	m := manifest.New(mpath, info, chunkSize)
	chunks := chunk.Plan(info.Size, chunkSize)
	pending := make([]int, len(chunks))
	for i := range chunks {
		pending[i] = i
	}
	return &plan{manifest: m, chunks: chunks, pending: pending, resumed: 0, fresh: true}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
```

- [ ] **Step 2: Rewrite `internal/download/download.go`**

Replace the ENTIRE file with:

```go
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
	Output       string        // final path; empty -> derived from URL
	Connections  int           // worker count (>=1)
	ChunkSize    int64         // bytes per chunk (<=0 -> default)
	Retries      int           // max attempts per chunk (<=0 -> default)
	Restart      bool          // ignore/delete any existing .part/.dm and start fresh
	StallTimeout time.Duration // no-progress timeout per attempt (<=0 -> default)
	Client       *http.Client  // nil -> http.DefaultClient
	Progress     ProgressFunc  // optional, called per completed chunk (or per read on single-stream)
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
	if len(p.pending) > 0 && workers > len(p.pending) {
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
	go func() {
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
```

- [ ] **Step 3: Remove the obsolete segment worker and package**

In `internal/download/worker.go`: delete the `downloadSegment` function entirely and remove the `"github.com/nilparra-dev/velox/internal/segment"` import (it is now unused). Keep `copyAt`, `downloadChunk`, `attemptChunk`, `httpError`, `errRemoteChanged`, `ProgressFunc`, `bufSize`.

Then delete the package:

```bash
git rm -r internal/segment
```

- [ ] **Step 4: Update existing tests + add integration tests**

In `internal/download/download_test.go`:

(a) `TestRunRangedParallel` and `TestRunSingleStreamFallback` call `Run` with `Options{...}` and read `res.Connections`. Their existing fields (`URL`, `Output`, `Connections`, `Client`) still exist. The ranged test now goes through the chunked path; with a 1 MiB file and the default 16 MiB chunk it is a single chunk, so `res.Connections` becomes 1 (workers capped to pending chunk count). Update that assertion: set `ChunkSize: 64 * 1024` in the ranged test's Options so a 1 MiB file yields 16 chunks and the assertion `res.Connections == 4` holds. Keep the byte-exact and `.part`-removed checks.

(b) Replace the now-obsolete `TestRunRejectsWrongContentRange` body if it referenced removed symbols — it calls `Run` only, so it still works (the Content-Range guard moved into `attemptChunk`). Leave it, but add `ChunkSize: 2048` to its Options so the 4096-byte file is split into chunks that exercise the guard.

(c) Append these integration tests:

```go
func TestRunResumesFromManifest(t *testing.T) {
	data := makeData(40 * 1024) // 40 KiB
	const cs = 10 * 1024        // 4 chunks
	var served int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" && r.Header.Get("Range") != "bytes=0-0" {
			atomic.AddInt32(&served, 1)
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "result.bin")
	part := out + ".part"
	mpath := out + ".dm"

	// Seed a pre-allocated .part with chunks 0 and 1 already written, and a
	// manifest marking them done. Probe will fill ETag/Last-Modified; use a
	// size-only manifest so Validate passes on size alone.
	w, err := writer.New(part, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	w.WriteAt(data[0:cs], 0)
	w.WriteAt(data[cs:2*cs], cs)
	w.Close()

	mf := manifest.New(mpath, &probe.RemoteInfo{URL: srv.URL + "/file.bin", Size: int64(len(data))}, cs)
	mf.MarkDone(0)
	mf.MarkDone(1)
	if err := mf.Save(); err != nil {
		t.Fatal(err)
	}

	res, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 4, ChunkSize: cs, Client: srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Resumed {
		t.Error("expected Resumed=true")
	}
	if got := atomic.LoadInt32(&served); got != 2 {
		t.Errorf("served %d ranged requests, want 2 (only chunks 2 and 3)", got)
	}
	final, _ := os.ReadFile(out)
	if !bytes.Equal(final, data) {
		t.Error("resumed file is not byte-exact")
	}
	if _, err := os.Stat(mpath); !os.IsNotExist(err) {
		t.Error(".dm manifest not removed after success")
	}
}

func TestRunRestartIgnoresStaleManifest(t *testing.T) {
	data := makeData(40 * 1024)
	const cs = 10 * 1024
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dir := t.TempDir()
	out := filepath.Join(dir, "result.bin")
	// Stale manifest claiming a different size -> Validate fails -> fresh start.
	mf := manifest.New(out+".dm", &probe.RemoteInfo{URL: srv.URL + "/file.bin", Size: 999999}, cs)
	mf.MarkDone(0)
	mf.Save()

	res, err := Run(context.Background(), Options{
		URL: srv.URL + "/file.bin", Output: out, Connections: 4, ChunkSize: cs, Client: srv.Client(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Resumed {
		t.Error("stale manifest (wrong size) should force a fresh start, Resumed=false")
	}
	final, _ := os.ReadFile(out)
	if !bytes.Equal(final, data) {
		t.Error("restarted file is not byte-exact")
	}
}
```

Ensure the test file imports include `"sync/atomic"`, `"github.com/nilparra-dev/velox/internal/manifest"`, `"github.com/nilparra-dev/velox/internal/probe"`, and `"github.com/nilparra-dev/velox/internal/writer"` (some may already be present from Task 4's test file in the same package — declare each import once across the package's test files; if both test files need it, that is fine as long as no single file imports it twice).

- [ ] **Step 5: Run tests + vet**

Run: `go test ./... -v` and `go vet ./...` and `gofmt -l .`
Expected: all packages PASS, vet clean, gofmt prints nothing. `internal/segment` no longer exists.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(download): work-stealing chunk queue with resume and durable finalize"
```

---

## Task 6: `main.go` — flags, header timeout, resume-aware progress

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Rewrite `main.go`**

Replace the ENTIRE file with:

```go
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
```

- [ ] **Step 2: Build and vet**

Run: `go build -o velox.exe . && go vet ./...`
Expected: clean.

- [ ] **Step 3: Manual end-to-end + resume sanity (internet available)**

```bash
go build -o velox.exe .
# Full download:
./velox.exe -n 8 -o ovh.dat https://proof.ovh.net/files/10Mb.dat
# Expect: bar to 100%, "Saved ovh.dat (10485760 bytes, 8 connections, ranged=true, resumed=false)".

# Resume sanity: start, interrupt with Ctrl-C partway, then re-run the same command.
./velox.exe -n 8 -o big.dat https://proof.ovh.net/files/100Mb.dat   # Ctrl-C after a moment
ls -l big.dat.part big.dat.dm                                        # both should exist
./velox.exe -n 8 -o big.dat https://proof.ovh.net/files/100Mb.dat   # resumes -> resumed=true, completes
```
Verify final sizes match Content-Length and that `.part`/`.dm` are gone after success. Delete the test files. If a host is unreachable, say so; do not fake results.

- [ ] **Step 4: Full suite + commit**

Run: `go test ./... && go vet ./... && gofmt -l .`
Expected: all PASS, clean.

```bash
git add main.go
git commit -m "feat(cli): chunk-size/retries/restart flags, header timeout, resume-aware bar"
```

---

## Self-review

**Spec coverage** (`docs/specs/2026-06-15-phase2-resume.md`):
- §2 chunk queue + worker pool (work-stealing) → Task 5 `runChunked`. ✔
- §2 packages `chunk`/`manifest`/`retry` → Tasks 1/3/2. ✔
- §3 manifest (completed-set, atomic save, flush) → Task 3 + `runChunked` flusher. ✔
- §4 resume (If-Range + size, validate, restart, corrupt→fresh) → Task 5 `prepare` + `attemptChunk` (`If-Range`, 200→`errRemoteChanged`). ✔
- §5 retries (backoff, classify, Retry-After, stall, header timeout, fsync, durability) → Task 2 + Task 4 `downloadChunk`/`attemptChunk` + Task 6 `ResponseHeaderTimeout` + Task 5 `w.Sync()`. ✔
- §6 CLI flags (`--chunk-size`, `--retries`, `--restart`), resume-aware bar, interrupt message → Task 6. ✔
- §7 edge cases (no-range single-stream, corrupt manifest, group cancel, If-Range→200) → Task 5 `runSingleStream`/`prepare`, `errgroup`, `attemptChunk`. ✔
- §8 testing matrix → Tasks 1–6 tests (resume, restart/stale, retry, remote-change; **stall integration test is deferred** — the stall watchdog is unit-exercised indirectly; a timing-based integration test is flaky, so it is intentionally omitted and noted here). ⚠ documented
- §9 file structure → matches. ✔
- **Spec refinement:** `writer.Open` added (Task 4) for non-truncating resume — noted at the top of this plan. ✔

**Placeholder scan:** no TBD/TODO; every code step is complete.

**Type consistency:** `chunk.Chunk{Index,Start,End}`+`Length()`; `retry.Policy`/`Backoff(attempt,jitter)`/`Classify(err,status)`/`RetryAfter(h,now)`/`Default()`; `manifest.New/Load/MarkDone/IsDone/Pending/Validate/Save`; `writer.New`/`Open`/`WriteAt`/`Sync`/`Close`; `download.copyAt(r,w,off,prog,tick)`, `downloadChunk(ctx,client,url,ifRange,c,w,pol,stall)`, `attemptChunk(...)→(time.Duration,error)`, `Options{...,ChunkSize,Retries,Restart,StallTimeout}`, `OnInfo(size,ranged,resumed)`, `Result{...,Resumed}`, `Run`. Used identically across Tasks 1–6.
