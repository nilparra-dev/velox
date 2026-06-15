# velox — Design

**Status:** approved (design phase) · **Date:** 2026-06-15 · **Language:** Go

`velox` is an ultra-fast, multi-connection HTTP/HTTPS download manager for large
files (target: up to ~24 GB) from a single direct link.

---

## 1. Goals and non-goals

**Goals**

- **Speed is the hard requirement.** Saturate the available link on a single large
  download, beating a single HTTP stream whenever the origin throttles per-flow.
- Resume interrupted downloads without re-fetching completed bytes.
- Verify integrity on completion.
- Single cross-platform binary (Windows, Linux, macOS), CLI-first.
- Small, maintainable codebase with a minimal dependency set.

**Non-goals (for now)**

- BitTorrent / metalink / FTP protocols (HTTP/HTTPS only).
- Browser integration or GUI (CLI only in early phases).
- Multi-file queue and mirror selection (deferred to Phase 4).

---

## 2. Performance model

A download manager does not "speed up the internet"; it distributes a fixed-size
transfer more efficiently. Limits, from hardest to most software-addressable:

1. **Origin server — usually the real bottleneck.** Most servers/CDNs throttle each
   TCP flow individually. Opening N parallel connections bypasses per-flow limits and
   sums their bandwidth. This is the primary reason multi-connection downloads are
   faster. If the server enforces an *aggregate* per-client cap, parallelism reaches
   that cap sooner but cannot exceed it.
2. **Local bandwidth.** A fast fiber line (~60–125 MB/s) is the absolute ceiling. A
   24 GB file needs ≥ ~3.5 min at 1 Gbps, ~7 min at 500 Mbps — the physical minimum.
3. **Latency / RTT.** For distant servers, a single TCP connection underuses a fat
   pipe (bandwidth-delay product + slow start). Parallel connections fix this.
4. **Disk.** On NVMe SSD (GB/s), never the bottleneck. Only relevant on mechanical HDD.

**What the software optimizes:** parallel connection count, connection reuse
(keep-alive), zero-copy writes straight to disk offsets, retry/resume, and a sane
connection count (too many → `429`/resets).

**What it cannot do:** exceed the line rate or the server's aggregate cap, make a
non-range server resumable, or beat latency.

**Realistic expectation:** 3×–10× over a single stream when the server throttles
per-flow; near-zero gain when one stream already saturates the link (good CDNs).
`velox` must be fast and correct in both cases.

---

## 3. Architecture

Each component has a single responsibility and a well-defined interface.

```
                 ┌──────────────┐
   URL  ───────▶ │   Probe      │  HEAD / GET Range: bytes=0-0
                 │              │  → size, Accept-Ranges, ETag, final URL
                 └──────┬───────┘
                        │
                 ┌──────▼───────┐
                 │   Planner    │  split [0, size) into segments
                 │              │  + load/create resume manifest
                 └──────┬───────┘
                        │  queue of chunks (offset, length)
        ┌───────────────┼───────────────┐
   ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
   │ Worker 1│ ... │ Worker k│ ... │ Worker N│   goroutines (errgroup)
   │GET Range│     │GET Range│     │GET Range│
   └────┬────┘     └────┬────┘     └────┬────┘
        │  WriteAt(offset)│              │
        └───────────────┬─┴──────────────┘
                 ┌───────▼────────┐   ┌──────────────┐
                 │  Writer        │   │  Manifest    │  periodic flush
                 │ single .part,  │   │  .dm sidecar │  of per-segment
                 │ pre-allocated  │   │              │  progress
                 └───────┬────────┘   └──────────────┘
                 ┌───────▼────────┐
                 │ Verify+Finalize│  size → (hash if available) → rename .part → final
                 └────────────────┘
```

### 3.1 Byte-range splitting

A queue of `(offset, length)` chunks. **MVP:** N equal segments (simple). **Later
(Phase 3):** bounded chunks (4–16 MB) consumed by workers via *work-stealing*, so a
slow chunk never stalls the whole download (tail-latency at the end of a transfer).

### 3.2 Parallel downloads

A goroutine pool coordinated with `golang.org/x/sync/errgroup`; the first error
cancels the rest via `context`. One shared `http.Client` with a tuned `Transport`
(keep-alive, `MaxConnsPerHost`, forced HTTP/1.1 — see Decision 7).

### 3.3 Joining chunks — there is no join step

A single output file is pre-allocated to the final size; each worker calls
`WriteAt(data, offset)` (pwrite, concurrency-safe). No per-segment temp files, no
concatenation, no double disk space. Optimal on NVMe.

### 3.4 Resume

A sidecar `<file>.dm` (JSON) next to the `.part` records: final URL, size,
ETag/Last-Modified, segment list, and bytes completed per segment. Flushed every
~1 s or every N MB. On restart: if ETag/size match, re-request only incomplete
chunks; if the remote file changed, restart cleanly.

### 3.5 Integrity

Always verify total size (Content-Length vs bytes written). Verify SHA-256/MD5 when
a checksum is available (`Content-MD5` header, a sibling `.sha256`, or a user-supplied
hash). Honest default: many servers expose no hash, so size + ETag is best-effort.
Only after verification is `.part` atomically renamed to the final name.

### 3.6 Error handling (first-class)

- Per-chunk retry with exponential backoff.
- `429 Too Many Requests` → honor `Retry-After`, reduce concurrency.
- Dropped connection / short read → resume that chunk from where it stopped.
- Redirects → resolved once during probe.
- Server stops honoring the range mid-stream → detect and reschedule the chunk.

---

## 4. Key decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Connection count | **Default 8**, configurable via `-n`, cap 16. Adaptive in a later phase. |
| 2 | Segment strategy | **MVP: N equal segments.** Later: 4–16 MB bounded chunks with work-stealing. |
| 3 | No range support | **Detect at probe, fall back gracefully to a single stream** (warn: no parallelism/resume). Never hard-fail. |
| 4 | Disk write | **Single pre-allocated file + `WriteAt`** by offset. No merge. |
| 5 | Resume granularity | **Per-segment**, persisted in the manifest every ~1 s / N MB. |
| 6 | Integrity | **Size always; SHA-256 when a hash is available or user-supplied.** |
| 7 | HTTP version | **Force HTTP/1.1 with multiple TCP connections.** HTTP/2 multiplexes over one connection and does not bypass per-flow throttling. |
| 8 | Finalize | **`.part` + manifest, atomic rename on verify.** Never leave a half file under the final name. |
| 9 | v1 scope | **One file at a time, as fast as possible.** Multi-URL queue is Phase 4. |

---

## 5. Tech stack

Go: native concurrency for parallel downloads, a single static cross-platform binary,
and an excellent stdlib HTTP client. Stdlib first; an external dependency only when it
clearly earns its place.

| Need | Library |
|------|---------|
| HTTP | `net/http` (stdlib) |
| Worker coordination | `golang.org/x/sync/errgroup` |
| Cancellation | `context` (stdlib) |
| Progress bar(s) | `github.com/vbauerster/mpb` |
| CLI / flags | `github.com/spf13/cobra` or `urfave/cli` (decide at Phase 1) |
| Rate limiting (optional) | `golang.org/x/time/rate` |
| Hashing | `crypto/sha256`, `crypto/md5` (stdlib) |

---

## 6. Roadmap

- **Phase 0 — Spike:** single-stream download to file, progress bar, `-o`. Validates
  HTTP/write/CLI/Ctrl-C plumbing. Disposable.
- **Phase 1 — MVP:** probe (size + range test) → split into N equal segments →
  N goroutines `GET Range` → `WriteAt` into a pre-allocated file → aggregate progress
  → size check. Flags `-n`, `-o`. Single-stream fallback. This already saturates a
  fast link.
- **Phase 2 — Robustness/resume:** sidecar manifest, per-segment resume, retries with
  backoff, ETag validation, `429`/`Retry-After`, atomic finalize.
- **Phase 3 — Performance:** work-stealing bounded chunks, adaptive connection count,
  optional rate limit, custom headers/cookies (authenticated links), checksum verify.
- **Phase 4 — Convenience:** per-segment multi-bar UI, config file, multi-URL queue,
  mirrors, packaged releases for Windows/Linux/macOS.

Real value ships at the end of Phase 1; every later phase is independent and optional.

---

## 7. Open questions

- CLI library choice (`cobra` vs `urfave/cli`) — defer to Phase 1.
- True file preallocation: `f.Truncate` everywhere vs `fallocate` (Linux,
  `golang.org/x/sys`) — evaluate in Phase 1 if it measurably helps.
