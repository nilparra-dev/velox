# velox Phase 2 — Resumable, hardened downloads

**Status:** approved (design) · **Date:** 2026-06-15 · **Builds on:** the MVP (`docs/DESIGN.md`, Phases 0–1)

Phase 2 makes velox robust enough for very large, long-running downloads (target: a real ~24 GB file): work-stealing chunks, resume across runs, per-chunk retries with backoff, stall detection, remote-change validation, and durable finalize.

---

## 1. Goals and non-goals

**Goals**
- Replace the MVP's N-equal-segments model with **fixed-size chunks pulled from a queue by N workers** (work-stealing): fine granularity, no slow-tail, trivial retry/resume.
- **Resume across runs**: a download interrupted (crash, Ctrl-C, network loss) continues from where it stopped on the next run, re-downloading at most one chunk's worth of bytes.
- **Per-chunk retries** with exponential backoff + jitter, honoring `Retry-After`.
- **Stall detection**: abort and retry an attempt that stops making progress.
- **Remote-change safety**: detect (via `If-Range` + size/validator) that the remote file changed and restart cleanly instead of corrupting the output.
- **Durable finalize**: `fsync` before the atomic rename; manifest written atomically.

**Non-goals (later phases)**
- Adaptive connection count, rate limiting, custom auth headers, mirrors (Phase 3).
- Checksum verification against an external hash (Phase 3) — Phase 2 verifies size + per-chunk `Content-Range`/length + validators.
- Multi-bar UI, config file, multi-URL queue, packaged releases (Phase 4).
- Explicit free-disk precheck: rely on `Truncate`/write failing with a clear error (on NTFS, `Truncate` already reserves space). Revisit if needed.

---

## 2. Architecture

```
probe → resume-check (manifest) → plan chunks → enqueue only the not-completed chunks
                                                       │
                          ┌──────────── chunk queue (channel) ────────────┐
                     ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
                     │worker 1 │ ...  │worker k │ ...  │worker N │   work-stealing:
                     │pull→get │      │pull→get │      │pull→get │   a faster worker
                     └────┬────┘      └────┬────┘      └────┬────┘   pulls more chunks
        retry + stall +   │  WriteAt(off)  │  If-Range      │
                     ┌────▼────────────────▼────────────────▼────┐
                     │ writer.Writer (.part, pre-allocated)        │
                     └──────────────────┬──────────────────────────┘
                              MarkDone(idx) │  periodic atomic flush
                     ┌────────────────────▼────────────────┐
                     │ manifest  <output>.dm  (JSON)        │
                     └──────────────────────────────────────┘
   on success: writer.Sync() (fsync) → os.Rename(.part → final) → delete .dm
```

### Packages (one responsibility each)
- **`internal/chunk`** (new, replaces `internal/segment`): `Chunk{Index, Start, End}`, `(Chunk) Length() int64`, `Plan(size, chunkSize int64) []Chunk` — fixed-size chunks, last one shorter. `internal/segment` is removed; the download worker now operates on a `Chunk`.
- **`internal/manifest`** (new): the resume state file. `Manifest` struct; `Load(path)`, `Save(path)` (atomic via `<path>.tmp` + rename), `MarkDone(idx)` (goroutine-safe), `Pending(total int) []int`, and validation against `probe.RemoteInfo`.
- **`internal/retry`** (new): `Policy{MaxAttempts, Base, Max time.Duration}`, `Backoff(attempt int) time.Duration` (exponential + full jitter), `Classify(err, statusCode) Decision` (`Retry`/`Fatal`), and honoring `Retry-After`.
- **`internal/download`** (rewritten, split for focus):
  - `download.go` — `Run`/`Options`/`Result`, orchestration, queue + worker pool, finalize.
  - `worker.go` — `downloadChunk` with retry loop, `If-Range`, stall detection, `Content-Range`/length validation.
  - `resume.go` — load+validate manifest, compute pending chunks and resumed byte count, restart decisions.
- **`internal/probe`**: unchanged (already returns size, range support, ETag, Last-Modified, final URL).
- **`main.go`**: new flags, resume-aware progress bar, interrupt messaging.

---

## 3. Manifest

Sidecar `<output>.dm` next to the `.part` file:

```json
{
  "version": 1,
  "url": "<final URL after redirects>",
  "size": 25769803776,
  "etag": "\"abc123\"",
  "lastModified": "Wed, 01 Jan 2025 00:00:00 GMT",
  "chunkSize": 16777216,
  "completed": [0, 1, 2, 5]
}
```

**Only fully-completed chunk indices are stored.** A chunk interrupted mid-flight is re-downloaded in full on resume (wasting at most `chunkSize`, e.g. 16 MiB). No partial-byte bookkeeping → the manifest is small and crash-safe.

- **Flush cadence:** at most once per ~1 s, plus when a chunk completes (debounced), plus on shutdown. Always write `<output>.dm.tmp` then `os.Rename` over `<output>.dm` (atomic; never leaves a half-written manifest).
- **Concurrency:** a single `Manifest` value guarded by a mutex; workers call `MarkDone(idx)`. A background flusher persists periodically.
- **Corrupt/unreadable manifest:** ignored with a warning; the download starts fresh.

---

## 4. Resume logic (decision: If-Range + size)

On startup, if both `<output>.part` and `<output>.dm` exist:
1. Re-`probe` the URL. Compare `size` and the validator (`ETag`, else `Last-Modified`) against the manifest.
2. **Match** → re-enqueue only chunks not in `completed`; the progress bar's initial value is the exact sum of completed-chunk lengths.
3. **Changed, or no validator and size differs** → discard `.part` and `.dm`, start fresh (warn the user).
4. Every chunk request carries `If-Range: <validator>` as a safety net. If a chunk request returns **200** (the server signals the resource changed mid-download), that chunk errors and aborts the run; on the next run, step 1 detects the change and restarts cleanly.

Servers **without range support** (single-stream path): no reliable resume → always restart; no manifest is written. Documented behavior.

---

## 5. Retry and hardening

**Per chunk attempt** (in `worker.go`):
- Request `Range: bytes=<start>-<end>` + `If-Range: <validator>` when available.
- `206` → validate `Content-Range` starts at `chunk.Start`, stream into the writer at the absolute offset, verify bytes received == `chunk.Length()`, then `MarkDone`.
- `200` → remote changed mid-download → fatal (abort run; next run restarts).
- Other → classify (below).

**Retry policy** (`internal/retry`, default `MaxAttempts = 6`):
- **Retryable:** network errors (timeout, connection reset, unexpected EOF), HTTP `408`, `429`, `500`, `502`, `503`, `504`.
- **Fatal:** `4xx` except `408`/`429` (e.g. `403`, `404`, `416`) → fail fast.
- **`429`/`503` with `Retry-After`:** wait the indicated delay (seconds or HTTP-date).
- **Backoff:** `min(Max, Base * 2^attempt)` with full jitter; defaults `Base = 500ms`, `Max = 30s`.
- After `MaxAttempts`, the chunk fails and the run fails — the manifest preserves completed chunks so a re-run resumes.

**Stall detection:** while reading a chunk body, if no bytes arrive for `stallTimeout` (default **30 s**), cancel the attempt (via its context) and let the retry loop handle it. Implemented by resetting a timer on each successful `Read`.

**Header timeout:** each attempt has a context deadline (default **30 s**) to receive the response headers; the deadline is cleared once the body starts streaming (the stall detector then governs the body).

**Durability:** `writer.Sync()` (fsync) before `Close` and the final rename. On success, delete the `.dm` manifest after the rename.

---

## 6. CLI changes (`main.go`)

- New flags: `--chunk-size` (default 16 MiB), `--retries` (default 6), `--restart` (delete any existing `.part`/`.dm` and start fresh).
- `-n` unchanged (default 8, cap 16).
- **Ctrl-C:** the context cancels; the `.part` and `.dm` are preserved; print `interrupted — re-run the same command to resume`.
- **Progress bar:** initialized to the resumed byte count so the bar reflects real total progress, not just this run.

---

## 7. Error handling / edge cases

- First unrecoverable chunk error cancels the worker group (`errgroup` + context); the manifest keeps completed chunks for resume.
- Corrupt/unreadable manifest → ignore, start fresh (warn).
- No-range server → single-stream, no resume, no manifest.
- `If-Range` → `200` mid-download → abort; next run restarts fresh.
- Manifest's `chunkSize` differs from the requested `--chunk-size` on resume → the stored `chunkSize` wins (chunk boundaries must match the on-disk layout); warn if the flag was set.

---

## 8. Testing (all without `-race`; no CGO on the dev host — CI runs `-race` on Linux)

- **`chunk.Plan`:** exact coverage, short last chunk, `chunkSize >= size`, guards.
- **`manifest`:** save/load roundtrip, atomic write (no partial file on simulated failure), concurrent `MarkDone`, `Pending`, validation match/mismatch, corrupt-file handling.
- **`retry`:** backoff sequence bounds + jitter range, error/status classification, `Retry-After` parsing (seconds + HTTP-date), success after N transient failures.
- **`download` integration (`httptest`):**
  - **Resume:** download with a handler that drops the connection after K chunks; assert the run errors and the manifest records the completed chunks; re-run against a healthy handler; assert the final file is byte-exact and `.dm` is gone.
  - **Retry:** a handler that fails the first M attempts of specific chunks then succeeds; assert completion.
  - **Remote changed:** seed a manifest with a stale ETag; assert a clean restart and a correct result.
  - **Stall:** a handler that stops sending mid-chunk; with a short `stallTimeout`, assert the attempt is cancelled and retried, then completes against a healthy retry.
  - **If-Range → 200 mid-download:** assert the run aborts with a clear error.

---

## 9. File structure delta

```
internal/
├── chunk/        (new; replaces segment/)
│   ├── chunk.go
│   └── chunk_test.go
├── manifest/     (new)
│   ├── manifest.go
│   └── manifest_test.go
├── retry/        (new)
│   ├── retry.go
│   └── retry_test.go
├── probe/        (unchanged)
├── writer/       (unchanged)
└── download/     (rewritten)
    ├── download.go      (Run, orchestration, queue, pool, finalize)
    ├── worker.go        (downloadChunk: retry + If-Range + stall + validation)
    ├── resume.go        (manifest load/validate, pending computation)
    └── download_test.go
main.go            (new flags, resume-aware bar, interrupt messaging)
```
