# velox

> An ultra-fast, multi-connection HTTP/HTTPS download manager for large files.

`velox` (Latin for *swift*) downloads large files (tested target: up to ~24 GB) as
fast as your connection and the origin server allow. It opens multiple parallel
HTTP range connections to a single direct link, writes every chunk straight to its
offset on disk (no merge step), resumes interrupted transfers, and verifies
integrity when it finishes.

> **Status:** MVP working. Parallel ranged downloads, single-stream fallback, and
> size verification are implemented and tested. Resume, retries, and checksums are
> next (see the roadmap).

## Install

```sh
go install github.com/nilparra-dev/velox@latest
```

Or build from source:

```sh
git clone https://github.com/nilparra-dev/velox && cd velox
go build -o velox .
```

## Usage

```sh
velox [-n N] [-o FILE] URL
```

- `-n N` — number of parallel connections (default 8, capped at 16).
- `-o FILE` — output path (default: derived from the URL).

```sh
velox -n 8 -o ubuntu.iso https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso
```

## Why

A single HTTP connection is often throttled per-flow by the origin server, so it
rarely saturates a fast link. `velox` opens several connections in parallel to get
around per-flow limits and fill your pipe — while staying fast and correct when the
server already gives you full speed on one connection.

What software can and cannot do here:

- **Can optimize:** parallel connections, connection reuse, zero-copy writes to disk,
  resume/retry, picking a sane connection count.
- **Cannot beat:** your line rate, the server's aggregate per-client cap, latency,
  or a server that does not support byte ranges.

## Planned features

- Parallel multi-connection downloads via HTTP `Range` requests.
- Single pre-allocated output file with concurrent `WriteAt` (no temp-part merge).
- Resume interrupted downloads via a sidecar manifest (per-segment progress).
- Automatic, graceful fallback to a single stream when the server has no range support.
- Integrity verification: size always; SHA-256/MD5 when a checksum is available.
- Cross-platform single binary (Windows, Linux, macOS).

## Roadmap

- **Phase 0** — Spike: single-stream download with progress bar. ✅
- **Phase 1 (MVP)** — Parallel range download, pre-allocated `WriteAt`, size check,
  single-stream fallback. ✅
- **Phase 2** — Resume manifest, retries with backoff, ETag validation, atomic finalize.
- **Phase 3** — Work-stealing chunks, adaptive connection count, rate limiting, auth headers.
- **Phase 4** — Multi-bar UI, config file, multi-URL queue, mirrors, packaged releases.

## License

[MIT](LICENSE)
