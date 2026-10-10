# IDM - Rust Parallel Segmented CLI File Downloader

A high-performance, asynchronous, parallel segmented file downloader written in Rust, built with **Reqwest**, **Tokio**, and **Indicatif**.

---

## Features

- **HTTP Probing & Range Capability Detection**:
  - Sends a `HEAD` request to inspect `Content-Length`, `Accept-Ranges: bytes`, `ETag`, and `Last-Modified`.
  - If `HEAD` is unsupported or lacks headers, falls back to `GET` with `Range: bytes=0-0` and parses the total size from `Content-Range`.
  - Automatically selects segmented download if supported, or falls back to single-stream.
- **Segmented Parallel Downloads**:
  - Preallocates disk file to the full size using `file.set_len(total_size)` to reduce fragmentation.
  - Divides the file into fixed-size chunks (default 4 MiB) with inclusive HTTP range bounds (`bytes=start-end`).
  - Distributes chunks across an asynchronous work queue using configurable concurrent worker connections (default 8, via `--connections`).
  - Each worker opens its own file handle, seeks to its byte offset, requires `206 Partial Content`, verifies `Content-Range`, and streams chunks directly to disk.
- **Graceful Single-Stream Fallback**:
  - If ranges are unsupported, file size is unknown, or the server returns `200 OK` instead of `206 Partial Content`, immediately aborts segmented mode and falls back to Stage 1 sequential streaming.
- **Retries with Exponential Backoff**:
  - Automatically retries failed chunks up to 5 times with exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms), restarting each failed chunk from its beginning and rolling back the byte counter.
- **Shared Atomic Progress Tracking**:
  - Uses a single `indicatif` progress bar driven by a shared `Arc<AtomicU64>` counter showing bytes done, total size, transfer speed, and ETA.
- **Intelligent Filename Detection & Security**:
  - Detects filenames from `Content-Disposition` (RFC 6266 / RFC 5987 `filename*` and `filename`) or the URL path.
  - Sanitizes filenames against directory traversal vulnerabilities (`../`).
- **Comprehensive Test Suite**:
  - Includes unit tests and integration tests running against a local asynchronous HTTP mock server that handles Range requests, testing various file sizes and fallback behaviors.

---

## How to Build & Run

### 1. Basic Download (default 8 connections, 4 MiB chunks)
```bash
cargo run -- https://speed.hetzner.de/100MB.bin
```

### 2. Custom Connections & Chunk Size
```bash
# 16 concurrent connections
cargo run -- --connections 16 https://speed.hetzner.de/100MB.bin

# Short flags (-c for connections, -s for chunk size in bytes)
cargo run -- -c 4 -s 1048576 https://speed.hetzner.de/100MB.bin
```

### 3. Custom Output File
```bash
cargo run -- -o my_download.bin https://speed.hetzner.de/100MB.bin
```

### 4. Run Automated Tests
```bash
cargo test
```

### 5. Build Release Binary
```bash
cargo build --release
./target/release/idm --help
```
