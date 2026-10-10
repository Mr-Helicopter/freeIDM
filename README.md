# IDM - Rust CLI File Downloader

An asynchronous, streaming command-line file downloader written in Rust, built with **Reqwest**, **Tokio**, and **Indicatif**.

---

## Features

- **Chunked Asynchronous Streaming**: Streams data from the network directly into disk storage via `tokio::fs::File`, preventing high memory usage even when downloading multi-gigabyte files.
- **Intelligent Filename Detection**:
  - Automatically parses `Content-Disposition` headers (`filename` and RFC 5987/6266 `filename*`).
  - Falls back to the URL's last path segment (handling URL encoding).
  - Sanitizes filenames against directory traversal vulnerabilities (`../`).
  - Fallback default to `downloaded_file`.
- **Dynamic Progress Bar**:
  - Shows an animated spinner, elapsed time, byte counts (`{bytes}/{total_bytes}`), transfer speed (`{bytes_per_sec}`), and ETA (`{eta}`).
  - Automatically falls back to an unbounded spinner if the server uses chunked transfer encoding (no `Content-Length`).
- **Robust Error Handling**: Uses `anyhow` for actionable, context-rich error reporting.
- **Beginner-Friendly Codebase**: Clean, documented code in `src/main.rs` with detailed inline comments and automated unit tests.

---

## Prerequisites

- [Rust & Cargo](https://www.rust-lang.org/tools/install) (Rust 1.75+ recommended)

---

## How to Build & Run

### 1. Run directly with Cargo
```bash
cargo run -- <URL>
```

#### Examples:
```bash
# Download from URL path
cargo run -- https://raw.githubusercontent.com/rust-lang/rust/master/README.md

# Download test file with Content-Disposition header
cargo run -- "https://httpbin.org/response-headers?Content-Disposition=attachment%3B%20filename%3D%22sample.txt%22"
```

### 2. Run Automated Tests
```bash
cargo test
```

### 3. Build Release Binary
To build an optimized binary:
```bash
cargo build --release
```
The compiled executable will be located at:
```bash
./target/release/idm <URL>
```
