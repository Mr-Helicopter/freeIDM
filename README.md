# IDM - Modular Rust Parallel Segmented File Downloader

A high-performance, resilient, asynchronous parallel segmented file download engine and CLI application written in Rust, built with **Reqwest**, **Tokio**, **Rusqlite**, **Serde**, and **Indicatif**.

Organized as a Cargo workspace:
- **`crates/idm-core`**: Headless download engine library. Provides `DownloadManager` with queue management, SQLite persistence, concurrency limits, runtime token-bucket speed regulation, and throttled event broadcasting. Contains zero console printing or progress bar dependencies.
- **`crates/idm-cli`**: Thin CLI application (`idm`) providing interactive console progress visualization (`indicatif`), overwrite confirmation prompts, graceful double Ctrl-C signal handling, and queue subcommands.

---

## Workspace Architecture

```
IDM/
├── Cargo.toml                    # Virtual workspace root
├── crates/
│   ├── idm-core/                 # Core engine library
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs            # Public API exports
│   │   │   ├── manager.rs        # DownloadManager & async scheduler loop
│   │   │   ├── storage.rs        # SQLite database (rusqlite WAL mode)
│   │   │   ├── limiter.rs        # Token-bucket global speed limiter
│   │   │   ├── events.rs         # Broadcast channel & throttled progress emitter
│   │   │   ├── probe.rs          # Server probing & Range capability detection
│   │   │   ├── chunks.rs         # Chunk partitioning & Range calculations
│   │   │   ├── state.rs          # Atomic .part.state persistence & POSIX flock
│   │   │   ├── segmented.rs      # Multi-connection parallel chunk streaming
│   │   │   └── single.rs         # Single-stream fallback
│   │   └── tests/
│   │       └── integration_tests.rs # 16 end-to-end integration tests
│   └── idm-cli/                  # Thin CLI binary
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs           # CLI subcommands, arguments, Ctrl-C handler
│           └── progress.rs       # Indicatif progress bar management
└── README.md
```

---

## Features

### 1. Robust Download Engine (`idm-core`)
- **Segmented Parallel Downloads**:
  - Preallocates output files with `set_len(total_size)` to prevent filesystem fragmentation.
  - Divides downloads into fixed-size chunks (default 4 MiB) with inclusive HTTP range bounds (`bytes=start-end`).
  - Distributes chunks across worker tasks over a configurable number of connections.
  - Ensures every worker calls `flush().await` before marking any chunk as finished.
- **Graceful Single-Stream Fallback**:
  - Automatically falls back to single-stream sequential streaming if ranges are unsupported, file size is unknown, or servers return `200 OK` on range requests.
  - Truncates output files to 0 bytes before writing in single-stream mode.
- **Durable SQLite Queue Persistence**:
  - Persists queue records (ID, URL, path, total size, status, chunk size, connections, timestamps) in SQLite with WAL journal mode.
  - Automatically recovers interrupted active downloads (`Downloading` &rarr; `Paused`) on startup.
- **Global Concurrency Limiting**:
  - Enforces a configurable global active download limit (default 3 concurrent downloads) via an asynchronous semaphore and scheduler loop.
  - Excess downloads remain `Pending` until slots open up.
- **Runtime-Configurable Token-Bucket Speed Limiter**:
  - Global `SpeedLimiter` allows dynamically setting or removing bandwidth limits at runtime.
  - Thread-safe token bucket refills tokens smoothly and suspends chunk streams with sub-second precision.
- **Throttled Event Broadcasting**:
  - Downloads emit structured events (`Started`, `Progress`, `SpeedEta`, `StateChanged`, `Finished`, `Error`) over `tokio::sync::broadcast`.
  - Rate-limited to **at most 10 progress events per second per download** (`MIN_PROGRESS_EMIT_INTERVAL = 100ms`) with terminal flush guarantees.
  - Pure library: strictly **zero** console printing or UI dependencies.
- **State Serialization & Concurrency Safety**:
  - Completed-chunk set updates and state writes are serialized behind a mutex.
  - State files use unique per-process temporary files with `sync_all().await` before atomic rename.
  - Disk data flushed via `sync_data()` at most every 2 seconds or 8 chunks, and always before final rename.
  - Advisory file locks (`flock(LOCK_EX | LOCK_NB)`) prevent concurrent instances from modifying the same download.
- **Restricted Retries**:
  - Retries transient errors up to 5 times with exponential backoff (timeouts, connection drops mid-stream, 5xx, 429 honoring `Retry-After`).
  - Client errors (4xx) and Content-Range mismatches fail immediately without retrying.

### 2. User-Friendly CLI (`idm-cli`)
- Progress bars powered by `indicatif` showing bytes completed, total size, percentage, transfer speed, and ETA.
- Interactive overwrite confirmation if the destination file exists (`--force` / `-f` to bypass).
- **Double Ctrl-C handling**:
  - First `Ctrl-C`: gracefully halts workers, persists state, and notifies the user how to resume.
  - Second `Ctrl-C`: immediately terminates the process with exit code 130.

---

## CLI Usage

### Basic Download
```bash
cargo run -p idm-cli -- https://example.com/largefile.zip
```

### Custom Options
```bash
# 16 parallel connections, 8 MiB chunk size, specific output filename, force overwrite
cargo run -p idm-cli -- -c 16 -s 8388608 -o my_file.zip -f https://example.com/largefile.zip

# Rate limit to 1 MiB/s (1048576 bytes/s)
cargo run -p idm-cli -- --limit-rate 1048576 https://example.com/largefile.zip
```

### Queue Management Subcommands
```bash
# List all downloads and their status (Pending, Downloading, Paused, Completed, Failed, Cancelled)
cargo run -p idm-cli -- list

# Pause an active download
cargo run -p idm-cli -- pause <DOWNLOAD_ID>

# Resume a paused or failed download
cargo run -p idm-cli -- resume <DOWNLOAD_ID>

# Cancel a download
cargo run -p idm-cli -- cancel <DOWNLOAD_ID>
```

---

## Library Usage (`idm-core`)

Add `idm-core` to your `Cargo.toml`:
```toml
[dependencies]
idm-core = { path = "crates/idm-core" }
tokio = { version = "1.43", features = ["full"] }
```

### Example: Programmatic Download Manager

```rust
use std::path::PathBuf;
use std::sync::Arc;
use idm_core::{DownloadManager, AddDownloadOptions, DownloadEvent, DownloadStatus};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize DownloadManager with a SQLite database path and max concurrency of 2
    let db_path = PathBuf::from("downloads.db");
    let manager = Arc::new(DownloadManager::new(db_path, 2)?);

    // Optional: Set global speed limit (e.g. 500 KB/s)
    manager.set_speed_limit(Some(500 * 1024)).await;

    // Add a download to the queue
    let id = manager.add_download(AddDownloadOptions {
        url: "https://example.com/file.bin".to_string(),
        output_path: Some(PathBuf::from("file.bin")),
        connections: 8,
        chunk_size: 4 * 1024 * 1024,
        force: true,
    }).await?;

    // Subscribe to download events (throttled to <= 10 progress events/sec)
    let mut rx = manager.subscribe(&id);
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match event {
                DownloadEvent::Progress { downloaded, total_size, .. } => {
                    println!("Progress: {} / {} bytes", downloaded, total_size);
                }
                DownloadEvent::StateChanged { status, .. } => {
                    println!("State: {:?}", status);
                }
                DownloadEvent::Finished { .. } => {
                    println!("Download completed successfully!");
                    break;
                }
                DownloadEvent::Error { error, .. } => {
                    eprintln!("Download error: {}", error);
                    break;
                }
                _ => {}
            }
        }
    });

    // Control downloads dynamically
    // manager.pause(&id).await?;
    // manager.resume(&id).await?;
    // manager.cancel(&id).await?;
    // let downloads = manager.list()?;

    Ok(())
}
```

---

## Testing & Quality Assurance

All 30 unit and integration tests run against mock local HTTP servers:

```bash
# Run all workspace tests (14 unit tests + 16 integration tests)
cargo test --workspace

# Run clippy across all workspace crates and targets
cargo clippy --workspace --all-targets --all-features
```

### Test Coverage Highlights
- Range boundaries and chunk calculation edge cases.
- RFC 6266 `Content-Disposition` and `Content-Range` header parsing.
- Server drop mid-chunk and exponential backoff retry recovery.
- Detection and immediate failure on mismatched `Content-Range` responses.
- Rejection of weak ETags (`W/"..."`) in `If-Range`.
- Rejection of resume if neither `ETag` nor `Last-Modified` is available.
- `If-Range` 200 OK single-restart loop prevention.
- POSIX file lock contention between concurrent instances.
- Orphan state cleanup and `--force` file overwrite safeguards.
- Pause/resume state serialization and byte-identical output verification.
- Global concurrency queue scheduling and pending task execution.
- Runtime speed limit adjustments on active downloads.
- SQLite queue state durability and process restart recovery.
