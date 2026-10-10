//! # Segmented Parallel Downloader Module (`segmented.rs`)
//!
//! Implements multi-connection, chunk-based parallel downloading.
//!
//! ## Key Responsibilities:
//! 1. Preallocates disk file using `file.set_len(total_size)`.
//! 2. Distributes chunks across an asynchronous queue among N worker tasks.
//! 3. Sends `GET` requests with inclusive `Range: bytes=start-end`.
//! 4. Verifies `206 Partial Content` status and matches the `Content-Range` header.
//! 5. (Requirement 1) Guarantees every worker calls `flush().await` before a chunk
//!    is marked complete.
//! 6. (Requirement 3) Restricts retries strictly to:
//!    - Timeouts
//!    - Connection errors / unexpected drops mid-chunk
//!    - HTTP 5xx Server Errors
//!    - HTTP 429 Too Many Requests (honoring `Retry-After` if present)
//!    - Fails immediately on any other 4xx Client Error (400, 401, 403, 404, etc.)
//!    - Fails immediately on Content-Range header mismatch.

use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use indicatif::{HumanBytes, ProgressBar};
use reqwest::header::{CONTENT_RANGE, RANGE, RETRY_AFTER};
use reqwest::{StatusCode, Url};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::chunks::{calculate_chunks, Chunk};
use crate::probe::verify_content_range;
use crate::progress::setup_progress_bar;

/// Default number of concurrent connections (workers).
pub const DEFAULT_CONNECTIONS: usize = 8;

/// Default chunk size: 4 MiB (4 * 1024 * 1024 bytes).
pub const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Maximum number of retries per chunk for retryable errors.
pub const MAX_CHUNK_RETRIES: usize = 5;

/// Base exponential backoff delay (100ms, 200ms, 400ms, 800ms, 1600ms...).
pub const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Internal chunk processing error.
#[derive(Debug)]
pub enum ChunkError {
    /// Server returned HTTP 200 OK instead of 206 Partial Content.
    /// Signals immediate fallback to single-stream mode.
    ServerReturned200,
    /// Unrecoverable error (e.g. 4xx other than 429, Content-Range mismatch, disk error,
    /// or retry exhaustion).
    Fatal(anyhow::Error),
}

/// The decision outcome of evaluating an individual chunk download attempt.
#[derive(Debug)]
enum AttemptDecision {
    /// Chunk was downloaded, written to disk, and flushed successfully!
    Success,
    /// Server returned 200 OK; must abort segmented mode immediately.
    ServerReturned200,
    /// A retryable condition occurred (timeout, connection drop, 5xx, 429).
    Retryable {
        error: anyhow::Error,
        retry_after: Option<Duration>,
    },
    /// A non-retryable failure (e.g. 4xx client error, Content-Range mismatch, disk I/O failure).
    Fatal(anyhow::Error),
}

/// Parses the `Retry-After` HTTP header if present (RFC 9110 §10.2.3).
///
/// Supports integer seconds format (e.g. `Retry-After: 5`).
pub fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let header_val = headers.get(RETRY_AFTER)?;
    let val_str = header_val.to_str().ok()?.trim();
    if let Ok(seconds) = val_str.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    None
}

/// Checks whether a reqwest error represents a transient network/connection failure or timeout.
fn is_transient_network_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request() || err.is_body()
}

/// Executes a single attempt to download a specific chunk range.
async fn download_chunk_attempt(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    chunk: &Chunk,
    downloaded_counter: &Arc<AtomicU64>,
    pb: &ProgressBar,
) -> AttemptDecision {
    // 1. Build and send GET with Range: bytes=start-end
    let range_val = format!("bytes={}-{}", chunk.start, chunk.end);
    let send_result = client
        .get(url.clone())
        .header(RANGE, range_val)
        .send()
        .await;

    let response = match send_result {
        Ok(resp) => resp,
        Err(err) => {
            // (Requirement 3): Allow retries for timeouts and connection errors
            if is_transient_network_error(&err) {
                return AttemptDecision::Retryable {
                    error: anyhow::anyhow!("Transient network error sending chunk request: {}", err),
                    retry_after: None,
                };
            }
            return AttemptDecision::Fatal(anyhow::anyhow!("Fatal network error: {}", err));
        }
    };

    let status = response.status();
    let headers = response.headers().clone();

    // 2. Classify response status code (Requirement 3)
    if status == StatusCode::OK {
        // Server returned 200 instead of 206; abort segmented mode to fall back to single stream
        return AttemptDecision::ServerReturned200;
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        // HTTP 429 Too Many Requests -> RETRYABLE, honoring Retry-After if present
        let retry_after = parse_retry_after(&headers);
        return AttemptDecision::Retryable {
            error: anyhow::anyhow!("HTTP 429 Too Many Requests"),
            retry_after,
        };
    } else if status.is_server_error() {
        // HTTP 5xx Server Error (500, 502, 503, 504) -> RETRYABLE
        return AttemptDecision::Retryable {
            error: anyhow::anyhow!("Server error HTTP {}", status),
            retry_after: None,
        };
    } else if status.is_client_error() {
        // Any other HTTP 4xx Client Error (400, 401, 403, 404, 416, etc.)
        // -> (Requirement 3): FAIL IMMEDIATELY without retrying!
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Fatal client error HTTP {} for chunk {}: cannot proceed",
            status,
            chunk.index
        ));
    } else if status != StatusCode::PARTIAL_CONTENT {
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Unexpected HTTP status {} for chunk {}",
            status,
            chunk.index
        ));
    }

    // 3. Verify Content-Range header matches requested chunk bounds
    let content_range = headers.get(CONTENT_RANGE).and_then(|v| v.to_str().ok());
    match content_range {
        Some(cr) => {
            if !verify_content_range(cr, chunk.start, chunk.end) {
                // (Requirement 3): Content-Range mismatch is not retryable -> fail immediately!
                return AttemptDecision::Fatal(anyhow::anyhow!(
                    "Content-Range mismatch for chunk {}: expected bytes {}-{}, got '{}'",
                    chunk.index,
                    chunk.start,
                    chunk.end,
                    cr
                ));
            }
        }
        None => {
            return AttemptDecision::Fatal(anyhow::anyhow!(
                "Missing Content-Range header in 206 Partial Content response for chunk {}",
                chunk.index
            ));
        }
    }

    // 4. Open independent file handle and seek to chunk offset
    let mut file = match OpenOptions::new().write(true).open(dest_path).await {
        Ok(f) => f,
        Err(e) => {
            return AttemptDecision::Fatal(anyhow::anyhow!(
                "Failed to open destination file for chunk {}: {}",
                chunk.index,
                e
            ));
        }
    };

    if let Err(e) = file.seek(SeekFrom::Start(chunk.start)).await {
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Failed to seek to offset {} in file: {}",
            chunk.start,
            e
        ));
    }

    // 5. Stream chunk body to disk
    let mut mut_resp = response;
    let mut bytes_written_this_attempt: u64 = 0;
    let expected_len = chunk.len();

    loop {
        match mut_resp.chunk().await {
            Ok(Some(bytes)) => {
                if let Err(e) = file.write_all(&bytes).await {
                    // Roll back counter
                    if bytes_written_this_attempt > 0 {
                        downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
                        pb.set_position(downloaded_counter.load(Ordering::Relaxed));
                    }
                    return AttemptDecision::Fatal(anyhow::anyhow!(
                        "Failed writing to disk for chunk {}: {}",
                        chunk.index,
                        e
                    ));
                }

                let len = bytes.len() as u64;
                bytes_written_this_attempt += len;

                let new_total = downloaded_counter.fetch_add(len, Ordering::Relaxed) + len;
                pb.set_position(new_total);
            }
            Ok(None) => {
                // End of stream
                break;
            }
            Err(err) => {
                // Connection dropped or timed out mid-chunk
                if bytes_written_this_attempt > 0 {
                    downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
                    pb.set_position(downloaded_counter.load(Ordering::Relaxed));
                }

                if is_transient_network_error(&err) {
                    return AttemptDecision::Retryable {
                        error: anyhow::anyhow!(
                            "Connection dropped mid-stream on chunk {}: {}",
                            chunk.index,
                            err
                        ),
                        retry_after: None,
                    };
                }

                return AttemptDecision::Fatal(anyhow::anyhow!(
                    "Fatal stream error on chunk {}: {}",
                    chunk.index,
                    err
                ));
            }
        }
    }

    // 6. Verify full chunk was received
    if bytes_written_this_attempt != expected_len {
        if bytes_written_this_attempt > 0 {
            downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
            pb.set_position(downloaded_counter.load(Ordering::Relaxed));
        }

        return AttemptDecision::Retryable {
            error: anyhow::anyhow!(
                "Incomplete chunk {}: expected {} bytes, received only {}",
                chunk.index,
                expected_len,
                bytes_written_this_attempt
            ),
            retry_after: None,
        };
    }

    // 7. CRITICAL (Requirement 1):
    // Ensure every worker explicitly calls flush().await before a chunk is considered complete!
    if let Err(e) = file.flush().await {
        if bytes_written_this_attempt > 0 {
            downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
            pb.set_position(downloaded_counter.load(Ordering::Relaxed));
        }
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Failed to flush chunk {} to disk: {}",
            chunk.index,
            e
        ));
    }

    AttemptDecision::Success
}

/// Downloads a chunk with retries restricted per Requirement 3.
async fn download_chunk_with_retries(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    chunk: &Chunk,
    downloaded_counter: &Arc<AtomicU64>,
    pb: &ProgressBar,
) -> Result<(), ChunkError> {
    let mut last_retry_after: Option<Duration> = None;

    for attempt in 0..=MAX_CHUNK_RETRIES {
        if attempt > 0 {
            let backoff = match last_retry_after.take() {
                Some(duration) => duration,
                None => {
                    // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms...
                    let backoff_ms = RETRY_BASE_BACKOFF.as_millis() as u64 * (1 << (attempt - 1));
                    Duration::from_millis(backoff_ms)
                }
            };
            tokio::time::sleep(backoff).await;
        }

        match download_chunk_attempt(client, url, dest_path, chunk, downloaded_counter, pb).await {
            AttemptDecision::Success => {
                return Ok(());
            }
            AttemptDecision::ServerReturned200 => {
                return Err(ChunkError::ServerReturned200);
            }
            AttemptDecision::Fatal(err) => {
                // Non-retryable error (e.g. 4xx other than 429, Content-Range mismatch)
                // Fails immediately!
                return Err(ChunkError::Fatal(err));
            }
            AttemptDecision::Retryable { error, retry_after } => {
                if attempt == MAX_CHUNK_RETRIES {
                    return Err(ChunkError::Fatal(error.context(format!(
                        "Chunk {} failed after {} retries",
                        chunk.index, MAX_CHUNK_RETRIES
                    ))));
                }
                last_retry_after = retry_after;
            }
        }
    }

    Err(ChunkError::Fatal(anyhow::anyhow!(
        "Chunk {} exhausted all retry attempts",
        chunk.index
    )))
}

/// Coordinates the parallel segmented download workflow.
pub async fn download_segmented(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    total_size: u64,
    chunk_size: u64,
    connections: usize,
) -> Result<(), ChunkError> {
    // 1. Preallocate output file to the full size
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)
            .await
            .map_err(|e| ChunkError::Fatal(anyhow::anyhow!("Failed to create destination file: {}", e)))?;

        file.set_len(total_size)
            .await
            .map_err(|e| ChunkError::Fatal(anyhow::anyhow!("Failed to preallocate file to {} bytes: {}", total_size, e)))?;
    }

    // 2. Partition into chunks
    let chunks = calculate_chunks(total_size, chunk_size);
    let num_chunks = chunks.len();
    let effective_connections = std::cmp::min(connections, num_chunks.max(1));

    println!(
        "Segmented Download: {} chunks of {} each using {} parallel connections.",
        num_chunks,
        HumanBytes(chunk_size),
        effective_connections
    );

    // 3. Shared state
    let work_queue = Arc::new(Mutex::new(VecDeque::from(chunks)));
    let abort_flag = Arc::new(AtomicBool::new(false));
    let downloaded_counter = Arc::new(AtomicU64::new(0));

    let pb = setup_progress_bar(Some(total_size))
        .map_err(|e| ChunkError::Fatal(anyhow::anyhow!("Progress bar setup error: {}", e)))?;

    // 4. Spawn concurrent worker tasks
    let mut handles = Vec::with_capacity(effective_connections);

    for _ in 0..effective_connections {
        let queue = Arc::clone(&work_queue);
        let abort = Arc::clone(&abort_flag);
        let counter = Arc::clone(&downloaded_counter);
        let pb = pb.clone();
        let client = client.clone();
        let url = url.clone();
        let dest_path = dest_path.to_path_buf();

        let handle = tokio::spawn(async move {
            loop {
                if abort.load(Ordering::Relaxed) {
                    break;
                }

                let chunk = {
                    let mut q = queue.lock().await;
                    q.pop_front()
                };

                let Some(chunk) = chunk else {
                    break;
                };

                match download_chunk_with_retries(
                    &client,
                    &url,
                    &dest_path,
                    &chunk,
                    &counter,
                    &pb,
                )
                .await
                {
                    Ok(()) => {}
                    Err(ChunkError::ServerReturned200) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(ChunkError::ServerReturned200);
                    }
                    Err(ChunkError::Fatal(e)) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(ChunkError::Fatal(e));
                    }
                }
            }
            Ok(())
        });

        handles.push(handle);
    }

    // 5. Await all worker tasks
    let mut fatal_error = None;
    let mut fallback_to_single = false;

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(ChunkError::ServerReturned200)) => {
                fallback_to_single = true;
            }
            Ok(Err(ChunkError::Fatal(e))) => {
                if fatal_error.is_none() {
                    fatal_error = Some(e);
                }
            }
            Err(join_err) => {
                if fatal_error.is_none() {
                    fatal_error = Some(anyhow::anyhow!("Worker panicked: {}", join_err));
                }
            }
        }
    }

    pb.finish_and_clear();

    if fallback_to_single {
        return Err(ChunkError::ServerReturned200);
    }

    if let Some(e) = fatal_error {
        return Err(ChunkError::Fatal(e));
    }

    Ok(())
}
