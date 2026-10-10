//! # Segmented Parallel Download Module (`segmented.rs`)
//!
//! Handles parallel segmented downloads with atomic pause/resume state tracking.
//!
//! ## Core Architecture:
//! 1. Preallocates disk file to full size using `file.set_len(total_size)` to reduce fragmentation.
//! 2. Splits the workload into fixed chunks and distributes them across asynchronous workers.
//! 3. Guarantees every worker calls `flush().await` before marking any chunk complete.
//! 4. Throttles `sync_data` calls on the `.part` file to at most once every 2 seconds or 8 chunks.
//! 5. Serializes completed-chunk updates and atomic state writes behind an `Arc<Mutex<DownloadState>>`.
//! 6. Integrates global token bucket speed limiting and throttled progress event emission.
//! 7. Zero printing or progress bars in idm-core.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::header::{CONTENT_RANGE, IF_RANGE, RANGE, RETRY_AFTER};
use reqwest::{StatusCode, Url};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

use crate::chunks::{calculate_chunks, Chunk};
use crate::events::ProgressEmitter;
use crate::limiter::SpeedLimiter;
use crate::probe::verify_content_range;
use crate::state::{get_part_path, get_state_path, DownloadState};

const MAX_CHUNK_RETRIES: usize = 5;
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(500);

/// Tracks and throttles disk synchronization (`sync_data`) calls on the partial download file.
#[derive(Debug)]
struct PartSyncTracker {
    last_sync: Instant,
    chunks_since_sync: usize,
}

impl Default for PartSyncTracker {
    fn default() -> Self {
        Self {
            last_sync: Instant::now(),
            chunks_since_sync: 0,
        }
    }
}

/// Status or error encountered during segmented download execution.
#[derive(Debug)]
pub enum SegmentedResult {
    /// All chunks completed and verified successfully.
    Completed,
    /// Server returned 200 OK or If-Range validator failed: the file changed remotely,
    /// so partial state was discarded and a clean restart from scratch is required.
    FileChangedRestart,
    /// Download was cancelled/paused gracefully (e.g., via cancellation token).
    Interrupted,
    /// Unrecoverable fatal error.
    Fatal(anyhow::Error),
}

/// The decision outcome of an individual chunk download attempt.
#[derive(Debug)]
enum AttemptDecision {
    /// Chunk was downloaded, written to disk, and flushed successfully!
    Success,
    /// Server returned 200 OK: the file changed remotely or range was refused.
    FileChanged200,
    /// Download cancelled mid-operation.
    Cancelled,
    /// A retryable condition occurred (timeout, connection drop, 5xx, 429).
    Retryable {
        #[allow(dead_code)]
        error: anyhow::Error,
        retry_after: Option<Duration>,
    },
    /// A non-retryable failure (e.g. 4xx client error, Content-Range mismatch, disk I/O failure).
    Fatal(anyhow::Error),
}

/// Parses the `Retry-After` HTTP header if present (RFC 9110 §10.2.3).
pub fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let header_val = headers.get(RETRY_AFTER)?;
    let val_str = header_val.to_str().ok()?.trim();
    if let Ok(seconds) = val_str.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    None
}

/// Checks whether a reqwest error represents a transient network/connection failure, timeout, or dropped stream.
fn is_transient_network_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() || err.is_decode()
}

/// Executes a single attempt to download a specific chunk range.
#[allow(clippy::too_many_arguments)]
async fn download_chunk_attempt(
    client: &reqwest::Client,
    url: &Url,
    part_path: &Path,
    chunk: &Chunk,
    if_range_validator: Option<&str>,
    downloaded_counter: &Arc<AtomicU64>,
    cancel_token: &Arc<AtomicBool>,
    speed_limiter: Option<&Arc<SpeedLimiter>>,
    progress_emitter: Option<&Arc<ProgressEmitter>>,
) -> AttemptDecision {
    if cancel_token.load(Ordering::Relaxed) {
        return AttemptDecision::Cancelled;
    }

    // 1. Build and send GET with Range: bytes=start-end and If-Range
    let range_val = format!("bytes={}-{}", chunk.start, chunk.end);
    let mut req = client.get(url.clone()).header(RANGE, range_val);

    if let Some(validator) = if_range_validator {
        req = req.header(IF_RANGE, validator);
    }

    let send_result = req.send().await;

    let response = match send_result {
        Ok(resp) => resp,
        Err(err) => {
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

    // 2. Classify response status code
    if status == StatusCode::OK {
        // Server returned 200 instead of 206: If-Range validator failed (file changed on server)
        return AttemptDecision::FileChanged200;
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after = parse_retry_after(&headers);
        return AttemptDecision::Retryable {
            error: anyhow::anyhow!("HTTP 429 Too Many Requests"),
            retry_after,
        };
    } else if status.is_server_error() {
        return AttemptDecision::Retryable {
            error: anyhow::anyhow!("Server error HTTP {}", status),
            retry_after: None,
        };
    } else if status.is_client_error() {
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

    // 4. Open independent file handle to the .part file and seek to chunk offset
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(part_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            return AttemptDecision::Fatal(anyhow::anyhow!(
                "Failed to open part file '{:?}' for chunk {}: {}",
                part_path,
                chunk.index,
                e
            ));
        }
    };

    if let Err(e) = file.seek(SeekFrom::Start(chunk.start)).await {
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Failed to seek to offset {} in part file: {}",
            chunk.start,
            e
        ));
    }

    // 5. Stream chunk body to disk
    let mut mut_resp = response;
    let mut bytes_written_this_attempt: u64 = 0;
    let expected_len = chunk.len();

    loop {
        if cancel_token.load(Ordering::Relaxed) {
            if bytes_written_this_attempt > 0 {
                downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
                if let Some(emitter) = progress_emitter {
                    emitter.emit_progress(false);
                }
            }
            return AttemptDecision::Cancelled;
        }

        match mut_resp.chunk().await {
            Ok(Some(bytes)) => {
                // Apply global speed limiter
                if let Some(limiter) = speed_limiter {
                    limiter.acquire(bytes.len()).await;
                }

                if let Err(e) = file.write_all(&bytes).await {
                    if bytes_written_this_attempt > 0 {
                        downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
                        if let Some(emitter) = progress_emitter {
                            emitter.emit_progress(false);
                        }
                    }
                    return AttemptDecision::Fatal(anyhow::anyhow!(
                        "Failed writing to disk for chunk {}: {}",
                        chunk.index,
                        e
                    ));
                }

                let len = bytes.len() as u64;
                bytes_written_this_attempt += len;

                downloaded_counter.fetch_add(len, Ordering::Relaxed);
                if let Some(emitter) = progress_emitter {
                    emitter.emit_progress(false);
                }
            }
            Ok(None) => break,
            Err(err) => {
                if bytes_written_this_attempt > 0 {
                    downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
                    if let Some(emitter) = progress_emitter {
                        emitter.emit_progress(false);
                    }
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
            if let Some(emitter) = progress_emitter {
                emitter.emit_progress(false);
            }
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

    // 7. Flush buffers to disk before chunk is considered complete
    if let Err(e) = file.flush().await {
        if bytes_written_this_attempt > 0 {
            downloaded_counter.fetch_sub(bytes_written_this_attempt, Ordering::Relaxed);
            if let Some(emitter) = progress_emitter {
                emitter.emit_progress(false);
            }
        }
        return AttemptDecision::Fatal(anyhow::anyhow!(
            "Failed to flush chunk {} to disk: {}",
            chunk.index,
            e
        ));
    }

    AttemptDecision::Success
}

/// Downloads a chunk with retries and exponential backoff.
#[allow(clippy::too_many_arguments)]
async fn download_chunk_with_retries(
    client: &reqwest::Client,
    url: &Url,
    part_path: &Path,
    chunk: &Chunk,
    if_range_validator: Option<&str>,
    downloaded_counter: &Arc<AtomicU64>,
    cancel_token: &Arc<AtomicBool>,
    speed_limiter: Option<&Arc<SpeedLimiter>>,
    progress_emitter: Option<&Arc<ProgressEmitter>>,
) -> Result<(), AttemptDecision> {
    let mut last_retry_after: Option<Duration> = None;

    for attempt in 0..=MAX_CHUNK_RETRIES {
        if cancel_token.load(Ordering::Relaxed) {
            return Err(AttemptDecision::Cancelled);
        }

        if attempt > 0 {
            let backoff = match last_retry_after.take() {
                Some(duration) => duration,
                None => {
                    let backoff_ms = RETRY_BASE_BACKOFF.as_millis() as u64 * (1 << (attempt - 1));
                    Duration::from_millis(backoff_ms)
                }
            };
            tokio::time::sleep(backoff).await;
        }

        match download_chunk_attempt(
            client,
            url,
            part_path,
            chunk,
            if_range_validator,
            downloaded_counter,
            cancel_token,
            speed_limiter,
            progress_emitter,
        )
        .await
        {
            AttemptDecision::Success => return Ok(()),
            AttemptDecision::FileChanged200 => return Err(AttemptDecision::FileChanged200),
            AttemptDecision::Cancelled => return Err(AttemptDecision::Cancelled),
            AttemptDecision::Fatal(err) => return Err(AttemptDecision::Fatal(err)),
            AttemptDecision::Retryable { error: _, retry_after } => {
                last_retry_after = retry_after;
            }
        }
    }

    Err(AttemptDecision::Fatal(anyhow::anyhow!(
        "Chunk {} exhausted retries",
        chunk.index
    )))
}

/// Performs a parallel segmented download with atomic state tracking and pause/resume.
#[allow(clippy::too_many_arguments)]
pub async fn download_segmented(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    total_size: u64,
    chunk_size: u64,
    connections: usize,
    state: DownloadState,
    cancel_token: Arc<AtomicBool>,
    speed_limiter: Option<&Arc<SpeedLimiter>>,
    progress_emitter: Option<&Arc<ProgressEmitter>>,
    downloaded_counter: Option<&Arc<AtomicU64>>,
) -> Result<SegmentedResult> {
    let part_path = get_part_path(dest_path);
    let state_path = get_state_path(dest_path);

    // 1. Preallocate or verify .part file
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&part_path)
            .await
            .with_context(|| format!("Failed to create part file '{:?}'", part_path))?;

        let current_len = file.metadata().await?.len();
        if current_len != total_size {
            file.set_len(total_size)
                .await
                .with_context(|| format!("Failed to preallocate part file to {} bytes", total_size))?;
        }
    }

    // 2. Partition into chunks and filter out already completed chunks
    let all_chunks = calculate_chunks(total_size, chunk_size);
    let total_chunks_count = all_chunks.len();

    // Calculate completed bytes from previously saved state
    let mut initial_downloaded_bytes: u64 = 0;
    for chunk in &all_chunks {
        if state.completed_chunks.contains(&chunk.index) {
            initial_downloaded_bytes += chunk.len();
        }
    }

    let pending_chunks: Vec<Chunk> = all_chunks
        .into_iter()
        .filter(|c| !state.completed_chunks.contains(&c.index))
        .collect();

    let pending_chunks_count = pending_chunks.len();
    let effective_connections = std::cmp::min(connections, pending_chunks_count.max(1));

    // 3. Shared synchronization state
    let downloaded_bytes = if let Some(counter) = downloaded_counter {
        Arc::clone(counter)
    } else {
        Arc::new(AtomicU64::new(initial_downloaded_bytes))
    };

    let if_range_validator = state.if_range_validator();
    let state_arc = Arc::new(Mutex::new(state));
    let sync_tracker = Arc::new(Mutex::new(PartSyncTracker::default()));

    let work_queue = Arc::new(Mutex::new(std::collections::VecDeque::from(pending_chunks)));

    // 4. Spawn concurrent worker tasks
    let mut handles = Vec::new();

    for _ in 0..effective_connections {
        let work_queue = Arc::clone(&work_queue);
        let client = client.clone();
        let url = url.clone();
        let part_path = part_path.clone();
        let state_path = state_path.clone();
        let downloaded = Arc::clone(&downloaded_bytes);
        let cancel = Arc::clone(&cancel_token);
        let if_range = if_range_validator.clone();
        let state_lock = Arc::clone(&state_arc);
        let sync_tracker = Arc::clone(&sync_tracker);
        let speed_limiter = speed_limiter.cloned();
        let progress_emitter = progress_emitter.cloned();

        let handle = tokio::spawn(async move {
            loop {
                if cancel.load(Ordering::Relaxed) {
                    return Err(SegmentedResult::Interrupted);
                }

                let chunk = {
                    let mut q = work_queue.lock().await;
                    q.pop_front()
                };

                let Some(chunk) = chunk else {
                    break;
                };

                let chunk_idx = chunk.index;

                match download_chunk_with_retries(
                    &client,
                    &url,
                    &part_path,
                    &chunk,
                    if_range.as_deref(),
                    &downloaded,
                    &cancel,
                    speed_limiter.as_ref(),
                    progress_emitter.as_ref(),
                )
                .await
                {
                    Ok(()) => {
                        // Throttled sync_data on .part file: at most every 2 seconds or 8 chunks
                        {
                            let mut tracker = sync_tracker.lock().await;
                            tracker.chunks_since_sync += 1;
                            if tracker.chunks_since_sync >= 8 || tracker.last_sync.elapsed() >= Duration::from_secs(2) {
                                if let Ok(part_file) = OpenOptions::new().write(true).open(&part_path).await {
                                    let _ = part_file.sync_data().await;
                                }
                                tracker.chunks_since_sync = 0;
                                tracker.last_sync = Instant::now();
                            }
                        }

                        // Atomic state update serialized behind state_lock Mutex
                        {
                            let mut s = state_lock.lock().await;
                            s.completed_chunks.insert(chunk_idx);
                            if let Err(e) = s.save_atomic(&state_path).await {
                                return Err(SegmentedResult::Fatal(anyhow::anyhow!(
                                    "Failed to atomically persist state after chunk {}: {}",
                                    chunk_idx,
                                    e
                                )));
                            }
                        }
                    }
                    Err(AttemptDecision::FileChanged200) => {
                        cancel.store(true, Ordering::Relaxed);
                        return Err(SegmentedResult::FileChangedRestart);
                    }
                    Err(AttemptDecision::Cancelled) => {
                        return Err(SegmentedResult::Interrupted);
                    }
                    Err(AttemptDecision::Fatal(e)) => {
                        cancel.store(true, Ordering::Relaxed);
                        return Err(SegmentedResult::Fatal(e));
                    }
                    Err(AttemptDecision::Success) | Err(AttemptDecision::Retryable { .. }) => unreachable!(),
                }
            }
            Ok(())
        });

        handles.push(handle);
    }

    // 5. Await all workers
    let mut fatal_error = None;
    let mut file_changed = false;
    let mut was_cancelled = false;

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(SegmentedResult::FileChangedRestart)) => {
                file_changed = true;
            }
            Ok(Err(SegmentedResult::Interrupted)) => {
                was_cancelled = true;
            }
            Ok(Err(SegmentedResult::Fatal(e))) => {
                if fatal_error.is_none() {
                    fatal_error = Some(e);
                }
            }
            Ok(Err(SegmentedResult::Completed)) => {}
            Err(join_err) => {
                if fatal_error.is_none() {
                    fatal_error = Some(anyhow::anyhow!("Worker panicked: {}", join_err));
                }
            }
        }
    }

    if file_changed {
        let _ = tokio::fs::remove_file(&part_path).await;
        let _ = tokio::fs::remove_file(&state_path).await;
        return Ok(SegmentedResult::FileChangedRestart);
    }

    if let Some(e) = fatal_error {
        return Ok(SegmentedResult::Fatal(e));
    }

    if was_cancelled || cancel_token.load(Ordering::Relaxed) {
        return Ok(SegmentedResult::Interrupted);
    }

    // 6. Completion verification
    let final_meta = tokio::fs::metadata(&part_path)
        .await
        .with_context(|| format!("Failed to read metadata of part file '{:?}'", part_path))?;

    if final_meta.len() != total_size {
        return Ok(SegmentedResult::Fatal(anyhow::anyhow!(
            "File size mismatch upon completion: expected {} bytes, got {}",
            total_size,
            final_meta.len()
        )));
    }

    {
        let s = state_arc.lock().await;
        if s.completed_chunks.len() != total_chunks_count {
            return Ok(SegmentedResult::Fatal(anyhow::anyhow!(
                "Not all chunks completed: expected {}, got {}",
                total_chunks_count,
                s.completed_chunks.len()
            )));
        }
    }

    // Explicitly call sync_data on the .part file before final rename
    {
        let part_file = OpenOptions::new()
            .write(true)
            .open(&part_path)
            .await
            .with_context(|| format!("Failed to open part file '{:?}' for final sync", part_path))?;
        part_file
            .sync_data()
            .await
            .with_context(|| format!("Failed to sync_data on part file '{:?}' before rename", part_path))?;
    }

    // Rename .part file to final destination
    tokio::fs::rename(&part_path, dest_path)
        .await
        .with_context(|| format!("Failed to rename '{:?}' to final destination '{:?}'", part_path, dest_path))?;

    // Delete state file upon successful completion
    let _ = tokio::fs::remove_file(&state_path).await;

    if let Some(emitter) = progress_emitter {
        emitter.emit_progress(true);
    }

    Ok(SegmentedResult::Completed)
}
