//! # Single-Stream Fallback Module (`single.rs`)
//!
//! Provides the fallback downloader: sequential, single-connection streaming.
//!
//! ## When Used:
//! - When the server does not support HTTP Range requests (`Accept-Ranges: bytes`).
//! - When the total file size is unknown (dynamic streaming or chunked transfer).
//! - When a server unexpectedly returns `200 OK` instead of `206 Partial Content`
//!   during a segmented parallel download.
//!
//! ## Safety Guarantees:
//! 1. Downloads into `<filename>.part` and renames to `<filename>` only after full completion.
//! 2. Explicitly truncates the `.part` file to 0 bytes before streaming data.
//! 3. Honors global speed limiting and throttled progress reporting.
//! 4. No direct stdout/stderr printing or progress bars in idm-core.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::Url;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::events::ProgressEmitter;
use crate::limiter::SpeedLimiter;
use crate::state::get_part_path;

/// Downloads the remote resource using a single sequential stream.
pub async fn download_single_stream(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    _total_size: Option<u64>,
    speed_limiter: Option<&Arc<SpeedLimiter>>,
    progress_emitter: Option<&Arc<ProgressEmitter>>,
    cancel_token: Option<&Arc<AtomicBool>>,
) -> Result<bool> {
    let part_path = get_part_path(dest_path);

    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Failed to connect to '{}'", url))?;

    let response = response
        .error_for_status()
        .context("Server returned an HTTP error status")?;

    // Truncate the .part file to 0 bytes before writing
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&part_path)
        .await
        .with_context(|| format!("Failed to create/truncate part file '{:?}'", part_path))?;

    file.set_len(0)
        .await
        .with_context(|| format!("Failed to set length of '{:?}' to 0", part_path))?;

    let mut mut_response = response;

    while let Some(chunk) = mut_response
        .chunk()
        .await
        .context("Error receiving data stream from server")?
    {
        if let Some(cancel) = cancel_token
            && cancel.load(Ordering::Relaxed)
        {
            file.flush().await.ok();
            return Ok(false); // Cancelled/paused
        }

        // Apply global speed limiter token bucket
        if let Some(limiter) = speed_limiter {
            limiter.acquire(chunk.len()).await;
        }

        file.write_all(&chunk)
            .await
            .with_context(|| format!("Failed to write chunk to '{:?}'", part_path))?;

        if let Some(emitter) = progress_emitter {
            emitter.emit_progress(false);
        }
    }

    // Flush OS write buffers to disk
    file.flush()
        .await
        .with_context(|| format!("Failed to flush file '{:?}'", part_path))?;

    file.sync_data()
        .await
        .with_context(|| format!("Failed to sync_data on '{:?}'", part_path))?;

    // Rename .part file to final destination upon successful completion
    tokio::fs::rename(&part_path, dest_path)
        .await
        .with_context(|| format!("Failed to rename '{:?}' to final destination '{:?}'", part_path, dest_path))?;

    if let Some(emitter) = progress_emitter {
        emitter.emit_progress(true);
    }

    Ok(true) // Successfully completed
}
