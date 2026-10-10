//! # Single-Stream Fallback Module (`single.rs`)
//!
//! Provides the Stage 1 fallback downloader: sequential, single-connection streaming.
//!
//! ## When Used:
//! - When the server does not support HTTP Range requests (`Accept-Ranges: bytes`).
//! - When the total file size is unknown (dynamic streaming or chunked transfer).
//! - When a server unexpectedly returns `200 OK` instead of `206 Partial Content`
//!   during a segmented parallel download.
//!
//! ## Important Safety Guarantee:
//! If falling back from an aborted segmented download that previously preallocated
//! disk space with `set_len(total_size)`, this module explicitly **truncates** the file
//! back to 0 bytes before streaming data to avoid corruption or leftover padding bytes.

use std::path::Path;

use anyhow::{Context, Result};
use indicatif::HumanBytes;
use reqwest::Url;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::progress::setup_progress_bar;

/// Downloads the remote resource using a single sequential stream.
///
/// # Arguments
/// * `client` - The HTTP client.
/// * `url` - The target URL to download.
/// * `dest_path` - The local file destination path.
/// * `total_size` - Optional expected total size in bytes (from probe or HEAD).
pub async fn download_single_stream(
    client: &reqwest::Client,
    url: &Url,
    dest_path: &Path,
    total_size: Option<u64>,
) -> Result<()> {
    println!("Starting single-stream sequential download ...");

    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Failed to connect to '{}'", url))?;

    let response = response
        .error_for_status()
        .context("Server returned an HTTP error status")?;

    let effective_total = response.content_length().or(total_size);

    // TRUNCATION GUARANTEE (Requirement 2):
    // If segmented mode previously preallocated the file with `file.set_len(total_size)`,
    // opening with `.truncate(true)` and calling `.set_len(0)` explicitly truncates the file
    // to 0 bytes before any streaming writes take place.
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest_path)
        .await
        .with_context(|| format!("Failed to create/truncate destination file '{:?}'", dest_path))?;

    file.set_len(0)
        .await
        .with_context(|| format!("Failed to set length of '{:?}' to 0", dest_path))?;

    let pb = setup_progress_bar(effective_total)?;
    let mut mut_response = response;
    let mut downloaded_bytes: u64 = 0;

    while let Some(chunk) = mut_response
        .chunk()
        .await
        .context("Error receiving data stream from server")?
    {
        file.write_all(&chunk)
            .await
            .with_context(|| format!("Failed to write chunk to '{:?}'", dest_path))?;

        let len = chunk.len() as u64;
        downloaded_bytes += len;
        pb.inc(len);
    }

    // Flush OS write buffers to disk
    file.flush()
        .await
        .with_context(|| format!("Failed to flush file '{:?}'", dest_path))?;

    pb.finish_and_clear();
    println!(
        "\n Single-stream download complete: {:?} ({})",
        dest_path,
        HumanBytes(downloaded_bytes)
    );

    Ok(())
}
