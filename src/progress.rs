//! # Progress Bar Module (`progress.rs`)
//!
//! Provides helper functions to configure and instantiate visual progress bars
//! using the `indicatif` crate.
//!
//! - If the total file size is known: shows a bounded bar with byte counters, transfer speed, and ETA.
//! - If the file size is unknown (chunked transfer): shows an animated spinner with byte count and transfer rate.

use std::time::Duration;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

/// Creates and configures an `indicatif::ProgressBar`.
///
/// # Arguments
/// * `total_size` - Optional total number of bytes. If `Some`, creates a bounded
///   progress bar. If `None`, creates an unbounded spinner.
///
/// # Progress Template Format:
/// - `{spinner:.green}`: Animated green spinner
/// - `[{elapsed_precise}]`: Elapsed time in HH:MM:SS format
/// - `[{bar:40.cyan/blue}]`: Progress bar 40 characters wide with cyan filled and blue unfilled
/// - `{bytes}/{total_bytes}`: Transferred bytes / Total bytes (e.g., "12.5 MiB/100 MiB")
/// - `({bytes_per_sec}, {eta})`: Current speed and estimated time to arrival
pub fn setup_progress_bar(total_size: Option<u64>) -> Result<ProgressBar> {
    match total_size {
        Some(total) => {
            let pb = ProgressBar::new(total);
            let style = ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .context("Failed to compile progress bar template")?
                .progress_chars("#>-");

            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));
            Ok(pb)
        }
        None => {
            let pb = ProgressBar::new_spinner();
            let style = ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})")
                .context("Failed to compile spinner template")?;

            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(100));
            Ok(pb)
        }
    }
}
