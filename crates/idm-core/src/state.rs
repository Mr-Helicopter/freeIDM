//! # State Module (`state.rs`)
//!
//! Manages persistent download state, atomic disk persistence, file locking,
//! and validation for resuming downloads.

use std::collections::BTreeSet;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::probe::ProbeInfo;

/// Computes the path to the temporary partial download file (`<filename>.part`).
pub fn get_part_path(dest_path: &Path) -> PathBuf {
    let mut part = dest_path.as_os_str().to_os_string();
    part.push(".part");
    PathBuf::from(part)
}

/// Computes the path to the metadata state file (`<filename>.part.state`).
pub fn get_state_path(dest_path: &Path) -> PathBuf {
    let mut state = dest_path.as_os_str().to_os_string();
    state.push(".part.state");
    PathBuf::from(state)
}

/// Computes the path to the concurrency lock file (`<filename>.part.lock`).
pub fn get_lock_path(dest_path: &Path) -> PathBuf {
    let mut lock = dest_path.as_os_str().to_os_string();
    lock.push(".part.lock");
    PathBuf::from(lock)
}

// ============================================================================
// File Lock for Concurrent Instances
// ============================================================================

/// An advisory file lock using OS `flock` to prevent multiple IDM instances
/// from concurrently writing to the same destination file.
pub struct FileLock {
    file: std::fs::File,
    path: PathBuf,
}

impl FileLock {
    /// Attempts to acquire an exclusive, non-blocking lock on `lock_path`.
    ///
    /// If another process holds the lock, immediately fails with a descriptive error.
    pub fn acquire(lock_path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .with_context(|| format!("Failed to open lock file '{:?}'", lock_path))?;

        let fd = file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EWOULDBLOCK) || err.raw_os_error() == Some(libc::EAGAIN) {
                bail!(
                    "Another instance of IDM is already downloading to this destination (lock file '{}' is busy).",
                    lock_path.display()
                );
            }
            return Err(err).context("Failed to acquire file lock");
        }

        Ok(Self {
            file,
            path: lock_path.to_path_buf(),
        })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let fd = self.file.as_raw_fd();
        unsafe {
            libc::flock(fd, libc::LOCK_UN);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

// ============================================================================
// Download State & Persistence
// ============================================================================

/// Metadata stored in `<filename>.part.state` to enable pausing and resuming.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadState {
    /// The original URL initially requested.
    pub original_url: String,
    /// The final URL reached after following any HTTP redirects.
    pub final_url: String,
    /// Total expected file size in bytes.
    pub total_size: u64,
    /// HTTP ETag entity tag, if provided by the server.
    pub etag: Option<String>,
    /// HTTP Last-Modified header, if provided by the server.
    pub last_modified: Option<String>,
    /// Fixed chunk size in bytes (e.g. 4 MiB).
    pub chunk_size: u64,
    /// Set of chunk indices that have been fully written and flushed to disk.
    pub completed_chunks: BTreeSet<usize>,
}

impl DownloadState {
    /// Creates a new state instance for an initial download.
    pub fn new(
        original_url: String,
        final_url: String,
        total_size: u64,
        etag: Option<String>,
        last_modified: Option<String>,
        chunk_size: u64,
    ) -> Self {
        Self {
            original_url,
            final_url,
            total_size,
            etag,
            last_modified,
            chunk_size,
            completed_chunks: BTreeSet::new(),
        }
    }

    /// Loads and parses the state file from disk, if it exists.
    pub async fn load(state_path: &Path) -> Result<Option<Self>> {
        if !state_path.exists() {
            return Ok(None);
        }

        let data = match tokio::fs::read(state_path).await {
            Ok(d) => d,
            Err(_) => {
                return Ok(None);
            }
        };

        match serde_json::from_slice::<Self>(&data) {
            Ok(state) => Ok(Some(state)),
            Err(_) => Ok(None),
        }
    }

    /// Atomically persists the state to disk.
    ///
    /// ## Guarantees:
    /// 1. Uses a globally unique temporary filename (`<state>.tmp.<pid>.<counter>.<nanos>`).
    /// 2. Calls `sync_all().await` on the temporary file before renaming it over the old one.
    pub async fn save_atomic(&self, state_path: &Path) -> Result<()> {
        let parent = state_path.parent().unwrap_or_else(|| Path::new("."));

        static TEMP_STATE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = TEMP_STATE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let tmp_file_name = format!(
            "{}.tmp.{}.{}.{}",
            state_path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id(),
            counter,
            nanos
        );
        let tmp_path = parent.join(tmp_file_name);

        let json = serde_json::to_vec_pretty(self)
            .context("Failed to serialize download state to JSON")?;

        // Write to temp file and sync_all before renaming
        {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .with_context(|| format!("Failed to create temporary state file '{:?}'", tmp_path))?;
            file.write_all(&json)
                .await
                .with_context(|| format!("Failed to write to temporary state file '{:?}'", tmp_path))?;
            file.flush()
                .await
                .with_context(|| format!("Failed to flush temporary state file '{:?}'", tmp_path))?;
            file.sync_all()
                .await
                .with_context(|| format!("Failed to sync_all temporary state file '{:?}'", tmp_path))?;
        }

        tokio::fs::rename(&tmp_path, state_path)
            .await
            .with_context(|| format!("Failed to atomically rename '{:?}' to '{:?}'", tmp_path, state_path))?;

        Ok(())
    }

    /// Checks whether the saved state is compatible with newly probed server details.
    ///
    /// If the server provides **neither ETag nor Last-Modified**, we can never safely verify
    /// file integrity across sessions, so we MUST NOT resume!
    pub fn is_resume_compatible(&self, probe: &ProbeInfo, chunk_size: u64) -> bool {
        if (probe.etag.is_none() && probe.last_modified.is_none())
            || (self.etag.is_none() && self.last_modified.is_none())
        {
            return false;
        }

        let size_matches = probe.total_size == Some(self.total_size);
        let etag_matches = self.etag == probe.etag;
        let last_modified_matches = self.last_modified == probe.last_modified;
        let chunk_size_matches = self.chunk_size == chunk_size;

        size_matches && etag_matches && last_modified_matches && chunk_size_matches
    }

    /// Returns the validator string for the HTTP `If-Range` header.
    ///
    /// RFC 9110 §13.1.5 specifies that weak entity-tags MUST NOT be used in If-Range.
    /// If the ETag begins with `W/` or `w/`, it is rejected, and we fall back to `Last-Modified`.
    pub fn if_range_validator(&self) -> Option<String> {
        if let Some(ref tag) = self.etag {
            let trimmed = tag.trim();
            if !trimmed.starts_with("W/") && !trimmed.starts_with("w/") {
                return Some(trimmed.to_string());
            }
        }
        self.last_modified.clone()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    pub fn test_weak_etag_rejected_in_if_range() {
        let state_weak = DownloadState::new(
            "http://example.com/file".to_string(),
            "http://example.com/file".to_string(),
            100,
            Some("W/\"weak-etag-123\"".to_string()),
            Some("Sun, 06 Nov 1994 08:49:37 GMT".to_string()),
            64,
        );
        assert_eq!(
            state_weak.if_range_validator(),
            Some("Sun, 06 Nov 1994 08:49:37 GMT".to_string())
        );

        let state_weak_only = DownloadState::new(
            "http://example.com/file".to_string(),
            "http://example.com/file".to_string(),
            100,
            Some("W/\"weak-etag-123\"".to_string()),
            None,
            64,
        );
        assert_eq!(state_weak_only.if_range_validator(), None);

        let state_strong = DownloadState::new(
            "http://example.com/file".to_string(),
            "http://example.com/file".to_string(),
            100,
            Some("\"strong-etag-123\"".to_string()),
            Some("Sun, 06 Nov 1994 08:49:37 GMT".to_string()),
            64,
        );
        assert_eq!(
            state_strong.if_range_validator(),
            Some("\"strong-etag-123\"".to_string())
        );
    }

    #[test]
    pub fn test_no_etag_and_no_last_modified_never_resumes() {
        let state = DownloadState::new(
            "http://example.com/file".to_string(),
            "http://example.com/file".to_string(),
            100,
            None,
            None,
            64,
        );

        let probe = ProbeInfo {
            total_size: Some(100),
            supports_ranges: true,
            etag: None,
            last_modified: None,
            filename: "file".to_string(),
            final_url: reqwest::Url::parse("http://example.com/file").unwrap(),
        };

        assert!(!state.is_resume_compatible(&probe, 64));
    }

    #[test]
    pub fn test_file_lock_prevents_concurrent_access() {
        let temp_dir = std::env::temp_dir();
        let lock_path = temp_dir.join(format!("test_flock_{}.lock", std::process::id()));

        let lock1 = FileLock::acquire(&lock_path).expect("First lock acquire should succeed");

        let lock2_res = FileLock::acquire(&lock_path);
        assert!(lock2_res.is_err(), "Second lock acquire should fail");
        let err_msg = lock2_res.err().unwrap().to_string();
        assert!(err_msg.contains("Another instance of IDM is already downloading"));

        drop(lock1);

        let lock3 = FileLock::acquire(&lock_path).expect("Should succeed after first lock dropped");
        drop(lock3);
    }
}
