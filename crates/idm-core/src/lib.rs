//! # idm-core
//!
//! High-performance, resilient asynchronous file download engine with
//! parallel segmented transfers, pause/resume persistence, concurrency limits,
//! SQLite queue management, throttled event broadcasting, and runtime token-bucket
//! speed regulation.

pub mod chunks;
pub mod events;
pub mod limiter;
pub mod manager;
pub mod probe;
pub mod segmented;
pub mod single;
pub mod state;
pub mod storage;

// Primary API re-exports
pub use events::{DownloadEvent, DownloadStatus, ProgressEmitter};
pub use limiter::SpeedLimiter;
pub use manager::{AddDownloadOptions, DownloadManager};
pub use probe::{determine_filename, probe_server, sanitize_filename, ProbeInfo};
pub use state::{get_lock_path, get_part_path, get_state_path, DownloadState, FileLock};
pub use storage::DownloadRecord;
