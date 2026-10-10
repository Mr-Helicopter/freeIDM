//! # Download Events & Throttling Module (`events.rs`)
//!
//! Provides the event bus and rate-throttled emitter for asynchronous download updates.
//!
//! ## Event Guarantees:
//! - State change and error events are emitted immediately upon occurrence.
//! - Progress events (bytes transferred, speed, ETA) are throttled to **at most 10 events per second**
//!   per download (minimum 100ms interval between consecutive emissions).
//! - All updates are distributed using a non-blocking `tokio::sync::broadcast` channel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Minimum duration between consecutive progress event broadcasts.
/// 100 milliseconds guarantees at most 10 progress events per second per download.
pub const MIN_PROGRESS_EMIT_INTERVAL: Duration = Duration::from_millis(100);

/// Lifecycle states of a managed download.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadStatus {
    /// In queue, waiting for an available concurrency slot.
    Pending,
    /// Currently actively downloading chunks.
    Downloading,
    /// Paused by user request or system signal; can be resumed.
    Paused,
    /// Completed successfully and verified on disk.
    Completed,
    /// Terminated due to an unrecoverable failure.
    Failed,
    /// Cancelled and terminated permanently.
    Cancelled,
}

impl std::fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Downloading => write!(f, "Downloading"),
            Self::Paused => write!(f, "Paused"),
            Self::Completed => write!(f, "Completed"),
            Self::Failed => write!(f, "Failed"),
            Self::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// An update event emitted by a download task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownloadEvent {
    /// Emitted when a download transitions between lifecycle states.
    StateChanged {
        id: String,
        status: DownloadStatus,
    },
    /// Emitted periodically to inform subscribers of data transfer progress.
    Progress {
        id: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bytes_per_sec: f64,
        eta: Option<Duration>,
    },
    /// Emitted when an error is encountered.
    Error {
        id: String,
        message: String,
    },
}

/// State used to calculate moving transfer speed and ETA.
#[derive(Debug)]
struct SpeedTracker {
    last_calc_time: Instant,
    last_calc_bytes: u64,
    current_speed: f64,
}

/// Throttled event emitter for a specific download instance.
#[derive(Debug)]
pub struct ProgressEmitter {
    download_id: String,
    sender: broadcast::Sender<DownloadEvent>,
    downloaded_bytes: Arc<AtomicU64>,
    total_bytes: Option<u64>,
    last_emit_time: std::sync::Mutex<Option<Instant>>,
    speed_tracker: std::sync::Mutex<SpeedTracker>,
}

impl ProgressEmitter {
    /// Creates a new `ProgressEmitter`.
    pub fn new(
        download_id: String,
        sender: broadcast::Sender<DownloadEvent>,
        downloaded_counter: Arc<AtomicU64>,
        total_bytes: Option<u64>,
    ) -> Self {
        let now = Instant::now();
        let initial_bytes = downloaded_counter.load(Ordering::Relaxed);
        Self {
            download_id,
            sender,
            downloaded_bytes: downloaded_counter,
            total_bytes,
            last_emit_time: std::sync::Mutex::new(None),
            speed_tracker: std::sync::Mutex::new(SpeedTracker {
                last_calc_time: now,
                last_calc_bytes: initial_bytes,
                current_speed: 0.0,
            }),
        }
    }

    /// Emits a progress update if the throttle threshold (100ms) has elapsed,
    /// or unconditionally if `force` is true (e.g., at completion).
    pub fn emit_progress(&self, force: bool) {
        let now = Instant::now();

        if !force {
            let mut last_emit = self.last_emit_time.lock().unwrap();
            if let Some(prev) = *last_emit
                && now.duration_since(prev) < MIN_PROGRESS_EMIT_INTERVAL
            {
                // Throttled: at most 10 progress events per second!
                return;
            }
            *last_emit = Some(now);
        } else {
            let mut last_emit = self.last_emit_time.lock().unwrap();
            *last_emit = Some(now);
        }

        let downloaded = self.downloaded_bytes.load(Ordering::Relaxed);

        // Calculate transfer speed and ETA
        let (speed, eta) = {
            let mut tracker = self.speed_tracker.lock().unwrap();
            let elapsed = now.duration_since(tracker.last_calc_time).as_secs_f64();

            if elapsed >= 0.25 {
                let delta_bytes = downloaded.saturating_sub(tracker.last_calc_bytes);
                let instantaneous_speed = delta_bytes as f64 / elapsed;

                // Exponential Moving Average (EMA) to smooth speed fluctuations
                tracker.current_speed = if tracker.current_speed == 0.0 {
                    instantaneous_speed
                } else {
                    tracker.current_speed * 0.7 + instantaneous_speed * 0.3
                };

                tracker.last_calc_time = now;
                tracker.last_calc_bytes = downloaded;
            }

            let current_speed = tracker.current_speed;
            let eta = if current_speed > 0.0 {
                self.total_bytes.map(|total| {
                    if total > downloaded {
                        let remaining_bytes = total - downloaded;
                        let secs = remaining_bytes as f64 / current_speed;
                        Duration::from_secs_f64(secs)
                    } else {
                        Duration::ZERO
                    }
                })
            } else {
                None
            };

            (current_speed, eta)
        };

        let event = DownloadEvent::Progress {
            id: self.download_id.clone(),
            downloaded_bytes: downloaded,
            total_bytes: self.total_bytes,
            speed_bytes_per_sec: speed,
            eta,
        };

        // Broadcast ignores send errors (occurs if there are no active listeners)
        let _ = self.sender.send(event);
    }

    /// Emits a state change event immediately (not throttled).
    pub fn emit_state(&self, status: DownloadStatus) {
        let event = DownloadEvent::StateChanged {
            id: self.download_id.clone(),
            status,
        };
        let _ = self.sender.send(event);
    }

    /// Emits an error event immediately (not throttled).
    pub fn emit_error(&self, message: impl Into<String>) {
        let event = DownloadEvent::Error {
            id: self.download_id.clone(),
            message: message.into(),
        };
        let _ = self.sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_progress_event_throttling_to_at_most_10_per_sec() {
        let (tx, mut rx) = broadcast::channel::<DownloadEvent>(100);
        let counter = Arc::new(AtomicU64::new(0));
        let emitter = ProgressEmitter::new("test-123".to_string(), tx, Arc::clone(&counter), Some(100_000));

        // Call emit_progress 100 times in rapid succession without force
        for i in 1..=100 {
            counter.store(i * 100, Ordering::Relaxed);
            emitter.emit_progress(false);
        }

        // Even though 100 rapid calls occurred, because interval is >= 100ms,
        // exactly 1 event should have been emitted!
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }

        assert_eq!(count, 1, "Rapid progress updates should be throttled to 1 event within <100ms window");

        // Wait 105ms and emit again: second event should now be emitted
        tokio::time::sleep(Duration::from_millis(110)).await;
        emitter.emit_progress(false);

        let second = rx.try_recv();
        assert!(second.is_ok(), "Event should be emitted after interval expires");
    }

    #[tokio::test]
    async fn test_force_progress_emission() {
        let (tx, mut rx) = broadcast::channel::<DownloadEvent>(10);
        let counter = Arc::new(AtomicU64::new(5000));
        let emitter = ProgressEmitter::new("test-456".to_string(), tx, Arc::clone(&counter), Some(10_000));

        emitter.emit_progress(true);
        emitter.emit_progress(true);

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }
}
