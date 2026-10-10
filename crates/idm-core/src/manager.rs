//! # Download Manager Module (`manager.rs`)
//!
//! Exposes the central `DownloadManager` orchestrating concurrent downloads,
//! queue persistence, runtime speed limiting, and event dispatching.
//!
//! ## Core Features:
//! - **Queue Operations**: `add`, `pause`, `resume`, `cancel`, and `list` downloads.
//! - **Global Concurrency Limit**: Enforces a configurable cap on simultaneously active transfers.
//! - **Durable SQLite Storage**: Maintains download queue and state across process restarts.
//! - **Event Broadcasting**: Emits throttled progress (<= 10 events/sec), state changes, and errors.
//! - **Zero Direct Printing**: Emits pure structured events without console printing or progress bars.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::Url;
use tokio::sync::{broadcast, Mutex, Notify};

use crate::events::{DownloadEvent, DownloadStatus, ProgressEmitter};
use crate::limiter::SpeedLimiter;
use crate::probe::{determine_filename, probe_server};
use crate::segmented::{download_segmented, SegmentedResult};
use crate::single::download_single_stream;
use crate::state::{get_lock_path, get_part_path, get_state_path, DownloadState, FileLock};
use crate::storage::{Database, DownloadRecord};

const DEFAULT_CONCURRENCY_LIMIT: usize = 4;
const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024; // 4 MiB
const DEFAULT_CONNECTIONS: usize = 8;
const BROADCAST_CHANNEL_CAPACITY: usize = 512;

/// Configuration options for adding a new download to the manager.
#[derive(Debug, Clone)]
pub struct AddDownloadOptions {
    /// URL of the resource to download.
    pub url: String,
    /// Optional target filesystem path. If None, filename is inferred via probing.
    pub destination_path: Option<PathBuf>,
    /// Optional chunk size in bytes (default 4 MiB).
    pub chunk_size: Option<u64>,
    /// Optional concurrent connections per download (default 8).
    pub connections: Option<usize>,
    /// Overwrite existing destination file if present.
    pub force: bool,
}

impl AddDownloadOptions {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            destination_path: None,
            chunk_size: None,
            connections: None,
            force: false,
        }
    }
}

/// Handle tracking a running background download task.
struct ActiveDownloadHandle {
    cancel_token: Arc<AtomicBool>,
}

/// Orchestrates downloads with concurrency limits, event broadcasting, and SQLite storage.
#[derive(Clone)]
pub struct DownloadManager {
    database: Arc<Database>,
    speed_limiter: Arc<SpeedLimiter>,
    concurrency_limit: usize,
    active_downloads: Arc<Mutex<HashMap<String, ActiveDownloadHandle>>>,
    per_download_senders: Arc<std::sync::Mutex<HashMap<String, broadcast::Sender<DownloadEvent>>>>,
    global_event_sender: broadcast::Sender<DownloadEvent>,
    client: reqwest::Client,
    scheduler_notify: Arc<Notify>,
}

impl DownloadManager {
    /// Creates a new `DownloadManager` backed by a SQLite file at `db_path`.
    pub fn new(db_path: &Path, concurrency_limit: Option<usize>) -> Result<Self> {
        let database = Arc::new(Database::new(db_path)?);
        Self::init(database, concurrency_limit.unwrap_or(DEFAULT_CONCURRENCY_LIMIT))
    }

    /// Creates an in-memory `DownloadManager` (ideal for tests).
    pub fn new_in_memory(concurrency_limit: Option<usize>) -> Result<Self> {
        let database = Arc::new(Database::new_in_memory()?);
        Self::init(database, concurrency_limit.unwrap_or(DEFAULT_CONCURRENCY_LIMIT))
    }

    fn init(database: Arc<Database>, concurrency_limit: usize) -> Result<Self> {
        let (global_tx, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .build()
            .context("Failed to build reqwest client for DownloadManager")?;

        let manager = Self {
            database,
            speed_limiter: Arc::new(SpeedLimiter::default()),
            concurrency_limit: concurrency_limit.max(1),
            active_downloads: Arc::new(Mutex::new(HashMap::new())),
            per_download_senders: Arc::new(std::sync::Mutex::new(HashMap::new())),
            global_event_sender: global_tx,
            client,
            scheduler_notify: Arc::new(Notify::new()),
        };

        // Start background scheduler loop
        let mgr_clone = manager.clone();
        tokio::spawn(async move {
            mgr_clone.scheduler_loop().await;
        });

        // Trigger scheduler to pick up any pending downloads from database
        manager.scheduler_notify.notify_one();

        Ok(manager)
    }

    /// Returns a reference to the global speed limiter.
    pub fn speed_limiter(&self) -> &Arc<SpeedLimiter> {
        &self.speed_limiter
    }

    /// Sets the global speed limit in bytes per second. `None` removes restrictions.
    pub async fn set_speed_limit(&self, bytes_per_sec: Option<u64>) {
        self.speed_limiter.set_rate(bytes_per_sec).await;
    }

    /// Gets the current global speed limit in bytes per second, if set.
    pub fn get_speed_limit(&self) -> Option<u64> {
        self.speed_limiter.get_rate()
    }

    /// Subscribes to events for a specific download ID.
    pub fn subscribe(&self, id: &str) -> broadcast::Receiver<DownloadEvent> {
        let mut senders = self.per_download_senders.lock().unwrap();
        if let Some(tx) = senders.get(id) {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
            senders.insert(id.to_string(), tx);
            rx
        }
    }

    /// Subscribes to all events across all downloads.
    pub fn subscribe_all(&self) -> broadcast::Receiver<DownloadEvent> {
        self.global_event_sender.subscribe()
    }

    /// Adds a download to the queue. Returns the unique download ID.
    pub async fn add(&self, opts: AddDownloadOptions) -> Result<String> {
        let parsed_url = Url::parse(&opts.url)
            .with_context(|| format!("Invalid URL '{}'", opts.url))?;

        let id = uuid::Uuid::new_v4().to_string();

        // Determine destination path: if not specified, probe or derive from URL
        let dest_path = if let Some(p) = opts.destination_path {
            p
        } else {
            let filename = match probe_server(&self.client, &parsed_url).await {
                Ok(info) => info.filename,
                Err(_) => determine_filename(&reqwest::header::HeaderMap::new(), &parsed_url),
            };
            PathBuf::from(filename)
        };

        if dest_path.exists() && !opts.force {
            bail!("Destination file '{}' already exists. Use --force to overwrite.", dest_path.display());
        }
        let chunk_size = opts.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
        let connections = opts.connections.unwrap_or(DEFAULT_CONNECTIONS);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let record = DownloadRecord {
            id: id.clone(),
            url: opts.url,
            destination_path: dest_path,
            total_size: None,
            downloaded_bytes: 0,
            status: DownloadStatus::Pending,
            chunk_size,
            connections,
            etag: None,
            last_modified: None,
            created_at: now,
            updated_at: now,
            error_message: None,
        };

        self.database.insert_download(&record)?;

        // Setup per-download broadcast channel
        {
            let mut senders = self.per_download_senders.lock().unwrap();
            let (tx, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
            senders.insert(id.clone(), tx);
        }

        self.emit_event(DownloadEvent::StateChanged {
            id: id.clone(),
            status: DownloadStatus::Pending,
        });

        // Signal scheduler to process queue
        self.scheduler_notify.notify_one();

        Ok(id)
    }

    /// Pauses an active or pending download.
    pub async fn pause(&self, id: &str) -> Result<()> {
        let active = {
            let active_map = self.active_downloads.lock().await;
            active_map.get(id).map(|h| Arc::clone(&h.cancel_token))
        };

        if let Some(token) = active {
            token.store(true, Ordering::SeqCst);
        } else {
            // If download was Pending, mark it Paused directly in storage
            if let Some(rec) = self.database.get_download(id)?
                && rec.status == DownloadStatus::Pending
            {
                self.database.update_status(id, DownloadStatus::Paused, None)?;
                self.emit_event(DownloadEvent::StateChanged {
                    id: id.to_string(),
                    status: DownloadStatus::Paused,
                });
            }
        }

        self.scheduler_notify.notify_one();
        Ok(())
    }

    /// Resumes a paused or failed download.
    pub async fn resume(&self, id: &str) -> Result<()> {
        let record = self.database.get_download(id)?
            .with_context(|| format!("Download '{}' not found", id))?;

        if record.status == DownloadStatus::Downloading || record.status == DownloadStatus::Completed {
            return Ok(());
        }

        self.database.update_status(id, DownloadStatus::Pending, None)?;
        self.emit_event(DownloadEvent::StateChanged {
            id: id.to_string(),
            status: DownloadStatus::Pending,
        });

        self.scheduler_notify.notify_one();
        Ok(())
    }

    /// Cancels a download.
    pub async fn cancel(&self, id: &str) -> Result<()> {
        let active = {
            let active_map = self.active_downloads.lock().await;
            active_map.get(id).map(|h| Arc::clone(&h.cancel_token))
        };

        if let Some(token) = active {
            token.store(true, Ordering::SeqCst);
        }

        self.database.update_status(id, DownloadStatus::Cancelled, None)?;
        self.emit_event(DownloadEvent::StateChanged {
            id: id.to_string(),
            status: DownloadStatus::Cancelled,
        });

        self.scheduler_notify.notify_one();
        Ok(())
    }

    /// Lists all downloads in the queue.
    pub fn list(&self) -> Result<Vec<DownloadRecord>> {
        self.database.list_downloads()
    }

    /// Retrieves status of a single download by ID.
    pub fn get(&self, id: &str) -> Result<Option<DownloadRecord>> {
        self.database.get_download(id)
    }

    fn emit_event(&self, event: DownloadEvent) {
        // Send to global channel
        let _ = self.global_event_sender.send(event.clone());

        // Send to download-specific channel
        let id = match &event {
            DownloadEvent::StateChanged { id, .. } => id,
            DownloadEvent::Progress { id, .. } => id,
            DownloadEvent::Error { id, .. } => id,
        };

        let senders = self.per_download_senders.lock().unwrap();
        if let Some(tx) = senders.get(id) {
            let _ = tx.send(event);
        }
    }

    /// Continuous scheduler loop managing concurrency limits and queue execution.
    async fn scheduler_loop(&self) {
        loop {
            self.scheduler_notify.notified().await;

            let current_active_count = {
                let active = self.active_downloads.lock().await;
                active.len()
            };

            let available_slots = self.concurrency_limit.saturating_sub(current_active_count);
            if available_slots == 0 {
                continue;
            }

            // Fetch pending downloads from SQLite
            let pending_list = match self.database.list_downloads() {
                Ok(list) => list
                    .into_iter()
                    .filter(|d| d.status == DownloadStatus::Pending)
                    .take(available_slots)
                    .collect::<Vec<_>>(),
                Err(_) => Vec::new(),
            };

            for record in pending_list {
                let id = record.id.clone();
                let cancel_token = Arc::new(AtomicBool::new(false));

                {
                    let mut active = self.active_downloads.lock().await;
                    active.insert(
                        id.clone(),
                        ActiveDownloadHandle {
                            cancel_token: Arc::clone(&cancel_token),
                        },
                    );
                }

                // Update status to Downloading in SQLite
                let _ = self.database.update_status(&id, DownloadStatus::Downloading, None);

                let mgr = self.clone();
                tokio::spawn(async move {
                    mgr.run_download_task(record, cancel_token).await;
                });
            }
        }
    }

    /// Executes the full download pipeline for a single item.
    async fn run_download_task(&self, mut record: DownloadRecord, cancel_token: Arc<AtomicBool>) {
        let id = record.id.clone();
        let target_url = match Url::parse(&record.url) {
            Ok(u) => u,
            Err(e) => {
                self.database.update_status(&id, DownloadStatus::Failed, Some(&e.to_string())).ok();
                self.emit_event(DownloadEvent::Error {
                    id: id.clone(),
                    message: format!("Invalid URL: {}", e),
                });
                self.emit_event(DownloadEvent::StateChanged {
                    id: id.clone(),
                    status: DownloadStatus::Failed,
                });
                self.cleanup_active(&id).await;
                return;
            }
        };

        let dest_path = record.destination_path.clone();
        let part_path = get_part_path(&dest_path);
        let state_path = get_state_path(&dest_path);
        let lock_path = get_lock_path(&dest_path);

        // Acquire advisory file lock to guard destination
        let _file_lock = match FileLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) => {
                let err_msg = format!("Failed to acquire destination file lock: {}", e);
                self.database.update_status(&id, DownloadStatus::Failed, Some(&err_msg)).ok();
                self.emit_event(DownloadEvent::Error {
                    id: id.clone(),
                    message: err_msg,
                });
                self.emit_event(DownloadEvent::StateChanged {
                    id: id.clone(),
                    status: DownloadStatus::Failed,
                });
                self.cleanup_active(&id).await;
                return;
            }
        };

        // Emit initial Downloading state change
        self.emit_event(DownloadEvent::StateChanged {
            id: id.clone(),
            status: DownloadStatus::Downloading,
        });

        // Check for orphan state file (.part.state without .part)
        if state_path.exists() && !part_path.exists() {
            let _ = tokio::fs::remove_file(&state_path).await;
        }

        // Check existing state
        let existing_state = DownloadState::load(&state_path).await.unwrap_or(None);
        let probe_target_url = match &existing_state {
            Some(s) => Url::parse(&s.original_url).unwrap_or_else(|_| target_url.clone()),
            None => target_url.clone(),
        };

        // Probe remote server
        let probe = match probe_server(&self.client, &probe_target_url).await {
            Ok(p) => p,
            Err(e) => {
                let err_msg = format!("Probe failed: {}", e);
                self.database.update_status(&id, DownloadStatus::Failed, Some(&err_msg)).ok();
                self.emit_event(DownloadEvent::Error {
                    id: id.clone(),
                    message: err_msg,
                });
                self.emit_event(DownloadEvent::StateChanged {
                    id: id.clone(),
                    status: DownloadStatus::Failed,
                });
                self.cleanup_active(&id).await;
                return;
            }
        };

        // Update database metadata with probed info
        record.total_size = probe.total_size;
        record.etag = probe.etag.clone();
        record.last_modified = probe.last_modified.clone();
        self.database.update_metadata(
            &id,
            &dest_path,
            probe.total_size,
            probe.etag.as_deref(),
            probe.last_modified.as_deref(),
        ).ok();

        // Setup throttled progress emitter
        let downloaded_counter = Arc::new(AtomicU64::new(0));

        let download_sender = {
            let senders = self.per_download_senders.lock().unwrap();
            senders.get(&id).cloned().unwrap_or_else(|| {
                let (tx, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
                tx
            })
        };

        let progress_emitter = Arc::new(ProgressEmitter::new(
            id.clone(),
            download_sender,
            Arc::clone(&downloaded_counter),
            probe.total_size,
        ));

        // Strategy selection
        let download_result: Result<DownloadStatus> = match (probe.supports_ranges, probe.total_size) {
            (true, Some(total_size)) if total_size > 0 => {
                let has_validators = probe.etag.is_some() || probe.last_modified.is_some();

                let mut state = if !has_validators {
                    if existing_state.is_some() || part_path.exists() {
                        let _ = tokio::fs::remove_file(&part_path).await;
                        let _ = tokio::fs::remove_file(&state_path).await;
                    }
                    DownloadState::new(
                        record.url.clone(),
                        probe.final_url.to_string(),
                        total_size,
                        probe.etag.clone(),
                        probe.last_modified.clone(),
                        record.chunk_size,
                    )
                } else {
                    match existing_state {
                        Some(ref s) if part_path.exists() && s.is_resume_compatible(&probe, record.chunk_size) => {
                            s.clone()
                        }
                        Some(_) => {
                            let _ = tokio::fs::remove_file(&part_path).await;
                            let _ = tokio::fs::remove_file(&state_path).await;
                            DownloadState::new(
                                record.url.clone(),
                                probe.final_url.to_string(),
                                total_size,
                                probe.etag.clone(),
                                probe.last_modified.clone(),
                                record.chunk_size,
                            )
                        }
                        None => DownloadState::new(
                            record.url.clone(),
                            probe.final_url.to_string(),
                            total_size,
                            probe.etag.clone(),
                            probe.last_modified.clone(),
                            record.chunk_size,
                        ),
                    }
                };

                let mut if_range_restarts: usize = 0;
                let status_result;

                loop {
                    match download_segmented(
                        &self.client,
                        &probe.final_url,
                        &dest_path,
                        total_size,
                        record.chunk_size,
                        record.connections,
                        state.clone(),
                        Arc::clone(&cancel_token),
                        Some(&self.speed_limiter),
                        Some(&progress_emitter),
                        Some(&downloaded_counter),
                    )
                    .await
                    {
                        Ok(SegmentedResult::Completed) => {
                            status_result = DownloadStatus::Completed;
                            break;
                        }
                        Ok(SegmentedResult::FileChangedRestart) => {
                            if_range_restarts += 1;
                            if if_range_restarts > 1 {
                                let msg = "Server repeatedly returned 200 OK under Range requests".to_string();
                                self.emit_event(DownloadEvent::Error { id: id.clone(), message: msg.clone() });
                                status_result = DownloadStatus::Failed;
                                break;
                            }
                            cancel_token.store(false, Ordering::SeqCst);
                            state = DownloadState::new(
                                record.url.clone(),
                                probe.final_url.to_string(),
                                total_size,
                                probe.etag.clone(),
                                probe.last_modified.clone(),
                                record.chunk_size,
                            );
                            continue;
                        }
                        Ok(SegmentedResult::Interrupted) => {
                            status_result = DownloadStatus::Paused;
                            break;
                        }
                        Ok(SegmentedResult::Fatal(e)) => {
                            self.emit_event(DownloadEvent::Error { id: id.clone(), message: e.to_string() });
                            status_result = DownloadStatus::Failed;
                            break;
                        }
                        Err(e) => {
                            self.emit_event(DownloadEvent::Error { id: id.clone(), message: e.to_string() });
                            status_result = DownloadStatus::Failed;
                            break;
                        }
                    }
                }
                Ok(status_result)
            }
            _ => {
                // Fallback single stream
                match download_single_stream(
                    &self.client,
                    &probe.final_url,
                    &dest_path,
                    probe.total_size,
                    Some(&self.speed_limiter),
                    Some(&progress_emitter),
                    Some(&cancel_token),
                )
                .await
                {
                    Ok(true) => Ok(DownloadStatus::Completed),
                    Ok(false) => Ok(DownloadStatus::Paused),
                    Err(e) => {
                        self.emit_event(DownloadEvent::Error { id: id.clone(), message: e.to_string() });
                        Ok(DownloadStatus::Failed)
                    }
                }
            }
        };

        let final_status = download_result.unwrap_or(DownloadStatus::Failed);

        let final_downloaded = downloaded_counter.load(Ordering::Relaxed);
        self.database.update_progress(&id, final_downloaded, probe.total_size).ok();
        self.database.update_status(&id, final_status, None).ok();

        self.emit_event(DownloadEvent::StateChanged {
            id: id.clone(),
            status: final_status,
        });

        self.cleanup_active(&id).await;
    }

    async fn cleanup_active(&self, id: &str) {
        {
            let mut active = self.active_downloads.lock().await;
            active.remove(id);
        }
        self.scheduler_notify.notify_one();
    }
}
