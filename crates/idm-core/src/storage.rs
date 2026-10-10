//! # SQLite Persistence Module (`storage.rs`)
//!
//! Provides durable storage of the download queue and metadata using SQLite.
//!
//! ## Schema:
//! - Tracks all downloads, URLs, local target paths, byte progress, status, and HTTP validators.
//! - On manager startup, any downloads left in `Downloading` state due to process termination
//!   are automatically marked `Paused` for clean resumption.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::events::DownloadStatus;

/// A persisted download record stored in SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub id: String,
    pub url: String,
    pub destination_path: PathBuf,
    pub total_size: Option<u64>,
    pub downloaded_bytes: u64,
    pub status: DownloadStatus,
    pub chunk_size: u64,
    pub connections: usize,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub error_message: Option<String>,
}

/// Thread-safe SQLite storage handle.
#[derive(Debug)]
pub struct Database {
    conn: Mutex<Connection>,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn status_to_str(s: DownloadStatus) -> &'static str {
    match s {
        DownloadStatus::Pending => "Pending",
        DownloadStatus::Downloading => "Downloading",
        DownloadStatus::Paused => "Paused",
        DownloadStatus::Completed => "Completed",
        DownloadStatus::Failed => "Failed",
        DownloadStatus::Cancelled => "Cancelled",
    }
}

fn str_to_status(s: &str) -> DownloadStatus {
    match s {
        "Downloading" => DownloadStatus::Downloading,
        "Paused" => DownloadStatus::Paused,
        "Completed" => DownloadStatus::Completed,
        "Failed" => DownloadStatus::Failed,
        "Cancelled" => DownloadStatus::Cancelled,
        _ => DownloadStatus::Pending,
    }
}

impl Database {
    /// Opens or creates a SQLite database file at `path`.
    pub fn new(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory '{:?}' for SQLite database", parent))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open SQLite database at '{:?}'", path))?;

        // Enable WAL mode for high concurrency performance
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;"
        ).ok();

        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init_schema()?;
        db.reset_interrupted_to_paused()?;
        Ok(db)
    }

    /// Creates an in-memory SQLite database, ideal for isolated unit testing.
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("Failed to open in-memory SQLite database")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                total_size INTEGER,
                downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                connections INTEGER NOT NULL,
                etag TEXT,
                last_modified TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                error_message TEXT
            );",
            [],
        )
        .context("Failed to initialize SQLite schema")?;
        Ok(())
    }

    /// On system startup, marks any downloads that were in `Downloading` as `Paused`.
    pub fn reset_interrupted_to_paused(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE downloads SET status = 'Paused', updated_at = ? WHERE status = 'Downloading'",
            params![now_unix()],
        ).context("Failed to update interrupted downloads to Paused")?;
        Ok(rows)
    }

    /// Inserts a new download record.
    pub fn insert_download(&self, record: &DownloadRecord) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO downloads (
                id, url, destination_path, total_size, downloaded_bytes,
                status, chunk_size, connections, etag, last_modified,
                created_at, updated_at, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                record.id,
                record.url,
                record.destination_path.to_string_lossy().as_ref(),
                record.total_size.map(|v| v as i64),
                record.downloaded_bytes as i64,
                status_to_str(record.status),
                record.chunk_size as i64,
                record.connections as i64,
                record.etag,
                record.last_modified,
                record.created_at as i64,
                record.updated_at as i64,
                record.error_message,
            ],
        )
        .with_context(|| format!("Failed to insert download '{}' into SQLite", record.id))?;
        Ok(())
    }

    /// Updates the status and optional error message of a download.
    pub fn update_status(&self, id: &str, status: DownloadStatus, error: Option<&str>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
            params![status_to_str(status), error, now_unix() as i64, id],
        )
        .with_context(|| format!("Failed to update status for download '{}'", id))?;
        Ok(())
    }

    /// Updates progress numbers for a download.
    pub fn update_progress(&self, id: &str, downloaded_bytes: u64, total_size: Option<u64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET downloaded_bytes = ?, total_size = COALESCE(?, total_size), updated_at = ? WHERE id = ?",
            params![
                downloaded_bytes as i64,
                total_size.map(|v| v as i64),
                now_unix() as i64,
                id
            ],
        )
        .with_context(|| format!("Failed to update progress for download '{}'", id))?;
        Ok(())
    }

    /// Updates destination path, total size, ETag, and Last-Modified after probing.
    pub fn update_metadata(
        &self,
        id: &str,
        dest_path: &Path,
        total_size: Option<u64>,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET
                destination_path = ?,
                total_size = ?,
                etag = ?,
                last_modified = ?,
                updated_at = ?
             WHERE id = ?",
            params![
                dest_path.to_string_lossy().as_ref(),
                total_size.map(|v| v as i64),
                etag,
                last_modified,
                now_unix() as i64,
                id
            ],
        )
        .with_context(|| format!("Failed to update metadata for download '{}'", id))?;
        Ok(())
    }

    /// Retrieves a single download record by ID.
    pub fn get_download(&self, id: &str) -> Result<Option<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, url, destination_path, total_size, downloaded_bytes,
                    status, chunk_size, connections, etag, last_modified,
                    created_at, updated_at, error_message
             FROM downloads WHERE id = ?",
        )?;

        let record = stmt
            .query_row(params![id], |row| {
                let total_size_i64: Option<i64> = row.get(3)?;
                let downloaded_i64: i64 = row.get(4)?;
                let status_s: String = row.get(5)?;
                let chunk_size_i64: i64 = row.get(6)?;
                let connections_i64: i64 = row.get(7)?;
                let dest_s: String = row.get(2)?;
                let created_i64: i64 = row.get(10)?;
                let updated_i64: i64 = row.get(11)?;

                Ok(DownloadRecord {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    destination_path: PathBuf::from(dest_s),
                    total_size: total_size_i64.map(|v| v as u64),
                    downloaded_bytes: downloaded_i64 as u64,
                    status: str_to_status(&status_s),
                    chunk_size: chunk_size_i64 as u64,
                    connections: connections_i64 as usize,
                    etag: row.get(8)?,
                    last_modified: row.get(9)?,
                    created_at: created_i64 as u64,
                    updated_at: updated_i64 as u64,
                    error_message: row.get(12)?,
                })
            })
            .optional()
            .with_context(|| format!("Failed to query download '{}'", id))?;

        Ok(record)
    }

    /// Lists all download records in the queue.
    pub fn list_downloads(&self) -> Result<Vec<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, url, destination_path, total_size, downloaded_bytes,
                    status, chunk_size, connections, etag, last_modified,
                    created_at, updated_at, error_message
             FROM downloads ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map([], |row| {
            let total_size_i64: Option<i64> = row.get(3)?;
            let downloaded_i64: i64 = row.get(4)?;
            let status_s: String = row.get(5)?;
            let chunk_size_i64: i64 = row.get(6)?;
            let connections_i64: i64 = row.get(7)?;
            let dest_s: String = row.get(2)?;
            let created_i64: i64 = row.get(10)?;
            let updated_i64: i64 = row.get(11)?;

            Ok(DownloadRecord {
                id: row.get(0)?,
                url: row.get(1)?,
                destination_path: PathBuf::from(dest_s),
                total_size: total_size_i64.map(|v| v as u64),
                downloaded_bytes: downloaded_i64 as u64,
                status: str_to_status(&status_s),
                chunk_size: chunk_size_i64 as u64,
                connections: connections_i64 as usize,
                etag: row.get(8)?,
                last_modified: row.get(9)?,
                created_at: created_i64 as u64,
                updated_at: updated_i64 as u64,
                error_message: row.get(12)?,
            })
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r?);
        }
        Ok(results)
    }

    /// Deletes a download record from SQLite.
    pub fn delete_download(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM downloads WHERE id = ?", params![id])
            .with_context(|| format!("Failed to delete download '{}'", id))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_crud_and_status() {
        let db = Database::new_in_memory().unwrap();

        let record = DownloadRecord {
            id: "dl-1".to_string(),
            url: "https://example.com/test.iso".to_string(),
            destination_path: PathBuf::from("test.iso"),
            total_size: Some(1048576),
            downloaded_bytes: 0,
            status: DownloadStatus::Pending,
            chunk_size: 1024,
            connections: 4,
            etag: Some("\"etag1\"".to_string()),
            last_modified: None,
            created_at: 100,
            updated_at: 100,
            error_message: None,
        };

        db.insert_download(&record).unwrap();

        let fetched = db.get_download("dl-1").unwrap().unwrap();
        assert_eq!(fetched.url, "https://example.com/test.iso");
        assert_eq!(fetched.status, DownloadStatus::Pending);

        db.update_status("dl-1", DownloadStatus::Downloading, None).unwrap();
        assert_eq!(db.get_download("dl-1").unwrap().unwrap().status, DownloadStatus::Downloading);

        db.update_progress("dl-1", 524288, Some(1048576)).unwrap();
        assert_eq!(db.get_download("dl-1").unwrap().unwrap().downloaded_bytes, 524288);

        let list = db.list_downloads().unwrap();
        assert_eq!(list.len(), 1);

        db.delete_download("dl-1").unwrap();
        assert!(db.get_download("dl-1").unwrap().is_none());
    }
}
