use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use idm_core::events::{DownloadEvent, DownloadStatus};
use idm_core::manager::{AddDownloadOptions, DownloadManager};
use idm_core::state::{get_part_path, get_state_path, DownloadState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

// ============================================================================
// Test Mock HTTP Server Supporting Ranges & Controlled Failure Modes
// ============================================================================

#[derive(Clone, Default)]
pub struct MockServerOptions {
    pub force_no_ranges: bool,
    pub drop_mid_chunk_first_attempt: bool,
    pub mismatched_content_range: bool,
    pub chunk_delay_ms: u64,
    pub omit_validators: bool,
    pub return_200_on_range: bool,
}

#[derive(Clone)]
pub struct MockServerState {
    pub data: Vec<u8>,
    pub etag: String,
    pub options: MockServerOptions,
}

async fn start_mock_server_stateful(
    state_arc: Arc<RwLock<MockServerState>>,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_url = format!("http://127.0.0.1:{}/testfile", port);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let dropped_once = Arc::new(AtomicBool::new(false));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                accept_res = listener.accept() => {
                    let (mut socket, _) = match accept_res {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let state_lock = Arc::clone(&state_arc);
                    let dropped_once = Arc::clone(&dropped_once);

                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };

                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let mut lines = req_str.lines();
                        let req_line = lines.next().unwrap_or("");
                        let parts: Vec<&str> = req_line.split_whitespace().collect();
                        if parts.is_empty() {
                            return;
                        }
                        let method = parts[0];

                        let mut range_header: Option<String> = None;
                        let mut if_range_header: Option<String> = None;

                        for line in lines {
                            if let Some(val) = line.strip_prefix("range: ").or_else(|| line.strip_prefix("Range: ")) {
                                range_header = Some(val.trim().to_string());
                            } else if let Some(val) = line.strip_prefix("if-range: ").or_else(|| line.strip_prefix("If-Range: ")) {
                                if_range_header = Some(val.trim().to_string());
                            }
                        }

                        let (data, etag, options) = {
                            let s = state_lock.read().await;
                            (s.data.clone(), s.etag.clone(), s.options.clone())
                        };

                        let total_len = data.len();

                        if method == "HEAD" {
                            if options.force_no_ranges {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    total_len
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else if options.omit_validators {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    total_len
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                    total_len, etag
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                        } else if method == "GET" {
                            if options.return_200_on_range {
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                    total_len, etag
                                );
                                let _ = socket.write_all(header.as_bytes()).await;
                                let _ = socket.write_all(&data).await;
                                return;
                            }

                            if let Some(client_validator) = if_range_header {
                                let expected_validator = format!("\"{}\"", etag);
                                if client_validator != etag && client_validator != expected_validator {
                                    let header = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                        total_len, etag
                                    );
                                    let _ = socket.write_all(header.as_bytes()).await;
                                    let _ = socket.write_all(&data).await;
                                    return;
                                }
                            }

                            if !options.force_no_ranges && range_header.as_deref().unwrap_or("").starts_with("bytes=") {
                                let range_val = range_header.unwrap();
                                let range_spec = range_val.trim_start_matches("bytes=");
                                if let Some((start_s, end_s)) = range_spec.split_once('-') {
                                    let start: usize = start_s.parse().unwrap_or(0);
                                    let end: usize = end_s.parse().unwrap_or(total_len.saturating_sub(1));
                                    let end = std::cmp::min(end, total_len.saturating_sub(1));

                                    if start <= end && start < total_len {
                                        if options.mismatched_content_range {
                                            let header = format!(
                                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 99999-99999/{}\r\nContent-Length: 1\r\nConnection: close\r\n\r\n",
                                                total_len
                                            );
                                            let _ = socket.write_all(header.as_bytes()).await;
                                            let _ = socket.write_all(&[42]).await;
                                            return;
                                        }

                                        if options.drop_mid_chunk_first_attempt && !dropped_once.swap(true, Ordering::SeqCst) {
                                            let slice = &data[start..=end];
                                            let header = format!(
                                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                                start, end, total_len, slice.len(), etag
                                            );
                                            let _ = socket.write_all(header.as_bytes()).await;

                                            let partial_len = std::cmp::min(10, slice.len() / 2);
                                            let _ = socket.write_all(&slice[..partial_len]).await;
                                            let _ = socket.shutdown().await;
                                            return;
                                        }

                                        if options.chunk_delay_ms > 0 {
                                            tokio::time::sleep(Duration::from_millis(options.chunk_delay_ms)).await;
                                        }

                                        let slice = &data[start..=end];
                                        let header = format!(
                                            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                            start, end, total_len, slice.len(), etag
                                        );
                                        let _ = socket.write_all(header.as_bytes()).await;
                                        let _ = socket.write_all(slice).await;
                                        return;
                                    }
                                }
                            }

                            let header = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                                total_len, etag
                            );
                            let _ = socket.write_all(header.as_bytes()).await;
                            let _ = socket.write_all(&data).await;
                        }
                    });
                }
            }
        }
    });

    (server_url, shutdown_tx)
}

async fn start_mock_server(
    data: Arc<Vec<u8>>,
    options: MockServerOptions,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let state = MockServerState {
        data: (*data).clone(),
        etag: "test-etag".to_string(),
        options,
    };
    start_mock_server_stateful(Arc::new(RwLock::new(state))).await
}

async fn wait_for_completion(manager: &DownloadManager, id: &str) -> DownloadStatus {
    let mut rx = manager.subscribe(id);
    if let Ok(Some(d)) = manager.get(id)
        && (d.status == DownloadStatus::Completed || d.status == DownloadStatus::Failed || d.status == DownloadStatus::Cancelled)
    {
        return d.status;
    }

    while let Ok(event) = rx.recv().await {
        if let DownloadEvent::StateChanged { status, .. } = event
            && (status == DownloadStatus::Completed || status == DownloadStatus::Failed || status == DownloadStatus::Cancelled)
        {
            return status;
        }
    }
    DownloadStatus::Failed
}

async fn run_test_download_with_options(
    test_size: usize,
    chunk_size: u64,
    connections: usize,
    options: MockServerOptions,
) -> Result<()> {
    let source_data: Vec<u8> = (0..test_size).map(|i| (i * 31 % 251) as u8).collect();
    let source_data_arc = Arc::new(source_data.clone());

    let (server_url, _shutdown) = start_mock_server(source_data_arc, options).await;

    static TEST_FILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let unique_id = TEST_FILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!(
        "test_output_{}_{}_{}_{}.bin",
        test_size,
        std::process::id(),
        unique_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let temp_path = PathBuf::from(&temp_filename);

    let manager = DownloadManager::new_in_memory(Some(4))?;
    let download_id = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(chunk_size),
            connections: Some(connections),
            force: true,
        })
        .await?;

    let result_status = wait_for_completion(&manager, &download_id).await;

    if result_status == DownloadStatus::Completed {
        let downloaded_bytes = tokio::fs::read(&temp_path)
            .await
            .expect("Downloaded file should be readable on success");

        assert_eq!(downloaded_bytes.len(), source_data.len());
        assert_eq!(downloaded_bytes, source_data);
    }

    let _ = tokio::fs::remove_file(&temp_path).await;
    let _ = tokio::fs::remove_file(get_part_path(&temp_path)).await;
    let _ = tokio::fs::remove_file(get_state_path(&temp_path)).await;

    if result_status == DownloadStatus::Completed {
        Ok(())
    } else {
        bail!("Download task failed");
    }
}

// ============================================================================
// Core Integration Tests (Ported & Verified)
// ============================================================================

#[tokio::test]
async fn test_integration_smaller_than_chunk_size() {
    let options = MockServerOptions::default();
    run_test_download_with_options(15 * 1024, 64 * 1024, 4, options)
        .await
        .expect("Should succeed");
}

#[tokio::test]
async fn test_integration_not_divisible_by_chunk_size() {
    let options = MockServerOptions::default();
    run_test_download_with_options(150 * 1024, 64 * 1024, 4, options)
        .await
        .expect("Should succeed");
}

#[tokio::test]
async fn test_integration_exact_multiple_of_chunk_size() {
    let options = MockServerOptions::default();
    run_test_download_with_options(128 * 1024, 64 * 1024, 4, options)
        .await
        .expect("Should succeed");
}

#[tokio::test]
async fn test_integration_arbitrary_odd_size() {
    let options = MockServerOptions::default();
    run_test_download_with_options(333, 1024, 4, options)
        .await
        .expect("Should succeed");
}

#[tokio::test]
async fn test_integration_fallback_when_ranges_unsupported() {
    let options = MockServerOptions {
        force_no_ranges: true,
        ..Default::default()
    };
    run_test_download_with_options(75 * 1024, 16 * 1024, 4, options)
        .await
        .expect("Fallback to single-stream should succeed");
}

#[tokio::test]
async fn test_integration_dropped_connection_retry() {
    let options = MockServerOptions {
        drop_mid_chunk_first_attempt: true,
        ..Default::default()
    };

    run_test_download_with_options(64 * 1024, 32 * 1024, 2, options)
        .await
        .expect("Download should recover from dropped connection and succeed on retry");
}

#[tokio::test]
async fn test_integration_mismatched_content_range() {
    let options = MockServerOptions {
        mismatched_content_range: true,
        ..Default::default()
    };

    let result = run_test_download_with_options(64 * 1024, 32 * 1024, 2, options).await;
    assert!(result.is_err(), "Download must fail when Content-Range is mismatched");
}

#[tokio::test]
async fn test_integration_pause_resume() {
    let test_size = 128 * 1024;
    let chunk_size = 32 * 1024;
    let source_data: Vec<u8> = (0..test_size).map(|i| (i * 31 % 251) as u8).collect();
    let source_data_arc = Arc::new(source_data.clone());

    let (server_url, _shutdown) = start_mock_server(
        source_data_arc,
        MockServerOptions {
            chunk_delay_ms: 30,
            ..Default::default()
        },
    )
    .await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_pause_resume_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);
    let state_path = get_state_path(&temp_path);
    let part_path = get_part_path(&temp_path);

    let manager = DownloadManager::new_in_memory(Some(4)).unwrap();
    let download_id = manager
        .add(AddDownloadOptions {
            url: server_url.clone(),
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(chunk_size),
            connections: Some(1),
            force: true,
        })
        .await
        .unwrap();

    // Wait until at least 1 chunk is saved
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Ok(Some(state)) = DownloadState::load(&state_path).await
            && !state.completed_chunks.is_empty()
        {
            break;
        }
    }

    // Pause download via manager
    manager.pause(&download_id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(part_path.exists(), ".part file should exist after pause");
    assert!(state_path.exists(), ".part.state file should exist after pause");

    // Resume download via manager
    manager.resume(&download_id).await.unwrap();

    let status = wait_for_completion(&manager, &download_id).await;
    assert_eq!(status, DownloadStatus::Completed);

    assert!(temp_path.exists());
    assert!(!part_path.exists());
    assert!(!state_path.exists());

    let downloaded_bytes = tokio::fs::read(&temp_path).await.unwrap();
    assert_eq!(downloaded_bytes, source_data);

    let _ = tokio::fs::remove_file(&temp_path).await;
}

#[tokio::test]
async fn test_integration_changed_etag_restart() {
    let test_size = 128 * 1024;
    let chunk_size = 32 * 1024;

    let data_v1: Vec<u8> = (0..test_size).map(|i| (i * 13 % 251) as u8).collect();
    let data_v2: Vec<u8> = (0..test_size).map(|i| (i * 47 % 251) as u8).collect();

    let server_state = Arc::new(RwLock::new(MockServerState {
        data: data_v1.clone(),
        etag: "etag-version-1".to_string(),
        options: MockServerOptions {
            chunk_delay_ms: 30,
            ..Default::default()
        },
    }));

    let (server_url, _shutdown) = start_mock_server_stateful(Arc::clone(&server_state)).await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_changed_etag_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);
    let state_path = get_state_path(&temp_path);
    let part_path = get_part_path(&temp_path);

    let manager = DownloadManager::new_in_memory(Some(4)).unwrap();
    let dl_id = manager
        .add(AddDownloadOptions {
            url: server_url.clone(),
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(chunk_size),
            connections: Some(1),
            force: true,
        })
        .await
        .unwrap();

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Ok(Some(state)) = DownloadState::load(&state_path).await
            && !state.completed_chunks.is_empty()
        {
            break;
        }
    }

    manager.pause(&dl_id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let state_before = DownloadState::load(&state_path).await.unwrap().unwrap();
    assert!(state_before.etag.as_deref().unwrap().contains("etag-version-1"));

    // Modify file data and ETag on the mock server
    {
        let mut s = server_state.write().await;
        s.data = data_v2.clone();
        s.etag = "etag-version-2".to_string();
        s.options.chunk_delay_ms = 0;
    }

    // Resume via manager
    manager.resume(&dl_id).await.unwrap();

    let status = wait_for_completion(&manager, &dl_id).await;
    assert_eq!(status, DownloadStatus::Completed);

    assert!(temp_path.exists());
    let downloaded_bytes = tokio::fs::read(&temp_path).await.unwrap();
    assert_eq!(downloaded_bytes, data_v2);

    let _ = tokio::fs::remove_file(&temp_path).await;
    let _ = tokio::fs::remove_file(&part_path).await;
    let _ = tokio::fs::remove_file(&state_path).await;
}

#[tokio::test]
async fn test_integration_neither_etag_nor_last_modified_never_resumes() {
    let test_size = 64 * 1024;
    let chunk_size = 32 * 1024;
    let data: Vec<u8> = (0..test_size).map(|i| (i % 251) as u8).collect();

    let (server_url, _shutdown) = start_mock_server(
        Arc::new(data.clone()),
        MockServerOptions {
            omit_validators: true,
            ..Default::default()
        },
    )
    .await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_no_validator_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);
    let state_path = get_state_path(&temp_path);

    let state = DownloadState::new(
        server_url.clone(),
        server_url.clone(),
        test_size as u64,
        None,
        None,
        chunk_size,
    );
    state.save_atomic(&state_path).await.unwrap();

    let manager = DownloadManager::new_in_memory(Some(2)).unwrap();
    let dl_id = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(chunk_size),
            connections: Some(2),
            force: true,
        })
        .await
        .unwrap();

    let status = wait_for_completion(&manager, &dl_id).await;
    assert_eq!(status, DownloadStatus::Completed);

    assert!(temp_path.exists());
    assert!(!state_path.exists());
    let read_bytes = tokio::fs::read(&temp_path).await.unwrap();
    assert_eq!(read_bytes, data);

    let _ = tokio::fs::remove_file(&temp_path).await;
}

#[tokio::test]
async fn test_integration_if_range_restart_at_most_once() {
    let test_size = 64 * 1024;
    let chunk_size = 32 * 1024;
    let data: Vec<u8> = (0..test_size).map(|i| (i % 251) as u8).collect();

    let (server_url, _shutdown) = start_mock_server(
        Arc::new(data),
        MockServerOptions {
            return_200_on_range: true,
            ..Default::default()
        },
    )
    .await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_200_loop_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);
    let manager = DownloadManager::new_in_memory(Some(2)).unwrap();
    let dl_id = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(chunk_size),
            connections: Some(2),
            force: true,
        })
        .await
        .unwrap();

    let final_status = wait_for_completion(&manager, &dl_id).await;
    assert_eq!(final_status, DownloadStatus::Failed, "Must fail after repeated 200 OK responses");

    let _ = tokio::fs::remove_file(&temp_path).await;
    let _ = tokio::fs::remove_file(get_part_path(&temp_path)).await;
    let _ = tokio::fs::remove_file(get_state_path(&temp_path)).await;
}

#[tokio::test]
async fn test_integration_existing_final_file_without_force_fails() {
    let data: Vec<u8> = vec![1, 2, 3, 4];
    let (server_url, _shutdown) = start_mock_server(Arc::new(data), MockServerOptions::default()).await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_existing_file_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);

    tokio::fs::write(&temp_path, b"existing content").await.unwrap();

    let manager = DownloadManager::new_in_memory(Some(1)).unwrap();
    let result = manager
        .add(AddDownloadOptions {
            url: server_url.clone(),
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(1024),
            connections: Some(1),
            force: false,
        })
        .await;

    assert!(result.is_err(), "Must fail when output file exists and force is false");

    // Force overwrite
    let res_force = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(1024),
            connections: Some(1),
            force: true,
        })
        .await;

    assert!(res_force.is_ok(), "Must succeed when force is true");

    let _ = tokio::fs::remove_file(&temp_path).await;
}

#[tokio::test]
async fn test_integration_orphan_state_cleaned_up() {
    let test_size = 32 * 1024;
    let data: Vec<u8> = (0..test_size).map(|i| (i % 251) as u8).collect();
    let (server_url, _shutdown) = start_mock_server(Arc::new(data.clone()), MockServerOptions::default()).await;

    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let temp_filename = format!("test_orphan_{}_{}.bin", std::process::id(), id);
    let temp_path = PathBuf::from(&temp_filename);
    let state_path = get_state_path(&temp_path);
    let part_path = get_part_path(&temp_path);

    let orphan_state = DownloadState::new(
        server_url.clone(),
        server_url.clone(),
        test_size as u64,
        Some("\"etag\"".to_string()),
        None,
        16 * 1024,
    );
    orphan_state.save_atomic(&state_path).await.unwrap();

    assert!(state_path.exists());
    assert!(!part_path.exists());

    let manager = DownloadManager::new_in_memory(Some(2)).unwrap();
    let dl_id = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp_path.clone()),
            chunk_size: Some(16 * 1024),
            connections: Some(2),
            force: true,
        })
        .await
        .unwrap();

    let status = wait_for_completion(&manager, &dl_id).await;
    assert_eq!(status, DownloadStatus::Completed);

    assert!(temp_path.exists());
    assert!(!state_path.exists());
    let read_bytes = tokio::fs::read(&temp_path).await.unwrap();
    assert_eq!(read_bytes, data);

    let _ = tokio::fs::remove_file(&temp_path).await;
}

// ============================================================================
// New Tests for DownloadManager, Concurrency Limit & Global Speed Limiter
// ============================================================================

#[tokio::test]
async fn test_manager_concurrency_limit_queuing() {
    // Test that when concurrency limit = 1, adding 2 downloads runs them sequentially!
    let data: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let (server_url, _shutdown) = start_mock_server(
        Arc::new(data),
        MockServerOptions {
            chunk_delay_ms: 25,
            ..Default::default()
        },
    )
    .await;

    let manager = DownloadManager::new_in_memory(Some(1)).unwrap();

    let temp1 = PathBuf::from(format!("test_conc_1_{}.bin", std::process::id()));
    let temp2 = PathBuf::from(format!("test_conc_2_{}.bin", std::process::id()));

    let id1 = manager
        .add(AddDownloadOptions {
            url: server_url.clone(),
            destination_path: Some(temp1.clone()),
            chunk_size: Some(4),
            connections: Some(1),
            force: true,
        })
        .await
        .unwrap();

    let id2 = manager
        .add(AddDownloadOptions {
            url: server_url.clone(),
            destination_path: Some(temp2.clone()),
            chunk_size: Some(4),
            connections: Some(1),
            force: true,
        })
        .await
        .unwrap();

    // id2 must initially be Pending because concurrency limit is 1!
    let d2_initial = manager.get(&id2).unwrap().unwrap();
    assert_eq!(d2_initial.status, DownloadStatus::Pending);

    // Wait for id2 completion
    let status2 = wait_for_completion(&manager, &id2).await;
    assert_eq!(status2, DownloadStatus::Completed);

    let d1_final = manager.get(&id1).unwrap().unwrap();
    let d2_final = manager.get(&id2).unwrap().unwrap();
    assert_eq!(d1_final.status, DownloadStatus::Completed);
    assert_eq!(d2_final.status, DownloadStatus::Completed);

    let _ = tokio::fs::remove_file(&temp1).await;
    let _ = tokio::fs::remove_file(&temp2).await;
}

#[tokio::test]
async fn test_manager_global_speed_limiter_runtime_adjustment() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (server_url, _shutdown) = start_mock_server(Arc::new(data), MockServerOptions::default()).await;

    let manager = DownloadManager::new_in_memory(Some(2)).unwrap();

    // Set speed limit to 500 KB/s
    manager.set_speed_limit(Some(512_000)).await;
    assert_eq!(manager.get_speed_limit(), Some(512_000));

    let temp = PathBuf::from(format!("test_speed_mgr_{}.bin", std::process::id()));
    let id = manager
        .add(AddDownloadOptions {
            url: server_url,
            destination_path: Some(temp.clone()),
            chunk_size: Some(32 * 1024),
            connections: Some(2),
            force: true,
        })
        .await
        .unwrap();

    // Change limit dynamically to unlimited while downloading
    manager.set_speed_limit(None).await;
    assert_eq!(manager.get_speed_limit(), None);

    let status = wait_for_completion(&manager, &id).await;
    assert_eq!(status, DownloadStatus::Completed);

    let record = manager.get(&id).unwrap().unwrap();
    assert_eq!(record.status, DownloadStatus::Completed);

    let _ = tokio::fs::remove_file(&temp).await;
}

#[tokio::test]
async fn test_manager_sqlite_queue_persistence_across_instances() {
    let temp_db = std::env::temp_dir().join(format!("test_queue_persist_{}.db", std::process::id()));

    {
        let manager1 = DownloadManager::new(&temp_db, Some(2)).unwrap();
        manager1
            .add(AddDownloadOptions {
                url: "https://example.com/file1.bin".to_string(),
                destination_path: Some(PathBuf::from("file1.bin")),
                chunk_size: Some(1024),
                connections: Some(2),
                force: true,
            })
            .await
            .unwrap();

        let list1 = manager1.list().unwrap();
        assert_eq!(list1.len(), 1);
    }

    // Open a second manager with the same DB file
    {
        let manager2 = DownloadManager::new(&temp_db, Some(2)).unwrap();
        let list2 = manager2.list().unwrap();
        assert_eq!(list2.len(), 1);
        assert_eq!(list2[0].url, "https://example.com/file1.bin");
    }

    let _ = tokio::fs::remove_file(&temp_db).await;
}
