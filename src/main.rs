//! # IDM - Parallel Segmented CLI File Downloader in Rust
//!
//! A modular, high-performance command-line file downloader inspired by IDM.
//!
//! ## Modules:
//! - [`probe`]: Server capability discovery, HEAD / GET 0-0 probing, filename sanitization.
//! - [`chunks`]: Byte range partitioning with inclusive HTTP range bounds.
//! - [`segmented`]: Multi-connection parallel downloading, work queue, and retries.
//! - [`single`]: Stage 1 single-stream fallback with explicit file truncation.
//! - [`progress`]: Indicatif progress bar styling and initialization.

pub mod chunks;
pub mod probe;
pub mod progress;
pub mod segmented;
pub mod single;

use std::env;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use indicatif::HumanBytes;
use reqwest::Url;

use crate::probe::probe_server;
use crate::segmented::{download_segmented, ChunkError, DEFAULT_CHUNK_SIZE, DEFAULT_CONNECTIONS};
use crate::single::download_single_stream;

// ============================================================================
// CLI Configuration & Argument Parsing
// ============================================================================

/// Stores the user's parsed command-line configuration options.
#[derive(Debug, Clone)]
pub struct CliConfig {
    /// The target HTTP/HTTPS URL.
    pub url: String,
    /// Number of concurrent worker connections (default 8).
    pub connections: usize,
    /// Size of each chunk in bytes (default 4 MiB).
    pub chunk_size: u64,
    /// Optional output file path override (useful in unit/integration tests).
    pub output_path: Option<PathBuf>,
}

impl CliConfig {
    /// Parses CLI arguments from a slice of strings.
    pub fn parse_from_args(args: &[String]) -> Result<Option<Self>> {
        if args.len() < 2 {
            Self::print_help(&args[0]);
            bail!("Error: Missing required <URL> argument.");
        }

        let mut url: Option<String> = None;
        let mut connections = DEFAULT_CONNECTIONS;
        let mut chunk_size = DEFAULT_CHUNK_SIZE;
        let mut output_path = None;

        let mut i = 1;
        while i < args.len() {
            let arg = &args[i];
            if arg == "--help" || arg == "-h" {
                Self::print_help(&args[0]);
                return Ok(None);
            } else if arg == "--connections" || arg == "-c" {
                i += 1;
                if i >= args.len() {
                    bail!("Error: Flag '{}' requires a numerical value.", arg);
                }
                connections = args[i]
                    .parse::<usize>()
                    .with_context(|| format!("Invalid connections count '{}'", args[i]))?;
                if connections == 0 {
                    bail!("Error: Connections count must be at least 1.");
                }
            } else if arg == "--chunk-size" || arg == "-s" {
                i += 1;
                if i >= args.len() {
                    bail!("Error: Flag '{}' requires a byte size value.", arg);
                }
                chunk_size = args[i]
                    .parse::<u64>()
                    .with_context(|| format!("Invalid chunk size '{}'", args[i]))?;
                if chunk_size == 0 {
                    bail!("Error: Chunk size must be greater than 0.");
                }
            } else if arg == "--output" || arg == "-o" {
                i += 1;
                if i >= args.len() {
                    bail!("Error: Flag '{}' requires a file path.", arg);
                }
                output_path = Some(PathBuf::from(&args[i]));
            } else if arg.starts_with('-') {
                bail!("Unknown flag '{}'. Run with --help for usage.", arg);
            } else if url.is_none() {
                url = Some(arg.clone());
            } else {
                bail!("Unexpected additional argument '{}'.", arg);
            }
            i += 1;
        }

        let url = url.context("Error: No URL provided.")?;

        Ok(Some(CliConfig {
            url,
            connections,
            chunk_size,
            output_path,
        }))
    }

    /// Prints a friendly help message explaining usage and options.
    pub fn print_help(program_name: &str) {
        println!("IDM - Rust Parallel Segmented CLI File Downloader");
        println!();
        println!("Usage:");
        println!("  {} [OPTIONS] <URL>", program_name);
        println!();
        println!("Options:");
        println!("  -c, --connections <N>   Number of concurrent connections (default: {})", DEFAULT_CONNECTIONS);
        println!("  -s, --chunk-size <B>    Fixed chunk size in bytes (default: {} [4 MiB])", DEFAULT_CHUNK_SIZE);
        println!("  -o, --output <PATH>     Destination file path (default: auto-detected)");
        println!("  -h, --help              Print this help information");
        println!();
        println!("Examples:");
        println!("  {} https://speed.hetzner.de/100MB.bin", program_name);
        println!("  {} -c 16 https://speed.hetzner.de/100MB.bin", program_name);
    }
}

// ============================================================================
// Main Orchestration
// ============================================================================

/// Coordinates probing, method selection (segmented vs single-stream), and fallback.
pub async fn run_downloader(config: CliConfig) -> Result<()> {
    let url = Url::parse(&config.url)
        .with_context(|| format!("Invalid URL '{}'. Please include protocol (https://...)", config.url))?;

    if url.scheme() != "http" && url.scheme() != "https" {
        bail!("Unsupported URL scheme '{}'. Only http and https are supported.", url.scheme());
    }

    let client = reqwest::Client::builder()
        .user_agent("idm-cli/0.2.0")
        .build()
        .context("Failed to build HTTP client")?;

    // 1. Probe the server
    let probe = probe_server(&client, &url).await?;

    // 2. Determine target filename and destination path
    let filename = if let Some(ref path) = config.output_path {
        path.clone()
    } else {
        PathBuf::from(&probe.filename)
    };

    println!("Target URL:   {}", probe.final_url);
    println!("Destination:  {}", filename.display());
    if let Some(total) = probe.total_size {
        println!("File Size:    {}", HumanBytes(total));
    } else {
        println!("File Size:    Unknown (chunked / dynamic stream)");
    }
    println!(
        "Range Support: {}",
        if probe.supports_ranges { "Yes (Accept-Ranges: bytes)" } else { "No" }
    );

    // 3. Select download strategy
    match (probe.supports_ranges, probe.total_size) {
        (true, Some(total_size)) if total_size > 0 => {
            // Attempt segmented parallel download
            match download_segmented(
                &client,
                &probe.final_url,
                &filename,
                total_size,
                config.chunk_size,
                config.connections,
            )
            .await
            {
                Ok(()) => {
                    println!(
                        "\n Segmented download completed successfully: '{}' ({})",
                        filename.display(),
                        HumanBytes(total_size)
                    );
                    Ok(())
                }
                Err(ChunkError::ServerReturned200) => {
                    println!("\n Notice: Server returned 200 OK instead of 206 Partial Content.");
                    println!("Falling back to Stage 1 single-stream download ...");
                    download_single_stream(&client, &probe.final_url, &filename, Some(total_size)).await
                }
                Err(ChunkError::Fatal(e)) => Err(e),
            }
        }
        _ => {
            // Fallback to single-stream directly
            if !probe.supports_ranges {
                println!("Server does not support range requests. Using single-stream fallback.");
            } else {
                println!("File size is unknown. Using single-stream fallback.");
            }
            download_single_stream(&client, &probe.final_url, &filename, probe.total_size).await
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config = CliConfig::parse_from_args(&args)?;

    let Some(config) = config else {
        return Ok(());
    };

    run_downloader(config).await
}

// ============================================================================
// Unit & Integration Tests
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Options to configure the behavior of the test HTTP mock server.
    #[derive(Default, Clone)]
    pub struct MockServerOptions {
        /// If true, always return 200 OK instead of 206 Partial Content.
        pub force_no_ranges: bool,
        /// If true, abruptly close the socket after sending only partial data on the first chunk request.
        pub drop_mid_chunk_first_attempt: bool,
        /// If true, return a Content-Range header that does not match the client's requested bounds.
        pub mismatched_content_range: bool,
    }

    /// Spawns a lightweight local HTTP mock server supporting Range requests.
    async fn start_mock_server(
        data: Arc<Vec<u8>>,
        options: MockServerOptions,
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
                        let data = Arc::clone(&data);
                        let options = options.clone();
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
                            for line in lines {
                                if let Some(val) = line.strip_prefix("range: ").or_else(|| line.strip_prefix("Range: ")) {
                                    range_header = Some(val.trim().to_string());
                                }
                            }

                            let total_len = data.len();

                            if method == "HEAD" {
                                if options.force_no_ranges {
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                        total_len
                                    );
                                    let _ = socket.write_all(response.as_bytes()).await;
                                } else {
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                        total_len
                                    );
                                    let _ = socket.write_all(response.as_bytes()).await;
                                }
                            } else if method == "GET" {
                                if !options.force_no_ranges && range_header.as_deref().unwrap_or("").starts_with("bytes=") {
                                    let range_val = range_header.unwrap();
                                    let range_spec = range_val.trim_start_matches("bytes=");
                                    if let Some((start_s, end_s)) = range_spec.split_once('-') {
                                        let start: usize = start_s.parse().unwrap_or(0);
                                        let end: usize = end_s.parse().unwrap_or(total_len.saturating_sub(1));
                                        let end = std::cmp::min(end, total_len.saturating_sub(1));

                                        if start <= end && start < total_len {
                                            // Case A: Simulated Mismatched Content-Range
                                            if options.mismatched_content_range {
                                                let header = format!(
                                                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 99999-99999/{}\r\nContent-Length: 1\r\nConnection: close\r\n\r\n",
                                                    total_len
                                                );
                                                let _ = socket.write_all(header.as_bytes()).await;
                                                let _ = socket.write_all(&[42]).await;
                                                return;
                                            }

                                            // Case B: Simulated Connection Drop Mid-Chunk on First Attempt
                                            if options.drop_mid_chunk_first_attempt && !dropped_once.swap(true, Ordering::SeqCst) {
                                                let slice = &data[start..=end];
                                                let header = format!(
                                                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                                    start, end, total_len, slice.len()
                                                );
                                                let _ = socket.write_all(header.as_bytes()).await;

                                                // Send only a partial fragment, then abruptly shutdown socket
                                                let partial_len = std::cmp::min(10, slice.len() / 2);
                                                let _ = socket.write_all(&slice[..partial_len]).await;
                                                let _ = socket.shutdown().await;
                                                return;
                                            }

                                            // Normal successful 206 Partial Content response
                                            let slice = &data[start..=end];
                                            let header = format!(
                                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                                start, end, total_len, slice.len()
                                            );
                                            let _ = socket.write_all(header.as_bytes()).await;
                                            let _ = socket.write_all(slice).await;
                                            return;
                                        }
                                    }
                                }

                                // Fallback normal 200 OK response
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    total_len
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

    /// Helper to run an integration test against the mock server with configurable options.
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

        let config = CliConfig {
            url: server_url,
            connections,
            chunk_size,
            output_path: Some(temp_path.clone()),
        };

        let result = run_downloader(config).await;

        if result.is_ok() {
            let downloaded_bytes = tokio::fs::read(&temp_path)
                .await
                .expect("Downloaded file should be readable on success");

            assert_eq!(
                downloaded_bytes.len(),
                source_data.len(),
                "Downloaded file size does not match source!"
            );
            assert_eq!(
                downloaded_bytes, source_data,
                "Downloaded file bytes do not match source byte-for-byte!"
            );
        }

        let _ = tokio::fs::remove_file(&temp_path).await;
        result
    }

    // --- Standard Integration Tests ---

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
        let mut options = MockServerOptions::default();
        options.force_no_ranges = true;
        run_test_download_with_options(75 * 1024, 16 * 1024, 4, options)
            .await
            .expect("Fallback to single-stream should succeed");
    }

    // --- New Tests (Requirement 4) ---

    /// Tests that when a connection drops mid-chunk on the first attempt,
    /// the worker detects the dropped connection as a retryable error, rolls back
    /// the progress counter, retries the chunk from the start, and completes
    /// the download with byte-identical output.
    #[tokio::test]
    async fn test_integration_dropped_connection_retry() {
        let mut options = MockServerOptions::default();
        options.drop_mid_chunk_first_attempt = true;

        run_test_download_with_options(64 * 1024, 32 * 1024, 2, options)
            .await
            .expect("Download should recover from dropped connection and succeed on retry");
    }

    /// Tests that when a server returns a mismatched Content-Range header,
    /// the downloader rejects it immediately as a fatal error without retrying.
    #[tokio::test]
    async fn test_integration_mismatched_content_range() {
        let mut options = MockServerOptions::default();
        options.mismatched_content_range = true;

        let result = run_test_download_with_options(64 * 1024, 32 * 1024, 2, options).await;
        assert!(result.is_err(), "Download must fail when Content-Range is mismatched");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Content-Range mismatch"),
            "Error message should clearly identify the Content-Range mismatch: '{}'",
            err_msg
        );
    }
}
