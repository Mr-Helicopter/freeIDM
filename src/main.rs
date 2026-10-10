//! # Rust CLI File Downloader
//!
//! A fast, asynchronous command-line file downloader inspired by IDM / wget / curl.
//!
//! Features:
//! - Asynchronous HTTP streaming using `reqwest` and `tokio`.
//! - Streams chunks directly to disk to prevent high memory (RAM) usage.
//! - Extracts filename automatically from `Content-Disposition` header or the URL path.
//! - Visual progress bar with download speed, ETA, and byte counters using `indicatif`.
//! - Comprehensive error handling using `anyhow`.

use std::env;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use percent_encoding::percent_decode_str;
use reqwest::header::{CONTENT_DISPOSITION, HeaderMap};
use reqwest::Url;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

// ============================================================================
// SECTION 1: Filename Extraction & Security Sanitization
// ============================================================================

/// Sanitizes a filename string to prevent Directory Traversal attacks (CWE-22).
///
/// In security, untrusted inputs (like server headers or URLs) might contain paths
/// such as `../../etc/passwd` or `C:\Windows\System32\cmd.exe`.
/// This helper extracts only the final file component, stripping any directory
/// separators (both `/` and `\`) and surrounding quotes or whitespace.
fn sanitize_filename(raw_name: &str) -> Option<String> {
    // 1. Strip whitespace and quotes that often wrap header values
    let trimmed = raw_name.trim().trim_matches('"').trim_matches('\'');

    // 2. Normalize Windows backslashes (`\`) to forward slashes (`/`)
    //    so this works uniformly across Windows, macOS, and Linux.
    let normalized = trimmed.replace('\\', "/");

    // 3. Take the last non-empty component after the last slash
    let base = normalized.split('/').rfind(|s| !s.is_empty())?;
    let base = base.trim();

    // 4. Reject dangerous special directory names like "." or ".."
    if base.is_empty() || base == "." || base == ".." {
        None
    } else {
        Some(base.to_string())
    }
}

/// Parses the `Content-Disposition` HTTP header to extract the recommended filename.
///
/// RFC 6266 and RFC 5987 define how headers convey filenames:
/// - `filename="example.zip"` (standard parameter)
/// - `filename*=UTF-8''example%20file.zip` (extended parameter with charset & percent-encoding)
///
/// If both are present, RFC 6266 specifies that `filename*` takes precedence.
fn parse_content_disposition(header_val: &str) -> Option<String> {
    let mut regular_filename = None;
    let mut star_filename = None;

    // Parameters are separated by semicolons (e.g. `attachment; filename="test.pdf"`)
    for part in header_val.split(';') {
        let part = part.trim();

        // Split key and value on the first '=' character
        if let Some((key, val)) = part.split_once('=') {
            let key = key.trim().to_ascii_lowercase();
            let val = val.trim();

            if key == "filename*" {
                // Extended format: charset'lang'encoded_value (e.g. UTF-8''%e2%82%ac%20rates.pdf)
                let encoded_part = if let Some((_, encoded)) = val.split_once("''") {
                    encoded
                } else {
                    val
                };

                // Decode percent-encoded characters (like %20 -> space)
                if let Ok(decoded) = percent_decode_str(encoded_part).decode_utf8() {
                    star_filename = sanitize_filename(&decoded);
                }
            } else if key == "filename" {
                // Standard format: filename="test.zip" or filename=test.zip
                regular_filename = sanitize_filename(val);
            }
        }
    }

    // Prefer `filename*` over `filename`, as recommended by RFC 6266
    star_filename.or(regular_filename)
}

/// Extracts a filename from the URL path.
///
/// For example:
/// - `https://example.com/downloads/v1.2.0/app.tar.gz` -> `app.tar.gz`
/// - `https://example.com/files/hello%20world.txt?token=xyz` -> `hello world.txt`
fn parse_url_filename(url: &Url) -> Option<String> {
    // `path_segments()` yields an iterator of path slices between '/'
    let last_segment = url.path_segments()?.rfind(|s| !s.is_empty())?;

    // Percent-decode the segment (e.g., `%20` becomes a space ` `)
    let decoded = percent_decode_str(last_segment).decode_utf8().ok()?;

    sanitize_filename(&decoded)
}

/// Determines the best filename for saving the download.
///
/// Priority:
/// 1. `Content-Disposition` response header (if provided by server)
/// 2. URL path segment (from the final URL after any redirects)
/// 3. Default fallback: `"downloaded_file"`
fn determine_filename(headers: &HeaderMap, url: &Url) -> String {
    // 1. Try Content-Disposition header
    if let Some(name) = headers
        .get(CONTENT_DISPOSITION)
        .and_then(|val| val.to_str().ok())
        .and_then(parse_content_disposition)
    {
        return name;
    }

    // 2. Try the URL path
    if let Some(name) = parse_url_filename(url) {
        return name;
    }

    // 3. Fallback when neither contains a valid filename (e.g. root URL `https://example.com/`)
    "downloaded_file".to_string()
}

// ============================================================================
// SECTION 2: Progress Bar Configuration
// ============================================================================

/// Creates and configures an `indicatif::ProgressBar`.
///
/// If the server provided a `Content-Length` header (`total_size` is Some):
/// - A bounded progress bar is displayed showing:
///   - Spinner icon
///   - Elapsed time
///   - Progress visual bar (`[====>---]`)
///   - Downloaded bytes / Total bytes
///   - Current transfer speed (e.g., 5.2 MB/s)
///   - Estimated time of arrival (ETA)
///
/// If the server uses chunked transfer encoding (`total_size` is None):
/// - An unbounded spinner is displayed showing bytes downloaded and current speed.
fn setup_progress_bar(total_size: Option<u64>) -> Result<ProgressBar> {
    match total_size {
        Some(total) => {
            let pb = ProgressBar::new(total);

            // Progress bar template documentation:
            // {spinner:.green}      - Animated spinner in green
            // [{elapsed_precise}]   - Elapsed time in HH:MM:SS format
            // [{bar:40.cyan/blue}]  - Progress bar 40 characters wide with cyan filled & blue background
            // {bytes}/{total_bytes} - Transferred bytes / Total bytes (formatted with units like MB)
            // {bytes_per_sec}       - Transfer rate in bytes/sec (auto formatted)
            // {eta}                 - Estimated time remaining
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

// ============================================================================
// SECTION 3: Main Asynchronous Entry Point
// ============================================================================

/// `#[tokio::main]` initializes the asynchronous Tokio runtime and wraps `main`.
/// Any function marked `async` can use `.await` to pause execution non-blockingly
/// until an asynchronous operation (like a network read or disk write) completes.
///
/// `anyhow::Result<()>` allows using the `?` operator to return early with an error
/// while providing detailed contextual information with `.with_context(...)`.
#[tokio::main]
async fn main() -> Result<()> {
    // ------------------------------------------------------------------------
    // Step 1: Read Command-Line Arguments
    // ------------------------------------------------------------------------
    let args: Vec<String> = env::args().collect();

    // Check if the user ran the program without arguments or asked for help
    if args.len() < 2 || args[1] == "--help" || args[1] == "-h" {
        println!("IDM - Rust CLI File Downloader");
        println!();
        println!("Usage:");
        println!("  {} <URL>", args[0]);
        println!();
        println!("Example:");
        println!("  {} https://speed.hetzner.de/100MB.bin", args[0]);

        if args.len() < 2 {
            bail!("Error: Missing required <URL> argument.");
        }
        return Ok(());
    }

    let url_input = &args[1];

    // Parse and validate the input URL
    let url = Url::parse(url_input)
        .with_context(|| format!("Invalid URL '{}'. Please provide a full URL with scheme (e.g., https://...)", url_input))?;

    if url.scheme() != "http" && url.scheme() != "https" {
        bail!("Unsupported protocol '{}'. Only 'http' and 'https' are supported.", url.scheme());
    }

    // ------------------------------------------------------------------------
    // Step 2: Initialize HTTP Client & Send Request
    // ------------------------------------------------------------------------
    println!("Connecting to {} ...", url);

    // Build an HTTP client with a descriptive User-Agent
    let client = reqwest::Client::builder()
        .user_agent("idm-cli/0.1.0")
        .build()
        .context("Failed to build HTTP client")?;

    // Send the GET request asynchronously
    // reqwest follows HTTP redirects (301, 302, 307, 308) automatically by default.
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Failed to connect to '{}'", url))?;

    // Verify HTTP status code (checks for 404, 500, etc.)
    let response = response
        .error_for_status()
        .context("Server returned an HTTP error status")?;

    // ------------------------------------------------------------------------
    // Step 3: Determine Destination Filename & File Size
    // ------------------------------------------------------------------------
    // Notice: `response.url()` gives the *final* URL after following any redirects!
    let filename = determine_filename(response.headers(), response.url());
    let total_size = response.content_length();

    println!("Saving to:   {}", filename);
    if let Some(total) = total_size {
        println!("File size:   {}", HumanBytes(total));
    } else {
        println!("File size:   Unknown (server did not send Content-Length)");
    }

    // Warn if a file with the same name already exists in the current directory
    if Path::new(&filename).exists() {
        println!("Notice:      File '{}' already exists and will be overwritten.", filename);
    }

    // ------------------------------------------------------------------------
    // Step 4: Open Local File for Asynchronous Writing
    // ------------------------------------------------------------------------
    // `tokio::fs::File::create` creates the file asynchronously without blocking the thread.
    let mut file = File::create(&filename)
        .await
        .with_context(|| format!("Failed to create local file '{}'", filename))?;

    // ------------------------------------------------------------------------
    // Step 5: Configure and Start the Progress Bar
    // ------------------------------------------------------------------------
    let pb = setup_progress_bar(total_size)?;

    // ------------------------------------------------------------------------
    // Step 6: Stream Response to Disk in Chunks
    // ------------------------------------------------------------------------
    // CRITICAL DESIGN POINT:
    // Instead of calling `response.bytes().await` (which buffers the ENTIRE file
    // in RAM, easily crashing the program on large files like videos or ISOs),
    // we use `response.chunk().await` in a loop.
    //
    // Each chunk (typically 8KB - 64KB) is:
    // 1. Received from the network socket asynchronously
    // 2. Written immediately to disk
    // 3. Used to update the progress bar
    // 4. Dropped from memory
    let mut mut_response = response;
    let mut downloaded_bytes: u64 = 0;

    while let Some(chunk) = mut_response
        .chunk()
        .await
        .context("Error encountered while receiving stream data from server")?
    {
        // Write the chunk to disk asynchronously
        file.write_all(&chunk)
            .await
            .with_context(|| format!("Failed to write chunk to file '{}'", filename))?;

        let chunk_len = chunk.len() as u64;
        downloaded_bytes += chunk_len;

        // Advance the progress bar counter
        pb.inc(chunk_len);
    }

    // Flush any OS write buffers to ensure all bytes are physically written to disk
    file.flush()
        .await
        .with_context(|| format!("Failed to flush file '{}'", filename))?;

    // ------------------------------------------------------------------------
    // Step 7: Completion
    // ------------------------------------------------------------------------
    pb.finish_and_clear();
    println!(
        "\n Download completed successfully: '{}' ({})",
        filename,
        HumanBytes(downloaded_bytes)
    );

    Ok(())
}

// ============================================================================
// SECTION 4: Unit Tests
// ============================================================================
// In Rust, tests are usually placed at the bottom in a `tests` module.
// Run with: `cargo test`
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("test.txt"), Some("test.txt".to_string()));
        assert_eq!(sanitize_filename("\"quoted.zip\""), Some("quoted.zip".to_string()));
        // Strips path traversal sequences
        assert_eq!(sanitize_filename("../../etc/passwd"), Some("passwd".to_string()));
        assert_eq!(sanitize_filename("C:\\Windows\\System32\\cmd.exe"), Some("cmd.exe".to_string()));
        // Disallows empty or root
        assert_eq!(sanitize_filename(""), None);
        assert_eq!(sanitize_filename(".."), None);
        assert_eq!(sanitize_filename("."), None);
    }

    #[test]
    fn test_parse_content_disposition_regular() {
        let header = r#"attachment; filename="archive.tar.gz""#;
        assert_eq!(parse_content_disposition(header), Some("archive.tar.gz".to_string()));

        let unquoted = "attachment; filename=image.png";
        assert_eq!(parse_content_disposition(unquoted), Some("image.png".to_string()));
    }

    #[test]
    fn test_parse_content_disposition_extended_utf8() {
        // RFC 5987 / 6266 extended syntax with percent-encoding
        let header = "attachment; filename*=UTF-8''my%20document.pdf";
        assert_eq!(parse_content_disposition(header), Some("my document.pdf".to_string()));

        // Extended takes precedence over regular
        let both = r#"attachment; filename="fallback.pdf"; filename*=UTF-8''special%20name.pdf"#;
        assert_eq!(parse_content_disposition(both), Some("special name.pdf".to_string()));
    }

    #[test]
    fn test_parse_url_filename() {
        let url1 = Url::parse("https://example.com/downloads/setup.exe").unwrap();
        assert_eq!(parse_url_filename(&url1), Some("setup.exe".to_string()));

        let url2 = Url::parse("https://example.com/files/hello%20world.txt?query=123").unwrap();
        assert_eq!(parse_url_filename(&url2), Some("hello world.txt".to_string()));

        let url_no_file = Url::parse("https://example.com/").unwrap();
        assert_eq!(parse_url_filename(&url_no_file), None);
    }

    #[test]
    fn test_determine_filename_fallback() {
        let headers = HeaderMap::new();
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(determine_filename(&headers, &url), "downloaded_file");
    }
}
