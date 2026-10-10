//! # Probe Module (`probe.rs`)
//!
//! Responsible for discovering server capabilities, inspecting HTTP headers,
//! extracting total file size, and validating range support before downloading.
//!
//! ## Probing Strategy:
//! 1. **HEAD Request**: Sends a lightweight `HEAD` request.
//!    - Checks if `Accept-Ranges: bytes` is present.
//!    - Checks for `Content-Length`.
//!    - Collects `ETag` and `Last-Modified` metadata.
//! 2. **GET Range: bytes=0-0 Fallback**:
//!    - If `HEAD` fails (e.g. 405 Method Not Allowed) or lacks needed headers,
//!      sends a 1-byte request (`Range: bytes=0-0`).
//!    - If the server replies with `206 Partial Content`, extracts total size from
//!      the `Content-Range: bytes 0-0/TOTAL` header.
//!    - If the server replies with `200 OK`, range requests are unsupported.

use anyhow::{Context, Result};
use percent_encoding::percent_decode_str;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE};
use reqwest::{StatusCode, Url};

/// Information gathered during server probing.
#[derive(Debug, Clone)]
pub struct ProbeInfo {
    /// Total size in bytes, if known.
    pub total_size: Option<u64>,
    /// Whether the server confirmed support for HTTP Range requests (`206 Partial Content`).
    pub supports_ranges: bool,
    /// HTTP ETag entity tag, if provided.
    pub etag: Option<String>,
    /// HTTP Last-Modified header, if provided.
    pub last_modified: Option<String>,
    /// Recommended destination filename.
    pub filename: String,
    /// Final URL after following HTTP redirects.
    pub final_url: Url,
}

/// Sanitizes a filename to prevent Directory Traversal attacks (CWE-22).
///
/// Untrusted inputs from servers or URLs might include malicious paths like
/// `../../etc/passwd` or `C:\bad\file.exe`. This function extracts only the safe
/// base filename, stripping path separators and quotes.
pub fn sanitize_filename(raw_name: &str) -> Option<String> {
    let trimmed = raw_name.trim().trim_matches('"').trim_matches('\'');
    let normalized = trimmed.replace('\\', "/");
    let base = normalized.split('/').rfind(|s| !s.is_empty())?;
    let base = base.trim();

    if base.is_empty() || base == "." || base == ".." {
        None
    } else {
        Some(base.to_string())
    }
}

/// Extracts a filename from the HTTP `Content-Disposition` header.
///
/// Implements RFC 6266 / RFC 5987:
/// - Standard: `filename="example.zip"`
/// - Extended: `filename*=UTF-8''example%20file.zip` (supports non-ASCII & percent-encoding)
///
/// If both are present, `filename*` takes precedence as specified by RFC 6266.
pub fn parse_content_disposition(header_val: &str) -> Option<String> {
    let mut regular_filename = None;
    let mut star_filename = None;

    for part in header_val.split(';') {
        let part = part.trim();
        if let Some((key, val)) = part.split_once('=') {
            let key = key.trim().to_ascii_lowercase();
            let val = val.trim();

            if key == "filename*" {
                let encoded_part = if let Some((_, encoded)) = val.split_once("''") {
                    encoded
                } else {
                    val
                };
                if let Ok(decoded) = percent_decode_str(encoded_part).decode_utf8() {
                    star_filename = sanitize_filename(&decoded);
                }
            } else if key == "filename" {
                regular_filename = sanitize_filename(val);
            }
        }
    }

    star_filename.or(regular_filename)
}

/// Extracts a filename from the final URL path segment, percent-decoding it.
pub fn parse_url_filename(url: &Url) -> Option<String> {
    let last_segment = url.path_segments()?.rfind(|s| !s.is_empty())?;
    let decoded = percent_decode_str(last_segment).decode_utf8().ok()?;
    sanitize_filename(&decoded)
}

/// Determines the best destination filename.
///
/// Priority:
/// 1. `Content-Disposition` response header
/// 2. URL path segment
/// 3. Default fallback: `"downloaded_file"`
pub fn determine_filename(headers: &reqwest::header::HeaderMap, url: &Url) -> String {
    if let Some(name) = headers
        .get(CONTENT_DISPOSITION)
        .and_then(|val| val.to_str().ok())
        .and_then(parse_content_disposition)
    {
        return name;
    }

    if let Some(name) = parse_url_filename(url) {
        return name;
    }

    "downloaded_file".to_string()
}

/// Parses the total length from a `Content-Range` header.
///
/// Example: `bytes 0-0/10485760` -> `Some(10485760)`
pub fn parse_content_range_total(header_val: &str) -> Option<u64> {
    let slash_idx = header_val.rfind('/')?;
    let total_part = header_val.get(slash_idx + 1..)?.trim();
    total_part.parse::<u64>().ok()
}

/// Verifies that a server's `Content-Range` response header matches the requested chunk range.
///
/// Expected format: `bytes {start}-{end}/{total}`
/// Example: `bytes 0-4194303/10485760`
pub fn verify_content_range(header_val: &str, expected_start: u64, expected_end: u64) -> bool {
    let trimmed = header_val.trim();
    let Some(range_spec) = trimmed.strip_prefix("bytes ") else {
        return false;
    };
    let Some((range_part, _)) = range_spec.split_once('/') else {
        return false;
    };
    let Some((start_str, end_str)) = range_part.split_once('-') else {
        return false;
    };

    let Ok(start) = start_str.trim().parse::<u64>() else {
        return false;
    };
    let Ok(end) = end_str.trim().parse::<u64>() else {
        return false;
    };

    start == expected_start && end == expected_end
}

/// Probes the server to extract size, range capabilities, and metadata.
pub async fn probe_server(client: &reqwest::Client, url: &Url) -> Result<ProbeInfo> {
    println!("Probing server at {} ...", url);

    // Step A: Attempt lightweight HEAD request
    let head_result = client.head(url.clone()).send().await;

    if let Ok(resp) = head_result
        && resp.status().is_success()
    {
        let accepts_ranges = resp
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        let content_length = resp.content_length();
        let etag = resp
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let last_modified = resp
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let filename = determine_filename(resp.headers(), resp.url());
        let final_url = resp.url().clone();

        // If HEAD succeeded and provided range support and content length, probe is complete!
        if accepts_ranges && content_length.is_some_and(|len| len > 0) {
            return Ok(ProbeInfo {
                total_size: content_length,
                supports_ranges: true,
                etag,
                last_modified,
                filename,
                final_url,
            });
        }
    }

    // Step B: Fallback to GET with Range: bytes=0-0
    println!("HEAD request inconclusive. Probing with 'Range: bytes=0-0' ...");
    let range_resp = client
        .get(url.clone())
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .with_context(|| format!("Failed to send probe GET request to '{}'", url))?;

    let filename = determine_filename(range_resp.headers(), range_resp.url());
    let final_url = range_resp.url().clone();
    let etag = range_resp
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let last_modified = range_resp
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if range_resp.status() == StatusCode::PARTIAL_CONTENT {
        // 206 Partial Content confirms range support!
        let total_size = range_resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range_total);

        let supports_ranges = total_size.is_some_and(|len| len > 0);

        Ok(ProbeInfo {
            total_size,
            supports_ranges,
            etag,
            last_modified,
            filename,
            final_url,
        })
    } else {
        // Server returned 200 OK or another status: range requests are unsupported
        let total_size = range_resp.content_length();
        Ok(ProbeInfo {
            total_size,
            supports_ranges: false,
            etag,
            last_modified,
            filename,
            final_url,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("file.txt"), Some("file.txt".to_string()));
        assert_eq!(sanitize_filename("\"quoted.iso\""), Some("quoted.iso".to_string()));
        assert_eq!(sanitize_filename("../../etc/shadow"), Some("shadow".to_string()));
        assert_eq!(sanitize_filename("C:\\Path\\To\\File.exe"), Some("File.exe".to_string()));
        assert_eq!(sanitize_filename(""), None);
        assert_eq!(sanitize_filename(".."), None);
        assert_eq!(sanitize_filename("."), None);
    }

    #[test]
    fn test_parse_content_disposition() {
        let regular = r#"attachment; filename="archive.tar.gz""#;
        assert_eq!(parse_content_disposition(regular), Some("archive.tar.gz".to_string()));

        let star = "attachment; filename*=UTF-8''my%20file.zip";
        assert_eq!(parse_content_disposition(star), Some("my file.zip".to_string()));

        let both = r#"attachment; filename="old.zip"; filename*=UTF-8''new.zip"#;
        assert_eq!(parse_content_disposition(both), Some("new.zip".to_string()));
    }

    #[test]
    fn test_parse_content_range_total() {
        assert_eq!(parse_content_range_total("bytes 0-0/10485760"), Some(10485760));
        assert_eq!(parse_content_range_total("bytes 100-200/5000"), Some(5000));
        assert_eq!(parse_content_range_total("bytes 0-0/*"), None);
        assert_eq!(parse_content_range_total("invalid"), None);
    }

    #[test]
    fn test_verify_content_range() {
        assert!(verify_content_range("bytes 0-4194303/10000000", 0, 4194303));
        assert!(verify_content_range("bytes 4194304-8388607/10000000", 4194304, 8388607));
        assert!(!verify_content_range("bytes 0-1000/10000000", 0, 999));
        assert!(!verify_content_range("invalid", 0, 1000));
    }
}
