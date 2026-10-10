//! # Chunks Module (`chunks.rs`)
//!
//! Handles calculating and dividing the download workload into byte ranges.
//!
//! ## Inclusive Bounds in HTTP:
//! In HTTP (RFC 9110), range requests use **inclusive** indices:
//! `Range: bytes=start-end`
//! For example, `bytes=0-1023` requests 1024 bytes (bytes 0 through 1023 inclusive).
//! To avoid off-by-one errors:
//! - A chunk starting at `start` with size `S` ends at `start + S - 1`.
//! - The last chunk in the file ends exactly at `total_size - 1`.

/// Represents an individual byte-range chunk to be downloaded by a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Zero-based sequential chunk index (0, 1, 2, ...).
    pub index: usize,
    /// Starting byte offset (inclusive, 0-indexed).
    pub start: u64,
    /// Ending byte offset (inclusive).
    pub end: u64,
}

impl Chunk {
    /// Number of bytes represented by this chunk range.
    pub fn len(&self) -> u64 {
        if self.start > self.end {
            0
        } else {
            self.end - self.start + 1
        }
    }

    /// Whether this chunk contains zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Splits the file into fixed-size chunks.
///
/// # Arguments
/// * `total_size` - Total size of the file in bytes.
/// * `chunk_size` - Desired size of each chunk in bytes (e.g., 4 MiB).
///
/// # Returns
/// A vector of [`Chunk`] structs covering the entire byte range from `0` to `total_size - 1`.
pub fn calculate_chunks(total_size: u64, chunk_size: u64) -> Vec<Chunk> {
    if total_size == 0 || chunk_size == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0;
    let mut index = 0;

    while start < total_size {
        // Calculate inclusive end, capped at total_size - 1
        let end = std::cmp::min(start + chunk_size - 1, total_size - 1);
        chunks.push(Chunk { index, start, end });
        start = end + 1;
        index += 1;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_chunks_edge_cases() {
        // 1. Smaller than chunk size: 15 bytes with 64 byte chunk size -> 1 chunk [0..14]
        let chunks = calculate_chunks(15, 64);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], Chunk { index: 0, start: 0, end: 14 });
        assert_eq!(chunks[0].len(), 15);

        // 2. Exact multiple of chunk size: 128 bytes with 64 byte chunks -> 2 chunks
        let chunks = calculate_chunks(128, 64);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], Chunk { index: 0, start: 0, end: 63 });
        assert_eq!(chunks[1], Chunk { index: 1, start: 64, end: 127 });
        assert_eq!(chunks[0].len() + chunks[1].len(), 128);

        // 3. Not divisible by chunk size: 150 bytes with 64 byte chunks -> 3 chunks (64 + 64 + 22)
        let chunks = calculate_chunks(150, 64);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], Chunk { index: 0, start: 0, end: 63 });
        assert_eq!(chunks[1], Chunk { index: 1, start: 64, end: 127 });
        assert_eq!(chunks[2], Chunk { index: 2, start: 128, end: 149 });
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<u64>(), 150);

        // 4. Zero length
        let chunks = calculate_chunks(0, 64);
        assert!(chunks.is_empty());
    }
}
