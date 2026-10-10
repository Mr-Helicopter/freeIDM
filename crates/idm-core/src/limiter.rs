//! # Global Speed Limiter Module (`limiter.rs`)
//!
//! Implements a token bucket rate limiter shared across all active downloads and workers.
//!
//! ## Token Bucket Mechanics:
//! 1. Tokens represent transferable bytes.
//! 2. The bucket accumulates tokens continuously at a configured rate (bytes per second).
//! 3. The bucket has a maximum capacity to prevent unbounded burst transfers after idle periods.
//! 4. When workers read data from the network, they request tokens via `acquire(bytes).await`.
//! 5. If insufficient tokens are present, the worker asynchronously sleeps until enough tokens refill.
//! 6. The speed limit can be reconfigured dynamically at runtime without restarting downloads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

/// State of the token bucket.
#[derive(Debug)]
struct BucketState {
    /// Available byte tokens in the bucket.
    tokens: f64,
    /// Maximum burst capacity of tokens.
    max_tokens: f64,
    /// Timestamp of the last token balance recalculation.
    last_update: Instant,
}

/// A thread-safe, asynchronous token bucket speed limiter.
#[derive(Debug)]
pub struct SpeedLimiter {
    /// Configured rate in bytes per second. `0` indicates unlimited bandwidth.
    rate_bytes_per_sec: AtomicU64,
    /// Internal bucket state protected by an asynchronous Mutex.
    state: Mutex<BucketState>,
}

impl Default for SpeedLimiter {
    fn default() -> Self {
        Self::new(None)
    }
}

impl SpeedLimiter {
    /// Creates a new `SpeedLimiter`.
    ///
    /// # Arguments
    /// * `rate_bytes_per_sec` - Optional maximum speed limit in bytes per second.
    ///   `None` or `Some(0)` disables rate limiting (unlimited).
    pub fn new(rate_bytes_per_sec: Option<u64>) -> Self {
        let rate = rate_bytes_per_sec.unwrap_or(0);
        let max_tokens = if rate > 0 {
            (rate as f64).max(64.0 * 1024.0)
        } else {
            0.0
        };

        Self {
            rate_bytes_per_sec: AtomicU64::new(rate),
            state: Mutex::new(BucketState {
                tokens: max_tokens,
                max_tokens,
                last_update: Instant::now(),
            }),
        }
    }

    /// Dynamically updates the speed limit at runtime.
    ///
    /// Passing `None` or `Some(0)` removes any active speed restriction.
    pub async fn set_rate(&self, rate_bytes_per_sec: Option<u64>) {
        let rate = rate_bytes_per_sec.unwrap_or(0);
        self.rate_bytes_per_sec.store(rate, Ordering::SeqCst);

        let mut state = self.state.lock().await;
        let now = Instant::now();
        if rate > 0 {
            let max_tokens = (rate as f64).max(64.0 * 1024.0);
            state.max_tokens = max_tokens;
            state.tokens = state.tokens.min(max_tokens);
        } else {
            state.tokens = 0.0;
            state.max_tokens = 0.0;
        }
        state.last_update = now;
    }

    /// Returns the currently active rate limit in bytes per second, if any.
    pub fn get_rate(&self) -> Option<u64> {
        let rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if rate > 0 {
            Some(rate)
        } else {
            None
        }
    }

    /// Acquires permission to transfer the specified number of bytes.
    ///
    /// If bandwidth is currently exhausted, asynchronously suspends execution
    /// until sufficient tokens have replenished.
    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
        if rate == 0 {
            // Rate limiting is disabled
            return;
        }

        let wait_duration = {
            let mut state = self.state.lock().await;
            let current_rate = self.rate_bytes_per_sec.load(Ordering::Relaxed);
            if current_rate == 0 {
                return;
            }

            let now = Instant::now();
            let elapsed_secs = now.duration_since(state.last_update).as_secs_f64();
            let rate_f64 = current_rate as f64;

            // Refill tokens based on elapsed time
            state.tokens = (state.tokens + elapsed_secs * rate_f64).min(state.max_tokens);
            state.last_update = now;

            let needed = bytes as f64;
            if state.tokens >= needed {
                // Immediate availability: deduct and proceed
                state.tokens -= needed;
                None
            } else {
                // Insufficient tokens: calculate required sleep duration
                let deficit = needed - state.tokens;
                let wait_secs = deficit / rate_f64;
                state.tokens = 0.0;
                // Advance the baseline to account for time spent waiting
                state.last_update = now + Duration::from_secs_f64(wait_secs);
                Some(Duration::from_secs_f64(wait_secs))
            }
        };

        if let Some(delay) = wait_duration {
            sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_speed_limiter_unlimited() {
        let limiter = SpeedLimiter::new(None);
        assert_eq!(limiter.get_rate(), None);

        let start = Instant::now();
        limiter.acquire(1024 * 1024).await;
        // Unlimited should return almost instantly
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_speed_limiter_rate_throttling() {
        // Limit to 100 KB/s = 100_000 bytes/sec
        let rate = 100_000;
        let limiter = SpeedLimiter::new(Some(rate));
        assert_eq!(limiter.get_rate(), Some(rate));

        // Drain the initial burst capacity (100_000 tokens)
        limiter.acquire(rate as usize).await;

        let start = Instant::now();
        // Request another 40_000 bytes: at 100_000 B/s, takes ~400ms
        limiter.acquire(40_000).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(350),
            "Elapsed was {:?}, expected >= 350ms",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_speed_limiter_runtime_reconfiguration() {
        let limiter = SpeedLimiter::new(Some(1000));
        assert_eq!(limiter.get_rate(), Some(1000));

        limiter.set_rate(Some(5000)).await;
        assert_eq!(limiter.get_rate(), Some(5000));

        limiter.set_rate(None).await;
        assert_eq!(limiter.get_rate(), None);
    }
}
