//! C8 chain step 4 — per-(subject, operation-class) token-bucket rate limiting (SA-RATE-01).
//!
//! Each `(ctx.user, OpClass)` key owns a [`TokenBucket`]. Read class refills at
//! `RECALL_RATE_READ_PER_MIN` (default 120/min) with a fixed burst of 40; write class (covering write
//! and forget routes) refills at `RECALL_RATE_WRITE_PER_MIN` (default 30/min) with a fixed burst of
//! 10. The burst constants are fixed by SA-RATE-01 (no env var). Every response carries
//! `RateLimit-Limit/Remaining/Reset`; a rejection also carries `Retry-After`.

use std::time::Instant;

/// The operation class a route belongs to. Read = recall / get-fact / capabilities; write = remember;
/// forget = retire / delete. Write and forget share the *write* token bucket (C8 chain step 4: "write
/// class (covering write and forget routes)"), so both map to [`OpClass::Write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OpClass {
    Read,
    Write,
}

/// Fixed read-class burst (SA-RATE-01).
pub const READ_BURST: u32 = 40;
/// Fixed write-class burst (SA-RATE-01).
pub const WRITE_BURST: u32 = 10;

/// The headers a rate-limit decision contributes to the response.
#[derive(Debug)]
pub struct RateHeaders {
    /// Per-minute refill rate for the bucket (the `RateLimit-Limit` value).
    pub limit: u32,
    /// Tokens remaining after this request's decrement.
    pub remaining: u32,
    /// Seconds until the bucket next has a token (`RateLimit-Reset`; also `Retry-After` on a reject).
    pub reset_secs: u64,
}

/// A continuous-refill token bucket: capacity = burst, refill = `per_min` tokens spread across 60 s.
/// `tokens` is fractional so a sub-second refill accrues correctly. Single-threaded access is assumed
/// (the caller holds the map mutex while deciding), so no interior atomics are needed.
pub struct TokenBucket {
    /// Maximum tokens (burst).
    capacity: u32,
    /// Tokens added per minute (the `RateLimit-Limit` value).
    per_min: u32,
    /// Current token count, fractional.
    tokens: f64,
    /// Last time the bucket was refilled.
    last_refill: Instant,
}

impl TokenBucket {
    /// Build a full bucket with the given burst capacity and per-minute refill.
    pub fn new(capacity: u32, per_min: u32) -> Self {
        Self {
            capacity,
            per_min,
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

    /// Build an *empty* bucket (zero tokens) — used by the test seam that drives the 429 path without
    /// sending `burst + 1` requests. The first refill tick accrues tokens from `now`.
    pub fn empty(capacity: u32, per_min: u32) -> Self {
        Self {
            capacity,
            per_min,
            tokens: 0.0,
            last_refill: Instant::now(),
        }
    }

    /// Accrue tokens for the time elapsed since the last refill, clamped to `capacity`.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let rate_per_sec = self.per_min as f64 / 60.0;
            self.tokens = (self.tokens + elapsed * rate_per_sec).min(self.capacity as f64);
            self.last_refill = now;
        }
    }

    /// Seconds until at least one token is available (0 if a token is already available). Used for the
    /// `RateLimit-Reset` / `Retry-After` value.
    fn reset_secs(&self) -> u64 {
        if self.tokens >= 1.0 {
            0
        } else {
            let rate_per_sec = self.per_min as f64 / 60.0;
            if rate_per_sec <= 0.0 {
                // No refill configured — never recovers; report a large bounded value.
                return u64::from(u32::MAX);
            }
            let deficit = 1.0 - self.tokens;
            (deficit / rate_per_sec).ceil() as u64
        }
    }

    /// Attempt to take one token. Returns the headers to emit; `Ok` on success (a token was taken),
    /// `Err` on rejection (bucket empty). On both paths the headers reflect the post-decision state.
    pub fn take(&mut self, now: Instant) -> Result<RateHeaders, RateHeaders> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(RateHeaders {
                limit: self.per_min,
                remaining: self.tokens.floor() as u32,
                reset_secs: self.reset_secs(),
            })
        } else {
            Err(RateHeaders {
                limit: self.per_min,
                remaining: 0,
                reset_secs: self.reset_secs(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn full_bucket_allows_burst_then_rejects() {
        let mut b = TokenBucket::new(3, 60); // burst 3, 1/sec refill
        let now = Instant::now();
        for _ in 0..3 {
            assert!(b.take(now).is_ok(), "burst tokens must be allowed");
        }
        // Fourth immediate request: bucket empty.
        let rej = b.take(now).unwrap_err();
        assert_eq!(rej.remaining, 0);
        assert!(rej.reset_secs >= 1, "reset must be at least 1s when empty");
    }

    #[test]
    fn empty_bucket_rejects_immediately() {
        let mut b = TokenBucket::empty(READ_BURST, 120);
        let rej = b.take(Instant::now()).unwrap_err();
        assert_eq!(rej.limit, 120);
        assert_eq!(rej.remaining, 0);
    }

    #[test]
    fn refill_accrues_over_time() {
        let mut b = TokenBucket::empty(10, 60); // 1 token/sec
        let start = Instant::now();
        // Drain attempt at t0 fails.
        assert!(b.take(start).is_err());
        // After 2 simulated seconds, ~2 tokens have accrued.
        let later = start + Duration::from_secs(2);
        assert!(b.take(later).is_ok(), "a token should have refilled after 2s");
    }

    #[test]
    fn remaining_decrements_per_take() {
        let mut b = TokenBucket::new(5, 60);
        let now = Instant::now();
        let h1 = b.take(now).unwrap();
        assert_eq!(h1.remaining, 4);
        let h2 = b.take(now).unwrap();
        assert_eq!(h2.remaining, 3);
    }
}
