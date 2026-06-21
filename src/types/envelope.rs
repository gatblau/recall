//! Response envelopes (§2C.1).
//!
//! Used by: C8 (every handler); the shape is mirrored by all error producers.

use serde::Serialize;

/// Success envelope. Every 2xx body is exactly this shape.
#[derive(Serialize)]
pub struct Success<T: Serialize> {
    pub data: T,
    pub meta: Meta,
}

/// Response metadata carried alongside every successful body.
#[derive(Serialize, Default)]
pub struct Meta {
    /// Opaque pagination cursor (SA-PAGE-01).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// True on a recall that gated out all candidates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abstained: Option<bool>,
    /// UUID; echoes the per-request id.
    pub correlation_id: String,
}

/// Error envelope. Every non-2xx body is exactly this shape.
#[derive(Serialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

/// The single error payload shape.
#[derive(Serialize)]
pub struct ErrorBody {
    /// SCREAMING_SNAKE_CASE, from the Phase 4 error registry (SA-ENV-02).
    pub code: String,
    /// Human-readable; never leaks internal state or PII.
    pub message: String,
    pub correlation_id: String,
}
