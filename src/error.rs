//! X1 — Error handling: the typed `AppError` (§2C.7) and the canonical error-code registry.
//!
//! `AppError` is produced everywhere; exactly one mapping function, [`map_error`], turns it into a
//! `(StatusCode, ErrorEnvelope)`. Codes are a closed registry — agents branch on `code` without
//! parsing prose. The full registry table lives in the Phase 4 X1 spec; the mapping below is its
//! single executable form.

use http::StatusCode;
use thiserror::Error;

use crate::types::envelope::{ErrorBody, ErrorEnvelope};
use crate::types::ports::{ProviderError, QueueError, StoreError};

/// Deployment environment; gates verbose error detail (`RECALL_ENV`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Env {
    Production,
    Development,
}

/// Discriminator for the `VAL_*` family carried by [`AppError::Validation`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ValidationKind {
    /// JSON parse failure or schema/structural validation failure -> `VAL_INVALID_BODY`.
    InvalidBody,
    /// A field outside its allowed numeric/length range -> `VAL_OUT_OF_RANGE`.
    OutOfRange,
    /// `memory_class` = `procedural` -> `VAL_UNSUPPORTED_CLASS`.
    UnsupportedClass,
    /// A write request without an `Idempotency-Key` header -> `VAL_MISSING_IDEMPOTENCY_KEY`.
    MissingIdempotencyKey,
    /// Request body exceeds `RECALL_MAX_BODY_BYTES` -> `VAL_BODY_TOO_LARGE`.
    BodyTooLarge,
}

/// Discriminator carried by [`AppError::Unauthenticated`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthKind {
    /// No `Authorization: Bearer` header -> `AUTH_MISSING_TOKEN`.
    Missing,
    /// Signature/issuer/audience/expiry/nbf validation failed -> `AUTH_INVALID_TOKEN`.
    Invalid,
}

/// Typed application error (§2C.7). Every component produces this; C8 owns the mapping.
#[derive(Error, Debug)]
pub enum AppError {
    /// -> 400 VAL_* (the precise code is selected by the carried [`ValidationKind`]).
    #[error("validation: {1}")]
    Validation(ValidationKind, String),
    /// -> 401 AUTH_* (the precise code is selected by the carried [`AuthKind`]).
    #[error("unauthenticated: {1}")]
    Unauthenticated(AuthKind, String),
    /// -> 403 SCOPE_FORBIDDEN.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// -> 403 AUTH_INSUFFICIENT_SCOPE.
    #[error("insufficient scope: {0}")]
    InsufficientScope(String),
    /// -> 404 NOT_FOUND.
    #[error("not found")]
    NotFound,
    /// -> 429 RATE_LIMITED.
    #[error("rate limited")]
    RateLimited,
    /// -> 503 STORE_UNAVAILABLE / 504 STORE_TIMEOUT (selected by the inner variant).
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// -> 503 QUEUE_UNAVAILABLE.
    #[error("queue: {0}")]
    Queue(#[from] QueueError),
    /// -> 502/504 by kind.
    #[error("provider: {0}")]
    Provider(#[from] ProviderError),
    /// -> 500 INTERNAL.
    #[error("internal")]
    Internal,
}

/// The canonical (status, code, fixed message) triple for one `AppError`. The fixed message never
/// contains a token, PII, fact content, or a stack trace.
fn registry(e: &AppError) -> (StatusCode, &'static str, &'static str) {
    match e {
        AppError::Validation(kind, _) => match kind {
            ValidationKind::InvalidBody => {
                (StatusCode::BAD_REQUEST, "VAL_INVALID_BODY", "invalid request body")
            }
            ValidationKind::OutOfRange => {
                (StatusCode::BAD_REQUEST, "VAL_OUT_OF_RANGE", "value out of range")
            }
            ValidationKind::UnsupportedClass => (
                StatusCode::BAD_REQUEST,
                "VAL_UNSUPPORTED_CLASS",
                "procedural memory is not supported",
            ),
            ValidationKind::MissingIdempotencyKey => (
                StatusCode::BAD_REQUEST,
                "VAL_MISSING_IDEMPOTENCY_KEY",
                "idempotency key required",
            ),
            ValidationKind::BodyTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "VAL_BODY_TOO_LARGE",
                "request body too large",
            ),
        },
        AppError::Unauthenticated(kind, _) => match kind {
            AuthKind::Missing => (
                StatusCode::UNAUTHORIZED,
                "AUTH_MISSING_TOKEN",
                "authentication required",
            ),
            AuthKind::Invalid => (
                StatusCode::UNAUTHORIZED,
                "AUTH_INVALID_TOKEN",
                "invalid token",
            ),
        },
        AppError::InsufficientScope(_) => (
            StatusCode::FORBIDDEN,
            "AUTH_INSUFFICIENT_SCOPE",
            "insufficient scope",
        ),
        AppError::Forbidden(_) => (StatusCode::FORBIDDEN, "SCOPE_FORBIDDEN", "forbidden"),
        AppError::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "not found"),
        AppError::RateLimited => (
            StatusCode::TOO_MANY_REQUESTS,
            "RATE_LIMITED",
            "rate limit exceeded",
        ),
        AppError::Store(s) => match s {
            StoreError::Timeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "STORE_TIMEOUT",
                "store operation timed out",
            ),
            StoreError::ScopeForbidden => {
                (StatusCode::FORBIDDEN, "SCOPE_FORBIDDEN", "forbidden")
            }
            StoreError::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "not found"),
            // `Validation` maps to `VAL_OUT_OF_RANGE` for score-range failures and
            // `VAL_INVALID_BODY` for every other structural failure (C1 error table).
            StoreError::Validation(detail) => {
                if detail.contains("range") {
                    (StatusCode::BAD_REQUEST, "VAL_OUT_OF_RANGE", "value out of range")
                } else {
                    (StatusCode::BAD_REQUEST, "VAL_INVALID_BODY", "invalid request body")
                }
            }
            StoreError::Unavailable(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "STORE_UNAVAILABLE",
                "store unavailable",
            ),
            // A partial delete is an internal invariant violation, never a client error.
            StoreError::PartialDelete { .. } | StoreError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                "internal error",
            ),
        },
        AppError::Queue(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "QUEUE_UNAVAILABLE",
            "work queue unavailable",
        ),
        AppError::Provider(p) => match p {
            ProviderError::Timeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "PROVIDER_TIMEOUT",
                "upstream provider timed out",
            ),
            ProviderError::Status(_)
            | ProviderError::Transport(_)
            | ProviderError::Malformed(_) => {
                (StatusCode::BAD_GATEWAY, "PROVIDER_ERROR", "upstream provider error")
            }
        },
        AppError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            "internal error",
        ),
    }
}

/// Map an [`AppError`] to its HTTP status and the closed-registry error envelope.
///
/// In `production` the message is the fixed human string per code; in `development` the inner
/// detail may be appended. The message never contains a token, PII, fact content, or a stack trace.
pub fn map_error(e: &AppError, correlation_id: &str, env: Env) -> (StatusCode, ErrorEnvelope) {
    let (status, code, fixed_msg) = registry(e);

    // Only ever surface the inner detail in development, and only for variants whose detail is a
    // safe operator string (never a store row, a token, or PII).
    let message = if env == Env::Development {
        match e {
            AppError::Validation(_, detail)
            | AppError::Unauthenticated(_, detail)
            | AppError::Forbidden(detail)
            | AppError::InsufficientScope(detail) => {
                if detail.is_empty() {
                    fixed_msg.to_string()
                } else {
                    format!("{fixed_msg}: {detail}")
                }
            }
            _ => fixed_msg.to_string(),
        }
    } else {
        fixed_msg.to_string()
    };

    let envelope = ErrorEnvelope {
        error: ErrorBody {
            code: code.to_string(),
            message,
            correlation_id: correlation_id.to_string(),
        },
    };
    (status, envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_maps_to_scope_forbidden_403() {
        let (status, env) = map_error(
            &AppError::Forbidden("record outside scope".into()),
            "c-9f3",
            Env::Production,
        );
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(env.error.code, "SCOPE_FORBIDDEN");
        assert_eq!(env.error.message, "forbidden");
        assert_eq!(env.error.correlation_id, "c-9f3");
    }

    #[test]
    fn production_hides_internal_detail() {
        let (status, env) = map_error(&AppError::Internal, "c-1", Env::Production);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(env.error.code, "INTERNAL");
        assert_eq!(env.error.message, "internal error");
    }

    #[test]
    fn development_appends_safe_detail() {
        let (_, env) = map_error(
            &AppError::Validation(ValidationKind::OutOfRange, "result_cap=99".into()),
            "c-1",
            Env::Development,
        );
        assert_eq!(env.error.code, "VAL_OUT_OF_RANGE");
        assert!(env.error.message.contains("result_cap=99"));
    }

    #[test]
    fn provider_timeout_is_504() {
        let (status, env) =
            map_error(&AppError::Provider(ProviderError::Timeout), "c-1", Env::Production);
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(env.error.code, "PROVIDER_TIMEOUT");
    }

    #[test]
    fn store_timeout_vs_unavailable() {
        let (s1, e1) =
            map_error(&AppError::Store(StoreError::Timeout), "c-1", Env::Production);
        assert_eq!(s1, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(e1.error.code, "STORE_TIMEOUT");

        let (s2, e2) = map_error(
            &AppError::Store(StoreError::Unavailable("conn lost".into())),
            "c-1",
            Env::Production,
        );
        assert_eq!(s2, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(e2.error.code, "STORE_UNAVAILABLE");
    }
}
