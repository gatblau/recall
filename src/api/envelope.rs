//! X1 — axum integration for the response envelopes. Turns an [`AppError`] into an HTTP response
//! via the canonical [`map_error`] registry, and provides a panic-recovery layer that maps any
//! handler panic to `INTERNAL` (500).

use axum::response::{IntoResponse, Response};
use axum::Json;
use http::StatusCode;
use tower_http::catch_panic::CatchPanicLayer;

use crate::error::{map_error, AppError, Env};
use crate::types::envelope::{ErrorBody, ErrorEnvelope};

/// An [`AppError`] paired with the per-request correlation id and environment, so it can render the
/// exact X1 envelope. Handlers return this; axum turns it into a response.
pub struct ApiError {
    pub error: AppError,
    pub correlation_id: String,
    pub env: Env,
}

impl ApiError {
    pub fn new(error: AppError, correlation_id: impl Into<String>, env: Env) -> Self {
        Self {
            error,
            correlation_id: correlation_id.into(),
            env,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, envelope) = map_error(&self.error, &self.correlation_id, self.env);
        (status, Json(envelope)).into_response()
    }
}

/// Build the tower panic-recovery layer. A panic in any handler is caught, mapped to an `INTERNAL`
/// (500) X1 error envelope, and returned. The correlation id is unavailable inside the catch closure
/// (the request extensions are gone by then), so the recovery body carries the fixed `INTERNAL`
/// shape with an empty correlation id; the request-scoped error log line (emitted by the layer) is
/// the place the correlation id is recorded in the full middleware stack (X1 Internal Logic 3).
pub fn panic_recovery_layer() -> CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> Response> {
    CatchPanicLayer::custom(handle_panic as fn(Box<dyn std::any::Any + Send>) -> Response)
}

fn handle_panic(err: Box<dyn std::any::Any + Send>) -> Response {
    let detail = panic_message(&err);
    tracing::error!(target: "recall::api", panic = %detail, "handler panicked; mapping to INTERNAL");
    let envelope = ErrorEnvelope {
        error: ErrorBody {
            code: "INTERNAL".to_string(),
            message: "internal error".to_string(),
            correlation_id: String::new(),
        },
    };
    (StatusCode::INTERNAL_SERVER_ERROR, Json(envelope)).into_response()
}

/// Extract a best-effort string from a panic payload, for the error log line only (never the
/// client-facing body).
fn panic_message(err: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}
