//! C8 — HTTP API Edge. The single externally-reachable surface of `recall`.
//!
//! The edge terminates HTTP, exposes the task-shaped `/v1` routes (capabilities, recall, remember,
//! get-fact, retire, delete, OpenAPI) and the three unauthenticated operational routes (`/healthz`,
//! `/readyz`, `/metrics`). It owns the per-request middleware chain and the two response contracts —
//! the single `Success<T>` envelope and the single `ErrorEnvelope` — with every component error mapped
//! to a registered `(HTTP status, machine code)` pair by `crate::error::map_error`. It holds no domain
//! logic: recall is delegated to C6, writes are enqueued on C2, fact reads + retire go through C1, and
//! the verifiable hard delete runs against C1 directly (see `v1::delete_memory`).
//!
//! ## Middleware chain order (binding, outermost-first)
//!
//! `correlation-id → body-size limit → auth (C3) → rate limit → idempotency (writes only) → handler
//! → audit`. The two outermost concerns (correlation-id, body-size limit) are tower layers on the
//! router so their order is structural. The inner concerns (auth → rate limit → idempotency → handler
//! → audit) run inside each `/v1` handler via [`EdgePipeline`], which executes them in exactly that
//! fixed order; the operational routes bypass auth/rate-limit/idempotency/audit entirely.

pub mod envelope;
pub mod health;
pub mod openapi;
pub mod ratelimit;
pub mod v1;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use http::{HeaderValue, StatusCode};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::config::Config;
use crate::error::{AppError, Env};
use crate::obs::metrics::Metrics;
use crate::queue::StoreWorkQueue;
use crate::retrieval::RetrievalEngine;
use crate::store::Store;
use crate::types::envelope::{ErrorBody, ErrorEnvelope};

use ratelimit::{OpClass, TokenBucket};

/// Header carrying the per-request correlation id, echoed on the response.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// The in-process rate-limiter state: one token bucket per `(subject, operation-class)`. Bounded —
/// the only in-process state the edge holds (the idempotency record lives in C1's store).
type RateLimiter = Arc<Mutex<HashMap<(String, OpClass), TokenBucket>>>;

/// Shared application state injected into every handler. Cheap to clone (Arc-backed).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub metrics: Arc<Metrics>,
    /// Concrete store: the edge needs `idempotency_get/put` + `hard_delete` + `append_audit` + reads.
    pub store: Arc<Store>,
    pub queue: Arc<StoreWorkQueue>,
    pub engine: Arc<RetrievalEngine>,
    pub auth: Arc<Authenticator>,
    /// Per-(subject, op-class) token buckets (SA-RATE-01).
    pub rate: RateLimiter,
}

impl AppState {
    /// Mint a fresh correlation id (UUID v4). Used by handlers reached without a middleware-assigned id.
    pub fn new_correlation_id(&self) -> String {
        Uuid::new_v4().to_string()
    }

    /// The configured deployment environment, used by the error-envelope mapping.
    pub fn env(&self) -> Env {
        self.config.env
    }
}

/// Build the production axum router: the `/v1` task routes under the middleware chain, plus the three
/// unauthenticated operational routes (`/healthz`, `/readyz`, `/metrics`).
///
/// The two outermost middleware concerns are expressed as tower layers so their order is structural:
///   1. correlation-id assignment (innermost-applied `.layer` runs *outermost*, see below)
///   2. body-size limit (`DefaultBodyLimit`)
///
/// In axum/tower, `.layer(a).layer(b)` makes `a` the outer layer. We therefore apply body-size limit
/// first then correlation-id, so correlation-id is the outermost concern (it must wrap everything so
/// even a 413 carries the id). The remaining concerns (auth → rate → idempotency → handler → audit)
/// run inside each handler in that fixed order.
pub fn build_router(state: AppState) -> Router {
    let max_body = state.config.max_body_bytes as usize;

    Router::new()
        // --- /v1 task routes (auth + rate-limit + idempotency + audit run inside the handlers) ---
        .route("/v1", get(v1::capabilities))
        .route("/v1/recall", post(v1::recall))
        .route("/v1/memories", post(v1::remember))
        .route("/v1/memories/:id", get(v1::get_memory))
        .route("/v1/memories/:id/retire", post(v1::retire_memory))
        .route("/v1/memories/:id", delete(v1::delete_memory))
        .route("/openapi.json", get(v1::openapi_doc))
        // --- operational routes (bypass auth/rate-limit/idempotency/audit) ---
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(health::metrics))
        .fallback(not_found_fallback)
        // Body-size limit (chain step 2) — applied before correlation-id so correlation-id wraps it.
        .layer(DefaultBodyLimit::max(max_body))
        // Correlation-id assignment (chain step 1, outermost).
        .layer(middleware::from_fn(correlation_id_middleware))
        .layer(envelope::panic_recovery_layer())
        .with_state(state)
}

/// Assign or propagate the correlation id (chain step 1). A valid inbound `x-correlation-id` is
/// honoured; otherwise a fresh UUID v4 is minted. The id is stored in request extensions for handlers
/// and echoed on the response header so a caller can correlate logs/traces.
async fn correlation_id_middleware(mut req: Request, next: Next) -> Response {
    let incoming = req
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| Uuid::parse_str(s).is_ok())
        .map(|s| s.to_string());
    let correlation_id = incoming.unwrap_or_else(|| Uuid::new_v4().to_string());

    req.extensions_mut().insert(CorrelationId(correlation_id.clone()));

    let mut resp = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&correlation_id) {
        resp.headers_mut().insert(CORRELATION_ID_HEADER, value);
    }
    resp
}

/// Per-request correlation id, stored as a request extension by the middleware.
#[derive(Clone)]
pub struct CorrelationId(pub String);

/// Read the correlation id from request extensions, minting a fresh one if absent (e.g. a request that
/// somehow bypassed the middleware).
pub(crate) fn correlation_from(req: &Request, state: &AppState) -> String {
    req.extensions()
        .get::<CorrelationId>()
        .map(|c| c.0.clone())
        .unwrap_or_else(|| state.new_correlation_id())
}

/// Render an [`AppError`] into the X1 error envelope response, carrying the correlation id in both the
/// body and the `X-Correlation-Id` header. The single conversion the edge uses for every error path.
pub(crate) fn error_response(err: &AppError, correlation_id: &str, env: Env) -> Response {
    let (status, mut envelope) = crate::error::map_error(err, correlation_id, env);
    // Guarantee the correlation id is the request id even if map_error was given a blank one.
    envelope.error.correlation_id = correlation_id.to_string();
    let mut resp = (status, Json(envelope)).into_response();
    if let Ok(value) = HeaderValue::from_str(correlation_id) {
        resp.headers_mut().insert(CORRELATION_ID_HEADER, value);
    }
    resp
}

/// Fallback for an unmatched route: the X1 `NOT_FOUND` (404) error envelope with the request's
/// correlation id.
async fn not_found_fallback(State(state): State<AppState>, req: Request) -> Response {
    let correlation_id = correlation_from(&req, &state);
    let (status, envelope) =
        crate::error::map_error(&AppError::NotFound, &correlation_id, state.env());
    debug_assert_eq!(status, StatusCode::NOT_FOUND);
    let envelope = ErrorEnvelope {
        error: ErrorBody {
            code: envelope.error.code,
            message: envelope.error.message,
            correlation_id,
        },
    };
    (status, Json(envelope)).into_response()
}
