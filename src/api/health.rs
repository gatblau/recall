//! X8 — Health checks. Liveness, readiness, and the metrics scrape endpoint. Unauthenticated; no
//! fact data, no tenant/user identifiers.

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::Json;
use http::StatusCode;
use serde_json::json;

use crate::api::AppState;
use crate::types::envelope::{Meta, Success};

/// `GET /healthz` → 200 `{"data":{"status":"live"},"meta":{...}}` whenever the process is running.
/// No dependency calls (X8 Internal Logic 1).
pub async fn healthz(State(state): State<AppState>) -> Response {
    let correlation_id = state.new_correlation_id();
    let body = Success {
        data: json!({ "status": "live" }),
        meta: Meta {
            correlation_id,
            ..Default::default()
        },
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// `GET /readyz` — readiness (unauthenticated; C8 Public Interface). Runs three checks:
///   1. store reachable — `MemoryStore::ready()` answers a trivial query.
///   2. OIDC discovery reachable — C3 reports a populated JWKS (warm cache, non-empty).
///   3. `RECALL_EMBED_DIM` equals the configured vector-index dimension (SA-EMBED-01).
///
/// `200` with all checks `true` when ready; `503` with the failing check(s) `false` and
/// `"status":"not_ready"`. No envelope, no fact data — only boolean check results.
///
/// Check (3) compares `RECALL_EMBED_DIM` against the store's configured index dimension. The embedded
/// store is built with one fixed `embed_dim`, so a *mismatch* is exercised by the test seam that builds
/// the store with a dimension differing from the config's `RECALL_EMBED_DIM`; in production the two are
/// the same value, so the check passes.
pub async fn readyz(State(state): State<AppState>) -> Response {
    use crate::types::ports::MemoryStore;

    // Check 1 — store reachable.
    let store_ok = state.store.ready().await.is_ok();
    // Check 2 — OIDC discovery reachable: a populated (non-empty) JWKS cache.
    let oidc_ok = state.auth.cached_key_count().await > 0;
    // Check 3 — embedding dimension matches the configured vector-index dimension (SA-EMBED-01).
    let embed_dim_ok = state.store.index_embed_dim() == state.config.embed_dim;

    let ready = store_ok && oidc_ok && embed_dim_ok;
    let status = if ready { "ready" } else { "not_ready" };
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "status": status,
        "checks": { "store": store_ok, "oidc": oidc_ok, "embed_dim": embed_dim_ok }
    });
    (code, Json(body)).into_response()
}

/// `GET /metrics` → 200 Prometheus text exposition (X4). No fact content, no identifiers.
pub async fn metrics(State(state): State<AppState>) -> Response {
    let body = state.metrics.render();
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}
