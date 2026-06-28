//! C8 — the `/v1` task handlers plus `GET /openapi.json`, now thin HTTP adapters over the Service
//! Layer (C9, ADR-016).
//!
//! Each handler does only HTTP-transport work: it reads the correlation id, extracts the bearer from
//! the `Authorization` header, reads the `Idempotency-Key` header (writes), passes the raw body / path
//! param, builds a [`CallContext`], calls the matching [`Service`] method, and renders the returned
//! [`CallResult`] (success envelope + `RateLimit-*`/`X-Correlation-Id`/`ETag` headers) or [`CallError`]
//! (the X1 error envelope + status). The security-critical chain — authenticate, authorise,
//! rate-limit, idempotency, the component call, and the audit write — lives once in C9 and is shared
//! with the MCP edge. Conditional-GET (`ETag`/`If-Modified-Since`) on `get_fact` stays here: it is an
//! HTTP optimisation the edge layers over the Service's full-`Fact` read. Operational routes do not use
//! this module.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use http::header::{CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, RETRY_AFTER};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::Serialize;
use uuid::Uuid;

use crate::api::{error_response, AppState, CorrelationId, CORRELATION_ID_HEADER};
use crate::error::AppError;
use crate::service::{CallContext, CallError, RateSnapshot};
use crate::types::domain::Fact;
use crate::types::envelope::{Meta, Success};

/// `RateLimit-*` header names (lower-case; HTTP/2 normalises anyway).
const RATELIMIT_LIMIT: &str = "ratelimit-limit";
const RATELIMIT_REMAINING: &str = "ratelimit-remaining";
const RATELIMIT_RESET: &str = "ratelimit-reset";

// --- Transport helpers ---------------------------------------------------------------------------

/// Extract the bearer token value (after "Bearer ") from the Authorization header, or "" when absent.
fn bearer_token(headers: &HeaderMap) -> String {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("")
        .to_string()
}

/// Read the `Idempotency-Key` header value (raw; the Service validates presence/length).
fn idempotency_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Read the correlation id from request extensions (the middleware always sets it); else honour a valid
/// inbound header, else mint.
fn correlation_id(headers: &HeaderMap, ext: Option<&CorrelationId>, state: &AppState) -> String {
    if let Some(c) = ext {
        return c.0.clone();
    }
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| Uuid::parse_str(s).is_ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| state.new_correlation_id())
}

/// Attach the `RateLimit-*` headers (and, when `rejected`, `Retry-After`) to a response.
fn attach_rate_headers(resp: &mut Response, rate: &RateSnapshot, rejected: bool) {
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&rate.limit.to_string()) {
        h.insert(HeaderName::from_static(RATELIMIT_LIMIT), v);
    }
    if let Ok(v) = HeaderValue::from_str(&rate.remaining.to_string()) {
        h.insert(HeaderName::from_static(RATELIMIT_REMAINING), v);
    }
    if let Ok(v) = HeaderValue::from_str(&rate.reset_secs.to_string()) {
        h.insert(HeaderName::from_static(RATELIMIT_RESET), v);
    }
    if rejected {
        if let Ok(v) = HeaderValue::from_str(&rate.reset_secs.to_string()) {
            h.insert(RETRY_AFTER, v);
        }
    }
}

/// Set the `X-Correlation-Id` response header.
fn set_correlation_header(resp: &mut Response, correlation_id: &str) {
    if let Ok(v) = HeaderValue::from_str(correlation_id) {
        resp.headers_mut().insert(CORRELATION_ID_HEADER, v);
    }
}

/// Render a [`CallError`] to the X1 error envelope response: the mapped status + envelope, plus the
/// rate-limit headers when the Service produced a snapshot (a 429 also gets `Retry-After`). A
/// pre-rate-limit failure (401/403) carries no snapshot and so no `RateLimit-*` headers, matching the
/// former C8 `begin` behaviour.
fn render_error(state: &AppState, correlation_id: &str, err: CallError) -> Response {
    let mut resp = error_response(&err.error, correlation_id, state.env());
    if let Some(rate) = &err.rate {
        let rejected = matches!(err.error, AppError::RateLimited);
        attach_rate_headers(&mut resp, rate, rejected);
    }
    resp
}

/// Render a `Success<T>` envelope response with the given status, attaching the rate snapshot's
/// `RateLimit-*` headers, the `X-Correlation-Id`, and any extra headers (e.g. `ETag`).
fn render_success<T: Serialize>(
    status: StatusCode,
    correlation_id: &str,
    rate: &RateSnapshot,
    body: Success<T>,
    extra_headers: Vec<(HeaderName, HeaderValue)>,
) -> Response {
    let mut resp = (status, Json(body)).into_response();
    attach_rate_headers(&mut resp, rate, false);
    set_correlation_header(&mut resp, correlation_id);
    for (name, value) in extra_headers {
        resp.headers_mut().insert(name, value);
    }
    resp
}

// --- Handlers ------------------------------------------------------------------------------------

/// `GET /v1` — capabilities. Read class.
pub async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: None,
    };
    match state.service().capabilities(cx).await {
        Ok(result) => {
            let body = Success {
                data: result.data,
                meta: Meta {
                    correlation_id: cid.clone(),
                    ..Default::default()
                },
            };
            render_success(StatusCode::OK, &cid, &result.rate, body, vec![])
        }
        Err(e) => render_error(&state, &cid, e),
    }
}

/// `POST /v1/recall` — read. Delegates to C6 via the Service. The raw body is passed through so the
/// Service runs auth/authorise/rate ahead of body deserialisation (preserving the C8 ordering).
pub async fn recall(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
    raw: Bytes,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: None,
    };
    match state.service().recall(cx, &raw).await {
        Ok(result) => {
            let outcome = result.data;
            let body = Success {
                data: outcome.response,
                meta: Meta {
                    next_cursor: outcome.next_cursor,
                    abstained: outcome.abstained.then_some(true),
                    correlation_id: cid.clone(),
                },
            };
            render_success(StatusCode::OK, &cid, &result.rate, body, vec![])
        }
        Err(e) => render_error(&state, &cid, e),
    }
}

/// `POST /v1/memories` — write (async). Enqueues an `ExtractFact` job via the Service, returns 202.
pub async fn remember(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
    raw: Bytes,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let idem = idempotency_header(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: idem.as_deref(),
    };
    match state.service().remember(cx, &raw).await {
        Ok(result) => {
            let body = Success {
                data: result.data,
                meta: Meta {
                    correlation_id: cid.clone(),
                    ..Default::default()
                },
            };
            render_success(StatusCode::ACCEPTED, &cid, &result.rate, body, vec![])
        }
        Err(e) => render_error(&state, &cid, e),
    }
}

/// `GET /v1/memories/{id}` — read. Honours `If-None-Match` / `If-Modified-Since` (→ 304). The
/// conditional-GET logic lives here; the Service returns the full `Fact`.
pub async fn get_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: None,
    };

    let result = match state.service().get_fact(cx, &id).await {
        Ok(r) => r,
        Err(e) => return render_error(&state, &cid, e),
    };
    let fact = result.data;

    let etag = compute_etag(&fact);
    // Conditional GET: 304 when If-None-Match matches the ETag, or If-Modified-Since >= ingested_at.
    if not_modified(&headers, &etag, fact.ingested_at) {
        // A 304 carries no envelope body. The success audit already ran inside the Service's get_fact.
        let mut resp = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(v) = HeaderValue::from_str(&etag) {
            resp.headers_mut().insert(ETAG, v);
        }
        attach_rate_headers(&mut resp, &result.rate, false);
        set_correlation_header(&mut resp, &cid);
        return resp;
    }

    let body = Success {
        data: fact,
        meta: Meta {
            correlation_id: cid.clone(),
            ..Default::default()
        },
    };
    let etag_header = HeaderValue::from_str(&etag).ok().map(|v| (ETAG, v));
    render_success(
        StatusCode::OK,
        &cid,
        &result.rate,
        body,
        etag_header.into_iter().collect(),
    )
}

/// Compute the ETag for a fact: a quoted hash derived from its `id` and `ingested_at` (C8 Public
/// Interface). A change to either yields a different ETag.
fn compute_etag(fact: &Fact) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(fact.id.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(fact.ingested_at.to_rfc3339().as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(16);
    for b in digest.iter().take(8) {
        hex.push_str(&format!("{b:02x}"));
    }
    format!("\"{hex}\"")
}

/// Whether the conditional request headers imply a 304: `If-None-Match` matches the ETag, or
/// `If-Modified-Since` is at/after the fact's `ingested_at`.
fn not_modified(headers: &HeaderMap, etag: &str, ingested_at: DateTime<Utc>) -> bool {
    if let Some(inm) = headers.get(IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if inm.split(',').any(|t| t.trim() == etag || t.trim() == "*") {
            return true;
        }
    }
    if let Some(ims) = headers.get(IF_MODIFIED_SINCE).and_then(|v| v.to_str().ok()) {
        if let Ok(since) = DateTime::parse_from_rfc3339(ims.trim()) {
            if ingested_at <= since.with_timezone(&Utc) {
                return true;
            }
        }
    }
    false
}

/// `POST /v1/memories/{id}/retire` — forget (non-destructive). Calls C1 `end_validity` via the Service.
pub async fn retire_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let idem = idempotency_header(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: idem.as_deref(),
    };

    match state.service().retire(cx, &id).await {
        Ok(result) => {
            let body = Success {
                data: result.data,
                meta: Meta {
                    correlation_id: cid.clone(),
                    ..Default::default()
                },
            };
            render_success(StatusCode::OK, &cid, &result.rate, body, vec![])
        }
        Err(e) => render_error(&state, &cid, e),
    }
}

/// `DELETE /v1/memories/{id}` — forget (verifiable hard delete). Calls C1 `hard_delete` directly via
/// the Service and returns the `DeletionProof`.
pub async fn delete_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let idem = idempotency_header(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: idem.as_deref(),
    };

    match state.service().delete(cx, &id).await {
        Ok(result) => {
            let body = Success {
                data: result.data,
                meta: Meta {
                    correlation_id: cid.clone(),
                    ..Default::default()
                },
            };
            render_success(StatusCode::OK, &cid, &result.rate, body, vec![])
        }
        Err(e) => render_error(&state, &cid, e),
    }
}

/// `GET /openapi.json` — the generated contract document (not envelope-wrapped). Read class; requires a
/// valid token (it sits under the authenticated surface, exposing contract data not fact data). Auth,
/// rate-limit, and the audit write run in the Service (operation "openapi").
pub async fn openapi_doc(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let bearer = bearer_token(&headers);
    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &cid,
        idempotency_key: None,
    };

    let result = match state.service().openapi(cx).await {
        Ok(r) => r,
        Err(e) => return render_error(&state, &cid, e),
    };
    let doc = crate::api::openapi::document("recall", env!("CARGO_PKG_VERSION"));
    let mut resp = (StatusCode::OK, Json(doc)).into_response();
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    attach_rate_headers(&mut resp, &result.rate, false);
    set_correlation_header(&mut resp, &cid);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::domain::MemoryClass;
    use crate::types::scope::ScopeRef as Sr;

    fn sample_fact() -> Fact {
        Fact {
            id: "fact:abc".into(),
            content: serde_json::json!({"k": "v"}),
            entities: vec!["entity:e1".into()],
            source_id: None,
            memory_class: MemoryClass::Semantic,
            visibility: crate::types::domain::Visibility::UserPrivate,
            owner: Sr {
                tenant: "acme".into(),
                team: None,
                user: "u-1".into(),
            },
            valid_from: Utc::now(),
            valid_to: None,
            ingested_at: DateTime::parse_from_rfc3339("2026-01-04T09:00:01.123Z")
                .unwrap()
                .with_timezone(&Utc),
            confidence: 0.9,
            salience: 0.5,
            stability: 1.0,
            pii_review: false,
            supersedes: None,
            superseded_by: None,
            derived_from: vec![],
            last_recalled_at: None,
        }
    }

    #[test]
    fn etag_is_stable_and_id_sensitive() {
        let f = sample_fact();
        let e1 = compute_etag(&f);
        let e2 = compute_etag(&f);
        assert_eq!(e1, e2, "ETag must be deterministic");
        let mut g = sample_fact();
        g.id = "fact:def".into();
        assert_ne!(compute_etag(&g), e1, "a different id must change the ETag");
    }

    #[test]
    fn not_modified_matches_if_none_match() {
        let f = sample_fact();
        let etag = compute_etag(&f);
        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, HeaderValue::from_str(&etag).unwrap());
        assert!(not_modified(&headers, &etag, f.ingested_at));

        let mut other = HeaderMap::new();
        other.insert(IF_NONE_MATCH, HeaderValue::from_static("\"deadbeef\""));
        assert!(!not_modified(&other, &etag, f.ingested_at));
    }

    #[test]
    fn not_modified_honours_if_modified_since() {
        let f = sample_fact();
        let etag = compute_etag(&f);
        let mut headers = HeaderMap::new();
        // A future date is at/after ingested_at -> 304.
        headers.insert(
            IF_MODIFIED_SINCE,
            HeaderValue::from_static("2026-06-20T00:00:00.000Z"),
        );
        assert!(not_modified(&headers, &etag, f.ingested_at));

        let mut older = HeaderMap::new();
        older.insert(
            IF_MODIFIED_SINCE,
            HeaderValue::from_static("2025-01-01T00:00:00.000Z"),
        );
        assert!(!not_modified(&older, &etag, f.ingested_at));
    }

    #[test]
    fn bearer_token_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer abc.def.ghi"),
        );
        assert_eq!(bearer_token(&headers), "abc.def.ghi");
        assert_eq!(bearer_token(&HeaderMap::new()), "");
    }
}
