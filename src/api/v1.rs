//! C8 — the `/v1` task handlers plus `GET /openapi.json`.
//!
//! Each handler is thin. The cross-cutting concerns run in the fixed order mandated by the C8 chain —
//! auth (C3) → rate limit → idempotency (writes only) → handler body → audit — via [`EdgePipeline`].
//! `EdgePipeline::begin` performs auth + per-operation-class authorisation + rate limiting and returns
//! the authenticated [`ScopeContext`] and the rate-limit headers; the handler then runs its body; and
//! the handler finishes by calling `finish_success` / `finish_error`, which writes the audit record
//! before the response is returned (chain step 8). Operational routes do not use this module.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use http::header::{
    CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, RETRY_AFTER,
};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::api::ratelimit::{OpClass, RateHeaders, TokenBucket, READ_BURST, WRITE_BURST};
use crate::api::{error_response, AppState, CorrelationId, CORRELATION_ID_HEADER};
use crate::auth::{Authenticator, Op};
use crate::error::{AppError, AuthKind, ValidationKind};
use crate::types::api::{
    Capabilities, JobAckStatus, RecallRequest, RememberRequest, RetireAck, WriteAck,
};
use crate::types::domain::Fact;
use crate::types::envelope::{Meta, Success};
use crate::types::job::{JobKind, WorkJob, JobStatus};
use crate::types::ports::{AuditEntry, MemoryStore, StoreError, WorkQueue};
use crate::types::scope::{ScopeContext, ScopeRef};

/// `RateLimit-*` header names (lower-case; HTTP/2 normalises anyway).
const RATELIMIT_LIMIT: &str = "ratelimit-limit";
const RATELIMIT_REMAINING: &str = "ratelimit-remaining";
const RATELIMIT_RESET: &str = "ratelimit-reset";

/// The per-request edge pipeline: authentication, authorisation, and rate limiting have already run by
/// the time it exists. It carries the authenticated context, the route's logical operation name (for
/// the audit record), and the rate-limit headers to attach to every response.
struct EdgePipeline {
    ctx: ScopeContext,
    operation: &'static str,
    rate: RateHeaders,
}

impl EdgePipeline {
    /// Chain steps 3 + 4: authenticate (C3), authorise the route's operation class, and decrement the
    /// rate-limit bucket. On any failure returns the fully-formed error `Response` (with `RateLimit-*`
    /// and, on 429, `Retry-After`), so the caller returns it directly. No audit row is written for an
    /// auth/scope/rate failure (the spec only audits authenticated routes after the response is decided
    /// — and a missing-token 401 explicitly writes no audit row, per the Gherkin).
    async fn begin(
        state: &AppState,
        headers: &HeaderMap,
        correlation_id: &str,
        operation: &'static str,
        op: Op,
        class: OpClass,
    ) -> Result<EdgePipeline, Response> {
        // Step 3 — authenticate. Pull the bearer token from the Authorization header.
        let bearer = bearer_token(headers);
        let ctx = match state.auth.validate(&bearer, correlation_id).await {
            Ok(ctx) => ctx,
            Err(e) => {
                let app = match e {
                    crate::auth::AuthError::MissingToken => {
                        AppError::Unauthenticated(AuthKind::Missing, String::new())
                    }
                    crate::auth::AuthError::InvalidToken(detail) => {
                        AppError::Unauthenticated(AuthKind::Invalid, detail)
                    }
                    // validate() never returns InsufficientScope; treat defensively as invalid.
                    crate::auth::AuthError::InsufficientScope(_) => {
                        AppError::Unauthenticated(AuthKind::Invalid, String::new())
                    }
                };
                return Err(error_response(&app, correlation_id, state.env()));
            }
        };

        // Step 3 (cont.) — authorise the route's operation class against the token scopes.
        if let Err(crate::auth::AuthError::InsufficientScope(_)) =
            Authenticator::authorise(&ctx, op)
        {
            let app = AppError::InsufficientScope(format!("{op:?}"));
            return Err(error_response(&app, correlation_id, state.env()));
        }

        // Step 4 — rate limit, keyed by (subject, op-class).
        let rate = decrement_rate(state, &ctx.user, class).await;
        let rate = match rate {
            Ok(h) => h,
            Err(h) => {
                let mut resp =
                    error_response(&AppError::RateLimited, correlation_id, state.env());
                attach_rate_headers(&mut resp, &h, true);
                return Err(resp);
            }
        };

        Ok(EdgePipeline {
            ctx,
            operation,
            rate,
        })
    }

    /// Finish a successful response: serialise the `Success<T>` body, attach the `RateLimit-*` headers,
    /// then write the audit record (chain step 8) before returning. An audit-write failure turns the
    /// success into a `503 STORE_UNAVAILABLE` (the call is not reported successful if it could not be
    /// audited).
    async fn finish_success<T: Serialize>(
        self,
        state: &AppState,
        status: StatusCode,
        body: Success<T>,
        extra_headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Response {
        let correlation_id = self.ctx.correlation_id.clone();
        if let Err(e) = self.write_audit(state, "success").await {
            return error_response(&AppError::Store(e), &correlation_id, state.env());
        }
        let mut resp = (status, Json(body)).into_response();
        attach_rate_headers(&mut resp, &self.rate, false);
        set_correlation_header(&mut resp, &correlation_id);
        for (name, value) in extra_headers {
            resp.headers_mut().insert(name, value);
        }
        resp
    }

    /// Finish an error response after auth succeeded: write the audit record with the error `code` as
    /// the outcome, then return the X1 error envelope with the rate-limit headers attached.
    async fn finish_error(self, state: &AppState, err: AppError) -> Response {
        let correlation_id = self.ctx.correlation_id.clone();
        let (_, envelope) = crate::error::map_error(&err, &correlation_id, state.env());
        let code = envelope.error.code.clone();
        // Best-effort audit of the failed outcome; an audit failure is itself a store error, but the
        // handler already failed, so surface the original error's envelope (do not mask it).
        let _ = self.write_audit(state, &code).await;
        let mut resp = error_response(&err, &correlation_id, state.env());
        attach_rate_headers(&mut resp, &self.rate, false);
        resp
    }

    /// Chain step 8 — write one append-only audit record to the per-tenant `audit_log` table. Never
    /// contains the token, request body, or fact content.
    async fn write_audit(&self, state: &AppState, outcome: &str) -> Result<(), StoreError> {
        let entry = AuditEntry {
            id: format!("audit_log:{}", Uuid::now_v7()),
            tenant: self.ctx.tenant.clone(),
            subject: self.ctx.user.clone(),
            operation: self.operation.to_string(),
            scope: ScopeRef::from(&self.ctx),
            outcome: outcome.to_string(),
            token_jti: self.ctx.token_jti.clone(),
            correlation_id: self.ctx.correlation_id.clone(),
            at: Utc::now(),
        };
        state.store.append_audit(&entry).await
    }
}

/// Build a `ScopeRef` from an authenticated context: the team is the first team membership (or none).
impl From<&ScopeContext> for ScopeRef {
    fn from(ctx: &ScopeContext) -> Self {
        ScopeRef {
            tenant: ctx.tenant.clone(),
            team: ctx.teams.first().cloned(),
            user: ctx.user.clone(),
        }
    }
}

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

/// Decrement the `(subject, class)` token bucket, creating a full bucket on first use. Returns the
/// rate headers (`Ok` allowed, `Err` rejected).
async fn decrement_rate(
    state: &AppState,
    user: &str,
    class: OpClass,
) -> Result<RateHeaders, RateHeaders> {
    let (per_min, burst) = match class {
        OpClass::Read => (state.config.rate_read_per_min, READ_BURST),
        OpClass::Write => (state.config.rate_write_per_min, WRITE_BURST),
    };
    let mut map = state.rate.lock().await;
    let bucket = map
        .entry((user.to_string(), class))
        .or_insert_with(|| TokenBucket::new(burst, per_min));
    bucket.take(std::time::Instant::now())
}

/// Attach the `RateLimit-*` headers (and, when `rejected`, `Retry-After`) to a response.
fn attach_rate_headers(resp: &mut Response, rate: &RateHeaders, rejected: bool) {
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

/// Read the correlation id from request extensions (the middleware always sets it).
fn correlation_id(headers: &HeaderMap, ext: Option<&CorrelationId>, state: &AppState) -> String {
    if let Some(c) = ext {
        return c.0.clone();
    }
    // Fallback: honour a valid inbound header, else mint.
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| Uuid::parse_str(s).is_ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| state.new_correlation_id())
}

// --- Idempotency helpers (chain step 5, writes only) ---------------------------------------------

/// Read + validate the `Idempotency-Key` header for a write route: present, non-empty, 1..=255 chars.
fn idempotency_key(headers: &HeaderMap) -> Result<String, AppError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_default();
    if key.is_empty() || key.chars().count() > 255 {
        return Err(AppError::Validation(
            ValidationKind::MissingIdempotencyKey,
            "Idempotency-Key header is required for writes".into(),
        ));
    }
    Ok(key)
}

/// Look up a stored idempotency outcome for a write route. On a hit, returns the replayed `Response`
/// (the original status + stored body verbatim, with rate headers + correlation id). On a miss returns
/// `Ok(None)`. A store error is surfaced as the mapped error response.
async fn idempotency_replay(
    state: &AppState,
    pipe: &EdgePipeline,
    route: &str,
    key: &str,
) -> Result<Option<Response>, Response> {
    match state
        .store
        .idempotency_get(&pipe.ctx.tenant, &pipe.ctx.user, route, key)
        .await
    {
        Ok(Some((status, body))) => {
            let status = StatusCode::from_u16(status as u16).unwrap_or(StatusCode::OK);
            let mut resp = (status, Json(body)).into_response();
            attach_rate_headers(&mut resp, &pipe.rate, false);
            set_correlation_header(&mut resp, &pipe.ctx.correlation_id);
            Ok(Some(resp))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(error_response(
            &AppError::Store(e),
            &pipe.ctx.correlation_id,
            state.env(),
        )),
    }
}

/// Persist a write route's produced outcome under the idempotency key (TTL = `RECALL_IDEMPOTENCY_TTL_SECS`).
async fn idempotency_persist(
    state: &AppState,
    pipe: &EdgePipeline,
    route: &str,
    key: &str,
    status: StatusCode,
    body: &Value,
) -> Result<(), AppError> {
    state
        .store
        .idempotency_put(
            &pipe.ctx.tenant,
            &pipe.ctx.user,
            route,
            key,
            i64::from(status.as_u16()),
            body,
            state.config.idempotency_ttl_secs,
        )
        .await
        .map_err(AppError::Store)
}

// --- Handlers ------------------------------------------------------------------------------------

/// `GET /v1` — capabilities. Read class.
pub async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe = match EdgePipeline::begin(
        &state,
        &headers,
        &cid,
        "capabilities",
        Op::Read,
        OpClass::Read,
    )
    .await
    {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let openapi_url = format!("http://{}/openapi.json", state.config.http_addr);
    let caps = Capabilities {
        service: "recall".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        operations: vec![
            "recall".into(),
            "remember".into(),
            "get_fact".into(),
            "retire".into(),
            "delete".into(),
            "capabilities".into(),
        ],
        openapi: openapi_url,
    };
    let body = Success {
        data: caps,
        meta: Meta {
            correlation_id: cid.clone(),
            ..Default::default()
        },
    };
    pipe.finish_success(&state, StatusCode::OK, body, vec![]).await
}

/// `POST /v1/recall` — read. Delegates to C6.
pub async fn recall(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
    raw: Bytes,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe = match EdgePipeline::begin(&state, &headers, &cid, "recall", Op::Read, OpClass::Read)
        .await
    {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Deserialise the body; a malformed body is VAL_INVALID_BODY.
    let req: RecallRequest = match serde_json::from_slice(&raw) {
        Ok(r) => r,
        Err(_) => {
            return pipe
                .finish_error(
                    &state,
                    AppError::Validation(ValidationKind::InvalidBody, "recall body".into()),
                )
                .await;
        }
    };
    // result_cap range [1,50] (the edge enforces the range; C6 enforces the hard max).
    if req.result_cap < 1 || req.result_cap > 50 {
        return pipe
            .finish_error(
                &state,
                AppError::Validation(ValidationKind::OutOfRange, "result_cap".into()),
            )
            .await;
    }

    match state.engine.recall(&pipe.ctx, &req).await {
        Ok(outcome) => {
            let body = Success {
                data: outcome.response,
                meta: Meta {
                    next_cursor: outcome.next_cursor,
                    abstained: outcome.abstained.then_some(true),
                    correlation_id: cid.clone(),
                },
            };
            pipe.finish_success(&state, StatusCode::OK, body, vec![]).await
        }
        Err(e) => pipe.finish_error(&state, e).await,
    }
}

/// `POST /v1/memories` — write (async). Enqueues an `ExtractFact` job on C2, returns 202.
pub async fn remember(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
    raw: Bytes,
) -> Response {
    const ROUTE: &str = "POST /v1/memories";
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe =
        match EdgePipeline::begin(&state, &headers, &cid, "remember", Op::Write, OpClass::Write)
            .await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    // Chain step 5 — idempotency key required for writes.
    let key = match idempotency_key(&headers) {
        Ok(k) => k,
        Err(e) => return pipe.finish_error(&state, e).await,
    };
    // Replay a stored outcome verbatim if present.
    match idempotency_replay(&state, &pipe, ROUTE, &key).await {
        Ok(Some(resp)) => return resp,
        Ok(None) => {}
        Err(resp) => return resp,
    }

    // Deserialise the request body (VAL_INVALID_BODY on failure).
    let req: RememberRequest = match serde_json::from_slice(&raw) {
        Ok(r) => r,
        Err(_) => {
            return pipe
                .finish_error(
                    &state,
                    AppError::Validation(ValidationKind::InvalidBody, "remember body".into()),
                )
                .await;
        }
    };

    // Build and enqueue an ExtractFact WorkJob; C2 assigns id/attempts/status/timestamps.
    let payload = remember_payload(&req);
    let job = WorkJob {
        id: format!("work_job:{}", Uuid::now_v7()),
        kind: JobKind::ExtractFact,
        payload,
        scope: ScopeRef::from(&pipe.ctx),
        idempotency_key: Some(key.clone()),
        attempts: 0,
        status: JobStatus::Pending,
        not_before: Utc::now(),
        created_at: Utc::now(),
        leased_until: None,
    };
    let job_id = match state.queue.enqueue(job).await {
        Ok(id) => id,
        Err(e) => return pipe.finish_error(&state, AppError::Queue(e)).await,
    };

    let ack = WriteAck {
        job_id,
        status: JobAckStatus::Accepted,
    };
    let body = Success {
        data: ack,
        meta: Meta {
            correlation_id: cid.clone(),
            ..Default::default()
        },
    };
    // Persist the outcome for idempotent replay (status AlreadyAccepted on replay is achieved by
    // storing the verbatim body; the spec's "already-accepted" status surfaces because the stored body
    // is replayed — see the note below: we store an AlreadyAccepted-shaped body for replay).
    let stored_body = success_value_with_status(&cid, &body.data.job_id, JobAckStatus::AlreadyAccepted);
    if let Err(e) = idempotency_persist(&state, &pipe, ROUTE, &key, StatusCode::ACCEPTED, &stored_body).await
    {
        return pipe.finish_error(&state, e).await;
    }
    pipe.finish_success(&state, StatusCode::ACCEPTED, body, vec![]).await
}

/// Build the work-job payload for a remember request: the request serialised as JSON (content +
/// optional source + agent_stated flag), validated by the C4 consumer.
fn remember_payload(req: &RememberRequest) -> Value {
    let mut payload = serde_json::json!({
        "content": req.content,
        "agent_stated": req.agent_stated,
    });
    if let Some(src) = &req.source {
        let mut s = serde_json::json!({ "origin_ref": src.origin_ref });
        if let Some(m) = &src.modification_marker {
            s["modification_marker"] = Value::String(m.clone());
        }
        payload["source"] = s;
    }
    payload
}

/// Build the stored idempotency body for a remember replay: the `Success<WriteAck>` shape carrying the
/// original `job_id` and `status = already-accepted`.
fn success_value_with_status(cid: &str, job_id: &str, status: JobAckStatus) -> Value {
    let status_str = match status {
        JobAckStatus::Accepted => "accepted",
        JobAckStatus::AlreadyAccepted => "already-accepted",
    };
    serde_json::json!({
        "data": { "job_id": job_id, "status": status_str },
        "meta": { "correlation_id": cid }
    })
}

/// `GET /v1/memories/{id}` — read. Honours `If-None-Match` / `If-Modified-Since` (→ 304).
pub async fn get_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe =
        match EdgePipeline::begin(&state, &headers, &cid, "get_fact", Op::Read, OpClass::Read).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    let fact = match state.store.get_fact(&pipe.ctx, &id).await {
        Ok(Some(f)) => f,
        Ok(None) => return pipe.finish_error(&state, AppError::NotFound).await,
        Err(e) => return pipe.finish_error(&state, AppError::Store(e)).await,
    };

    let etag = compute_etag(&fact);
    // Conditional GET: 304 when If-None-Match matches the ETag, or If-Modified-Since >= ingested_at.
    if not_modified(&headers, &etag, fact.ingested_at) {
        // A 304 carries no envelope body and is still audited as a success.
        let correlation = pipe.ctx.correlation_id.clone();
        if let Err(e) = pipe.write_audit(&state, "success").await {
            return error_response(&AppError::Store(e), &correlation, state.env());
        }
        let mut resp = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(v) = HeaderValue::from_str(&etag) {
            resp.headers_mut().insert(ETAG, v);
        }
        attach_rate_headers(&mut resp, &pipe.rate, false);
        set_correlation_header(&mut resp, &correlation);
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
    pipe.finish_success(
        &state,
        StatusCode::OK,
        body,
        etag_header.into_iter().collect(),
    )
    .await
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

/// `POST /v1/memories/{id}/retire` — forget (non-destructive). Calls C1 `end_validity`.
pub async fn retire_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    const ROUTE: &str = "POST /v1/memories/{id}/retire";
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe =
        match EdgePipeline::begin(&state, &headers, &cid, "retire", Op::Forget, OpClass::Write).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    let key = match idempotency_key(&headers) {
        Ok(k) => k,
        Err(e) => return pipe.finish_error(&state, e).await,
    };
    match idempotency_replay(&state, &pipe, ROUTE, &key).await {
        Ok(Some(resp)) => return resp,
        Ok(None) => {}
        Err(resp) => return resp,
    }

    let now = Utc::now();
    match state.store.end_validity(&pipe.ctx, &id, now).await {
        Ok(()) => {}
        Err(StoreError::NotFound) => {
            return pipe.finish_error(&state, AppError::NotFound).await
        }
        Err(e) => return pipe.finish_error(&state, AppError::Store(e)).await,
    }

    let ack = RetireAck {
        record_id: id.clone(),
        retired_at: now,
    };
    let body = Success {
        data: ack,
        meta: Meta {
            correlation_id: cid.clone(),
            ..Default::default()
        },
    };
    let stored = serde_json::json!({
        "data": { "record_id": id, "retired_at": now.to_rfc3339() },
        "meta": { "correlation_id": cid }
    });
    if let Err(e) = idempotency_persist(&state, &pipe, ROUTE, &key, StatusCode::OK, &stored).await {
        return pipe.finish_error(&state, e).await;
    }
    pipe.finish_success(&state, StatusCode::OK, body, vec![]).await
}

/// `DELETE /v1/memories/{id}` — forget (verifiable hard delete). Calls C1 `hard_delete` directly and
/// returns the `DeletionProof`.
///
/// DEVIATION (documented follow-up): the spec describes enqueuing a `HardDelete` job on C2 and blocking
/// up to 30 s on a C1 job-result read for the proof. There is no job-result store for the edge to await,
/// and C7's `handle_hard_delete` itself just calls `store.hard_delete`; calling it directly here is
/// functionally identical, preserves SA-DELETE-01 (never report completion without the proof), and
/// satisfies the route contract. A `StoreError::NotFound` is a 404; other store errors map per X1.
pub async fn delete_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    const ROUTE: &str = "DELETE /v1/memories/{id}";
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe =
        match EdgePipeline::begin(&state, &headers, &cid, "delete", Op::Forget, OpClass::Write).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    let key = match idempotency_key(&headers) {
        Ok(k) => k,
        Err(e) => return pipe.finish_error(&state, e).await,
    };
    match idempotency_replay(&state, &pipe, ROUTE, &key).await {
        Ok(Some(resp)) => return resp,
        Ok(None) => {}
        Err(resp) => return resp,
    }

    let proof = match state.store.hard_delete(&pipe.ctx, &id).await {
        Ok(p) => p,
        Err(StoreError::NotFound) => return pipe.finish_error(&state, AppError::NotFound).await,
        Err(e) => return pipe.finish_error(&state, AppError::Store(e)).await,
    };

    let stored = serde_json::json!({
        "data": serde_json::to_value(&proof).unwrap_or(Value::Null),
        "meta": { "correlation_id": cid }
    });
    let body = Success {
        data: proof,
        meta: Meta {
            correlation_id: cid.clone(),
            ..Default::default()
        },
    };
    if let Err(e) = idempotency_persist(&state, &pipe, ROUTE, &key, StatusCode::OK, &stored).await {
        return pipe.finish_error(&state, e).await;
    }
    pipe.finish_success(&state, StatusCode::OK, body, vec![]).await
}

/// `GET /openapi.json` — the generated contract document (not envelope-wrapped). Read class; requires a
/// valid token (it sits under the authenticated surface, exposing contract data not fact data).
pub async fn openapi_doc(
    State(state): State<AppState>,
    headers: HeaderMap,
    ext: Option<axum::Extension<CorrelationId>>,
) -> Response {
    let cid = correlation_id(&headers, ext.as_deref(), &state);
    let pipe =
        match EdgePipeline::begin(&state, &headers, &cid, "openapi", Op::Read, OpClass::Read).await {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    // Audit before flushing, like every authenticated route.
    let correlation = pipe.ctx.correlation_id.clone();
    if let Err(e) = pipe.write_audit(&state, "success").await {
        return error_response(&AppError::Store(e), &correlation, state.env());
    }
    let doc = crate::api::openapi::document("recall", env!("CARGO_PKG_VERSION"));
    let mut resp = (StatusCode::OK, Json(doc)).into_response();
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    attach_rate_headers(&mut resp, &pipe.rate, false);
    set_correlation_header(&mut resp, &correlation);
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
    fn missing_idempotency_key_rejected() {
        let headers = HeaderMap::new();
        let err = idempotency_key(&headers).unwrap_err();
        assert!(matches!(
            err,
            AppError::Validation(ValidationKind::MissingIdempotencyKey, _)
        ));
    }

    #[test]
    fn over_long_idempotency_key_rejected() {
        let mut headers = HeaderMap::new();
        let long = "k".repeat(256);
        headers.insert("idempotency-key", HeaderValue::from_str(&long).unwrap());
        assert!(idempotency_key(&headers).is_err());
    }

    #[test]
    fn valid_idempotency_key_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_static("k-001"));
        assert_eq!(idempotency_key(&headers).unwrap(), "k-001");
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
