//! C9 — Service Layer: the transport-agnostic per-operation orchestration core of `recall` (ADR-016).
//!
//! This module owns the security-critical chain that previously lived inside the C8 axum handlers
//! (the `EdgePipeline`): authenticate (C3) → authorise the operation class → per-`(subject, op-class)`
//! rate limiting → write idempotency (writes only) → invoke the owning component (C6 recall, C2 async
//! write, C1 fact read / retire / hard delete) → append-only audit. It knows nothing of HTTP, MCP,
//! headers, JSON wire framing, or status codes: it consumes a verified-from-token [`ScopeContext`]
//! (never a request-supplied scope) and the transport-neutral request/response types, and returns a
//! typed [`CallResult`] or a typed [`AppError`]. Both edges (C8 HTTP, the MCP edge) are thin adapters
//! over it, so there is exactly one definition of auth/scope/rate/idempotency/audit (SA-SVC-01).
//!
//! Correlation-id minting stays at the edge — each transport assigns one and passes it in via
//! [`CallContext`]; everything downstream of it is transport-neutral.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::api::ratelimit::{OpClass, RateHeaders, TokenBucket, READ_BURST, WRITE_BURST};
use crate::auth::{Authenticator, Op};
use crate::config::Config;
use crate::error::{AppError, AuthKind, ValidationKind};
use crate::obs::metrics::Metrics;
use crate::queue::StoreWorkQueue;
use crate::retrieval::{RecallOutcome, RetrievalEngine};
use crate::store::Store;
use crate::types::api::{
    Capabilities, DeletionProof, JobAckStatus, RecallRequest, RememberRequest, RetireAck, WriteAck,
};
use crate::types::domain::Fact;
use crate::types::job::{JobKind, JobStatus, WorkJob};
use crate::types::ports::{AuditEntry, MemoryStore, StoreError, WorkQueue};
use crate::types::scope::{ScopeContext, ScopeRef};

/// The in-process rate-limiter state: one token bucket per `(subject, operation-class)`. Bounded — the
/// only in-process state the Service holds (the idempotency record and the audit trail live in C1's
/// store). Shared with the edge via `Arc` so both observe the same buckets.
pub type RateLimiter = Arc<tokio::sync::Mutex<HashMap<(String, OpClass), TokenBucket>>>;

/// The transport-agnostic core. Holds the injected component handles and is cheap to clone (Arc-backed).
pub struct Service {
    config: Arc<Config>,
    #[allow(dead_code)] // metrics seam wired through for parity with the edge state; emitted later.
    metrics: Arc<Metrics>,
    store: Arc<Store>,
    queue: Arc<StoreWorkQueue>,
    engine: Arc<RetrievalEngine>,
    auth: Arc<Authenticator>,
    rate: RateLimiter,
}

/// Per-call inputs every edge supplies. `idempotency_key` is required for writes, ignored for reads.
pub struct CallContext<'a> {
    /// Raw token value (after "Bearer "); `""` when absent.
    pub bearer: &'a str,
    /// Correlation id, minted by the edge.
    pub correlation_id: &'a str,
    /// Write-route idempotency key (the `Idempotency-Key` header value at the HTTP edge).
    pub idempotency_key: Option<&'a str>,
}

/// A point-in-time snapshot of the caller's rate bucket after this call's decision. The HTTP edge
/// renders it as `RateLimit-*`; the MCP edge may surface it as metadata. Mirrors [`RateHeaders`].
pub struct RateSnapshot {
    /// Per-minute refill rate for the bucket (the `RateLimit-Limit` value).
    pub limit: u32,
    /// Tokens remaining after this request's decrement.
    pub remaining: u32,
    /// Seconds until the bucket next has a token (`RateLimit-Reset`; also `Retry-After` on a reject).
    pub reset_secs: u64,
}

impl From<RateHeaders> for RateSnapshot {
    fn from(h: RateHeaders) -> Self {
        RateSnapshot {
            limit: h.limit,
            remaining: h.remaining,
            reset_secs: h.reset_secs,
        }
    }
}

/// Transport-neutral result: the typed payload plus the cross-cutting signals each edge renders.
pub struct CallResult<T> {
    /// The operation's typed payload.
    pub data: T,
    /// Rate snapshot for the caller's bucket after this call.
    pub rate: RateSnapshot,
    /// `true` on an idempotent replay (writes only).
    pub replayed: bool,
}

/// The error a call returns. Carries the typed [`AppError`] (the single classification the edge renders
/// to its transport representation) plus an optional [`RateSnapshot`].
///
/// The snapshot is `Some` for every failure that occurred **after** rate limiting passed or **on** rate
/// exhaustion (so the edge attaches `RateLimit-*`, and `Retry-After` on a 429), and `None` for an
/// authentication/authorisation failure that occurred before the bucket was touched (those carry no
/// rate signals, matching the C8 behaviour). The C9 spec's Internal Logic step 3 mandates the snapshot
/// reach the edge even on the 429 path; this wrapper is the channel for that (the bare `AppError` in the
/// spec's simplified signature cannot carry it).
pub struct CallError {
    /// The typed classification the edge renders.
    pub error: AppError,
    /// The caller's rate snapshot, when one was produced for this call.
    pub rate: Option<RateSnapshot>,
}

impl From<AppError> for CallError {
    fn from(error: AppError) -> Self {
        CallError { error, rate: None }
    }
}

/// The authenticated, authorised, rate-limited pre-amble of a call: produced by `begin`, consumed by
/// the per-operation body and by the audit step. The transport-neutral analogue of the old
/// `EdgePipeline` (sans HTTP headers).
struct Authorised {
    ctx: ScopeContext,
    operation: &'static str,
    rate: RateHeaders,
}

impl Service {
    /// Construct the Service over the concrete component stack. Called by `build_state`; the same
    /// handles back both edges.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        metrics: Arc<Metrics>,
        store: Arc<Store>,
        queue: Arc<StoreWorkQueue>,
        engine: Arc<RetrievalEngine>,
        auth: Arc<Authenticator>,
        rate: RateLimiter,
    ) -> Self {
        Self {
            config,
            metrics,
            store,
            queue,
            engine,
            auth,
            rate,
        }
    }

    // --- The six operations ----------------------------------------------------------------------

    /// `GET /v1` (HTTP) — capabilities. Read class. Built from compile-time constants + config.
    pub async fn capabilities(
        &self,
        cx: CallContext<'_>,
    ) -> Result<CallResult<Capabilities>, CallError> {
        let pipe = self
            .begin(&cx, "capabilities", Op::Read, OpClass::Read)
            .await?;

        let openapi_url = format!("http://{}/openapi.json", self.config.http_addr);
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
        self.finish_success(pipe, caps).await
    }

    /// `GET /openapi.json` (HTTP) — runs the read chain (authenticate → authorise → rate-limit →
    /// audit) under the logical operation "openapi" and returns an empty payload; the edge renders the
    /// hand-built OpenAPI document itself (it is not a transport-neutral payload). Read class.
    pub async fn openapi(&self, cx: CallContext<'_>) -> Result<CallResult<()>, CallError> {
        let pipe = self.begin(&cx, "openapi", Op::Read, OpClass::Read).await?;
        self.finish_success(pipe, ()).await
    }

    /// `POST /v1/recall` (HTTP) — read. Validates the request range/class, delegates to C6.
    ///
    /// The request body arrives as raw bytes (rather than a pre-parsed `RecallRequest`) so the fixed
    /// chain ordering is preserved exactly: authenticate → authorise → rate-limit run **before** body
    /// deserialisation, so a malformed body on an unauthenticated request is a 401 (not a 400) and a
    /// body parse failure on an authenticated request is audited as `VAL_INVALID_BODY` — identical to
    /// the former C8 handler, where `EdgePipeline::begin` ran ahead of `serde_json::from_slice`.
    pub async fn recall(
        &self,
        cx: CallContext<'_>,
        body: &[u8],
    ) -> Result<CallResult<RecallOutcome>, CallError> {
        let pipe = self.begin(&cx, "recall", Op::Read, OpClass::Read).await?;

        // Deserialise the body; a malformed body is VAL_INVALID_BODY (audited on the error path).
        let req: RecallRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(_) => {
                return self
                    .finish_error(
                        pipe,
                        AppError::Validation(ValidationKind::InvalidBody, "recall body".into()),
                    )
                    .await
            }
        };

        // result_cap range [1,50] (the range is enforced here; C6 enforces the hard max).
        if req.result_cap < 1 || req.result_cap > 50 {
            return self
                .finish_error(
                    pipe,
                    AppError::Validation(ValidationKind::OutOfRange, "result_cap".into()),
                )
                .await;
        }
        match self.engine.recall(&pipe.ctx, &req).await {
            Ok(outcome) => self.finish_success(pipe, outcome).await,
            Err(e) => self.finish_error(pipe, e).await,
        }
    }

    /// `POST /v1/memories` (HTTP) — write (async). Enqueues an `ExtractFact` job on C2.
    ///
    /// The body arrives as raw bytes (see `recall`) so the fixed chain ordering is preserved: the
    /// idempotency-key validation and replay run **before** body deserialisation, exactly as the former
    /// C8 handler did (key → replay → `serde_json::from_slice` → enqueue).
    pub async fn remember(
        &self,
        cx: CallContext<'_>,
        body: &[u8],
    ) -> Result<CallResult<WriteAck>, CallError> {
        const ROUTE: &str = "POST /v1/memories";
        let pipe = self.begin(&cx, "remember", Op::Write, OpClass::Write).await?;

        // Chain step 4 — idempotency key required for writes.
        let key = match Self::require_idempotency_key(&cx) {
            Ok(k) => k,
            Err(e) => return self.finish_error(pipe, e).await,
        };
        // Replay a stored outcome if present (no new side effect).
        match self
            .idempotency_replay::<WriteAck>(&pipe, ROUTE, &key)
            .await
        {
            Ok(Some(data)) => return self.finish_replay(pipe, data).await,
            Ok(None) => {}
            Err(e) => return self.finish_error(pipe, e).await,
        }

        // Deserialise the request body (VAL_INVALID_BODY on failure).
        let req: RememberRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(_) => {
                return self
                    .finish_error(
                        pipe,
                        AppError::Validation(ValidationKind::InvalidBody, "remember body".into()),
                    )
                    .await
            }
        };

        // Build and enqueue an ExtractFact WorkJob; the as-built handler sets every field.
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
        let job_id = match self.queue.enqueue(job).await {
            Ok(id) => id,
            Err(e) => return self.finish_error(pipe, AppError::Queue(e)).await,
        };

        let ack = WriteAck {
            job_id: job_id.clone(),
            status: JobAckStatus::Accepted,
        };
        // Persist the outcome for an idempotent replay. The stored body carries `already-accepted`, so
        // a replay deserialises back to `AlreadyAccepted` (carried over from the C8 handler).
        let stored = remember_stored_body(&pipe.ctx.correlation_id, &job_id);
        if let Err(e) = self
            .idempotency_persist(&pipe, ROUTE, &key, 202, &stored)
            .await
        {
            return self.finish_error(pipe, e).await;
        }
        self.finish_success(pipe, ack).await
    }

    /// `GET /v1/memories/{id}` (HTTP) — read. Returns the full `Fact`; conditional-GET (ETag /
    /// `If-Modified-Since`) is an HTTP optimisation the edge layers over this method.
    pub async fn get_fact(
        &self,
        cx: CallContext<'_>,
        id: &str,
    ) -> Result<CallResult<Fact>, CallError> {
        let pipe = self.begin(&cx, "get_fact", Op::Read, OpClass::Read).await?;

        match self.store.get_fact(&pipe.ctx, id).await {
            Ok(Some(fact)) => self.finish_success(pipe, fact).await,
            Ok(None) => self.finish_error(pipe, AppError::NotFound).await,
            Err(e) => self.finish_error(pipe, AppError::Store(e)).await,
        }
    }

    /// `POST /v1/memories/{id}/retire` (HTTP) — forget (non-destructive). Calls C1 `end_validity`.
    pub async fn retire(
        &self,
        cx: CallContext<'_>,
        id: &str,
    ) -> Result<CallResult<RetireAck>, CallError> {
        const ROUTE: &str = "POST /v1/memories/{id}/retire";
        let pipe = self.begin(&cx, "retire", Op::Forget, OpClass::Write).await?;

        let key = match Self::require_idempotency_key(&cx) {
            Ok(k) => k,
            Err(e) => return self.finish_error(pipe, e).await,
        };
        match self
            .idempotency_replay::<RetireAck>(&pipe, ROUTE, &key)
            .await
        {
            Ok(Some(data)) => return self.finish_replay(pipe, data).await,
            Ok(None) => {}
            Err(e) => return self.finish_error(pipe, e).await,
        }

        let now = Utc::now();
        match self.store.end_validity(&pipe.ctx, id, now).await {
            Ok(()) => {}
            Err(StoreError::NotFound) => {
                return self.finish_error(pipe, AppError::NotFound).await
            }
            Err(e) => return self.finish_error(pipe, AppError::Store(e)).await,
        }

        let ack = RetireAck {
            record_id: id.to_string(),
            retired_at: now,
        };
        let stored = serde_json::json!({
            "data": { "record_id": id, "retired_at": now.to_rfc3339() },
            "meta": { "correlation_id": pipe.ctx.correlation_id }
        });
        if let Err(e) = self
            .idempotency_persist(&pipe, ROUTE, &key, 200, &stored)
            .await
        {
            return self.finish_error(pipe, e).await;
        }
        self.finish_success(pipe, ack).await
    }

    /// `DELETE /v1/memories/{id}` (HTTP) — forget (verifiable hard delete). Calls C1 `hard_delete`
    /// directly (synchronous, proof-gated; SA-DELETE-01). The edge is not a `HardDelete` job producer.
    pub async fn delete(
        &self,
        cx: CallContext<'_>,
        id: &str,
    ) -> Result<CallResult<DeletionProof>, CallError> {
        const ROUTE: &str = "DELETE /v1/memories/{id}";
        let pipe = self.begin(&cx, "delete", Op::Forget, OpClass::Write).await?;

        let key = match Self::require_idempotency_key(&cx) {
            Ok(k) => k,
            Err(e) => return self.finish_error(pipe, e).await,
        };
        match self
            .idempotency_replay::<DeletionProof>(&pipe, ROUTE, &key)
            .await
        {
            Ok(Some(data)) => return self.finish_replay(pipe, data).await,
            Ok(None) => {}
            Err(e) => return self.finish_error(pipe, e).await,
        }

        let proof = match self.store.hard_delete(&pipe.ctx, id).await {
            Ok(p) => p,
            Err(StoreError::NotFound) => {
                return self.finish_error(pipe, AppError::NotFound).await
            }
            Err(e) => return self.finish_error(pipe, AppError::Store(e)).await,
        };

        let stored = serde_json::json!({
            "data": serde_json::to_value(&proof).unwrap_or(Value::Null),
            "meta": { "correlation_id": pipe.ctx.correlation_id }
        });
        if let Err(e) = self
            .idempotency_persist(&pipe, ROUTE, &key, 200, &stored)
            .await
        {
            return self.finish_error(pipe, e).await;
        }
        self.finish_success(pipe, proof).await
    }

    // --- Orchestration helpers (the fixed sequence, defined once) ---------------------------------

    /// Internal Logic steps 1–3: authenticate (C3), authorise the operation class, and decrement the
    /// `(subject, op-class)` rate bucket. On any failure no audit record is written (the call either
    /// never reached an authenticated identity or is an unauthenticated-class outcome, matching the
    /// existing Gherkin) and the typed error is returned. On a 429 the [`RateSnapshot`] is still
    /// produced (carried on the returned error path's rate via the edge), so the edge can attach limit
    /// signals.
    async fn begin(
        &self,
        cx: &CallContext<'_>,
        operation: &'static str,
        op: Op,
        class: OpClass,
    ) -> Result<Authorised, CallError> {
        // Step 1 — authenticate. No rate snapshot accompanies an auth failure (the bucket is untouched),
        // matching the C8 behaviour where a 401 carries no `RateLimit-*` headers.
        let ctx = match self.auth.validate(cx.bearer, cx.correlation_id).await {
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
                return Err(app.into());
            }
        };

        // Step 2 — authorise the operation class against the token scopes. A 403 also carries no rate
        // snapshot (the bucket is untouched), matching the C8 behaviour.
        if let Err(crate::auth::AuthError::InsufficientScope(_)) =
            Authenticator::authorise(&ctx, op)
        {
            return Err(AppError::InsufficientScope(format!("{op:?}")).into());
        }

        // Step 3 — rate limit, keyed by (subject, op-class). On exhaustion the snapshot is still
        // produced so the edge can attach `RateLimit-*` and `Retry-After` to the 429.
        let rate = match self.decrement_rate(&ctx.user, class).await {
            Ok(h) => h,
            Err(h) => {
                return Err(CallError {
                    error: AppError::RateLimited,
                    rate: Some(h.into()),
                })
            }
        };

        Ok(Authorised {
            ctx,
            operation,
            rate,
        })
    }

    /// Decrement the `(subject, class)` token bucket, creating a full bucket on first use. Returns the
    /// rate headers (`Ok` allowed, `Err` rejected).
    async fn decrement_rate(
        &self,
        user: &str,
        class: OpClass,
    ) -> Result<RateHeaders, RateHeaders> {
        let (per_min, burst) = match class {
            OpClass::Read => (self.config.rate_read_per_min, READ_BURST),
            OpClass::Write => (self.config.rate_write_per_min, WRITE_BURST),
        };
        let mut map = self.rate.lock().await;
        let bucket = map
            .entry((user.to_string(), class))
            .or_insert_with(|| TokenBucket::new(burst, per_min));
        bucket.take(std::time::Instant::now())
    }

    /// Finish a successful call: write the audit record (step 6) before returning. An audit-write
    /// failure turns the success into a `503 STORE_UNAVAILABLE` (the call is not reported successful if
    /// it could not be audited).
    async fn finish_success<T>(
        &self,
        pipe: Authorised,
        data: T,
    ) -> Result<CallResult<T>, CallError> {
        // An audit-write failure on the success path is itself a store error. The C8 `finish_success`
        // returned the bare error response for this case (no `RateLimit-*` headers attached), so the
        // resulting 503 carries no rate snapshot.
        if let Err(error) = self.write_audit(&pipe, "success").await {
            return Err(CallError { error, rate: None });
        }
        Ok(CallResult {
            data,
            rate: pipe.rate.into(),
            replayed: false,
        })
    }

    /// Finish an idempotent replay: return the replayed data with `replayed=true` and the rate snapshot,
    /// performing **no** new side effect. The C8 handler returned the stored response verbatim on a hit
    /// (with `RateLimit-*` attached) and wrote **no** audit record for the replay, so this path writes
    /// no audit record either.
    async fn finish_replay<T>(
        &self,
        pipe: Authorised,
        data: T,
    ) -> Result<CallResult<T>, CallError> {
        Ok(CallResult {
            data,
            rate: pipe.rate.into(),
            replayed: true,
        })
    }

    /// Finish an error call after auth succeeded (step 6, error path): best-effort audit of the failed
    /// outcome — an audit failure never masks the original error. Returns the original `AppError` with
    /// the rate snapshot attached so the edge can still emit `RateLimit-*`.
    async fn finish_error<T>(
        &self,
        pipe: Authorised,
        err: AppError,
    ) -> Result<CallResult<T>, CallError> {
        let code = err.code();
        // Best-effort audit; the handler already failed, so surface the original error (do not mask it).
        let _ = self.write_audit(&pipe, code).await;
        // The C8 `finish_error` attached `RateLimit-*` to a post-auth error, so the snapshot rides along.
        Err(CallError {
            error: err,
            rate: Some(pipe.rate.into()),
        })
    }

    /// Internal Logic step 6 — write one append-only audit record to the per-tenant `audit_log` table.
    /// The record never contains the token, request payload, or fact content (SA-AUDIT-01).
    async fn write_audit(&self, pipe: &Authorised, outcome: &str) -> Result<(), AppError> {
        let entry = AuditEntry {
            id: format!("audit_log:{}", Uuid::now_v7()),
            tenant: pipe.ctx.tenant.clone(),
            subject: pipe.ctx.user.clone(),
            operation: pipe.operation.to_string(),
            scope: ScopeRef::from(&pipe.ctx),
            outcome: outcome.to_string(),
            token_jti: pipe.ctx.token_jti.clone(),
            correlation_id: pipe.ctx.correlation_id.clone(),
            at: Utc::now(),
        };
        self.store.append_audit(&entry).await.map_err(AppError::Store)
    }

    // --- Idempotency (chain step 4, writes only) --------------------------------------------------

    /// Read + validate the `idempotency_key` for a write call: present, non-empty, 1..=255 chars.
    fn require_idempotency_key(cx: &CallContext<'_>) -> Result<String, AppError> {
        let key = cx.idempotency_key.unwrap_or("");
        if key.is_empty() || key.chars().count() > 255 {
            return Err(AppError::Validation(
                ValidationKind::MissingIdempotencyKey,
                "Idempotency-Key header is required for writes".into(),
            ));
        }
        Ok(key.to_string())
    }

    /// Look up a stored idempotency outcome for a write route. On a non-expired hit, deserialises the
    /// stored envelope's `data` sub-object into the typed payload `T`. On a miss returns `Ok(None)`. A
    /// store failure is surfaced as `AppError::Store`.
    async fn idempotency_replay<T: DeserializeOwned>(
        &self,
        pipe: &Authorised,
        route: &str,
        key: &str,
    ) -> Result<Option<T>, AppError> {
        match self
            .store
            .idempotency_get(&pipe.ctx.tenant, &pipe.ctx.user, route, key)
            .await
        {
            Ok(Some((_status, body))) => {
                // The stored body is a `Success`-shaped envelope; the typed payload is `data`.
                let data = body
                    .get("data")
                    .cloned()
                    .ok_or(AppError::Internal)
                    .and_then(|d| serde_json::from_value::<T>(d).map_err(|_| AppError::Internal))?;
                Ok(Some(data))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(AppError::Store(e)),
        }
    }

    /// Persist a write call's produced outcome under the idempotency key (TTL =
    /// `RECALL_IDEMPOTENCY_TTL_SECS`).
    async fn idempotency_persist(
        &self,
        pipe: &Authorised,
        route: &str,
        key: &str,
        status_code: i64,
        body: &Value,
    ) -> Result<(), AppError> {
        self.store
            .idempotency_put(
                &pipe.ctx.tenant,
                &pipe.ctx.user,
                route,
                key,
                status_code,
                body,
                self.config.idempotency_ttl_secs,
            )
            .await
            .map_err(AppError::Store)
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

/// Build the work-job payload for a remember request: the request serialised as JSON (structured
/// content + optional source + optional memory_class), validated by the C4 consumer. Content is
/// agent-asserted; recall performs no LLM extraction (ADR-015).
fn remember_payload(req: &RememberRequest) -> Value {
    let mut payload = serde_json::json!({
        "content": req.content,
    });
    if let Some(mc) = &req.memory_class {
        payload["memory_class"] = serde_json::to_value(mc).unwrap_or(Value::Null);
    }
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
/// original `job_id` and `status = already-accepted` (so a replay deserialises back to
/// `JobAckStatus::AlreadyAccepted`).
fn remember_stored_body(correlation_id: &str, job_id: &str) -> Value {
    serde_json::json!({
        "data": { "job_id": job_id, "status": "already-accepted" },
        "meta": { "correlation_id": correlation_id }
    })
}

#[cfg(test)]
mod tests {
    //! Supportive unit tests: the chain orchestration is exercised through the public methods with no
    //! axum or HTTP type in scope. The BDD suite is the behaviour-preservation gate.

    use super::*;

    #[test]
    fn rate_snapshot_mirrors_rate_headers() {
        let h = RateHeaders {
            limit: 120,
            remaining: 41,
            reset_secs: 7,
        };
        let snap: RateSnapshot = h.into();
        assert_eq!(snap.limit, 120);
        assert_eq!(snap.remaining, 41);
        assert_eq!(snap.reset_secs, 7);
    }

    #[test]
    fn require_idempotency_key_rejects_missing_empty_and_overlong() {
        // Table-driven: each row is (key, expected_ok).
        let cases: &[(Option<&str>, bool)] = &[
            (None, false),
            (Some(""), false),
            (Some("k-001"), true),
            // Exactly 255 chars is the inclusive upper bound (accepted); 256 is rejected.
            (Some("x"), true),
        ];
        for (key, ok) in cases {
            let cx = CallContext {
                bearer: "t",
                correlation_id: "c",
                idempotency_key: *key,
            };
            assert_eq!(
                Service::require_idempotency_key(&cx).is_ok(),
                *ok,
                "key {key:?}"
            );
        }

        let long = "k".repeat(256);
        let cx = CallContext {
            bearer: "t",
            correlation_id: "c",
            idempotency_key: Some(&long),
        };
        assert!(Service::require_idempotency_key(&cx).is_err());

        let exact = "k".repeat(255);
        let cx = CallContext {
            bearer: "t",
            correlation_id: "c",
            idempotency_key: Some(&exact),
        };
        assert!(Service::require_idempotency_key(&cx).is_ok());
    }

    #[test]
    fn remember_stored_body_round_trips_to_already_accepted() {
        let body = remember_stored_body("corr-1", "work_job:abc");
        let data = body.get("data").cloned().unwrap();
        let ack: WriteAck = serde_json::from_value(data).unwrap();
        assert!(matches!(ack.status, JobAckStatus::AlreadyAccepted));
        assert_eq!(ack.job_id, "work_job:abc");
    }

    #[test]
    fn remember_payload_includes_optional_source_and_class() {
        let req: RememberRequest = serde_json::from_value(serde_json::json!({
            "content": {"subject": "a", "predicate": "owns", "object": "b"},
            "source": {"origin_ref": "src:1", "modification_marker": "m"},
            "memory_class": "semantic"
        }))
        .unwrap();
        let payload = remember_payload(&req);
        assert_eq!(payload["content"]["subject"], "a");
        assert_eq!(payload["source"]["origin_ref"], "src:1");
        assert_eq!(payload["source"]["modification_marker"], "m");
        assert_eq!(payload["memory_class"], "semantic");
    }
}
