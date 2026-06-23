### SPEC: HTTP API Edge
**File:** `src/api` | **Package:** `recall::api` | **Phase:** 5 | **Dependencies:** C1 (Memory Store), C2 (Durable Work Queue), C3 (Auth & Scope), C6 (Retrieval Engine)

> **Mode:** greenfield
> **derivedFromHld:** 0.6.0

#### Purpose

The HTTP API Edge is the single externally-reachable surface of `recall`. It terminates HTTPS, exposes the small set of task-shaped endpoints (capabilities, recall, remember, get-fact, retire, delete, OpenAPI) under `/v1`, and the three unauthenticated operational endpoints (`/healthz`, `/readyz`, `/metrics`). It owns the per-request middleware chain — correlation-id assignment, body-size limiting, authentication delegation to C3, per-(subject, operation-class) rate limiting, idempotency enforcement on writes — and it owns the two response contracts: the single success envelope `Success<T>` and the single error envelope `ErrorEnvelope`, with every component error mapped to a registered (HTTP status, machine `code`) pair. On every authenticated call it writes the per-call append-only audit record to the per-tenant `audit_log` table (owned by C1) before the response is returned. It holds no domain logic: recall is delegated to C6, the `remember` write is enqueued on C2, fact reads, retire, and the verifiable hard delete go through C1 directly.

#### Approach

The edge is an `axum` `Router` (committed by the stack, ADR-009) with the middleware chain expressed as a fixed, ordered `tower` layer stack so that ordering is a compile-time property of the router, not an emergent runtime accident. The chain runs outermost-first: correlation-id → body-size limit → auth → rate limit → idempotency → handler → audit. Each concern is one layer; handlers stay thin. An alternative — embedding each cross-cutting check inline at the top of every handler — was rejected because it duplicates the chain across seven handlers and makes ordering unverifiable. A second alternative — a single monolithic middleware doing all checks — was rejected because it prevents per-concern unit testing and per-concern metrics. The edge owns no persistent state of its own except the idempotency record, which it stores in C1's store (no new datastore); the audit record is written to a C1-owned table. The router is built by `build_router(state: AppState)`; the `AppState` (config, metrics, the concrete `Arc<Store>`, the `Arc<StoreWorkQueue>`, the `Arc<RetrievalEngine>`, the `Arc<Authenticator>`, and the in-process `RateLimiter` token-bucket map) is assembled by the **async** `build_state(config)`, which wires the full stack — opening the embedded store via `Store::connect` (SurrealKV at `RECALL_STORE_PATH`). The two outermost middleware concerns (correlation-id, body-size limit via `DefaultBodyLimit`) are tower layers on the router so their order is structural; the inner concerns (auth → rate limit → idempotency → handler → audit) run inside each `/v1` handler via an `EdgePipeline` in that fixed order. A panic-recovery layer wraps the stack.

#### Shared Context

Duplicated from Phase 2C and 2D (no cross-reference — implement from this section alone). All public API JSON uses `snake_case` field names. All timestamps are UTC RFC 3339 with millisecond precision.

##### Response envelopes (2C.1)

```rust
// src/types/envelope.rs

/// Success envelope. Every 2xx body is exactly this shape.
#[derive(Serialize)]
pub struct Success<T: Serialize> {
    pub data: T,
    pub meta: Meta,
}

#[derive(Serialize, Default)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,   // opaque pagination cursor (SA-PAGE-01)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abstained: Option<bool>,       // true on a recall that gated out all candidates
    pub correlation_id: String,        // uuid, echoes the per-request id
}

/// Error envelope. Every non-2xx body is exactly this shape.
#[derive(Serialize)]
pub struct ErrorEnvelope { pub error: ErrorBody }

#[derive(Serialize)]
pub struct ErrorBody {
    pub code: String,        // SCREAMING_SNAKE_CASE, from the error registry (SA-ENV-02)
    pub message: String,     // human-readable, never leaks internal state or PII
    pub correlation_id: String,
}
```

`correlation_id` MUST be present on every response. `code` MUST be a registered value.

##### Scope & auth context (2C.3)

```rust
// src/types/scope.rs

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef { pub tenant: String, pub team: Option<String>, pub user: String }

/// Built by C3 from the validated token. Never constructed from request-body input.
#[derive(Clone)]
pub struct ScopeContext {
    pub tenant: String,
    pub teams: Vec<String>,
    pub user: String,
    pub token_jti: String,        // for the audit trail (never the token itself)
    pub allowed_ops: OpSet,       // read / write / forget, from token scopes
    pub correlation_id: String,
}

#[derive(Clone, Copy)]
pub struct OpSet { pub read: bool, pub write: bool, pub forget: bool }
```

##### API request/response payloads (2C.4)

```rust
// src/types/api.rs

#[derive(Deserialize)]
pub struct RecallRequest {
    pub query: String,                       // 1..=4096 chars, non-empty
    #[serde(default)] pub filters: RecallFilters,
    #[serde(default = "default_result_cap")] pub result_cap: u8, // [1,50], default 10 (SA-CAP-01)
    #[serde(default)] pub cursor: Option<String>,               // opaque, from a prior meta.next_cursor
    #[serde(default)] pub include_provenance: bool,             // opt-in: attach source origin_ref + marker (SA-PROV-01)
}

#[derive(Deserialize, Default)]
pub struct RecallFilters {
    pub memory_class: Option<MemoryClass>,
    pub visibility: Option<Visibility>,
    pub entity: Option<String>,
    pub valid_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)] pub struct RecallResponse { pub facts: Vec<RankedFact> }

#[derive(Serialize)] pub struct RankedFact {
    pub fact: Fact,
    pub score: f64,
    #[serde(skip_serializing_if = "Option::is_none")] pub source: Option<SourceProvenance>, // only when include_provenance (SA-PROV-01)
}

#[derive(Serialize)]
pub struct SourceProvenance {
    pub origin_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub modification_marker: Option<String>,
}

#[derive(Deserialize)]
pub struct RememberRequest {
    pub content: serde_json::Value,          // structured assertion object (the fact to store)
    pub source: Option<SourceInput>,
    #[serde(default)] pub memory_class: Option<MemoryClass>,
}

#[derive(Deserialize)]
pub struct SourceInput { pub origin_ref: String, pub modification_marker: Option<String> }

#[derive(Serialize)] pub struct WriteAck { pub job_id: String, pub status: JobAckStatus }

#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum JobAckStatus { Accepted, AlreadyAccepted }   // AlreadyAccepted => idempotent replay

#[derive(Serialize)]
pub struct DeletionProof {
    pub deleted_at: DateTime<Utc>,
    pub record_id: String,
    pub derived_removed: Vec<String>,
    pub embeddings_removed: u32,
    pub digest: String,                      // sha256 hex over sorted removed ids (SA-DELETE-01)
}

fn default_result_cap() -> u8 { 10 }
```

`Fact`, `MemoryClass`, `Visibility` are the domain types (2C.2); the edge serialises a `Fact` for `GET /v1/memories/{id}` and inside `RankedFact`, and never mutates one. `MemoryClass` and `Visibility` serialise kebab-case (`episodic`/`semantic`/`consolidated`; `user-private`/`team-shared`/`tenant-shared`).

##### Work-queue type (2C.5)

```rust
// src/types/job.rs

#[derive(Serialize, Deserialize, Clone)]
pub struct WorkJob {
    pub id: String,                          // "work_job:<uuidv7>"
    pub kind: JobKind,
    pub payload: serde_json::Value,          // kind-specific, validated by the consumer
    pub scope: ScopeRef,
    pub idempotency_key: Option<String>,
    pub attempts: u32,
    pub status: JobStatus,
    pub not_before: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind { ExtractFact, ReEmbedFact, HardDelete }  // ReReadSource removed by ADR-014; Consolidate removed by ADR-015

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus { Pending, Leased, Done, DeadLetter }
```

The edge produces `ExtractFact` jobs only (on `POST /v1/memories`) and submits them via the C2 `WorkQueue::enqueue` port. The as-built handler constructs the full `WorkJob` — it sets `id` (`work_job:<uuidv7>`), `kind`, `payload`, `scope`, `idempotency_key`, and the initial `attempts: 0`, `status: Pending`, `not_before`/`created_at` (`Utc::now()`), `leased_until: None` — and `enqueue` returns the assigned job id. `HardDelete` remains a valid `JobKind` for queue-driven / maintenance-triggered deletes (consumed by C7), but the HTTP edge is **not** its producer: `DELETE /v1/memories/{id}` calls C1 `hard_delete` directly (see the route and Internal Logic step 6).

##### Typed application error (2C.7)

```rust
// src/error.rs

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("validation: {1}")]      Validation(ValidationKind, String),  // -> 400 VAL_* (code per ValidationKind)
    #[error("unauthenticated: {1}")] Unauthenticated(AuthKind, String),   // -> 401 AUTH_* (code per AuthKind)
    #[error("forbidden: {0}")]       Forbidden(String),      // -> 403 SCOPE_FORBIDDEN
    #[error("insufficient scope: {0}")] InsufficientScope(String), // -> 403 AUTH_INSUFFICIENT_SCOPE
    #[error("not found")]            NotFound,               // -> 404 NOT_FOUND
    #[error("rate limited")]         RateLimited,            // -> 429 RATE_LIMITED
    #[error("store: {0}")]           Store(#[from] StoreError),
    #[error("queue: {0}")]           Queue(#[from] QueueError),
    #[error("provider: {0}")]        Provider(#[from] ProviderError),
    #[error("internal")]             Internal,               // -> 500 INTERNAL
}
```

##### Ports consumed (2C.6)

```rust
#[async_trait]
pub trait MemoryStore: Send + Sync {                 // C1
    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError>;
    async fn end_validity(&self, ctx: &ScopeContext, id: &str, at: DateTime<Utc>) -> Result<(), StoreError>;
    // recall / hard_delete / put_fact and the audit persistence used by the edge are
    // declared in full in the C1 spec (see Data Model below for the exact methods the edge calls).
}

// Idempotency get/put are NOT on the MemoryStore trait. They are concrete inherent methods on the
// C1 `Store` struct, called directly by the edge:
//   async fn idempotency_get(&self, tenant: &str, user: &str, route: &str, key: &str)
//       -> Result<Option<(i64, serde_json::Value)>, StoreError>;   // (status_code, response_body)
//   async fn idempotency_put(&self, tenant: &str, user: &str, route: &str, key: &str,
//       status_code: i64, body: &serde_json::Value, ttl_secs: u32) -> Result<(), StoreError>;
// `idempotency_get` returns only non-expired rows (WHERE expires_at > time::now()); an expired row
// reads as a miss. `idempotency_put` writes a deterministic record id (see Data Model).

#[async_trait]
pub trait WorkQueue: Send + Sync {                    // C2
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>;
}
```

C3 (Auth & Scope) exposes a tower-compatible authentication entry point that, given the `Authorization` header and the request's correlation id, returns `Result<ScopeContext, AppError>` (`AppError::Unauthenticated` for a missing/invalid token, `AppError::Forbidden` for a valid token lacking the requested operation scope). C6 (Retrieval Engine) exposes `async fn recall(ctx: &ScopeContext, req: &RecallRequest) -> Result<RecallOutcome, AppError>`, where `RecallOutcome { response: RecallResponse, next_cursor: Option<String>, abstained: bool }` carries the response plus the gating and pagination signals. The edge maps the outcome into `Meta` as `next_cursor: outcome.next_cursor`, `abstained: outcome.abstained.then_some(true)` (the field is `Some(true)` only when C6 abstained, otherwise omitted), and `correlation_id`.

##### Configuration & environment variables (2D, edge-owned + consumed)

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_HTTP_ADDR` | socket addr | `0.0.0.0:8080` | no | Bind address for the HTTP API. |
| `RECALL_MAX_BODY_BYTES` | u32 | `1048576` | no | Max request body in bytes (1 MiB). |
| `RECALL_IDEMPOTENCY_TTL_SECS` | u32 | `86400` | no | Idempotency-key retention window (24 h, SA-IDEM-01). |
| `RECALL_RATE_READ_PER_MIN` | u32 | `120` | no | Read-class token-bucket refill (burst 40, SA-RATE-01). |
| `RECALL_RATE_WRITE_PER_MIN` | u32 | `30` | no | Write-class token-bucket refill (burst 10). |
| `RECALL_ENV` | enum `production\|development` | `production` | no | Gates verbose error detail off in production. |
| `RECALL_EMBED_DIM` | u32 | `1024` | no | Embedding dimension; `/readyz` asserts it equals the vector-index dimension (SA-EMBED-01). |

Read-class burst 40 and write-class burst 10 are fixed constants from SA-RATE-01 (no env var), exposed as `READ_BURST = 40` and `WRITE_BURST = 10` in `src/api/ratelimit.rs`. The `TokenBucket` also exposes a `TokenBucket::empty(capacity, per_min)` constructor — a test seam that drives the 429 path without sending `burst + 1` requests; production always builds full buckets via `TokenBucket::new`. The hard `result_cap` upper bound (50) is validated by C6; the edge enforces only the `[1,50]` range after deserialisation.

#### Public Interface

Each route below states method + path + request schema + response schema + status codes. Every `/v1` route except `GET /v1` and `GET /openapi.json` requires `Authorization: Bearer <OIDC JWT>`; `GET /v1` and `GET /openapi.json` require a valid token too (they expose contract data, not fact data, but sit under `/v1`). The three operational routes are unauthenticated. Operation classes: **read** = `recall`, `get-fact`; **write** = `remember`; **forget** = `retire`, `delete`.

**`GET /v1`** — capabilities. Request: no body. Response `200`: `Success<Capabilities>` where `Capabilities { service: String, version: String, operations: Vec<String>, openapi: String }` (`openapi` is the absolute URL of `/openapi.json`). Errors: `401` (missing/invalid token), `429`.

**`POST /v1/recall`** — read. Request body: `RecallRequest` (including the optional `include_provenance` flag, passed through to C6). Calls C6 `recall`. Response `200`: `Success<RecallResponse>` with `meta.abstained` set when C6 gated out all candidates and `meta.next_cursor` set when more results exist; each returned fact carries a `source` object only when `include_provenance` was set and the fact has a source (ADR-014). Errors: `400` `VAL_INVALID_BODY` / `VAL_OUT_OF_RANGE` / `VAL_UNSUPPORTED_CLASS`, `401`, `403` `AUTH_INSUFFICIENT_SCOPE`, `413`, `429`, `502`/`504` (provider), `503`/`504` (store).

**`POST /v1/memories`** — write (async, ADR-004). Headers: `Idempotency-Key` (required, 1–255 chars). Request body: `RememberRequest` — `content` must be a structured assertion object (recall holds no LLM and runs no extraction, ADR-015; C4 wraps the object directly). Enqueues an `ExtractFact` `WorkJob` on C2. Response `202`: `Success<WriteAck>` with `status = Accepted` (first submission) or `status = AlreadyAccepted` (replay within the idempotency window). Errors: `400` `VAL_INVALID_BODY` (malformed body or `content` not a JSON object) / `VAL_MISSING_IDEMPOTENCY_KEY`, `401`, `403` `AUTH_INSUFFICIENT_SCOPE`, `413`, `429`, `503` `QUEUE_UNAVAILABLE`.

**`GET /v1/memories/{id}`** — read. Path param `id` (a `fact:<uuidv7>` record id). Optional headers: `If-Modified-Since` (RFC 3339) and `If-None-Match` (ETag). Calls C1 `get_fact` (scope-checked by C1 via `ScopeContext`). Response `200`: `Success<Fact>` with an `ETag` header derived from the fact's `ingested_at` and `id`; `304 Not Modified` (empty body) when `If-None-Match` matches the computed ETag or `If-Modified-Since` is at/after the fact's `ingested_at`. Errors: `401`, `403` `SCOPE_FORBIDDEN` (a different user's private fact in the same tenant), `404` `NOT_FOUND` (no such fact, or out of scope), `429`, `503`/`504` (store).

**`POST /v1/memories/{id}/retire`** — forget (non-destructive). Path param `id`. Header `Idempotency-Key` (required). No body. Calls C1 `end_validity(ctx, id, now)`. Response `200`: `Success<RetireAck>` where `RetireAck { record_id: String, retired_at: DateTime<Utc> }`; a replay within the window returns the original `retired_at`. Errors: `400` `VAL_MISSING_IDEMPOTENCY_KEY`, `401`, `403` `SCOPE_FORBIDDEN`, `404` `NOT_FOUND`, `429`, `503`/`504` (store).

**`DELETE /v1/memories/{id}`** — forget (verifiable hard delete). Path param `id`. Header `Idempotency-Key` (required). No body. The edge calls C1 `hard_delete(ctx, id)` **directly** — a synchronous, proof-gated removal that returns the `DeletionProof` on the same call — and returns `Success<DeletionProof>`. *Justification:* the Forget sequence (HLD 03) models the API receiving the proof and returning "deleted with proof" on the same call; SA-DELETE-01 forbids reporting completion before every derived summary and embedding is removed; the route set defines no separate job-status endpoint, so the proof can only surface on this response. There is no job-result store for the edge to await, and C7's `handle_hard_delete` itself just calls `store.hard_delete`, so the direct call is functionally identical to enqueue-and-await, keeps the work off no separate async hop, and never returns an unproven deletion — SA-DELETE-01 remains honoured. **SCOPE-GAP:** the `HardDelete` `JobKind` and the C7 consumer remain in the system for queue-driven and maintenance-triggered deletes, but the HTTP edge is **not** their producer — this route does not enqueue a `WorkJob`. Response `200`: `Success<DeletionProof>`; a replay within the idempotency window returns the original proof. Errors: `400` `VAL_MISSING_IDEMPOTENCY_KEY`, `401`, `403` `SCOPE_FORBIDDEN`, `404` `NOT_FOUND` (C1 `StoreError::NotFound`: no such fact, or out of scope), `429`, `503`/`504` (store).

**`GET /openapi.json`** — OpenAPI document. Request: no body. Response `200`: an OpenAPI 3.1 JSON document (SA-VER-01), served with `content-type: application/json`. The document is **hand-built** with `serde_json` in `src/api/openapi.rs` (`document(service, version)`) — it enumerates each route, its method, its `operationId`, and its success status — rather than being auto-derived from the handler types. Not wrapped in the success envelope (it is itself the contract document). Errors: `401`, `429`.

**`GET /healthz`** — liveness, unauthenticated. Response `200`: the success envelope `{"data":{"status":"live"},"meta":{"correlation_id":"<uuid>"}}` whenever the process is serving (a fresh correlation id is minted per call; no dependency calls). No fact data.

**`GET /readyz`** — readiness, unauthenticated. Checks (1) store reachable (`Store::ready` answers a trivial query), (2) OIDC discovery reachable (C3 reports a populated JWKS — `Authenticator::cached_key_count` is greater than zero), (3) `RECALL_EMBED_DIM` equals the configured vector-index dimension (`Store::index_embed_dim`, SA-EMBED-01). Response `200`: `{"status":"ready","checks":{"store":true,"oidc":true,"embed_dim":true}}` when all pass; `503`: the same shape with the failing check(s) `false` and `"status":"not_ready"`. No envelope, no fact data.

**`GET /metrics`** — Prometheus exposition format, unauthenticated. Response `200`: `text/plain; version=0.0.4`. No fact content (counter/histogram names + label keys only).

#### Example

Request — recall:

```
POST /v1/recall HTTP/1.1
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
Content-Type: application/json

{"query":"who owns the orders table","result_cap":5,"filters":{"memory_class":"semantic"}}
```

Response — recall hit:

```
HTTP/1.1 200 OK
Content-Type: application/json
RateLimit-Limit: 120
RateLimit-Remaining: 118
RateLimit-Reset: 41

{
  "data": {
    "facts": [
      {
        "fact": {
          "id": "fact:018f7a2b-6c11-7a31-9b44-0d2e5f9c1a77",
          "content": {"subject":"Team Alpha","predicate":"owns","object":"orders table"},
          "entities": ["entity:018f7a2b-6c11-7a31-9b44-aaaa00000001"],
          "source_id": "source:018f7a2b-6c11-7a31-9b44-bbbb00000002",
          "memory_class": "semantic",
          "visibility": "team-shared",
          "owner": {"tenant":"acme","team":"platform","user":"u-42"},
          "valid_from": "2026-01-04T09:00:00.000Z",
          "valid_to": null,
          "ingested_at": "2026-01-04T09:00:01.123Z",
          "confidence": 0.9,
          "salience": 0.7,
          "stability": 4.0,
          "supersedes": null,
          "superseded_by": null,
          "derived_from": [],
          "last_recalled_at": "2026-06-19T11:22:00.000Z"
        },
        "score": 0.87
      }
    ]
  },
  "meta": {"correlation_id": "9b1d...-c3a2"}
}
```

Response — recall abstention (no candidate cleared the gate):

```
HTTP/1.1 200 OK

{"data":{"facts":[]},"meta":{"abstained":true,"correlation_id":"9b1d...-c3a2"}}
```

Response — missing idempotency key on a write:

```
HTTP/1.1 400 Bad Request

{"error":{"code":"VAL_MISSING_IDEMPOTENCY_KEY","message":"Idempotency-Key header is required for writes","correlation_id":"7f0e...-1d55"}}
```

#### Internal Logic

The middleware chain is a fixed `tower` layer stack on the `axum` `Router`. Layers execute outermost-first on the way in and innermost-first on the way out. The order below is binding.

1. **Assign correlation id.** Read an inbound `X-Correlation-Id` header; if absent or not a valid UUID, generate a UUIDv4. Store it in the request extensions and on a tracing span (`http.request` span, see Observability). It is echoed in every response body's `correlation_id` and in the `X-Correlation-Id` response header. Logged: request method, path, correlation id. No error path.

2. **Body-size limit.** Reject any request whose body exceeds `RECALL_MAX_BODY_BYTES` (default 1 048 576) before reading it, using the `Content-Length` header when present and a streaming byte counter otherwise. Possible error: `AppError::Validation` mapped to `413 VAL_BODY_TOO_LARGE`. Logged: rejected size vs limit. Applies to routes with a body (`POST /v1/recall`, `POST /v1/memories`); GET/DELETE bodies are ignored.

3. **Authenticate (delegate to C3).** For every `/v1` route (including `GET /v1` and `GET /openapi.json`), pass the `Authorization` header and the correlation id to C3, which validates the token (signature via JWKS, issuer, audience, expiry) and returns a `ScopeContext`. The operational routes (`/healthz`, `/readyz`, `/metrics`) skip this layer entirely. Possible errors from C3: `AppError::Unauthenticated` → `401 AUTH_MISSING_TOKEN` (no header) or `401 AUTH_INVALID_TOKEN` (signature/issuer/audience/expiry failure); `AppError::Forbidden` → `403 AUTH_INSUFFICIENT_SCOPE` when the token lacks the operation class the route requires (`recall`/`get-fact` need `read`, `remember` needs `write`, `retire`/`delete` need `forget`). The `ScopeContext` is placed in request extensions. Logged: subject (`ctx.user`), tenant, `jti`, decision. The token value is never logged.

4. **Rate limit.** Look up (or create) the token bucket keyed by `(ctx.user, operation_class)` where the class is derived from the route. Read class refills at `RECALL_RATE_READ_PER_MIN` (default 120/min, burst 40); write class (covering write and forget routes) refills at `RECALL_RATE_WRITE_PER_MIN` (default 30/min, burst 10). Decrement one token; if the bucket is empty, reject. On every response (allowed or rejected) emit `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` (seconds until the bucket next has a token). On rejection also emit `Retry-After` (same value as `RateLimit-Reset`). Possible error: `AppError::RateLimited` → `429 RATE_LIMITED`. Logged: subject, class, remaining tokens. Operational routes are not rate-limited.

5. **Idempotency check (writes only: `POST /v1/memories`, `POST /v1/memories/{id}/retire`, `DELETE /v1/memories/{id}`).** Read the `Idempotency-Key` header; reject a missing or empty key, or one over 255 characters, with `AppError::Validation` → `400 VAL_MISSING_IDEMPOTENCY_KEY`. Call the concrete C1 `Store::idempotency_get(ctx.tenant, ctx.user, route, idempotency_key)` (an inherent `Store` method, not a `MemoryStore` trait method). If a non-expired record exists, return its stored response verbatim — the stored body and status code, with the rate-limit headers and correlation id attached — without invoking the handler or producing a new side effect (for `remember` the stored body carries `status = already-accepted`; for retire/delete it is the original `RetireAck`/`DeletionProof`). If no record exists, run the handler, then call `Store::idempotency_put(..., status_code, body, RECALL_IDEMPOTENCY_TTL_SECS)` (default 86 400) to persist the produced response. Possible errors: `AppError::Store` → `503 STORE_UNAVAILABLE` / `504 STORE_TIMEOUT` when the idempotency store is unreachable. Logged: key (hashed), hit/miss.

6. **Dispatch to the handler.** Each handler is thin:
   - `GET /v1` builds `Capabilities` from compile-time constants and config; returns `200`.
   - `POST /v1/recall` deserialises `RecallRequest` (rejecting a malformed body with `400 VAL_INVALID_BODY`; `result_cap` outside `[1,50]` with `400 VAL_OUT_OF_RANGE`; a filter `memory_class` of `procedural` with `400 VAL_UNSUPPORTED_CLASS`), calls C6 `recall(ctx, req)` (the `include_provenance` flag rides along in `req`; C6 attaches each sourced fact's `source` when set), maps `RecallOutcome { response, next_cursor, abstained }` into the body (`data: outcome.response`) and `Meta` (`next_cursor`, `abstained: outcome.abstained.then_some(true)`), returns `200 Success<RecallResponse>`. Provider/store failures from C6 surface as `AppError::Provider`/`AppError::Store`.
   - `POST /v1/memories` deserialises `RememberRequest`, constructs a `WorkJob { kind: ExtractFact, payload: <request as JSON>, scope: ScopeRef::from(ctx), idempotency_key: Some(key) }`, calls C2 `enqueue`, returns `202 Success<WriteAck { job_id, status: Accepted }>`. A queue failure is `AppError::Queue` → `503 QUEUE_UNAVAILABLE`.
   - `GET /v1/memories/{id}` calls C1 `get_fact(ctx, id)`; `Ok(None)` → `404 NOT_FOUND`; `Ok(Some(fact))` → compute the ETag, honour `If-None-Match`/`If-Modified-Since` (→ `304`), else return `200 Success<Fact>` with the `ETag` header.
   - `POST /v1/memories/{id}/retire` calls C1 `end_validity(ctx, id, now)`; a store "not found / not owned" maps to `404 NOT_FOUND`; success → `200 Success<RetireAck>`.
   - `DELETE /v1/memories/{id}` calls C1 `hard_delete(ctx, id)` directly (synchronous, proof-gated); on success returns `200 Success<DeletionProof>`; a `StoreError::NotFound` (fact absent or out of scope) maps to `404 NOT_FOUND`; any other store error maps per the Error Table. The handler enqueues no `WorkJob` (see the route definition's SCOPE-GAP note: the `HardDelete` `JobKind` and the C7 consumer remain for queue-driven / maintenance-triggered deletes, but the edge is not their producer).
   - `GET /openapi.json` returns the hand-built OpenAPI 3.1 document from `src/api/openapi.rs`.

7. **Map errors to the envelope.** Any `AppError` returned by a layer or handler is converted by a single `IntoResponse` implementation to (HTTP status, `ErrorEnvelope`) using the registry in the Error Table. In `RECALL_ENV=production` the `message` is the registry's fixed human text (no internal detail, no PII, no source error string); in `development` the underlying error string is appended after a `: `. The `correlation_id` is read from request extensions. Exactly one error envelope shape is ever emitted.

8. **Emit the audit record (authenticated routes only, before the response is returned).** Build an `AuditEntry` and write it via `Store::append_audit` to the per-tenant `audit_log` table (owned by C1). The entry contains: `id` (`audit_log:<uuidv7>`), `tenant` (`ctx.tenant`), `subject` (`ctx.user`), `operation` (the route's logical op: `capabilities`/`recall`/`remember`/`get_fact`/`retire`/`delete`/`openapi`), `scope` (`ScopeRef::from(&ctx)` — tenant, the first team membership or none, user), `outcome` (`success` or the error `code`), `token_jti` (`ctx.token_jti`), `correlation_id`, and `at` (server time, `Utc::now()`). The record never contains the token, request body, or fact content. On a **success** path the write is synchronous and completes before the HTTP response is flushed; an audit-write failure is `AppError::Store` → `503 STORE_UNAVAILABLE` (the call is not reported as successful if it could not be audited). On an **error** path (`finish_error`) the audit write is best-effort: an audit failure does not mask the original handler error's envelope. A `304 Not Modified` from `get_fact` is audited as a `success`. Operational routes write no audit record. Logged: audit row id, outcome.

The operational routes bypass layers 3–5 and step 8: `/healthz` returns immediately; `/readyz` runs the three reachability checks; `/metrics` renders the registry.

#### Data Model

The edge owns one table — the idempotency record store — held inside the tenant namespace alongside the audit trail. It reuses C1's embedded SurrealDB store (no new datastore; opened via `Store::connect`, SurrealKV at `RECALL_STORE_PATH`). The idempotency get/put operations are concrete inherent methods on the C1 `Store` struct (`idempotency_get` / `idempotency_put`), called directly by the edge — they are not part of the `MemoryStore` trait. The `audit_log` table is owned and defined by C1; the edge only writes rows to it (via `Store::append_audit`).

The `idempotency_record` table is defined in the single squashed initial migration `migrations/0001_init.up.surql` (version 1; FU-018 squashed the five originally-numbered migrations into one, so there is no separate `0005_idempotency` migration). The as-built DDL:

```sql
-- Defined in migrations/0001_init.up.surql (version 1). Held within each tenant namespace (ADR-011),
-- like audit_log. SurrealDB DDL (SurrealQL); one row per (tenant, user, route, key) write attempt.
DEFINE TABLE IF NOT EXISTS idempotency_record SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS tenant          ON idempotency_record TYPE string;
DEFINE FIELD IF NOT EXISTS user            ON idempotency_record TYPE string;
DEFINE FIELD IF NOT EXISTS route           ON idempotency_record TYPE string;   -- e.g. "POST /v1/memories"
DEFINE FIELD IF NOT EXISTS idempotency_key ON idempotency_record TYPE string    ASSERT string::len($value) >= 1 AND string::len($value) <= 255;
DEFINE FIELD IF NOT EXISTS response_body   ON idempotency_record TYPE object FLEXIBLE; -- the original Success payload, replayed verbatim
DEFINE FIELD IF NOT EXISTS status_code     ON idempotency_record TYPE int;
DEFINE FIELD IF NOT EXISTS created_at      ON idempotency_record TYPE datetime   VALUE time::now();
DEFINE FIELD IF NOT EXISTS expires_at      ON idempotency_record TYPE datetime;  -- created_at + RECALL_IDEMPOTENCY_TTL_SECS
-- Uniqueness: one stored outcome per (tenant, user, route, key).
DEFINE INDEX IF NOT EXISTS idx_idem_key    ON idempotency_record FIELDS tenant, user, route, idempotency_key UNIQUE;
-- Expiry lookup for the sweeper / lazy-expire-on-read.
DEFINE INDEX IF NOT EXISTS idx_idem_expiry ON idempotency_record FIELDS expires_at;
```

`response_body` is `TYPE object FLEXIBLE` so the verbatim `Success` payload (with its arbitrary nested `data`) is stored without a fixed schema. The record id is **deterministic, not random**: `idempotency_record:<uuidv5>`, where the UUID v5 is computed (namespace `Uuid::NAMESPACE_OID`) over the seed `"{tenant}\u{1f}{user}\u{1f}{route}\u{1f}{key}"` (the four key components joined by the unit-separator `\u{1f}`). A replay-after-expiry therefore overwrites the same row rather than racing the UNIQUE index.

Expiry is enforced two ways: `idempotency_get` filters on `expires_at > time::now()`, so a read past `expires_at` is treated as a miss (the handler re-runs and `idempotency_put` overwrites the row); a background sweep (run by C7 maintenance or a periodic task) deletes rows where `expires_at < time::now()`. Rollback: the down migration `migrations/0001_init.down.surql` removes the table along with the rest of the initial schema. This is destructive — it drops all stored idempotency outcomes, so an in-flight retry after rollback would re-execute its write (at-least-once instead of exactly-once within the window); acceptable because the down path is a teardown step, not a production operation. No `audit_log` DDL appears here: C1 owns it; the edge depends on a C1 audit-write port and writes the fields enumerated in Internal Logic step 8.

`audit_log` fields the edge writes (defined by C1 in `migrations/0001_init.up.surql`, restated for the implementer): `subject: string`, `operation: string`, `scope: object FLEXIBLE`, `outcome: string`, `token_jti: string`, `correlation_id: string`, `at: datetime`. The `scope` field is `TYPE object FLEXIBLE` (a 3.x audit-write fix so the full `ScopeRef` sub-keys — tenant/team/user — persist). The table is append-only (no update/delete grant for the edge) and per-tenant (within the namespace), so it is erased with the tenant (ADR-011).

#### Error Table

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| Request body exceeds `RECALL_MAX_BODY_BYTES` | 413 | VAL_BODY_TOO_LARGE | `{"error":{"code":"VAL_BODY_TOO_LARGE","message":"Request body exceeds the maximum permitted size","correlation_id":"<uuid>"}}` |
| Malformed JSON / missing required field in `RecallRequest` or `RememberRequest` | 400 | VAL_INVALID_BODY | `{"error":{"code":"VAL_INVALID_BODY","message":"Request body is invalid","correlation_id":"<uuid>"}}` |
| `result_cap` outside `[1,50]` | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"A field is outside its permitted range","correlation_id":"<uuid>"}}` |
| Recall filter `memory_class` is `procedural` | 400 | VAL_UNSUPPORTED_CLASS | `{"error":{"code":"VAL_UNSUPPORTED_CLASS","message":"The requested memory class is not supported","correlation_id":"<uuid>"}}` |
| Write route with no / empty / over-255-char `Idempotency-Key` | 400 | VAL_MISSING_IDEMPOTENCY_KEY | `{"error":{"code":"VAL_MISSING_IDEMPOTENCY_KEY","message":"Idempotency-Key header is required for writes","correlation_id":"<uuid>"}}` |
| `/v1` route with no `Authorization` header | 401 | AUTH_MISSING_TOKEN | `{"error":{"code":"AUTH_MISSING_TOKEN","message":"Authentication token is required","correlation_id":"<uuid>"}}` |
| Token fails signature / issuer / audience / expiry validation (C3) | 401 | AUTH_INVALID_TOKEN | `{"error":{"code":"AUTH_INVALID_TOKEN","message":"Authentication token is invalid","correlation_id":"<uuid>"}}` |
| Valid token lacks the operation-class scope the route needs | 403 | AUTH_INSUFFICIENT_SCOPE | `{"error":{"code":"AUTH_INSUFFICIENT_SCOPE","message":"Token does not grant the required operation","correlation_id":"<uuid>"}}` |
| Caller's scope does not own the targeted fact (get/retire/delete) | 403 | SCOPE_FORBIDDEN | `{"error":{"code":"SCOPE_FORBIDDEN","message":"The caller may not access this record","correlation_id":"<uuid>"}}` |
| Fact id not found, or out of scope and indistinguishable from absent | 404 | NOT_FOUND | `{"error":{"code":"NOT_FOUND","message":"The requested record was not found","correlation_id":"<uuid>"}}` |
| Token bucket for `(subject, op-class)` exhausted | 429 | RATE_LIMITED | `{"error":{"code":"RATE_LIMITED","message":"Rate limit exceeded","correlation_id":"<uuid>"}}` (with `RateLimit-*` + `Retry-After` headers) |
| Store / idempotency store / audit write unreachable | 503 | STORE_UNAVAILABLE | `{"error":{"code":"STORE_UNAVAILABLE","message":"The store is temporarily unavailable","correlation_id":"<uuid>"}}` |
| Store read/write exceeds its timeout (including a `hard_delete` that does not complete in time) | 504 | STORE_TIMEOUT | `{"error":{"code":"STORE_TIMEOUT","message":"The store did not respond in time","correlation_id":"<uuid>"}}` |
| Work-queue enqueue fails (remember only — delete no longer enqueues) | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"The work queue is temporarily unavailable","correlation_id":"<uuid>"}}` |
| Embedding/reranker provider times out during recall (C6) | 504 | PROVIDER_TIMEOUT | `{"error":{"code":"PROVIDER_TIMEOUT","message":"An upstream provider did not respond in time","correlation_id":"<uuid>"}}` |
| Embedding/reranker provider returns an error during recall (C6) | 502 | PROVIDER_ERROR | `{"error":{"code":"PROVIDER_ERROR","message":"An upstream provider returned an error","correlation_id":"<uuid>"}}` |
| Unexpected internal failure (panic guard, unmapped error) | 500 | INTERNAL | `{"error":{"code":"INTERNAL","message":"An internal error occurred","correlation_id":"<uuid>"}}` |

Every code is from the central registry (SA-ENV-02); the edge invents none. In `production` the `message` is exactly the text above; in `development` the underlying error string is appended.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: HTTP API Edge

  Scenario: Happy path — recall returns ranked facts in the success envelope
    Given a valid OIDC bearer token granting the read scope
    And the store holds two facts matching "who owns the orders table" within the caller's scope
    When the broker sends POST /v1/recall with body {"query":"who owns the orders table","result_cap":5}
    Then the response status is 200
    And the body matches {"data":{"facts":[...]},"meta":{"correlation_id":"<uuid>"}}
    And the response carries RateLimit-Limit, RateLimit-Remaining and RateLimit-Reset headers
    And an append-only audit record with operation "recall" and outcome "success" is written before the response

  Scenario: Happy path — remember enqueues an ExtractFact job and acks 202
    Given a valid OIDC bearer token granting the write scope
    And an Idempotency-Key header "k-001"
    When the broker sends POST /v1/memories with body {"content":{"text":"Team Alpha owns orders"}}
    Then an ExtractFact WorkJob carrying idempotency_key "k-001" and the caller's scope is enqueued on the queue
    And the response status is 202
    And the body is {"data":{"job_id":"<id>","status":"accepted"},"meta":{"correlation_id":"<uuid>"}}

  Scenario: Edge case — idempotent replay returns the original ack without a new side effect
    Given a write with Idempotency-Key "k-001" was accepted 1 minute ago within the 24h window
    When the broker re-sends POST /v1/memories with the same Idempotency-Key "k-001"
    Then no new WorkJob is enqueued
    And the response status is 202
    And the body data.status is "already-accepted"

  Scenario: Edge case — recall abstains when no candidate clears the gate
    Given a valid OIDC bearer token granting the read scope
    And the Retrieval Engine returns zero facts with abstained=true
    When the broker sends POST /v1/recall with a novel query
    Then the response status is 200
    And the body is {"data":{"facts":[]},"meta":{"abstained":true,"correlation_id":"<uuid>"}}

  Scenario: Edge case — conditional GET returns 304 when the fact is unchanged
    Given a fact "fact:018f...-77" exists within the caller's scope with ingested_at "2026-01-04T09:00:01.123Z"
    And the caller sends If-None-Match with the matching ETag
    When the broker sends GET /v1/memories/fact:018f...-77
    Then the response status is 304
    And the response body is empty

  Scenario: Edge case — DELETE removes the fact and returns the deletion proof
    Given a valid OIDC bearer token granting the forget scope
    And an Idempotency-Key header "d-009"
    When the broker sends DELETE /v1/memories/fact:018f...-77
    Then the edge calls C1 hard_delete directly (no WorkJob is enqueued) and obtains a deletion proof
    And the response status is 200
    And the body data contains record_id, embeddings_removed and a sha256 digest

  Scenario: Error path — write without Idempotency-Key is rejected
    Given a valid OIDC bearer token granting the write scope and no Idempotency-Key header
    When the broker sends POST /v1/memories with a valid body
    Then it returns 400 with code VAL_MISSING_IDEMPOTENCY_KEY

  Scenario: Error path — missing bearer token on a /v1 route
    Given no Authorization header
    When the broker sends POST /v1/recall
    Then it returns 401 with code AUTH_MISSING_TOKEN
    And no audit record is written

  Scenario: Error path — rate limit exhausted
    Given the read token bucket for the caller is empty
    When the broker sends POST /v1/recall with a valid token and body
    Then it returns 429 with code RATE_LIMITED
    And the response carries Retry-After and RateLimit-Reset headers

  Scenario: Error path — body exceeds the size limit
    Given RECALL_MAX_BODY_BYTES is 1048576
    When the broker sends POST /v1/memories with a 2 MiB body
    Then it returns 413 with code VAL_BODY_TOO_LARGE

  Scenario: Error path — readiness fails when the embedding dimension mismatches the index
    Given RECALL_EMBED_DIM is 1024 but the vector index dimension is 768
    When a probe sends GET /readyz
    Then the response status is 503
    And the body checks.embed_dim is false
```

#### Performance, Security, Observability

- **Performance targets:** in-process edge overhead (correlation id, body-size check, rate-limit decrement, error mapping, audit write excluding the downstream call) **≤ 5 ms p95** (SA-LAT-01 in-process-overhead sub-budget). The recall route's end-to-end **≤ 200 ms p95** target (NFR-P2) is met by C6; the edge adds only its ≤ 5 ms overhead plus the synchronous audit write. The synchronous audit write is bounded by the store's write timeout and is included in the per-request overhead measurement. `DELETE` is the one route that performs a synchronous `hard_delete` (removing the fact, its derived summaries, and its embeddings) on the request path; its store-bound removal time is excluded from the ≤ 5 ms in-process overhead budget. Memory: bounded — token buckets and the JWKS cache (held by C3) are the only in-process state; the idempotency record lives in C1's store, not in process.
- **Security:** every `/v1` route requires a valid OIDC bearer token (layer 3); authorisation is per-operation-class (read/write/forget) checked against `ctx.allowed_ops`; scope ownership of a targeted fact is enforced by C1 using `ScopeContext` (never a body-supplied scope). Input is validated at the boundary (body size, JSON shape, `result_cap` range, `memory_class` allow-list, `Idempotency-Key` length). The three operational routes are unauthenticated and expose no fact data — `/healthz` returns a fixed `status: "live"` inside the success envelope, `/readyz` returns only boolean check results, `/metrics` returns counter/histogram names and label keys. Tokens, request bodies, and fact content are never logged or placed in the audit record. In `production` (`RECALL_ENV`) error messages carry no internal detail.
- **Observability:** metrics (OpenTelemetry, exported on `/metrics`) — `recall_http_requests_total{route,method,status,op_class}`, `recall_http_request_duration_seconds{route,op_class}` (histogram), `recall_http_edge_overhead_seconds{route}` (histogram, the ≤ 5 ms budget), `recall_rate_limited_total{op_class}`, `recall_idempotency_replays_total{route}`, `recall_audit_writes_total{outcome}`, `recall_readiness_check{check}` (gauge 0/1). Log fields (structured, never tokens/PII): `correlation_id`, `subject`, `tenant`, `jti`, `route`, `op_class`, `status`, `code`, `latency_ms`. Trace span names: `http.request` (root, attributes `http.method`, `http.route`, `http.status_code`, `recall.correlation_id`, `recall.op_class`), child spans `auth.validate`, `rate_limit.check`, `idempotency.lookup`, `handler.dispatch`, `audit.write`.

#### Gaps

None.
