### SPEC: Service Layer
**File:** `src/service` | **Package:** `recall::service` | **Phase:** 5 | **Dependencies:** C1 (Memory Store), C2 (Durable Work Queue), C3 (Auth & Scope), C6 (Retrieval Engine)

> **Mode:** greenfield
> **derivedFromHld:** 0.7.0

#### Purpose

The Service Layer is the transport-agnostic core of `recall` (ADR-016). It owns every operation's per-call orchestration — authenticate the bearer to a `ScopeContext` (delegating to C3), authorise the operation class, apply per-(subject, operation-class) rate limiting, enforce write idempotency, invoke the owning component (C6 for recall, C2 for the async write, C1 for fact read / retire / hard delete), write the per-call append-only audit record, and classify any failure to a single typed `AppError`. It is keyed on a verified `ScopeContext` and the existing transport-neutral request/response types, and returns a typed result or a typed error — it knows nothing of HTTP, MCP, headers, JSON wire framing, or status codes. Both edges (C8 HTTP, C10 MCP) are thin adapters over it. It holds no domain logic of its own: that lives in C1/C2/C6. This component is the extraction of the orchestration that previously lived inside the C8 axum handlers (the `EdgePipeline`); after this change there is exactly one definition of auth/scope/rate/idempotency/audit, consumed by both edges (SA-SVC-01).

#### Approach

A plain `Service` struct holds the injected component dependencies (config, metrics, the concrete `Arc<Store>`, `Arc<StoreWorkQueue>`, `Arc<RetrievalEngine>`, `Arc<Authenticator>`, and the in-process `RateLimiter`) and exposes one async method per operation. Each method runs a fixed orchestration sequence implemented once in a private `run` helper: validate → authorise → rate-limit → (idempotency for writes) → component call → audit → classify. The chosen design is a **method-per-operation facade** rather than (a) a generic `dispatch(op_enum, payload)` — rejected because it erases the per-operation type safety the edges rely on and forces stringly-typed payloads — or (b) leaving orchestration in each edge — rejected because it duplicates the security-critical chain per transport (the exact hazard ADR-016 prevents). The Service owns no persistent state of its own; idempotency records and the audit trail live in C1's store, exactly as the former edge used them. Correlation-id minting stays at the edge (each transport assigns one and passes it in), because it is the unit that observes the wire request; everything downstream of it is transport-neutral.

#### Shared Context

Duplicated from Phase 2C/2D (implement from this section alone). All timestamps are UTC RFC 3339 millisecond precision. Field names are `snake_case`.

##### Scope & auth context (2C.3)

```rust
// src/types/scope.rs
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef { pub tenant: String, pub team: Option<String>, pub user: String }

#[derive(Clone)]
pub struct ScopeContext {
    pub tenant: String,
    pub teams: Vec<String>,
    pub user: String,
    pub token_jti: String,
    pub allowed_ops: OpSet,
    pub correlation_id: String,
}
#[derive(Clone, Copy)]
pub struct OpSet { pub read: bool, pub write: bool, pub forget: bool }

/// Operation kind (authorisation) and operation class (rate-limit bucket).
#[derive(Clone, Copy)] pub enum Op { Read, Write, Forget }
#[derive(Clone, Copy, PartialEq, Eq, Hash)] pub enum OpClass { Read, Write }
```

##### Typed application error (2C.7)

```rust
// src/error.rs — the single classification source; both edges render from the AppError's registry code.
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    Validation(ValidationKind, String),   // 400 VAL_*
    Unauthenticated(AuthKind, String),    // 401 AUTH_*
    Forbidden(String),                    // 403 SCOPE_FORBIDDEN
    InsufficientScope(String),            // 403 AUTH_INSUFFICIENT_SCOPE
    NotFound,                             // 404 NOT_FOUND
    RateLimited,                          // 429 RATE_LIMITED
    Store(StoreError), Queue(QueueError), Provider(ProviderError),
    Internal,                             // 500 INTERNAL
}
```

##### Request/response payloads (2C.4)

`RecallRequest`, `RecallResponse`, `RankedFact`, `RememberRequest`, `WriteAck`, `JobAckStatus`, `RetireAck`, `DeletionProof`, `Capabilities`, `Fact` are exactly as defined in the C8 spec §Shared Context (duplicated there in full). The Service Layer consumes and returns these unchanged; it adds no wire-format fields.

##### Ports consumed (2C.6)

C3 exposes `async fn validate(bearer: &str, correlation_id: &str) -> Result<ScopeContext, AuthError>` and `fn authorise(ctx: &ScopeContext, op: Op) -> Result<(), AuthError>`. C6 exposes `async fn recall(ctx, &RecallRequest) -> Result<RecallOutcome, AppError>` with `RecallOutcome { response: RecallResponse, next_cursor: Option<String>, abstained: bool }`. C1 exposes `get_fact`, `end_validity`, `hard_delete`, `append_audit`, and the inherent `idempotency_get`/`idempotency_put` (all exactly as in the C8 spec). C2 exposes `enqueue(WorkJob) -> Result<String, QueueError>`.

##### Configuration (2D)

| Variable | Type | Default | Owner | Description |
|---|---|---|---|---|
| `RECALL_IDEMPOTENCY_TTL_SECS` | u32 | `86400` | C9 | Idempotency-key retention window (SA-IDEM-01). |
| `RECALL_RATE_READ_PER_MIN` | u32 | `120` | C9 | Read-class refill (burst 40). |
| `RECALL_RATE_WRITE_PER_MIN` | u32 | `30` | C9 | Write-class refill (burst 10). |

`READ_BURST = 40` / `WRITE_BURST = 10` are fixed constants (SA-RATE-01).

#### Public Interface

```rust
pub struct Service { /* config, metrics, store, queue, engine, auth, rate — Arc-backed, Clone */ }

/// Per-call inputs every edge supplies. `idempotency_key` is required for writes, ignored for reads.
pub struct CallContext<'a> {
    pub bearer: &'a str,           // raw token value (after "Bearer "), "" if absent
    pub correlation_id: &'a str,   // minted by the edge
    pub idempotency_key: Option<&'a str>,
}

/// Transport-neutral result: the typed payload plus the cross-cutting signals each edge renders.
pub struct CallResult<T> {
    pub data: T,
    pub rate: RateSnapshot,        // { limit, remaining, reset_secs } — HTTP renders as RateLimit-*; MCP may surface as metadata
    pub replayed: bool,            // true on an idempotent replay (writes)
}

impl Service {
    pub async fn capabilities(&self, cx: CallContext<'_>) -> Result<CallResult<Capabilities>, AppError>;
    pub async fn recall(&self, cx: CallContext<'_>, req: RecallRequest) -> Result<CallResult<RecallOutcome>, AppError>;
    pub async fn remember(&self, cx: CallContext<'_>, req: RememberRequest) -> Result<CallResult<WriteAck>, AppError>;
    pub async fn get_fact(&self, cx: CallContext<'_>, id: &str) -> Result<CallResult<Fact>, AppError>;
    pub async fn retire(&self, cx: CallContext<'_>, id: &str) -> Result<CallResult<RetireAck>, AppError>;
    pub async fn delete(&self, cx: CallContext<'_>, id: &str) -> Result<CallResult<DeletionProof>, AppError>;
}
```

Operation classes: `recall`/`get_fact`/`capabilities` → `Op::Read`/`OpClass::Read`; `remember` → `Op::Write`/`OpClass::Write`; `retire`/`delete` → `Op::Forget`/`OpClass::Write`. Conditional-GET (ETag / `If-Modified-Since`) is **not** modelled here — it is an HTTP transport optimisation the C8 edge layers over `get_fact`; the Service returns the full `Fact`.

##### Example

`service.remember(CallContext{ bearer:"eyJ…", correlation_id:"9b1d…", idempotency_key:Some("k-001") }, RememberRequest{ content: {"subject":"Team Alpha","predicate":"owns","object":"orders table"}, source:None, memory_class:None })` → on first call `Ok(CallResult{ data: WriteAck{ job_id:"work_job:018f…", status: Accepted }, rate: {limit:30,remaining:29,reset_secs:2}, replayed:false })`; on a replay within 24 h `Ok(CallResult{ data: WriteAck{ job_id:"work_job:018f…", status: AlreadyAccepted }, replayed:true, .. })` with no new job enqueued.

#### Internal Logic

Every method runs this fixed sequence (the private `run` helper); steps are binding and ordered.

1. **Authenticate.** Call C3 `validate(cx.bearer, cx.correlation_id)`. A missing token → `AppError::Unauthenticated(Missing, …)`; an invalid token → `AppError::Unauthenticated(Invalid, …)`. On failure, **no audit record is written** (the call never reached an authenticated identity) and the error is returned. On success a `ScopeContext` is obtained. Logged: subject, tenant, jti, decision — never the token.
2. **Authorise.** Call C3 `authorise(&ctx, op)` for the method's `Op`. An insufficient scope → `AppError::InsufficientScope(op)`. No audit on this failure (unauthenticated-class outcome, matching the existing Gherkin). Logged: op, decision.
3. **Rate-limit.** Decrement the `(ctx.user, op_class)` token bucket. On exhaustion → `AppError::RateLimited`; the `RateSnapshot` (limit, remaining=0, reset_secs) is still produced so the edge can attach limit signals. Logged: subject, class, remaining.
4. **Idempotency (writes only: `remember`, `retire`, `delete`).** Require `cx.idempotency_key` (1–255 chars); a missing/empty/over-long key → `AppError::Validation(MissingIdempotencyKey, …)`. Call C1 `idempotency_get(ctx.tenant, ctx.user, route, key)`. On a non-expired hit, return the stored typed result with `replayed=true` and **no** new side effect (no job enqueued, no validity ended, no delete). On a miss, continue; after the component call (step 5) persist the outcome via `idempotency_put(..., RECALL_IDEMPOTENCY_TTL_SECS)`. A store failure here → `AppError::Store`.
5. **Invoke the owning component.**
   - `capabilities` — build `Capabilities` from compile-time constants + config.
   - `recall` — validate `result_cap ∈ [1,50]` (else `AppError::Validation(OutOfRange, …)`) and a non-`procedural` filter class (else `Validation(UnsupportedClass, …)`); call C6 `recall(&ctx, &req)`; return its `RecallOutcome` (the `include_provenance` flag rides in `req`).
   - `remember` — require `content` to be a JSON object (else `Validation(InvalidBody, …)`; no LLM extraction, ADR-015); construct `WorkJob{ kind: ExtractFact, payload: req-as-JSON, scope: ScopeRef::from(&ctx), idempotency_key: Some(key) }`; call C2 `enqueue`; produce `WriteAck{ job_id, status: Accepted }`. A queue failure → `AppError::Queue`.
   - `get_fact` — call C1 `get_fact(&ctx, id)`; `Ok(None)` → `AppError::NotFound`; `Ok(Some(fact))` → the `Fact`.
   - `retire` — call C1 `end_validity(&ctx, id, now)`; not-found/not-owned → `AppError::NotFound`; success → `RetireAck`.
   - `delete` — call C1 `hard_delete(&ctx, id)` directly (synchronous, proof-gated; SA-DELETE-01); `StoreError::NotFound` → `AppError::NotFound`; success → `DeletionProof`. The edge is not a `HardDelete` job producer (carried over from the C8 spec).
6. **Audit (authenticated calls, both success and error after step 2 passed).** Build an `AuditEntry` (id, tenant, subject, operation, scope=`ScopeRef::from(&ctx)`, outcome=`success` or the error `code`, token_jti, correlation_id, at) and write it via C1 `append_audit` **before** returning. On a **success** path an audit-write failure becomes `AppError::Store` (the call is not reported successful if it could not be audited). On an **error** path the audit write is best-effort and never masks the original error. The record never contains the token, request payload, or fact content (SA-AUDIT-01).
7. **Return.** `Ok(CallResult{ data, rate, replayed })` or `Err(AppError)`. The Service performs no status-code or wire mapping — the edge classifies the `AppError` to its transport representation via the one registry.

#### Data Model

N/A — the Service Layer owns no tables. It reuses C1's `idempotency_record` and `audit_log` tables through C1 ports (both defined in `migrations/0001_init.up.surql`, owned by C1). No DDL is introduced by this component.

#### Error Table

The Service Layer returns typed `AppError` values, not HTTP statuses; the **status/code** below is the canonical registry mapping each edge applies (HTTP directly; MCP renders the same `code` as a tool/protocol error — see C10).

| Condition | Status (HTTP canonical) | Code | Returned AppError |
|-----------|------|------|---------------|
| No / empty bearer | 401 | AUTH_MISSING_TOKEN | `Unauthenticated(Missing, …)` |
| Token fails signature/issuer/audience/expiry | 401 | AUTH_INVALID_TOKEN | `Unauthenticated(Invalid, …)` |
| Token lacks the operation-class scope | 403 | AUTH_INSUFFICIENT_SCOPE | `InsufficientScope(op)` |
| Caller's scope does not own the targeted fact | 403 | SCOPE_FORBIDDEN | `Forbidden(…)` |
| Fact id absent or out of scope | 404 | NOT_FOUND | `NotFound` |
| Write with missing/invalid `idempotency_key` | 400 | VAL_MISSING_IDEMPOTENCY_KEY | `Validation(MissingIdempotencyKey, …)` |
| `remember.content` not a JSON object / malformed request | 400 | VAL_INVALID_BODY | `Validation(InvalidBody, …)` |
| `result_cap` outside `[1,50]` | 400 | VAL_OUT_OF_RANGE | `Validation(OutOfRange, …)` |
| Recall filter `memory_class` = `procedural` | 400 | VAL_UNSUPPORTED_CLASS | `Validation(UnsupportedClass, …)` |
| `(subject, op-class)` bucket exhausted | 429 | RATE_LIMITED | `RateLimited` |
| Store / idempotency / audit write unreachable | 503 | STORE_UNAVAILABLE | `Store(Unavailable)` |
| Store exceeds timeout | 504 | STORE_TIMEOUT | `Store(Timeout)` |
| Queue enqueue fails (remember) | 503 | QUEUE_UNAVAILABLE | `Queue(Unavailable)` |
| Provider error / timeout during recall | 502 / 504 | PROVIDER_ERROR / PROVIDER_TIMEOUT | `Provider(...)` |
| Unmapped internal failure | 500 | INTERNAL | `Internal` |

Every code is from the central registry (SA-ENV-02); the Service invents none.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Service Layer

  Scenario: Happy path — recall runs auth→authorise→rate→component→audit with no transport types in scope
    Given a Service built over the test component stack and a valid read-scoped bearer
    When recall(CallContext{bearer, correlation_id, idempotency_key:None}, RecallRequest{query:"who owns orders", result_cap:5}) is called
    Then it returns Ok(CallResult) whose data is the C6 RecallOutcome
    And an audit record with operation "recall" and outcome "success" was written before it returned
    And no axum or MCP type appeared in the call

  Scenario: Edge case — idempotent replay returns the original ack with no new side effect
    Given remember was accepted with idempotency_key "k-001" one minute ago within the 24h window
    When remember is called again with the same key
    Then no new WorkJob is enqueued
    And the result data.status is AlreadyAccepted and replayed is true

  Scenario: Edge case — rate-limit exhaustion returns RateLimited with a rate snapshot
    Given the caller's read bucket is empty
    When recall is called with a valid bearer
    Then it returns Err(AppError::RateLimited)
    And the produced RateSnapshot has remaining 0 and a non-zero reset_secs

  Scenario: Error path — missing bearer is unauthenticated and writes no audit
    Given an empty bearer
    When recall is called
    Then it returns Err(AppError::Unauthenticated) classifying to AUTH_MISSING_TOKEN
    And no audit record is written
```

#### Performance, Security, Observability

- **Performance targets:** in-process orchestration overhead (validate-cache-hit + authorise + rate decrement + idempotency lookup + audit write, excluding the downstream component call) **≤ 5 ms p95** (SA-LAT-01 in-process-overhead sub-budget) — the same budget the former edge carried, now measured at the Service boundary so both edges inherit it. The synchronous audit write is bounded by the store write timeout and included in the measurement.
- **Security:** identity is **only** ever a `ScopeContext` derived by C3 from a verified token (never a request-supplied scope); authorisation is per-operation-class; scope ownership of a targeted fact is enforced by C1 via `ScopeContext`. The token, request payload, and fact content are never logged or audited. There is exactly one definition of each of auth/scope/rate/idempotency/audit, consumed by both edges (SA-SVC-01) — a second copy is a defect.
- **Observability:** metrics `recall_service_calls_total{operation,outcome}`, `recall_service_overhead_seconds{operation}` (histogram, the ≤ 5 ms budget), `recall_rate_limited_total{op_class}`, `recall_idempotency_replays_total{operation}`, `recall_audit_writes_total{outcome}`. Log fields: `correlation_id`, `subject`, `tenant`, `jti`, `operation`, `op_class`, `outcome`, `code`, `latency_ms`. Trace spans: `service.call` (root for an operation, attributes `recall.operation`, `recall.op_class`, `recall.correlation_id`), child spans `auth.validate`, `rate_limit.check`, `idempotency.lookup`, `component.invoke`, `audit.write`.

#### Gaps

None.
