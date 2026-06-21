# Phase 4 — Cross-Cutting Concern Specifications

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.4.1 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20

Each concern below is a first-class component spec applying the Phase 3 template. These specs sit at
**Phase 0** of the build order — foundational libraries/middleware consumed by the eight Phase 1–5
components — and introduce no dependency cycle (they depend only on Phase 2C shared types). The HLD
cross-cutting stances (HLD `07-cross-cutting.md`) are binding: every concern marked **Applies** there
is specified here; **CORS** is **Does not apply (default)** and is recorded as such with its config
hook.

The single most-referenced artefact here is the **Error Code Registry** (X1): every component error
table draws its `code` values from it.

## Table of contents

- [X1 — Error Handling & Envelopes (canonical registry)](#x1--error-handling--envelopes)
- [X2 — Authentication & Authorisation (policy)](#x2--authentication--authorisation-policy)
- [X3 — Logging](#x3--logging)
- [X4 — Metrics](#x4--metrics)
- [X5 — Tracing](#x5--tracing)
- [X6 — Configuration](#x6--configuration)
- [X7 — Database Migrations](#x7--database-migrations)
- [X8 — Health Checks](#x8--health-checks)
- [X9 — Rate Limiting](#x9--rate-limiting)
- [X10 — Pagination](#x10--pagination)
- [X11 — CORS](#x11--cors)
- [X12 — Input Validation](#x12--input-validation)
- [X13 — Graceful Shutdown](#x13--graceful-shutdown)

---

## X1 — Error Handling & Envelopes

**File:** `src/error.rs`, `src/api/envelope.rs` | **Package:** `recall::error` | **Phase:** 0 | **Dependencies:** §2C.1, §2C.7

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Defines the one success envelope, the one error envelope, the closed error-code registry, the
`AppError` → (HTTP status, code) mapping, and panic recovery. It is the single source every component
error table cites (SA-ENV-01, SA-ENV-02; HLD 07 "Error handling / envelope").

#### Approach
A single typed `AppError` enum (§2C.7) is produced everywhere; exactly one mapping function turns it
into `(StatusCode, ErrorBody)`. Codes are a closed `&'static str` registry, not free-form strings, so
agents branch on `code` without parsing prose. Rejected alternative: per-handler ad-hoc error shapes
(rejected — agents could not reliably branch; HLD mandates one envelope).

#### Shared Context
```rust
// §2C.1 — duplicated
pub struct Success<T: Serialize> { pub data: T, pub meta: Meta }
pub struct ErrorEnvelope { pub error: ErrorBody }
pub struct ErrorBody { pub code: String, pub message: String, pub correlation_id: String }
// §2C.7 — AppError variants: Validation, Unauthenticated, Forbidden, NotFound, RateLimited,
//          Store, Queue, Provider, Internal.
```
Env: `RECALL_ENV` (`production` hides internal detail in `message`; `development` may include it).

#### Public Interface — the Error Code Registry (canonical)
Every `code` is SCREAMING_SNAKE_CASE and appears here exactly once. No other value is valid.

| Code | HTTP | Meaning | Produced by |
|---|---|---|---|
| `VAL_INVALID_BODY` | 400 | JSON parse failure or schema/structural validation failure | C1, C4, C8 |
| `VAL_OUT_OF_RANGE` | 400 | A field outside its allowed numeric/length range | C1, C6, C8 |
| `VAL_UNSUPPORTED_CLASS` | 400 | `memory_class` = `procedural` (HLD non-goal, SA-CLASS-01) | C4, C8 |
| `VAL_MISSING_IDEMPOTENCY_KEY` | 400 | A write request without an `Idempotency-Key` header | C8 |
| `VAL_BODY_TOO_LARGE` | 413 | Request body exceeds `RECALL_MAX_BODY_BYTES` | C8 |
| `AUTH_MISSING_TOKEN` | 401 | No `Authorization: Bearer` header | C3, C8 |
| `AUTH_INVALID_TOKEN` | 401 | Signature/issuer/audience/expiry/nbf validation failed | C3, C8 |
| `AUTH_INSUFFICIENT_SCOPE` | 403 | Token lacks the scope for the operation class | C3, C8 |
| `SCOPE_FORBIDDEN` | 403 | Caller does not own / cannot see the target record | C1, C8 |
| `NOT_FOUND` | 404 | Target record absent or excluded by the read filter | C1, C8 |
| `RATE_LIMITED` | 429 | Token-bucket limit exceeded (carries `Retry-After`) | C8 |
| `STORE_UNAVAILABLE` | 503 | Memory store connection lost / unreachable | C1, C8 |
| `STORE_TIMEOUT` | 504 | Store op exceeded its per-statement budget | C1, C6 |
| `QUEUE_UNAVAILABLE` | 503 | Work-queue backend unreachable | C2, C8 |
| `PROVIDER_TIMEOUT` | 504 | Embedding/reranker/LLM/broker call timed out | C4, C6, C7 |
| `PROVIDER_ERROR` | 502 | Provider returned an error status / malformed response | C4, C6, C7 |
| `INTERNAL` | 500 | Unhandled internal invariant violation (incl. partial delete) | all |

```rust
pub fn map_error(e: &AppError, correlation_id: &str, env: Env) -> (StatusCode, ErrorEnvelope);
```

##### Example
`AppError::Forbidden("record outside scope")` →
`(403, {"error":{"code":"SCOPE_FORBIDDEN","message":"forbidden","correlation_id":"c-9f3..."}})`.

#### Internal Logic
1. Map the `AppError` variant to its `(StatusCode, code)` per the registry. `Validation` carries an
   inner discriminator (`out_of_range` | `unsupported_class` | `missing_idempotency_key` |
   `body_too_large` | else) selecting the precise `VAL_*` code; `Unauthenticated` carries
   `missing` | `invalid` selecting `AUTH_MISSING_TOKEN`/`AUTH_INVALID_TOKEN`; `Provider` carries
   `timeout` | `other` selecting `PROVIDER_TIMEOUT`/`PROVIDER_ERROR`; `Store` carries
   `unavailable` | `timeout` selecting `STORE_UNAVAILABLE`/`STORE_TIMEOUT`.
2. Build `message`: a fixed human string per code in `production`; the inner detail may be appended in
   `development`. The message never contains a token, PII, fact content, or a stack trace.
3. A panic in a handler is caught by a `catch_unwind`/tower layer, logged at `error` with the
   correlation id, and returned as `INTERNAL` (500). Library code never panics (C5 rule); only `main`
   may panic on unrecoverable bootstrap.

#### Data Model
N/A — stateless mapping.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Any `AppError::Validation(out_of_range)` | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"value out of range","correlation_id":"<uuid>"}}` |
| Handler panic caught by recovery layer | 500 | INTERNAL | `{"error":{"code":"INTERNAL","message":"internal error","correlation_id":"<uuid>"}}` |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Error handling & envelopes
  Scenario: Happy path — typed error maps to the registered code
    Given a handler returns AppError::Forbidden
    When the error is mapped
    Then the response is 403 with body code SCOPE_FORBIDDEN and a correlation_id

  Scenario: Edge case — production hides internal detail
    Given RECALL_ENV is "production"
    And a handler returns AppError::Internal with detail "nil deref at store.rs:42"
    When the error is mapped
    Then the response message is the fixed string "internal error" and contains no file path

  Scenario: Error path — panic becomes INTERNAL
    Given a handler panics
    When the recovery layer catches it
    Then the response is 500 with code INTERNAL
    And an error log line with the correlation_id is emitted
```

#### Performance, Security, Observability
- **Performance:** mapping is allocation-light, sub-microsecond; not on any latency budget.
- **Security:** error bodies never leak tokens, PII, fact content, or stack traces (HLD 07; C11).
- **Observability:** every mapped 5xx increments `http_errors_total{code}`; 4xx at `info`, 5xx at `error`.

#### Gaps
None.

---

## X2 — Authentication & Authorisation (policy)

**File:** `src/auth` (mechanism) + `src/api/middleware/auth.rs` | **Package:** `recall::auth` | **Phase:** 0 | **Dependencies:** C3, §2C.3

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
States the system-wide authn/authz policy that every authenticated endpoint enforces (ADR-001,
ADR-011; HLD 07). The token-validation mechanism and `ScopeContext` construction are owned by **C3
(Auth & Scope)**; this concern specifies the *policy* applied uniformly across all handlers.

#### Approach
Authentication is OIDC bearer-JWT, validated by C3; authorisation is per-operation-class
(`read`/`write`/`forget`) from token scopes plus the hierarchical data-isolation read-filter (§2C.3).
The policy is applied as one middleware layer ahead of every `/v1` handler, so no handler is reachable
without a `ScopeContext`. Rejected alternative: per-handler auth checks (rejected — easy to forget one;
a single layer is fail-closed).

#### Shared Context
`ScopeContext`, `OpSet`, the §2C.3 read-filter rule (duplicated in C1/C3). Env: `RECALL_OIDC_ISSUER`,
`RECALL_OIDC_AUDIENCE`, `RECALL_OIDC_SUBJECT_CLAIM`, `RECALL_OIDC_TEAMS_CLAIM`,
`RECALL_OIDC_TENANT_CLAIM`, `RECALL_JWKS_REFRESH_SECS`.

#### Public Interface
Policy (not a new API): operational endpoints `/healthz`, `/readyz`, `/metrics`, `GET /openapi.json`
are unauthenticated and expose no fact data; every other `/v1` route requires a valid token and the
matching operation scope (recall/GET → `read`; remember → `write`; retire/delete → `forget`).

#### Internal Logic
1. Middleware extracts the bearer token; absent → `AUTH_MISSING_TOKEN` (401).
2. C3 validates the token and builds `ScopeContext`; failure → `AUTH_INVALID_TOKEN` (401).
3. The handler's required op class is checked against `ctx.allowed_ops`; absent → `AUTH_INSUFFICIENT_SCOPE` (403).
4. All downstream store access is scoped by `ScopeContext`; a record outside scope → `SCOPE_FORBIDDEN` (403) or `NOT_FOUND` (404) on the read side (a caller learns nothing about records it cannot read).

#### Data Model
N/A — identity is stateless; the only state is C3's in-memory JWKS cache.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| No bearer token | 401 | AUTH_MISSING_TOKEN | `{"error":{"code":"AUTH_MISSING_TOKEN","message":"authentication required","correlation_id":"<uuid>"}}` |
| Token valid but lacks the op scope | 403 | AUTH_INSUFFICIENT_SCOPE | `{"error":{"code":"AUTH_INSUFFICIENT_SCOPE","message":"insufficient scope","correlation_id":"<uuid>"}}` |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Authentication & authorisation policy
  Scenario: Happy path — valid read token recalls
    Given a valid token whose scopes include read
    When POST /v1/recall is called
    Then the request is authorised and reaches the Retrieval Engine

  Scenario: Edge case — operational endpoint needs no token
    Given no Authorization header
    When GET /healthz is called
    Then the response is 200 and contains no fact data

  Scenario: Error path — write token cannot forget
    Given a token whose scopes include write but not forget
    When DELETE /v1/memories/{id} is called
    Then the response is 403 with code AUTH_INSUFFICIENT_SCOPE
```

#### Performance, Security, Observability
- **Performance:** token validation p95 ≤ 5 ms with a warm JWKS cache (C3); no network on the hot path.
- **Security:** the system's primary boundary — alg-allowlist (reject `alg=none`), iss/aud/exp/nbf checked, scope never trusted from the body, cross-tenant access structurally impossible (ADR-011).
- **Observability:** `auth_decisions_total{outcome}`; never log the token (only `jti` for audit, SA-AUDIT-01).

#### Gaps
None.

---

## X3 — Logging

**File:** `src/obs/log.rs` | **Package:** `recall::obs` | **Phase:** 0 | **Dependencies:** §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Structured operational logging with correlation-id propagation and strict redaction — for debugging
and operations, **not** the compliance record (the audit trail X-ref SA-AUDIT-01 / C1 is the
authority). HLD 07 "Logging".

#### Approach
`tracing` + `tracing-subscriber` emitting JSON to stdout; one span per request carrying
`correlation_id`. Redaction is enforced at the field layer, not left to call sites. Rejected
alternative: free-text logging (rejected — not machine-parseable, easy to leak secrets).

#### Shared Context
Env: `RECALL_LOG_LEVEL` (`error|warn|info|debug|trace`, default `info`), `RECALL_ENV`.

#### Public Interface
JSON line shape: `{ "ts": <rfc3339ms>, "level": <lvl>, "target": <module>, "msg": <str>,
"correlation_id": <uuid>, "span": <name>, ...fields }`. Required fields on a request-scoped line:
`ts`, `level`, `correlation_id`. **Never logged:** `Authorization` header, token, `jti` value beyond
the audit trail, raw PII, fact `content`, embedding vectors, store credentials, provider API keys.

#### Internal Logic
1. Initialise the subscriber from `RECALL_LOG_LEVEL` at startup; format JSON; in `development` a
   human-readable formatter is permitted.
2. A tower layer assigns/propagates `correlation_id` into the request span so every line inherits it.
3. A redaction filter drops or masks the never-log fields above before serialisation; tenant id is
   logged as a boolean `tenant_present` at `debug` and below (no identifying data in operational logs).

#### Data Model
N/A — logs are emitted to stdout, not stored by `recall`.

#### Error Table
N/A — logging is best-effort and never returns a client-facing error; a subscriber-init failure at
startup is a fatal bootstrap error (panics in `main`, never in library code).

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Structured logging
  Scenario: Happy path — request line carries correlation_id
    Given a request with correlation_id "c-1"
    When the handler logs an info line
    Then the JSON line includes "correlation_id":"c-1" and a level and ts

  Scenario: Edge case — tenant id is not in operational logs
    Given a scoped store operation for tenant "acme" at debug level
    When the store logs
    Then the line contains tenant_present:true and does not contain "acme"

  Scenario: Error path — a token is never logged
    Given a request whose Authorization header is "Bearer eyJ..."
    When any line is emitted for that request
    Then no line contains the token string
```

#### Performance, Security, Observability
- **Performance:** non-blocking writer; logging adds < 1 ms to a request at `info`.
- **Security:** redaction is enforced centrally; no secrets/PII/content ever serialised.
- **Observability:** self — log volume metric `log_lines_total{level}`.

#### Gaps
None.

---

## X4 — Metrics

**File:** `src/obs/metrics.rs` | **Package:** `recall::obs` | **Phase:** 0 | **Dependencies:** §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
The four quality layers from `good-mem.md` §13 (HLD 07 "Metrics"): task usage, memory quality,
efficiency, governance — exported for scrape at `GET /metrics`.

#### Approach
OpenTelemetry/Prometheus-style counters, gauges, histograms with bounded label cardinality. Labels
never include unbounded values (no tenant id, no fact id, no user id) to prevent cardinality blow-up.
Rejected alternative: per-tenant labels (rejected — cardinality explodes with tenant count).

#### Shared Context
Env: `RECALL_OTLP_ENDPOINT` (observability disabled if unset).

#### Public Interface — metric catalogue (names · type · labels)
- **Task usage:** `recall_requests_total{op,outcome}` (counter); `recall_abstain_total` (counter).
- **Memory quality:** `memory_contradictions_superseded_total` (counter);
  `memory_facts_stale_pending_refresh_total` (counter); `memory_facts_total` (gauge).
- **Efficiency:** `recall_latency_seconds{stage}` (histogram, buckets `5,10,25,50,100,200,500,1000` ms);
  `recall_response_tokens` (histogram); `store_size_bytes` (gauge); `store_ann_duration_seconds` (histogram).
- **Governance:** `writes_rejected_total{reason}` (counter); `writes_quarantined_total` (counter);
  `deletions_total{kind}` (counter); `auth_decisions_total{outcome}` (counter).

#### Internal Logic
1. Register the catalogue at startup; expose `/metrics` (X8/C8) with no fact content.
2. Components increment/observe via injected handles; latency histograms use the bucket set above so
   the NFR-P2 p95 ≤ 200 ms boundary is a bucket edge.

#### Data Model
N/A — metrics are in-memory and scraped; not persisted by `recall`.

#### Error Table
N/A — metrics emission never fails a request; a missing `RECALL_OTLP_ENDPOINT` disables export silently (logged once at startup).

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Metrics
  Scenario: Happy path — a recall increments the request counter
    When a recall returns ranked facts
    Then recall_requests_total{op="recall",outcome="ok"} increases by 1

  Scenario: Edge case — abstain is counted distinctly
    When a recall abstains
    Then recall_abstain_total increases by 1 and outcome label is "abstained"

  Scenario: Error path — labels carry no unbounded ids
    When metrics are scraped
    Then no metric label value equals a tenant id, user id, or fact id
```

#### Performance, Security, Observability
- **Performance:** counter/histogram ops are lock-free and sub-microsecond.
- **Security:** `/metrics` exposes no fact content, no identifiers.
- **Observability:** this concern *is* observability; self-metric `metrics_scrape_total`.

#### Gaps
None.

---

## X5 — Tracing

**File:** `src/obs/trace.rs` | **Package:** `recall::obs` | **Phase:** 0 | **Dependencies:** §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Distributed tracing across the API edge, the read path, and the async workers so a request can be
followed end-to-end (HLD 07 "Tracing").

#### Approach
OpenTelemetry spans exported via OTLP to `RECALL_OTLP_ENDPOINT`; context propagated through the
in-process call chain and onto enqueued jobs (the job carries the trace context so an async re-read or
consolidation links back to the request that triggered it). Rejected alternative: no async propagation
(rejected — breaks the link between a read-path freshness check and its async re-read).

#### Shared Context
Env: `RECALL_OTLP_ENDPOINT`, `RECALL_ENV`.

#### Public Interface
Span naming convention `<component>.<operation>` (e.g. `api.recall`, `store.recall.vector`,
`write.extract`, `maintenance.consolidate_tenant`). Sampling: parent-based, default ratio 0.1 in
`production`, 1.0 in `development`. The trace/correlation id is the same `correlation_id` surfaced in
responses and logs.

#### Internal Logic
1. Initialise the tracer at startup; install the propagator.
2. The API edge opens the root span and injects the trace context into every job payload it enqueues;
   workers extract it and continue the trace.
3. Spans carry `correlation_id`; they never carry fact content, tokens, or PII as attributes.

#### Data Model
N/A — spans are exported, not stored by `recall`.

#### Error Table
N/A — tracing never fails a request; an unreachable collector drops spans and is logged once.

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Tracing
  Scenario: Happy path — a recall produces a root span with child store spans
    When POST /v1/recall is handled
    Then a span "api.recall" exists with children "store.recall.vector" and "store.recall.keyword"

  Scenario: Edge case — async re-read links to the triggering request
    Given a recall enqueues a ReReadSource job
    When the Write/Maintenance worker processes it
    Then its span shares the trace id of the original "api.recall" span

  Scenario: Error path — span attributes carry no PII
    When any span is exported
    Then no attribute value contains fact content or a token
```

#### Performance, Security, Observability
- **Performance:** sampling keeps overhead bounded; sampled-out requests add negligible cost.
- **Security:** no content/tokens/PII in span attributes.
- **Observability:** `traces_exported_total`, `traces_dropped_total`.

#### Gaps
None.

---

## X6 — Configuration

**File:** `src/config.rs` | **Package:** `recall::config` | **Phase:** 0 | **Dependencies:** §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Load, validate, and freeze all configuration at startup with precedence **env var > config file >
built-in default**, failing fast on a missing required value or a failed validation (HLD 07
"Configuration"; SA-EMBED-01 startup check).

#### Approach
A single typed `Config` struct deserialised from the layered sources, validated once, then held
immutable behind an `Arc`. Secrets are read from env only and never echoed. Rejected alternative:
ambient `std::env::var` reads scattered across the code (rejected — untestable, no startup validation,
secrets risk leaking into logs).

#### Shared Context
The full §2D variable table is the authoritative list (40 keys). Required keys: `RECALL_OIDC_ISSUER`,
`RECALL_OIDC_AUDIENCE`, `RECALL_EMBED_URL`, `RECALL_EMBED_API_KEY`, `RECALL_RERANK_URL`,
`RECALL_RERANK_API_KEY`, `RECALL_LLM_URL`, `RECALL_LLM_API_KEY`, `RECALL_BROKER_URL`.

#### Public Interface
```rust
pub struct Config { /* typed fields, one per §2D key */ }
pub fn load() -> Result<Config, ConfigError>;   // called once in main()
```

#### Internal Logic
1. Read defaults; overlay an optional config file; overlay env vars (highest precedence).
2. Parse each value to its typed field; a parse failure is `ConfigError::Parse{key}`.
3. Validate: required keys present (`ConfigError::Missing{key}`); enum domains
   (`RECALL_STORE_BACKEND ∈ {surrealkv,rocksdb}`, `RECALL_QUEUE_BACKEND ∈ {store,nats}`); conditional
   rules (`RECALL_QUEUE_NATS_URL` required iff backend is `nats`; `RECALL_STORE_REMOTE_URL` XOR
   `RECALL_STORE_PATH` — remote wins with a warning); ranges (thresholds in `[0,1]`, `result_cap_max ≤ 50`).
4. On any error, log it (key name only, never the value for a secret) and exit non-zero. On success,
   freeze into `Arc<Config>`.

#### Data Model
N/A — configuration is in-memory.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Required key missing at startup | n/a (process exit ≠ 0) | — | startup log `config.missing key=<KEY>`; process does not serve traffic |
| Enum/range/conditional validation fails | n/a (process exit ≠ 0) | — | startup log `config.invalid key=<KEY>`; process exits |

*(Configuration errors are startup-only and never reach an HTTP client — the process refuses to start
rather than serve with bad config. The two rows document the two failure modes per the ≥2-row rule.)*

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Configuration
  Scenario: Happy path — env overrides default
    Given RECALL_RESULT_CAP_MAX is unset (default 50) and RECALL_STAGE1_K=80 in env
    When load() runs
    Then Config.stage1_k is 80 and result_cap_max is 50

  Scenario: Edge case — NATS backend requires its URL
    Given RECALL_QUEUE_BACKEND=nats and RECALL_QUEUE_NATS_URL unset
    When load() runs
    Then load() returns ConfigError::Missing for RECALL_QUEUE_NATS_URL and the process exits non-zero

  Scenario: Error path — a secret value is never logged
    Given RECALL_EMBED_API_KEY is missing
    When load() fails
    Then the log line names the key but contains no value
```

#### Performance, Security, Observability
- **Performance:** one-time startup cost; no runtime overhead (frozen `Arc`).
- **Security:** secrets read from env only, never logged, never in `/metrics` or error bodies.
- **Observability:** `config.loaded` log at startup with non-secret keys only.

#### Gaps
None.

---

## X7 — Database Migrations

**File:** `src/store/migrate.rs`, `migrations/*.surql` | **Package:** `recall::store` | **Phase:** 0 | **Dependencies:** C1, §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Evolve the per-tenant SurrealDB schema through numbered, ordered, reversible migrations; dry-run before
apply; applies to a shared store are an explicit user action (HLD 07 "Migrations"; SA-MIG-01;
sql-safety layer rule).

#### Approach
A `Migrator` applies numbered `NNNN_<slug>.up.surql`/`.down.surql` pairs idempotently per tenant
namespace, recording applied versions in a `schema_migrations` table. Tables start **schemaless** and
tighten to **schemafull** in later migrations as the model stabilises. Rejected alternative:
auto-migrate-on-write (rejected — uncontrolled, no dry-run, unsafe on a shared store).

#### Shared Context
Tables owned by C1 (`fact`, `entity`, `relationship`, `source`, `audit_log`) and C7
(`maintenance_state`), C8 (`idempotency_record`), C2 (`work_job`, `dead_letter`). Env: `RECALL_STORE_*`.

#### Public Interface
```rust
pub struct Migrator;
impl Migrator {
  pub async fn current_version(&self, tenant: &str) -> Result<u32, StoreError>;
  pub async fn migrate_up(&self, tenant: &str) -> Result<u32, StoreError>;   // applies pending, returns new version
  pub async fn dry_run(&self, tenant: &str) -> Result<Vec<String>, StoreError>; // statements that WOULD run
  pub async fn migrate_down(&self, tenant: &str, to: u32) -> Result<(), StoreError>; // explicit, user-invoked
}
```
Naming: `NNNN_<slug>.{up,down}.surql`, `NNNN` zero-padded, monotonic. CI runs `dry_run`; a deploy does
**not** auto-apply against a shared store.

#### Internal Logic
1. On `ensure_tenant_namespace`, read `schema_migrations`; apply each pending `up` in order inside a
   transaction; record the version. Idempotent (`IF NOT EXISTS`).
2. `dry_run` returns the statements without executing — the review artefact before a shared apply.
3. A destructive change (DROP, data-losing ALTER, NOT NULL backfill, type narrowing, index removal on a
   hot path, schemafull tightening on a populated table) must ship a tested `down` path and surface
   rollback considerations; `schemagen` stops on a destructive change without an approved waiver.

#### Data Model
```surql
DEFINE TABLE IF NOT EXISTS schema_migrations SCHEMAFULL;
DEFINE FIELD version    ON schema_migrations TYPE int;
DEFINE FIELD applied_at ON schema_migrations TYPE datetime;
DEFINE INDEX sm_version ON schema_migrations FIELDS version UNIQUE;
```
**Rollback:** `migrate_down` runs the `down` pair to a target version; destructive downs (e.g. the
`0001` table drop) only ever target an empty store; against a populated namespace they are refused
without an explicit user override (sql-safety).

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| A migration statement fails mid-apply | 503/exit | STORE_UNAVAILABLE | transaction rolled back; `migrate.failed version=<N>` logged; namespace left at prior version |
| `migrate_down` against a populated namespace without override | n/a (refused) | — | `migrate.down.refused` logged; no statement runs |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Database migrations
  Scenario: Happy path — pending migration applies once
    Given a fresh namespace at version 0
    When migrate_up runs with one pending migration 0001
    Then current_version becomes 1 and a second migrate_up is a no-op

  Scenario: Edge case — dry_run executes nothing
    Given a pending migration 0002
    When dry_run is called
    Then it returns the statements of 0002 and current_version is unchanged

  Scenario: Error path — failed statement rolls back
    Given migration 0003 contains an invalid statement
    When migrate_up runs
    Then the transaction rolls back and current_version stays at 2
```

#### Performance, Security, Observability
- **Performance:** migrations run at startup/maintenance windows, off the request path.
- **Security:** applies to shared stores are user actions; parameterised DDL; no secrets in migration files.
- **Observability:** `migrate.applied{version}`, `migrate.failed{version}` logs; `schema_version` gauge.

#### Gaps
None.

---

## X8 — Health Checks

**File:** `src/api/health.rs` | **Package:** `recall::api` | **Phase:** 0 | **Dependencies:** C1, C3, §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Liveness and readiness endpoints so an orchestrator can route traffic only to a healthy instance (HLD
07 "Health checks"; HLD 08 operational endpoints). Unauthenticated, no fact data.

#### Approach
`/healthz` is a cheap process-up check; `/readyz` verifies real dependencies (store reachable +
vector-index dimension correct, OIDC discovery reachable) with bounded timeouts. Rejected alternative:
a single combined endpoint (rejected — liveness must not fail on a transient dependency blip or the pod
is killed needlessly).

#### Shared Context
`MemoryStore::ready()` (C1); C3 OIDC-discovery reachability. Env: `RECALL_OIDC_ISSUER`, `RECALL_EMBED_DIM`.

#### Public Interface
- `GET /healthz` → 200 `{"data":{"status":"live"}}` whenever the process is running.
- `GET /readyz` → 200 `{"data":{"status":"ready"}}` when store + OIDC discovery are reachable and the
  vector-index dimension equals `RECALL_EMBED_DIM`; otherwise 503 `{"data":{"status":"not-ready","checks":{...}}}`.
- `GET /metrics` → 200 Prometheus exposition (X4).

#### Internal Logic
1. `/healthz` returns 200 immediately (no dependency calls).
2. `/readyz` calls `MemoryStore::ready()` (≤ 500 ms timeout) and checks OIDC discovery freshness; any
   failed check → 503 with a per-check breakdown (no fact data, no identifiers).

#### Data Model
N/A — stateless probes.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Store unreachable on readiness | 503 | — | `{"data":{"status":"not-ready","checks":{"store":"fail","oidc":"ok"}}}` |
| Vector-index dimension mismatch | 503 | — | `{"data":{"status":"not-ready","checks":{"store":"dim-mismatch"}}}` |

*(Health endpoints return a status document, not the X1 error envelope, so a load balancer can read the
check breakdown; the two rows document the two readiness-failure modes.)*

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Health checks
  Scenario: Happy path — ready when dependencies are up
    Given the store is reachable and OIDC discovery is fresh and the index dim matches RECALL_EMBED_DIM
    When GET /readyz is called
    Then the response is 200 with status "ready"

  Scenario: Edge case — liveness stays up during a store blip
    Given the store is briefly unreachable
    When GET /healthz is called
    Then the response is 200 with status "live"

  Scenario: Error path — not ready on dim mismatch
    Given the persisted vector index dimension is 768 and RECALL_EMBED_DIM is 1024
    When GET /readyz is called
    Then the response is 503 with status "not-ready" and checks.store "dim-mismatch"
```

#### Performance, Security, Observability
- **Performance:** `/healthz` < 1 ms; `/readyz` bounded ≤ 500 ms.
- **Security:** unauthenticated but exposes no fact data, no tenant/user identifiers.
- **Observability:** `readiness_checks_total{check,outcome}`.

#### Gaps
None.

---

## X9 — Rate Limiting

**File:** `src/api/middleware/ratelimit.rs` | **Package:** `recall::api` | **Phase:** 0 | **Dependencies:** C3, §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Agent-aware rate limiting with standard headers (HLD 07 "Rate limiting"; SA-RATE-01) — agents emit far
more calls than humans, so limits are per-(subject, operation-class).

#### Approach
A token-bucket limiter keyed on `(ctx.user, op_class)`: read class and write class have independent
buckets. Token bucket gives smooth burst tolerance. Rejected alternative: fixed-window counter
(rejected — allows 2× burst at window edges).

#### Shared Context
`ScopeContext` (for `ctx.user`). Env: `RECALL_RATE_READ_PER_MIN` (120), `RECALL_RATE_WRITE_PER_MIN` (30).

#### Public Interface
Applied after auth, before the handler. On every response sets `RateLimit-Limit`,
`RateLimit-Remaining`, `RateLimit-Reset`; on rejection sets `Retry-After` and returns `429
RATE_LIMITED`. Read class: recall + GET fact; write class: remember + retire + delete (burst read 40,
write 10).

#### Internal Logic
1. Resolve the bucket for `(ctx.user, op_class)`; refill at the configured per-minute rate.
2. If a token is available, consume one and proceed, decorating the response with `RateLimit-*`.
3. If none, return `429 RATE_LIMITED` with `Retry-After` = seconds to the next token.
4. Buckets are in-memory per instance; a per-tenant override map is config (not a contract change).

#### Data Model
N/A — buckets are in-memory (single-node first; a shared limiter store is a later scale-out concern).

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Read-class bucket empty | 429 | RATE_LIMITED | `{"error":{"code":"RATE_LIMITED","message":"rate limit exceeded","correlation_id":"<uuid>"}}` + `Retry-After` |
| Write-class bucket empty | 429 | RATE_LIMITED | `{"error":{"code":"RATE_LIMITED","message":"rate limit exceeded","correlation_id":"<uuid>"}}` + `Retry-After` |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Rate limiting
  Scenario: Happy path — within limit succeeds with headers
    Given a user under the read limit
    When POST /v1/recall is called
    Then the response is 200 and includes RateLimit-Remaining

  Scenario: Edge case — read and write buckets are independent
    Given the write bucket is exhausted
    When POST /v1/recall (read) is called
    Then it is not rate limited

  Scenario: Error path — over limit returns 429 with Retry-After
    Given the read bucket is exhausted
    When POST /v1/recall is called
    Then the response is 429 with code RATE_LIMITED and a Retry-After header
```

#### Performance, Security, Observability
- **Performance:** O(1) per request; in-memory, negligible latency.
- **Security:** keyed on the authenticated `ctx.user`, never a body value; cannot be bypassed by spoofing.
- **Observability:** `rate_limit_decisions_total{op_class,outcome}`.

#### Gaps
None.

---

## X10 — Pagination

**File:** `src/api/pagination.rs` | **Package:** `recall::api` | **Phase:** 0 | **Dependencies:** C6, §2C.1, §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Bounded, capped, token-efficient, stably-ordered result pages (HLD 07 "Pagination"; SA-PAGE-01,
SA-CAP-01).

#### Approach
Opaque cursor encoding `(final_score, record_id)` for a stable total order, returned as
`meta.next_cursor`. Cursors are stable under concurrent writes where offsets are not. Rejected
alternative: offset pagination (rejected — duplicates/skips under concurrent inserts).

#### Shared Context
`Meta.next_cursor` (§2C.1), `RecallRequest.cursor`/`result_cap` (§2C.4). Env: `RECALL_RESULT_CAP_MAX` (50).

#### Public Interface
Request: `result_cap ∈ [1,50]` (default 10), optional opaque `cursor`. Response: `Success<...>` with
`meta.next_cursor` = base64url of `(final_score, record_id)`, or null when the page is the last. The
cursor is opaque to callers; its internal shape may change without a contract break.

#### Internal Logic
1. The Retrieval Engine orders by `(final_score DESC, record_id ASC)` for a stable total order.
2. After truncation to `result_cap`, if more candidates remain, encode the last item's
   `(final_score, record_id)` as `next_cursor`; else null.
3. A request with a `cursor` resumes strictly after the encoded position; an invalid/garbage cursor →
   `VAL_INVALID_BODY` (400).

#### Data Model
N/A — cursors are stateless (encoded position), held by the caller.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| `result_cap` outside `[1,50]` | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"result_cap out of range","correlation_id":"<uuid>"}}` |
| Malformed `cursor` | 400 | VAL_INVALID_BODY | `{"error":{"code":"VAL_INVALID_BODY","message":"invalid cursor","correlation_id":"<uuid>"}}` |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Pagination
  Scenario: Happy path — second page resumes after the first
    Given a recall returns 10 facts and a next_cursor
    When POST /v1/recall is called again with that cursor
    Then the returned facts strictly follow the first page in (score, id) order

  Scenario: Edge case — last page has null cursor
    Given a recall whose results fit in one page
    When the page is returned
    Then meta.next_cursor is null

  Scenario: Error path — bad cursor is rejected
    Given a cursor value "not-base64!"
    When POST /v1/recall is called with it
    Then the response is 400 with code VAL_INVALID_BODY
```

#### Performance, Security, Observability
- **Performance:** cursor encode/decode is O(1); no extra store round-trip.
- **Security:** the cursor encodes only public ranking position, no scope data (scope comes from the token).
- **Observability:** `recall_page_size` histogram.

#### Gaps
None.

---

## X11 — CORS

**File:** `src/api/middleware/cors.rs` | **Package:** `recall::api` | **Phase:** 0 | **Dependencies:** §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
HLD 07 records CORS as **Does not apply (default)** — callers are server-side (the broker), not
browsers. This spec records that decision and the config hook that enables CORS if a first-party
browser client is ever added.

#### Approach
CORS middleware is **disabled by default**; no `Access-Control-*` headers are emitted, and preflight
`OPTIONS` to `/v1` routes returns 404 (no browser contract). A future config key would enable a strict
allowlist. Rejected alternative: permissive `Access-Control-Allow-Origin: *` (rejected — `recall`
serves authenticated fact data; a wildcard CORS policy would be a cross-origin data-exposure risk).

#### Shared Context
No CORS env keys are defined in §2D (the concern is off). Enabling it later adds
`RECALL_CORS_ALLOWED_ORIGINS` (explicit allowlist, never `*`) via the HLD-impact-pass.

#### Public Interface
Default: no CORS headers; cross-origin browser requests are not supported. (Recorded as a Phase 1A
assumption equivalent: CORS off by default, per HLD 07.)

#### Internal Logic
1. The CORS layer is absent from the default middleware stack.
2. If enabled by future config, it emits `Access-Control-Allow-Origin` only for an origin in the
   explicit allowlist, with `Allow-Methods` limited to the actual routes and `Allow-Credentials`
   handled per the allowlist — never a wildcard with credentials.

#### Data Model
N/A — no state.

#### Error Table
N/A — with CORS off, there is no CORS-specific error path; a browser preflight simply hits the normal
404 for an undefined `OPTIONS` route. (Documented exception to the ≥2-row rule: this concern is
out-of-scope per HLD 07; if enabled later it gains an error path via the HLD-impact-pass.)

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: CORS (disabled by default)
  Scenario: Happy path — server-side call needs no CORS
    Given the broker calls POST /v1/recall without an Origin header
    When the request is handled
    Then no Access-Control-* headers are present and the call succeeds

  Scenario: Edge case — preflight is not supported by default
    Given a browser sends OPTIONS /v1/recall with an Origin header
    When the request is handled
    Then no Access-Control-Allow-Origin header is returned

  Scenario: Error path — wildcard credentialed CORS is never emitted
    Given CORS is later enabled with an allowlist
    When a request from a non-allowlisted origin arrives
    Then no Access-Control-Allow-Origin header is returned for that origin
```

#### Performance, Security, Observability
- **Performance:** zero — layer absent by default.
- **Security:** off-by-default is the safe stance for an authenticated fact store; no wildcard CORS.
- **Observability:** N/A while disabled.

#### Gaps
None.

---

## X12 — Input Validation

**File:** `src/api/validate.rs` | **Package:** `recall::api` | **Phase:** 0 | **Dependencies:** §2C, §2D

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
Validate at every API boundary, bound request size, and reject malformed input with the standard error
shape before it reaches business logic (HLD 07 "Input validation"; C10).

#### Approach
Typed deserialisation (`serde`) plus explicit per-field validators, applied in the handler before any
store/queue call; a body-size limit layer runs first. Parameterised store queries only (no
interpolation) — enforced in C1. Rejected alternative: validate deep in business logic (rejected —
lets malformed input consume work before rejection).

#### Shared Context
`RecallRequest`, `RememberRequest`, `SourceInput` (§2C.4); domain validation rules (§2C.2). Env:
`RECALL_MAX_BODY_BYTES` (1 MiB).

#### Public Interface
Validated bounds: `query` 1..=4096 chars non-empty; `result_cap ∈ [1,50]`; `content` MUST be a JSON
object; `memory_class` ∈ {episodic,semantic,consolidated} (`procedural` → `VAL_UNSUPPORTED_CLASS`);
`canonical_name` 1..=512; `kind` 1..=128; scores in `[0,1]`; body ≤ `RECALL_MAX_BODY_BYTES`.

#### Internal Logic
1. The body-size layer rejects `> RECALL_MAX_BODY_BYTES` with `VAL_BODY_TOO_LARGE` (413) before reading.
2. Deserialise; a parse failure → `VAL_INVALID_BODY` (400).
3. Run field validators; a range/length failure → `VAL_OUT_OF_RANGE` (400); a `procedural` class →
   `VAL_UNSUPPORTED_CLASS` (400). All validation runs before any store/queue/provider call.

#### Data Model
N/A — validation is stateless.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Body exceeds `RECALL_MAX_BODY_BYTES` | 413 | VAL_BODY_TOO_LARGE | `{"error":{"code":"VAL_BODY_TOO_LARGE","message":"request body too large","correlation_id":"<uuid>"}}` |
| `content` is not a JSON object | 400 | VAL_INVALID_BODY | `{"error":{"code":"VAL_INVALID_BODY","message":"content must be an object","correlation_id":"<uuid>"}}` |
| `memory_class` = `procedural` | 400 | VAL_UNSUPPORTED_CLASS | `{"error":{"code":"VAL_UNSUPPORTED_CLASS","message":"procedural memory is not supported","correlation_id":"<uuid>"}}` |

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Input validation
  Scenario: Happy path — a valid recall passes validation
    Given a recall body with query "who owns orders" and result_cap 5
    When the request is validated
    Then validation passes and the handler runs

  Scenario: Edge case — empty query is rejected
    Given a recall body with query ""
    When the request is validated
    Then the response is 400 with code VAL_OUT_OF_RANGE

  Scenario: Error path — oversize body is rejected before parsing
    Given a request body larger than RECALL_MAX_BODY_BYTES
    When the request is received
    Then the response is 413 with code VAL_BODY_TOO_LARGE and the body is not parsed
```

#### Performance, Security, Observability
- **Performance:** validation is O(body size); the size cap bounds worst-case work.
- **Security:** rejects malformed/oversize input early; parameterised store queries downstream (C1).
- **Observability:** `validation_failures_total{code}`.

#### Gaps
None.

---

## X13 — Graceful Shutdown

**File:** `src/shutdown.rs` | **Package:** `recall` | **Phase:** 0 | **Dependencies:** C2, C4, C6, C7, C8

> **Mode:** greenfield · **derivedFromHld:** 0.4.1

#### Purpose
On SIGTERM/SIGINT, drain in-flight requests, finish or re-enqueue in-flight async jobs, and close the
store cleanly so no work is lost on restart (HLD 07 "Graceful shutdown"; NFR-AV3).

#### Approach
A broadcast `CancellationToken` signals all subsystems; the HTTP server stops accepting, drains
in-flight requests within a deadline, workers finish their current job or release its lease back to
Pending (so it is reclaimed), then the store is flushed/closed. Rejected alternative: hard exit on
signal (rejected — drops in-flight reads and risks half-done writes; violates NFR-AV3).

#### Shared Context
`WorkQueue` lease semantics (C2 — an unreleased lease auto-reverts via the reaper). Drain deadline
constant (default 25 s, below a typical 30 s orchestrator grace).

#### Public Interface
```rust
pub async fn run_until_shutdown(app: App, signals: Signals);  // wires the token to all subsystems
```
Order: (1) stop accepting new connections; (2) drain in-flight HTTP requests ≤ drain-deadline;
(3) signal workers to stop claiming and finish/leave their current job (lease reverts if unfinished);
(4) flush + close the store; (5) exit 0.

#### Internal Logic
1. Install signal handlers; on first signal, trigger the `CancellationToken`.
2. The HTTP server stops accepting and waits for active handlers up to the drain deadline; a handler
   still running at the deadline is cancelled and its response (if any) is best-effort.
3. Workers stop calling `claim`; the in-flight job either completes (→ `complete`) or, if interrupted,
   its lease is left to expire and the C2 reaper reverts it to Pending (idempotent re-processing).
4. `MemoryStore` is flushed and closed; the process exits 0. A second signal forces an immediate exit.

#### Data Model
N/A — shutdown owns no tables; it relies on C2 lease semantics for job safety.

#### Error Table
| Condition | Status | Code | Response Body |
|---|---|---|---|
| Drain deadline exceeded with requests still active | n/a (forced cancel) | — | `shutdown.drain_timeout active=<N>` logged; those requests are cancelled |
| Store flush fails on close | n/a (exit non-zero) | — | `shutdown.store_flush_failed` logged; exit ≠ 0 so the orchestrator notices |

*(Shutdown produces no client-facing error envelope — it is a process lifecycle event; the two rows
document its two failure modes per the ≥2-row rule.)*

#### Acceptance Criteria (Gherkin)
```gherkin
Feature: Graceful shutdown
  Scenario: Happy path — in-flight request completes on SIGTERM
    Given a recall is in flight
    When SIGTERM is received
    Then the server stops accepting new connections and the in-flight recall completes before exit

  Scenario: Edge case — interrupted job is reclaimed
    Given a worker is processing an ExtractFact job when shutdown fires
    And the job does not finish within the drain deadline
    When the process exits
    Then the job's lease expires and the C2 reaper reverts it to Pending for reprocessing

  Scenario: Error path — second signal forces exit
    Given shutdown is draining
    When a second SIGTERM is received
    Then the process exits immediately without waiting for the drain deadline
```

#### Performance, Security, Observability
- **Performance:** drain deadline default 25 s (under a 30 s orchestrator grace); fast path exits sooner.
- **Security:** no new surface; in-flight auth context is honoured to completion or cancelled.
- **Observability:** `shutdown.initiated`, `shutdown.drained{active}`, `shutdown.complete` logs.

#### Gaps
None.
