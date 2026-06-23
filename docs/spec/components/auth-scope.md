### SPEC: Auth & Scope

**File:** `src/auth` | **Package:** `recall::auth` | **Phase:** 2 | **Dependencies:** none (shared types only — `ScopeRef`, `ScopeContext`, `OpSet`, `Visibility`, `AppError`)

> **Mode:** greenfield
> **derivedFromHld:** 0.6.0

#### Purpose

Auth & Scope is the single security boundary of `recall`. It validates the OIDC bearer JWT presented on every request, and from the validated token it derives an authenticated `ScopeContext` — the only trusted source of caller identity (tenant, user, team membership) and per-operation permission. Nothing downstream ever trusts a scope value taken from the request body: identity is read from the token's verified claims, the token's issuer is trusted, the body is not (ADR-001, ADR-011). The module owns OIDC discovery and JWKS caching, signature and claim validation, the mapping from claims to a `ScopeContext`, the per-operation authorisation check, and the read-filter predicate that every data component applies to enforce tenant/team/user visibility.

#### Approach

Token validation uses the `jsonwebtoken` crate (pinned at `jsonwebtoken = "10"` in `Cargo.toml`) for signature/claim checks; OIDC discovery and the JWKS fetch are performed with `reqwest` directly (GET `<issuer>/.well-known/openid-configuration` to resolve `jwks_uri`, then GET `jwks_uri`), parsing the JWK set through `jsonwebtoken::jwk` rather than the `openidconnect` crate (ADR-009, Phase 2 stack note). The chosen design keeps an in-memory JWKS cache populated at startup and refreshed by a background task, so the hot path performs zero network I/O — token validation is a CPU-bound signature check against cached keys (Performance target p95 ≤ 5 ms). An on-demand refresh, rate-limited to once per 60 s, covers key rotation without a fetch per request (SA-JWKS-01). Two alternatives were rejected: (1) fetching JWKS per request — rejected because it adds a network round-trip to every call and defeats the read-path latency budget; (2) validating against a static long-lived public key in config — rejected because it cannot follow IdP key rotation and is not IdP-agnostic (ADR-001). Authorisation is a pure function over the `OpSet` carried in the context; the read filter is a pure predicate so data components can apply it inside store queries without re-entering this module.

#### Shared Context

The following types and configuration are duplicated here verbatim from Phase 2 so this spec is implementable from this section alone.

**Scope & auth context (Phase 2C.3), used by this component to produce `ScopeContext`:**

```rust
// src/types/scope.rs

/// The owning scope stored on every record.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef {
    pub tenant: String,            // tenant id -> SurrealDB namespace (ADR-011)
    pub team: Option<String>,      // team id, null for user-only facts
    pub user: String,              // user id, bound to the OIDC subject claim
}

/// The authenticated request context derived by C3 from the validated token.
/// Never constructed from request-body input.
#[derive(Clone)]
pub struct ScopeContext {
    pub tenant: String,
    pub teams: Vec<String>,        // membership claim — teams the user belongs to
    pub user: String,              // = token subject claim
    pub token_jti: String,         // for the audit trail (never the token itself)
    pub allowed_ops: OpSet,        // read / write / forget, from token scopes
    pub correlation_id: String,
}

#[derive(Clone, Copy)]
pub struct OpSet { pub read: bool, pub write: bool, pub forget: bool }
```

**Read filter rule (Phase 2C.3 — binding for every store query, copied verbatim):** a caller may read a Fact/Entity/Relationship iff `record.owner.tenant == ctx.tenant` **and** ( `record.owner.user == ctx.user` **or** (`record.visibility == TeamShared` and `record.owner.team ∈ ctx.teams`) **or** `record.visibility == TenantShared` ). Cross-tenant access is structurally impossible — a different tenant is a different namespace.

**Visibility enum (Phase 2C.2), referenced by the read filter:**

```rust
// src/types/domain.rs
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Visibility { UserPrivate, TeamShared, TenantShared } // (SA-VIS-01)
```

**Typed application error (Phase 2C.7), the variants this component produces:**

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
    // ...remaining variants (Store/Queue/Provider/Internal) defined in Phase 2C.7.
}
```

This component drives `AppError::Unauthenticated(AuthKind, String)` and `AppError::InsufficientScope(String)`. The exact `AppError` → (HTTP status, `code`) mapping is owned by the Phase 4 Error Handling spec; the rows this component drives are: `Unauthenticated(AuthKind::Missing, _)` → `401 AUTH_MISSING_TOKEN`; `Unauthenticated(AuthKind::Invalid, _)` → `401 AUTH_INVALID_TOKEN`; an `authorise` scope denial → `403 AUTH_INSUFFICIENT_SCOPE`. The `AuthKind` discriminator selects between the two 401 codes from a single variant. At its own boundary this component returns a typed `AuthError` (below — `MissingToken` / `InvalidToken(String)` / `InsufficientScope(Op)`); the caller (C8, HTTP API Edge) maps it onto `AppError` and the registry codes.

**Configuration & environment variables (Phase 2D) owned or read by this component:**

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_OIDC_ISSUER` | url | _(none)_ | **yes** | OIDC issuer; discovery at `<issuer>/.well-known/openid-configuration`. |
| `RECALL_OIDC_AUDIENCE` | string | _(none)_ | **yes** | Expected `aud` claim. |
| `RECALL_OIDC_SUBJECT_CLAIM` | string | `sub` | no | Claim mapped to the user id. |
| `RECALL_OIDC_TEAMS_CLAIM` | string | `groups` | no | Claim carrying team membership. |
| `RECALL_OIDC_TENANT_CLAIM` | string | `tenant` | no | Claim carrying the tenant id. |
| `RECALL_JWKS_REFRESH_SECS` | u32 | `3600` | no | JWKS background refresh interval (SA-JWKS-01). |

The on-demand-refresh rate limit (once per 60 s on unknown `kid`) and the discovery/JWKS HTTP request timeout (5 s) are component-local constants, not configuration (SA-JWKS-01). Secrets are not involved: `recall` holds no end-user credentials and stores no token (HLD 01 trust boundaries, SA-AUDIT-01).

**Deployment assumption — production IdP claim set (OQ-IDP):** This component requires the `subject`, `tenant`, and `jti` claims to be present and non-empty on every validated token, and reads team membership from the configured `teams` claim. A stock Dex password connector emits only `iss`, `sub`, `aud`, `exp`, `iat`, and `email` for static users — it does not emit a custom `tenant` claim, a `jti`, or `groups`. The production IdP must therefore be configured to issue, on each access token: the tenant id under the claim named by `RECALL_OIDC_TENANT_CLAIM` (default `tenant`), team membership under the claim named by `RECALL_OIDC_TEAMS_CLAIM` (default `groups`), and a `jti` (required for the audit trail, SA-AUDIT-01). A deployment whose IdP omits any of `subject`/`tenant`/`jti` will see every token rejected with `AuthError::InvalidToken` (`"missing subject claim"`, `"missing tenant claim"`, or `"missing jti"`); an omitted teams claim yields an empty `teams` vector (the user belongs to no team). The integration test coverage reflects this split honestly: a real Dex instance exercises crypto, OIDC discovery, JWKS, and the standard registered claims, while a local real-crypto issuer exercises the custom `tenant`/`groups`/`scope`/`jti` mapping and the negative paths that Dex's static-user tokens cannot produce.

#### Public Interface

The component exposes one constructor (`Authenticator::new`, which performs startup discovery and the first JWKS fetch and spawns the background refresh task), one validation entry point (token → `ScopeContext`), one authorisation function, and one read-filter helper. Two additive helpers accompany them as built: `AuthConfig::from_config(&Config)`, which derives an `AuthConfig` from the frozen application `Config` for the C8 wiring path, and `Authenticator::cached_key_count()`, an async observability/test helper that returns the current cached JWKS key count. Neither widens the security boundary.

```rust
// src/auth/mod.rs

use chrono::{DateTime, Utc};
use std::time::Duration;

/// Per-operation kinds an authenticated caller may be permitted to perform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op { Read, Write, Forget }

/// Errors at the auth boundary. C8 maps each onto AppError + a registry code.
#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    /// No bearer token, or the Authorization header is absent/malformed.
    /// -> AppError::Unauthenticated -> 401 AUTH_MISSING_TOKEN
    #[error("missing bearer token")]
    MissingToken,
    /// Token present but failed validation (signature, iss, aud, exp/nbf,
    /// unknown kid after refresh, missing/empty required claim, malformed JWT).
    /// The String is an operator-facing reason that never contains token bytes.
    /// -> AppError::Unauthenticated -> 401 AUTH_INVALID_TOKEN
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// Authenticated but lacks the required operation scope.
    /// -> AppError::Forbidden -> 403 AUTH_INSUFFICIENT_SCOPE
    #[error("insufficient scope: requires {0:?}")]
    InsufficientScope(Op),
}

/// Errors that can occur only at startup (before serving traffic).
#[derive(thiserror::Error, Debug)]
pub enum AuthStartupError {
    #[error("oidc discovery failed: {0}")]
    Discovery(String),
    #[error("jwks fetch failed: {0}")]
    JwksFetch(String),
    #[error("missing required config: {0}")]
    MissingConfig(String),
}

/// Construction-time configuration, populated from environment variables (Phase 2D).
#[derive(Clone)]
pub struct AuthConfig {
    pub issuer: String,            // RECALL_OIDC_ISSUER (required)
    pub audience: String,          // RECALL_OIDC_AUDIENCE (required)
    pub subject_claim: String,     // RECALL_OIDC_SUBJECT_CLAIM, default "sub"
    pub teams_claim: String,       // RECALL_OIDC_TEAMS_CLAIM, default "groups"
    pub tenant_claim: String,      // RECALL_OIDC_TENANT_CLAIM, default "tenant"
    pub jwks_refresh: Duration,    // RECALL_JWKS_REFRESH_SECS, default 3600 s
}

impl AuthConfig {
    /// Additive convenience constructor. Derives the auth configuration from the frozen
    /// application `crate::config::Config`, copying `oidc_issuer`, `oidc_audience`,
    /// `oidc_subject_claim`, `oidc_teams_claim`, `oidc_tenant_claim`, and converting
    /// `jwks_refresh_secs` (u32) to a `Duration`. This is the wiring path C8 uses; tests may
    /// still build an `AuthConfig` directly. No I/O.
    pub fn from_config(config: &crate::config::Config) -> Self;
}

/// Holds the OIDC config and the in-memory JWKS cache; thread-safe and cheap to clone (Arc inside).
#[derive(Clone)]
pub struct Authenticator { /* see Data Model */ }

impl Authenticator {
    /// Performs OIDC discovery against `<issuer>/.well-known/openid-configuration`,
    /// fetches the initial JWKS, and spawns the background refresh task
    /// (interval = config.jwks_refresh). Returns Err before any traffic is served
    /// if discovery or the first JWKS fetch fails, so readiness can fail fast.
    pub async fn new(config: AuthConfig) -> Result<Self, AuthStartupError>;

    /// Validates a raw bearer token string (the value after "Bearer ") and
    /// returns the derived ScopeContext. `correlation_id` is the per-request id
    /// minted by C8 and threaded into the context for response/audit correlation.
    /// Performs no network I/O on the warm-cache path; on an unknown `kid` it may
    /// trigger one rate-limited on-demand JWKS refresh before failing.
    pub async fn validate(
        &self,
        bearer_token: &str,
        correlation_id: &str,
    ) -> Result<ScopeContext, AuthError>;

    /// Pure authorisation check. Ok(()) if the context permits `op`, else
    /// Err(AuthError::InsufficientScope(op)). No I/O, no logging side effects.
    pub fn authorise(ctx: &ScopeContext, op: Op) -> Result<(), AuthError>;

    /// Additive observability/test helper. Returns the number of keys currently held in the
    /// in-memory JWKS cache, backing the `auth_jwks_keys` gauge and letting tests assert a warm
    /// cache. Takes a read lock for one count; no network I/O.
    pub async fn cached_key_count(&self) -> usize;
}

/// Reusable read-filter predicate (Phase 2C.3, copied verbatim into the body).
/// Data components (C1/C6/C8) call this to decide whether `ctx` may read a record
/// with the given owning scope and visibility. Pure, no I/O.
///
/// True iff record.owner.tenant == ctx.tenant AND
///   ( record.owner.user == ctx.user
///     OR (visibility == TeamShared AND record.owner.team ∈ ctx.teams)
///     OR visibility == TenantShared ).
pub fn can_read(ctx: &ScopeContext, owner: &ScopeRef, visibility: Visibility) -> bool;
```

##### Example

Request header (input to C8, forwarded to `validate`):

```
Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIn0.<payload>.<sig>
```

Decoded payload claims (after the broker obtained the token from the IdP):

```json
{
  "iss": "https://idp.example.com",
  "aud": "recall-api",
  "sub": "user-42",
  "tenant": "acme",
  "groups": ["platform", "sre"],
  "scope": "memory.read memory.write",
  "jti": "f1b9c2e0-1a2b-4c3d-9e8f-0123456789ab",
  "exp": 1782000000,
  "nbf": 1781990000
}
```

Result of `validate(token, "c0rr-id-001")` (`AuthConfig` using defaults `sub`/`groups`/`tenant`):

```rust
ScopeContext {
    tenant: "acme".to_string(),
    teams: vec!["platform".to_string(), "sre".to_string()],
    user: "user-42".to_string(),
    token_jti: "f1b9c2e0-1a2b-4c3d-9e8f-0123456789ab".to_string(),
    allowed_ops: OpSet { read: true, write: true, forget: false },
    correlation_id: "c0rr-id-001".to_string(),
}
```

`Authenticator::authorise(&ctx, Op::Forget)` then returns `Err(AuthError::InsufficientScope(Op::Forget))` because the `scope` claim granted only `memory.read` and `memory.write`.

`can_read(&ctx, &ScopeRef { tenant: "acme", team: Some("platform"), user: "user-99" }, Visibility::TeamShared)` returns `true` (same tenant, team-shared, and `"platform"` is in `ctx.teams`). The same record with `Visibility::UserPrivate` returns `false` (different user, not shared).

#### Internal Logic

**Startup — `Authenticator::new` (runs once, before serving traffic):**

1. Read and validate `AuthConfig` from the environment. If `issuer` or `audience` is empty, return `AuthStartupError::MissingConfig` naming the variable. Apply defaults `sub`/`groups`/`tenant`/`3600 s` for the optional fields.
2. Perform OIDC discovery: HTTPS GET `<issuer>/.well-known/openid-configuration` with a 5 s timeout. On a non-2xx response, a timeout, or a body missing the `jwks_uri` field, return `AuthStartupError::Discovery` with a reason that names the failure kind, never the response body bytes. Log at `error` with fields `event="oidc_discovery_failed"`, `issuer`, `correlation_id="startup"`.
3. Fetch the JWKS: HTTPS GET the discovered `jwks_uri` with a 5 s timeout. Parse the JWK set; index keys by `kid`. On a non-2xx response, a timeout, or an empty/unparseable key set, return `AuthStartupError::JwksFetch`. Store the indexed keys and `fetched_at = now()` in the in-memory cache.
4. Spawn the background refresh task on the `tokio` runtime: every `config.jwks_refresh`, re-fetch the JWKS (step 3 logic); on success, atomically replace the cached key set and update `fetched_at`; on failure, keep the previous key set, log at `warn` with `event="jwks_refresh_failed"`, and retry on the next tick. Log each successful refresh at `info` with `event="jwks_refreshed"` and key count.

**Token validation — `Authenticator::validate` (hot path, no network on the warm-cache branch):**

1. Reject an empty `bearer_token` with `AuthError::MissingToken`. (C8 has already stripped the `Bearer ` prefix; an absent or malformed `Authorization` header is reported by C8 as `MissingToken` before calling here.)
2. Decode the JWT header without verifying the signature, to read `alg` and `kid`. If the header is unparseable, return `AuthError::InvalidToken("malformed jwt header")`. Reject any `alg` outside the RS256/RS384/RS512/ES256/ES384 allowlist with `AuthError::InvalidToken("unsupported alg")` — this blocks the `alg=none` downgrade and HS/RS confusion attacks. If `kid` is absent, return `AuthError::InvalidToken("missing kid")`.
3. Look up `kid` in the cached JWKS. On a hit, go to step 5. On a miss, go to step 4.
4. Unknown `kid`: attempt one on-demand JWKS refresh, but only if at least 60 s have elapsed since the last on-demand refresh attempt (a monotonic per-`Authenticator` rate limiter, SA-JWKS-01). If the refresh runs and now contains `kid`, continue to step 5. If the rate limit blocks the refresh, or the refresh runs but still lacks `kid`, return `AuthError::InvalidToken("unknown signing key")`. Log at `warn` with `event="jwks_ondemand_refresh"`, `rate_limited=<bool>`, `correlation_id`.
5. Verify the JWT against the matched key with the `jsonwebtoken` validator configured for: signature over the decoded `alg`; `iss == config.issuer`; `aud == config.audience`; `exp` in the future and `nbf` not in the future, both with a fixed 60 s leeway for clock skew. Any verification failure returns `AuthError::InvalidToken(<reason>)` where the reason names the failed check (`"signature"`, `"issuer mismatch"`, `"audience mismatch"`, `"expired"`, `"not yet valid"`) and never contains claim values or token bytes.
6. Extract claims into the context. Read the user id from `claims[config.subject_claim]`; if absent or empty, return `AuthError::InvalidToken("missing subject claim")`. Read the tenant id from `claims[config.tenant_claim]`; if absent or empty, return `AuthError::InvalidToken("missing tenant claim")`. Read the teams from `claims[config.teams_claim]`; accept a JSON array of strings or a single string (coerced to a one-element vector); a missing claim yields an empty `Vec` (the user belongs to no team). Read `jti`; if absent or empty, return `AuthError::InvalidToken("missing jti")` (the audit trail requires it, SA-AUDIT-01).
7. Parse the OAuth `scope` claim (a single space-delimited string per RFC 6749) into an `OpSet`: `read = scope contains "memory.read"`, `write = scope contains "memory.write"`, `forget = scope contains "memory.forget"`. An absent `scope` claim yields `OpSet { read: false, write: false, forget: false }` (authenticated but unauthorised for every operation; the later `authorise` call rejects each). A scope token outside the three known values is ignored (forward compatibility), not an error.
8. Construct and return `ScopeContext { tenant, teams, user, token_jti: jti, allowed_ops, correlation_id: correlation_id.to_string() }`. Never store, cache, or log the raw token. Log success at `debug` with `event="token_validated"`, `tenant`, `user`, `jti`, `correlation_id` — never the token, never other claim values.

**Authorisation — `Authenticator::authorise` (pure):**

1. Map `op` to the corresponding `OpSet` flag: `Op::Read → ctx.allowed_ops.read`, `Op::Write → ctx.allowed_ops.write`, `Op::Forget → ctx.allowed_ops.forget`.
2. If the flag is `true`, return `Ok(())`. Otherwise return `Err(AuthError::InsufficientScope(op))`. No logging here; C8 logs the denial with the correlation id when it maps the error.

**Read filter — `can_read` (pure, copied verbatim from Phase 2C.3):**

1. If `owner.tenant != ctx.tenant`, return `false`. (Cross-tenant reads are also structurally blocked by the namespace boundary; this guard is defence in depth.)
2. If `owner.user == ctx.user`, return `true`.
3. If `visibility == TeamShared` and `owner.team` is `Some(t)` with `t ∈ ctx.teams`, return `true`.
4. If `visibility == TenantShared`, return `true`.
5. Otherwise return `false`.

#### Data Model

`N/A — stateless apart from the in-memory JWKS cache.` This component owns no SurrealDB table and emits no migration. The only state is the in-memory JWKS cache (local to the running OS process), held inside `Authenticator` and shared via `Arc`. It is never persisted; on restart it is repopulated by `Authenticator::new`.

```rust
// src/auth/cache.rs — in-memory only, no DDL.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// A single decoded verification key, indexed by its JWK `kid`.
struct CachedKey {
    /// Decoding key prepared for `jsonwebtoken` (RSA/EC public key material).
    decoding_key: jsonwebtoken::DecodingKey,
    /// The JWS algorithm this key signs with (from the JWK `alg`/`kty`).
    alg: jsonwebtoken::Algorithm,
}

struct JwksCache {
    /// kid -> key; replaced atomically on every refresh.
    keys: HashMap<String, CachedKey>,
    /// When the current key set was fetched (for observability).
    fetched_at: Instant,
}

/// Shared state behind the `Authenticator`.
struct AuthState {
    config: AuthConfig,
    jwks_uri: String,                          // resolved during discovery
    cache: RwLock<JwksCache>,                   // read-mostly; writes only on refresh
    last_ondemand_refresh: RwLock<Instant>,     // on-demand refresh rate limiter (60 s)
    http: reqwest::Client,                       // discovery/JWKS fetch client, 5 s timeout
}
// Authenticator wraps Arc<AuthState> so clones are cheap and share one cache.
```

No destructive change applies (no schema, no rollback). Cache invariants: `keys` is replaced wholesale under the write lock so readers never observe a partially-updated set; a failed refresh leaves the previous set intact.

#### Error Table

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| No bearer token / absent or malformed `Authorization` header | 401 | AUTH_MISSING_TOKEN | `{"error":{"code":"AUTH_MISSING_TOKEN","message":"authentication required","correlation_id":"<uuid>"}}` |
| Malformed JWT, unsupported `alg`, missing/unknown `kid` (after on-demand refresh) | 401 | AUTH_INVALID_TOKEN | `{"error":{"code":"AUTH_INVALID_TOKEN","message":"invalid token","correlation_id":"<uuid>"}}` |
| Signature invalid, `iss`/`aud` mismatch, expired (`exp`), not-yet-valid (`nbf`) | 401 | AUTH_INVALID_TOKEN | `{"error":{"code":"AUTH_INVALID_TOKEN","message":"invalid token","correlation_id":"<uuid>"}}` |
| Required claim missing/empty (subject, tenant, or `jti`) | 401 | AUTH_INVALID_TOKEN | `{"error":{"code":"AUTH_INVALID_TOKEN","message":"invalid token","correlation_id":"<uuid>"}}` |
| Authenticated but the `scope` claim does not grant the requested operation | 403 | AUTH_INSUFFICIENT_SCOPE | `{"error":{"code":"AUTH_INSUFFICIENT_SCOPE","message":"operation not permitted for this token","correlation_id":"<uuid>"}}` |

The `message` text is deliberately generic so it never reveals which validation check failed (no oracle for an attacker) and never contains token bytes or claim values; the specific reason is carried only in the typed `AuthError` and the operator log, not in the HTTP body. Startup errors (`AuthStartupError`) are not HTTP responses — they fail readiness before traffic is served (HLD 07 Health checks: readiness reflects IdP-discovery reachability).

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Auth & Scope

  Scenario: Happy path — valid token yields a scoped, authorised context
    Given the JWKS cache holds the signing key with kid "key-1"
    And a bearer token signed by "key-1" with iss matching RECALL_OIDC_ISSUER, aud matching RECALL_OIDC_AUDIENCE, a future exp, sub "user-42", tenant "acme", groups ["platform"], scope "memory.read memory.write", and a jti
    When validate is called with that token and correlation id "c1"
    Then it returns a ScopeContext with tenant "acme", user "user-42", teams ["platform"], allowed_ops { read: true, write: true, forget: false }, token_jti set, and correlation_id "c1"
    And authorise(ctx, Op::Read) returns Ok
    And authorise(ctx, Op::Forget) returns Err(InsufficientScope(Forget))

  Scenario: Edge case — unknown kid triggers one rate-limited on-demand refresh
    Given a bearer token signed by kid "key-2" which is not in the JWKS cache
    And no on-demand refresh has occurred in the last 60 seconds
    And the IdP JWKS endpoint now serves "key-2"
    When validate is called with that token
    Then exactly one on-demand JWKS refresh is performed
    And the refreshed cache contains "key-2"
    And validation succeeds against "key-2"

  Scenario: Edge case — read filter respects tenant, team, and visibility
    Given a ScopeContext for user "user-42" in tenant "acme" with teams ["platform"]
    When can_read is evaluated against a record owned by tenant "acme", team "platform", user "user-99" with visibility TeamShared
    Then can_read returns true
    And the same record with visibility UserPrivate yields false
    And a record owned by tenant "globex" with visibility TenantShared yields false

  Scenario: Error path — missing bearer token
    Given a request with no Authorization header
    When the request reaches the auth boundary
    Then it returns 401 with code AUTH_MISSING_TOKEN
    And the response body contains no token bytes

  Scenario: Error path — expired token
    Given a bearer token signed by a cached key but whose exp is in the past beyond the 60 second leeway
    When validate is called with that token
    Then it returns AuthError::InvalidToken("expired")
    And C8 maps it to 401 with code AUTH_INVALID_TOKEN
    And the response message does not reveal that expiry specifically failed

  Scenario: Error path — alg=none downgrade is rejected
    Given a bearer token whose header declares alg "none"
    When validate is called with that token
    Then it returns AuthError::InvalidToken("unsupported alg")
    And no signature verification is attempted
    And C8 maps it to 401 with code AUTH_INVALID_TOKEN

  Scenario: Error path — authenticated but lacking the required scope
    Given a valid token whose scope claim is "memory.read"
    When authorise(ctx, Op::Write) is called
    Then it returns Err(InsufficientScope(Write))
    And C8 maps it to 403 with code AUTH_INSUFFICIENT_SCOPE
```

#### Performance, Security, Observability

- **Performance targets:** Token validation p95 ≤ 5 ms with a warm JWKS cache and no network on the hot path (signature verification plus claim checks are CPU-bound against cached keys). The warm-cache branch performs zero network I/O; the only network calls are startup discovery, the timed background refresh (interval `RECALL_JWKS_REFRESH_SECS`, default 3600 s), and at most one on-demand refresh per 60 s on an unknown `kid`. Discovery and JWKS fetch each have a 5 s timeout. The JWKS cache holds a small set of public keys (single-digit to low-tens), so memory is negligible; the read lock is held only for the duration of one key lookup.
- **Security:** This module *is* the security boundary of `recall`. Every request is authenticated here before any data access. Concrete checks: (1) JWT signature verified against the IdP's JWKS public key matched by `kid`; (2) `alg` restricted to an RS256/RS384/RS512/ES256/ES384 allowlist, blocking the `alg=none` downgrade and HS/RS key-confusion attacks; (3) `iss == RECALL_OIDC_ISSUER`; (4) `aud == RECALL_OIDC_AUDIENCE`; (5) `exp` in the future and `nbf` not in the future, each with a 60 s clock-skew leeway; (6) subject, tenant, and `jti` claims present and non-empty. Identity (tenant, user, teams) is read only from verified claims — never from the request body (ADR-001, ADR-011), so a sandboxed agent cannot construct or widen its own scope. Cross-tenant access is structurally impossible (namespace-per-tenant) and additionally guarded by `can_read` step 1. The raw token is never stored, cached, or logged; only its `jti` is retained, for the audit trail (SA-AUDIT-01). Error messages returned to the caller are generic (no failed-check oracle) and contain no token bytes or claim values. No rate limiting is enforced here (that is C8, SA-RATE-01); the 60 s on-demand-refresh limiter protects the IdP from a refresh storm on forged unknown `kid` values.
- **Observability:** Structured log events (no token, no claim values beyond identifiers): `oidc_discovery_failed` (`error`; fields `issuer`), `jwks_fetch_failed` / `jwks_refresh_failed` (`error`/`warn`; field `attempt_reason`), `jwks_refreshed` (`info`; field `key_count`), `jwks_ondemand_refresh` (`warn`; fields `rate_limited`, `correlation_id`), `token_validated` (`debug`; fields `tenant`, `user`, `jti`, `correlation_id`). Metrics: `auth_validations_total{outcome}` where `outcome ∈ {ok, missing_token, invalid_token, insufficient_scope}`; `auth_jwks_keys` gauge (current cached key count); `auth_jwks_refresh_total{kind, result}` where `kind ∈ {background, ondemand}` and `result ∈ {ok, failed, rate_limited}`; `auth_validation_duration_seconds` histogram. Trace span `auth.validate` wraps `validate`, carrying the `correlation_id`; a child span `auth.jwks_ondemand_refresh` is recorded only when the on-demand refresh path runs.

#### Gaps

None.
