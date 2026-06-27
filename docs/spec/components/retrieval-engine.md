### SPEC: Retrieval Engine

**File:** `src/retrieval` | **Package:** `recall::retrieval` | **Phase:** 4 | **Dependencies:** C1 Memory Store

> **Mode:** greenfield
> **derivedFromHld:** 0.7.0

#### Purpose

The Retrieval Engine is the synchronous read path of `recall`. Given an authenticated, scoped `RecallRequest`, it returns a bounded, ranked set of facts with confidence, an opaque pagination cursor, and — when the request opts in via `include_provenance` — each sourced fact's `origin_ref` + `modification_marker` so the **agent** can verify source freshness itself (ADR-014); or an explicit abstention when no candidate is relevant enough. The path makes **no LLM call** (NFR-P1) and **no outbound source-change check** (ADR-014); it is **not model-free**: it performs exactly two read-path model inferences — query embedding and a cross-encoder rerank (ADR-012) — each with its own latency sub-budget inside NFR-P2 (whole-path p95 ≤ 200 ms, excluding caller network time). It owns query embedding, stage-1 multi-signal recall delegation, reranking, recency weighting, retrieval gating, result truncation plus cursor construction, and conditional provenance attachment.

#### Approach

The Internal Logic is a fixed pipeline (reformulate → embed → stage-1 recall → rerank → recency weight → gate → truncate + cursor → conditional provenance attach), each step carrying an explicit SA-LAT-01 sub-budget and an explicit degradation rule, so a sub-budget breach degrades the result rather than blocking the response. Two-stage retrieval (cheap multi-signal recall then expensive cross-encoder rerank over a bounded set) is chosen per ADR-005 over single-stage vector search, which misses exact terms and relationships and ranks weaker. The engine owns no persistence: all candidate retrieval, scope filtering, and bi-temporal `valid_at` filtering are delegated to the Memory Store (C1) via the `MemoryStore::recall` trait method, so this component is pure orchestration plus the ranking and gating arithmetic. Query reformulation is kept off by default behind an A/B flag rather than made a fixed stage, because `good-mem.md` §7.3 reports it can underperform plain dense retrieval.

#### Shared Context

Types, traits, config keys, and env vars this component references. Duplicated from Phase 2C; implementable from this section alone.

**Domain entity (read and ranked here) — `src/types/domain.rs`:**

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Fact {
    pub id: String,                       // "fact:<uuidv7>"
    pub content: serde_json::Value,       // structured assertion (object), not free text
    pub entities: Vec<String>,            // entity ids this fact connects (>=1)
    pub source_id: Option<String>,        // "source:<uuidv7>" provenance, null for agent-stated
    pub memory_class: MemoryClass,
    pub visibility: Visibility,
    pub owner: ScopeRef,                  // owning (tenant, team, user)
    pub valid_from: DateTime<Utc>,        // RFC3339 ms
    pub valid_to: Option<DateTime<Utc>>,  // null = currently true (open interval)
    pub ingested_at: DateTime<Utc>,       // server-set
    pub confidence: f64,                  // [0,1]
    pub salience: f64,                    // [0,1]
    pub stability: f64,                   // decay stability `s`, >=0.0
    pub supersedes: Option<String>,
    pub superseded_by: Option<String>,
    pub derived_from: Vec<String>,
    pub last_recalled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryClass { Episodic, Semantic, Consolidated }

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Visibility { UserPrivate, TeamShared, TenantShared }
```

**Scope context (input, never built from request body) — `src/types/scope.rs`:**

```rust
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef {
    pub tenant: String,
    pub team: Option<String>,
    pub user: String,
}

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
```

**Read-filter rule (the store enforces it; C6 passes `ScopeContext` so the store can):** a caller may read a Fact iff `record.owner.tenant == ctx.tenant` **and** ( `record.owner.user == ctx.user` **or** (`record.visibility == TeamShared` and `record.owner.team ∈ ctx.teams`) **or** `record.visibility == TenantShared` ). Cross-tenant access is structurally impossible — a different tenant is a different SurrealDB namespace. The Retrieval Engine does not re-apply this filter in process; it passes `ctx` to `MemoryStore::recall`, which applies it.

**API request/response payloads — `src/types/api.rs`:**

```rust
#[derive(Deserialize)]
pub struct RecallRequest {
    pub query: String,                       // 1..=4096 chars, non-empty
    #[serde(default)]
    pub filters: RecallFilters,
    #[serde(default = "default_result_cap")]
    pub result_cap: u8,                      // [1,50], default 10
    #[serde(default)]
    pub cursor: Option<String>,              // opaque, from a prior meta.next_cursor
    #[serde(default)]
    pub include_provenance: bool,            // opt-in: attach source origin_ref + marker (SA-PROV-01)
}

#[derive(Deserialize, Default)]
pub struct RecallFilters {
    pub memory_class: Option<MemoryClass>,
    pub visibility: Option<Visibility>,
    pub entity: Option<String>,              // restrict to facts touching this entity id
    pub valid_at: Option<DateTime<Utc>>,     // as-of query into the bi-temporal history
}

#[derive(Serialize)]
pub struct RecallResponse { pub facts: Vec<RankedFact> }   // wrapped in Success<RecallResponse>

#[derive(Serialize)]
pub struct RankedFact {
    pub fact: Fact,
    pub score: f64,                          // final ranking score [0,1]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceProvenance>,    // present only when include_provenance and fact has a source (SA-PROV-01)
}

/// Returned per sourced fact when the recall request opts in (SA-PROV-01, ADR-014). Lets the agent run
/// its own source-freshness check; `recall` performs no check and asserts no currency.
#[derive(Serialize)]
pub struct SourceProvenance {
    pub origin_ref: String,                  // document/system handle the agent resolves
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modification_marker: Option<String>, // ETag / Last-Modified token captured at write time
}

fn default_result_cap() -> u8 { 10 }
```

**Response envelope (C6 returns `RecallResponse` plus the `Meta` fields C8 places on the success envelope) — `src/types/envelope.rs`:**

```rust
#[derive(Serialize, Default)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,   // opaque pagination cursor
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abstained: Option<bool>,       // true on a recall that gated out all candidates
    pub correlation_id: String,        // uuid, echoes the per-request id
}
```

**Typed application error — `src/error.rs`:**

```rust
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
    #[error("internal")]             Internal,
}
```

C8 owns the `AppError` → (HTTP status, registry `code`) mapping. C6 produces the variant; C8 renders it. The error-code values C6's failures map to are taken from the Phase 4 registry and shown in this spec's Error Table.

**Provider and store traits C6 depends on — `src/types/ports.rs`:**

```rust
#[async_trait]
pub trait MemoryStore: Send + Sync {                 // impl: embedded SurrealDB (C1)
    async fn recall(&self, ctx: &ScopeContext, q: &StageOneQuery)
        -> Result<Vec<Candidate>, StoreError>;        // multi-signal stage-1, scope+metadata filtered
    async fn get_source(&self, ctx: &ScopeContext, id: &str)
        -> Result<Option<Source>, StoreError>;        // load a fact's cited Source for provenance attach (step 9)
    // ...other methods exist on the trait; C6 calls only `recall` and `get_source`.
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync {              // impl: HTTP adapter (read-path inference #1)
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, ProviderError>; // dim = RECALL_EMBED_DIM
}

#[async_trait]
pub trait RerankClient: Send + Sync {                 // impl: HTTP adapter (read-path inference #2)
    async fn rerank(&self, query: &str, docs: &[String]) -> Result<Vec<f64>, ProviderError>;
}
```

**Rerank wire contract (DEFINED by the C6 HTTP adapter `HttpRerankClient` in `src/providers/mod.rs`; the spec-less reranker provider has no spec of its own, so the contract is fixed here):** `POST {RECALL_RERANK_URL}/rerank` with `Content-Type: application/json` and, where `RECALL_RERANK_API_KEY` is configured, an `Authorization: Bearer <key>` header.

- Request body: `{ "query": "<effective query>", "documents": ["<fact.content as string>", ..] }`.
- Response body: `{ "scores": [f64, ..] }` — one relevance score per input document, positionally aligned with `documents`.
- A `reqwest` timeout maps to `ProviderError::Timeout` (C6 degrades to stage-1 order, step 5); a non-2xx status to `ProviderError::Status(code)`; a transport failure to `ProviderError::Transport`; a malformed body to `ProviderError::Malformed`.

```rust
// Source (owned by C1/§2C.2; duplicated — C6 loads it for the provenance attach in step 9)
pub struct Source {
    pub id: String,                          // "source:<uuidv7>"
    pub origin_ref: String,
    pub modification_marker: Option<String>, // ETag / Last-Modified token
    pub trust_signal: f64,
    pub owner: ScopeRef,
}
```

**Candidate and StageOneQuery (owned and defined in full by C1; their shapes are duplicated here because C6 constructs `StageOneQuery` and consumes `Candidate`):**

```rust
// A StageOneQuery is the multi-signal recall request C6 builds and passes to MemoryStore::recall:
// the query vector, the keyword terms, the metadata filters, the scope, and the stage-1 fan-out.
pub struct StageOneQuery {
    pub query_vector: Vec<f32>,        // from EmbeddingClient::embed, len == RECALL_EMBED_DIM
    pub keyword_terms: Vec<String>,    // terms from keyword_terms(effective_query) for BM25
    pub filters: RecallFilters,        // memory_class / visibility / entity / valid_at (as-of)
    pub scope: ScopeContext,           // C6 sets scope: ctx.clone(); store applies the read filter
    pub stage1_k: u16,                 // RECALL_STAGE1_K; max candidates the store returns
}

// A Candidate is a fact plus its per-signal stage-1 scores, returned by MemoryStore::recall.
pub struct Candidate {
    pub fact: Fact,
    pub semantic_score: f64,           // ANN similarity signal
    pub keyword_score: f64,            // BM25 signal
    pub graph_score: f64,              // graph-proximity signal
}
```

The exact field set of `StageOneQuery` and `Candidate` is owned by the C1 Memory Store spec; the fields above are the subset C6 reads and writes. See *Gaps*.

**Typed error from the store — `StoreError` (defined in full by C1; the two variants C6 distinguishes):**

```rust
pub enum StoreError {
    Unavailable(String),   // store down / connection refused  -> AppError::Store -> 503 STORE_UNAVAILABLE
    Timeout,               // store call exceeded its deadline -> AppError::Store -> 504 STORE_TIMEOUT
    // ...other variants owned by C1.
}
```

**Typed error from providers — `ProviderError` (the canonical §2C.6 type, owned by the provider adapters; the variants C6 distinguishes):**

```rust
pub enum ProviderError {
    Timeout,            // call exceeded its deadline    -> AppError::Provider -> 504 PROVIDER_TIMEOUT
    Status(u16),        // provider returned a non-2xx   -> AppError::Provider -> 502 PROVIDER_ERROR
    Transport(String),  // transport / connection failure -> AppError::Provider -> 502 PROVIDER_ERROR
    Malformed(String),  // unexpected / unparseable body  -> AppError::Provider -> 502 PROVIDER_ERROR
}
```

**Config keys and env vars (owner C6 unless noted):**

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_EMBED_URL` | url | _(none)_ | yes | Embedding provider endpoint (shared C4/C6). |
| `RECALL_EMBED_API_KEY` | secret | _(none)_ | yes | Embedding provider key, env only (shared C4/C6). |
| `RECALL_EMBED_DIM` | u32 | `1024` | no | Embedding dimension; query vector length must equal this and the store's vector-index dimension (shared C1/C4/C6). |
| `RECALL_RERANK_URL` | url | _(none)_ | yes | Cross-encoder reranker endpoint. |
| `RECALL_RERANK_API_KEY` | secret | _(none)_ | yes | Reranker key, env only. |
| `RECALL_STAGE1_K` | u16 | `50` | no | Stage-1 candidate count fed to rerank (SA-RERANK-01). |
| `RECALL_RESULT_CAP_MAX` | u8 | `50` | no | Hard upper bound on `result_cap` (SA-CAP-01). |
| `RECALL_ABSTAIN_THRESHOLD` | f64 | `0.2` | no | Top-final score below which recall abstains (SA-GATE-01). |
| `RECALL_RECENCY_WEIGHT` | f64 | `0.15` | no | Recency boost weight `w` (SA-RECENCY-01). |
| `RECALL_RECENCY_TAU_DAYS` | f64 | `30` | no | Recency decay constant `τ` in days. |

**Component-local config (not an env var in Phase 2D; introduced here):** `RECALL_REFORMULATION_ENABLED` (bool, default `false`) — the A/B flag that gates the optional query-reformulation step (step 1). See *Gaps*.

#### Public Interface

The single public entry point, called in-process by the HTTP API Edge (C8) after C8 has validated the token, mapped identity to scope, authorised the read operation, and deserialised the request body.

```rust
pub struct RetrievalEngine {
    store: Arc<dyn MemoryStore>,
    embedder: Arc<dyn EmbeddingClient>,
    reranker: Arc<dyn RerankClient>,
    config: RetrievalConfig,
}

impl RetrievalEngine {
    /// Construct the engine from its injected seams and resolved config; build once and share
    /// via `Arc`. The argument order is fixed: store, embedder, reranker, config.
    pub fn new(
        store: Arc<dyn MemoryStore>,
        embedder: Arc<dyn EmbeddingClient>,
        reranker: Arc<dyn RerankClient>,
        config: RetrievalConfig,
    ) -> Self;

    /// The synchronous read path. `ctx` is the authenticated scope (never derived from `req`);
    /// `req` is the validated recall request. Returns the ranked facts plus the Meta fields
    /// (next_cursor, abstained, correlation_id) C8 places on the Success envelope.
    pub async fn recall(
        &self,
        ctx: &ScopeContext,
        req: &RecallRequest,
    ) -> Result<RecallOutcome, AppError>;
}

/// The resolved C6 configuration (the Phase 2D subset C6 reads), projected from the global
/// `Config` via `RetrievalConfig::from_config(&Config)`. `Clone, Copy`.
pub struct RetrievalConfig {
    pub embed_dim: u32,             // RECALL_EMBED_DIM
    pub stage1_k: u16,              // RECALL_STAGE1_K
    pub result_cap_max: u8,         // RECALL_RESULT_CAP_MAX
    pub abstain_threshold: f64,     // RECALL_ABSTAIN_THRESHOLD
    pub recency_weight: f64,        // RECALL_RECENCY_WEIGHT
    pub recency_tau_days: f64,      // RECALL_RECENCY_TAU_DAYS
    pub reformulation_enabled: bool,// RECALL_REFORMULATION_ENABLED (default false)
}

impl RetrievalConfig {
    /// Project the resolved global `Config` onto the C6 subset.
    pub fn from_config(c: &Config) -> Self;
}

/// What C6 returns to C8: the response payload plus the Meta fields C8 serialises.
pub struct RecallOutcome {
    pub response: RecallResponse,   // { facts: Vec<RankedFact> }
    pub next_cursor: Option<String>,// opaque cursor; None on the last page or on abstain
    pub abstained: bool,            // true => facts is empty by gating, not by absence
}
```

**Private helpers (not part of the public surface; named here so the as-built ranking and tokenisation arithmetic is traceable):**

- `recency_final_score(rerank_score: f64, age_days: f64, w: f64, tau: f64) -> f64` — the SA-RECENCY-01 boost `rerank · (1 + w·exp(−age_days/τ))`, factored out for table-driven testing.
- `keyword_terms(query: &str) -> Vec<String>` — the stage-1 keyword extraction (see *Internal Logic* step 4 and the *Data Model* tokenisation note).
- `Cursor { s: f64, id: String }` — the pagination cursor payload with `encode`/`decode` methods (see the *Data Model* cursor note).
- `Scored { fact: Fact, final_score: f64 }` — a candidate carried through ranking, gating, and pagination.

**How C8 calls it:** C8 constructs `ScopeContext` from the validated token (C3), deserialises `RecallRequest` from the JSON body, calls `engine.recall(&ctx, &req).await`, then maps `RecallOutcome` onto `Success<RecallResponse>` — `outcome.response` becomes `data`; `outcome.next_cursor` becomes `meta.next_cursor`; `outcome.abstained` (when `true`) becomes `meta.abstained = Some(true)`; `ctx.correlation_id` becomes `meta.correlation_id`. On `Err(AppError)`, C8 maps the variant to the HTTP status and registry `code` per its Error Handling spec.

##### Example

Request (`POST /v1/recall` body, deserialised into `RecallRequest`; `ctx` carries tenant `acme`, user `u-7`, teams `["platform"]`):

```json
{
  "query": "who owns the orders table",
  "filters": { "memory_class": "semantic", "valid_at": "2026-06-01T00:00:00.000Z" },
  "result_cap": 3
}
```

Successful `RecallOutcome` rendered by C8 into the success envelope:

```json
{
  "data": {
    "facts": [
      {
        "fact": {
          "id": "fact:018f9a2b-7c41-7e30-9d22-1a2b3c4d5e6f",
          "content": { "subject": "team:platform", "predicate": "owns", "object": "table:orders" },
          "entities": ["entity:018f9a2b-aaaa-7e30-9d22-000000000001"],
          "source_id": "source:018f9a2b-bbbb-7e30-9d22-000000000002",
          "memory_class": "semantic",
          "visibility": "team-shared",
          "owner": { "tenant": "acme", "team": "platform", "user": "u-7" },
          "valid_from": "2026-01-04T09:00:00.000Z",
          "valid_to": null,
          "ingested_at": "2026-01-04T09:00:01.000Z",
          "confidence": 0.91,
          "salience": 0.6,
          "stability": 3.2,
          "supersedes": null,
          "superseded_by": null,
          "derived_from": [],
          "last_recalled_at": "2026-05-30T11:00:00.000Z"
        },
        "score": 0.88
      }
    ]
  },
  "meta": {
    "next_cursor": "eyJzIjowLjg4LCJpZCI6ImZhY3Q6MDE4ZjlhMmI..." ,
    "correlation_id": "9d2c1f0a-3b4c-4d5e-8f90-aabbccddeeff"
  }
}
```

Abstention example (top final score `0.14 < RECALL_ABSTAIN_THRESHOLD`): `data` is `{ "facts": [] }`; `meta.abstained` is `true`; `meta.next_cursor` is absent.

#### Internal Logic

`recall(ctx, req)` runs the pipeline below in order. The whole path targets p95 ≤ 200 ms (NFR-P2); each step carries its SA-LAT-01 sub-budget and degradation rule. All step latencies are recorded as metrics (see Observability). In-process overhead across all non-inference steps is budgeted at ≤ 5 ms.

1. **Input guard and pagination decode.** Validate `req.query` is non-empty and ≤ 4096 chars and `1 ≤ req.result_cap ≤ RECALL_RESULT_CAP_MAX`; clamp the effective cap to `min(req.result_cap, RECALL_RESULT_CAP_MAX)`. If `req.cursor` is present, decode it via `Cursor::decode` (base64url URL-safe no-padding, then JSON to `Cursor { s, id }` — see the *Data Model* cursor note); a cursor that fails either the base64url decode or the JSON parse returns `AppError::Validation`. *Errors:* `Validation` on empty/over-length query, out-of-range cap, or undecodable cursor. *Logged:* `query_len`, `result_cap_effective`, `cursor_present`. Budget: counts toward the ≤ 5 ms in-process overhead.

2. **Optional query reformulation (A/B-gated, off by default).** As built, the effective query is always `req.query` (the step holds an `effective_query = req.query.clone()` pass-through), and `RECALL_REFORMULATION_ENABLED` is read and logged but does not yet branch into a rewrite path — when the flag is `false` (the default per ADR-012 / good-mem §7.3) this step is a no-op and the original `req.query` is used for both embedding (step 3) and keyword extraction (step 4). A reformulation rewrite for the `true` case is not implemented; the flag is plumbed through `RetrievalConfig::reformulation_enabled` so the branch can be added later without a signature change. *Errors:* none. *Logged:* `reformulation_enabled` (on the `retrieval.input` debug line, together with `query_len`, `result_cap_effective`, `cursor_present`). Budget: counts toward the ≤ 5 ms in-process overhead.

3. **Embed the query (read-path model inference #1).** Call `EmbeddingClient::embed(&[effective_query])`; take element 0 as the query vector. Assert its length equals `RECALL_EMBED_DIM`; a mismatch is `AppError::Internal` (a misconfiguration that startup should have caught per SA-EMBED-01). Sub-budget: ≤ 60 ms. *Errors:* `ProviderError::Timeout` → `AppError::Provider` (→ 504 PROVIDER_TIMEOUT); `ProviderError::{Status,Transport,Malformed}` → `AppError::Provider` (→ 502 PROVIDER_ERROR). **Degradation on embed timeout: fail fast** — without a query vector semantic recall cannot run, so the call returns the error rather than degrading. *Logged:* `embed_ms`, `embed_dim`.

4. **Stage-1 multi-signal recall (scope- and metadata-filtered).** Build a `StageOneQuery` from the query vector, the keyword terms (`keyword_terms(&effective_query)` — see the *Data Model* tokenisation note), `req.filters` mapped onto the store's `RecallFilters` (`memory_class`, `visibility`, `entity`, `valid_at` as the bi-temporal as-of bound), the `ctx` scope, and `stage1_k = RECALL_STAGE1_K`. Call `MemoryStore::recall(ctx, &query)`. The store applies the read-filter rule using `ctx` (semantic ANN + BM25 keyword + graph signals, scope- and metadata-filtered) and returns up to `RECALL_STAGE1_K` `Candidate`s. Sub-budget: ANN component ≤ 50 ms (NFR-P3). *Errors:* `StoreError::Timeout` → `AppError::Store` (→ 504 STORE_TIMEOUT); `StoreError::Unavailable` → `AppError::Store` (→ 503 STORE_UNAVAILABLE). **Degradation: fail fast** — a store failure on the read path is a typed error, not a partial result (HLD 03 error posture). *Logged:* `stage1_candidates`, `stage1_ms`. If zero candidates are returned, skip to step 8 with an empty set and `abstained = true`.

5. **Cross-encoder rerank (read-path model inference #2, not an LLM).** Serialise each candidate's `fact.content` to a document string (`content.to_string()`); call `RerankClient::rerank(&effective_query, &docs)` over the bounded set (≤ `RECALL_STAGE1_K` documents). The HTTP adapter implements this as `POST {RECALL_RERANK_URL}/rerank` with body `{ "query": "..", "documents": ["..", ..] }` and parses `{ "scores": [f64, ..] }`; the returned `Vec<f64>` aligns positionally with the input documents (see the *Rerank wire contract* note below). Each value is taken as that candidate's `rerank_score`, clamped to `[0,1]`; a candidate with no aligned score (a short `scores` array) falls back to its `semantic_score`. Sub-budget: ≤ 60 ms. *Errors:* `ProviderError::Timeout` / `ProviderError::{Status,Transport,Malformed}`. **Degradation on reranker timeout or upstream error: skip rerank and proceed with stage-1 order** — set each candidate's `rerank_score` to its `semantic_score` and continue (the response is still returned, ranked by stage-1 signal). The degraded path is logged at `warn`. *Logged:* `rerank_ms`, `rerank_skipped`.

6. **Recency weighting.** For each candidate compute `age_days = (now_utc − fact.ingested_at)` in days (floored at 0), then `recency_boost = RECALL_RECENCY_WEIGHT · exp(−age_days / RECALL_RECENCY_TAU_DAYS)` and `final = rerank_score · (1 + recency_boost)` (SA-RECENCY-01). Sort candidates by `final` descending, breaking ties by `fact.id` ascending for a stable total order. *Errors:* none. *Logged:* `recency_applied = true`. Budget: counts toward the ≤ 5 ms in-process overhead.

7. **Retrieval gating / abstain.** If the highest `final` score across all candidates is `< RECALL_ABSTAIN_THRESHOLD`, discard every candidate, set `abstained = true`, and continue to step 8 with an empty surviving set (SA-GATE-01). Otherwise `abstained = false` and all candidates survive into step 8. *Errors:* none. *Logged:* `top_final_score`, `abstained`. Budget: counts toward the ≤ 5 ms in-process overhead.

8. **Pagination window.** Apply the decoded cursor (step 1) by dropping every surviving candidate whose `(final, fact.id)` is not strictly after `(last_score, last_id)` in the step-6 total order, then truncate the remaining list to the effective `result_cap` (step 1). If, after truncation, more surviving candidates remain than were emitted, the page is non-final. *Errors:* none. *Logged:* `page_emitted`, `page_has_more`. Budget: counts toward the ≤ 5 ms in-process overhead.

9. **Provenance attach (conditional; ADR-014).** If `req.include_provenance` is `false` (the default), every `RankedFact.source` is `None` and this step is a no-op. If `true`, for each fact on the truncated page with `source_id == Some(id)`, load the cited `Source` via `MemoryStore::get_source(ctx, id)` and set `source = Some(SourceProvenance { origin_ref, modification_marker })`; a fact with `source_id == None` keeps `source: None`. `recall` performs **no** source-change check — the agent (with its co-located broker) verifies freshness from the returned `origin_ref` + `modification_marker`. **Degradation: best-effort** — a `get_source` miss or store error for an individual source is absorbed (that fact's `source` stays `None`); provenance never fails the read and produces no `AppError`. Budget: one bounded store read per distinct cited source, folded into store time. *Logged:* `provenance_attached_count`.

10. **Build the outcome.** Map each surviving candidate to a `RankedFact { fact, score: final, source }` (the `source` from step 9, `None` unless `include_provenance` was set) with `score` clamped to `[0,1]`. Build `RecallResponse { facts }`. If `abstained` (step 7) the facts vector is empty and `next_cursor` is `None`. Otherwise, if the page is non-final (step 8), set `next_cursor = Some(Cursor { s: last_emitted_final, id: last_emitted_id }.encode())` (base64url URL-safe no-padding over the JSON encoding — see the *Data Model* cursor note); if final, `next_cursor = None`. Return `RecallOutcome { response, next_cursor, abstained }`. *Errors:* a cursor-serialisation failure is `AppError::Internal`. *Logged:* `facts_returned`, `next_cursor_present`.

#### Data Model

N/A — reads via `MemoryStore`, owns no tables. All persistence, indexes, and the bi-temporal `valid_at` query belong to the C1 Memory Store.

**Pagination cursor (in-flight only; SA-PAGE-01).** The opaque cursor encodes the `(final_score, fact_id)` of the last emitted fact. As built it is a private `Cursor { s: f64, id: String }` struct serialised to JSON, then base64url-encoded with the URL-safe, no-padding alphabet (`base64::engine::general_purpose::URL_SAFE_NO_PAD`): `cursor = base64url(JSON {s: f64, id: String})`. `Cursor::decode` reverses both steps; a failure at either step is `AppError::Validation` (VAL_OUT_OF_RANGE, step 1). `s` carries the last emitted fact's `final_score` and `id` carries its `fact.id`. The cursor is never persisted.

**Keyword tokenisation (`keyword_terms`).** The stage-1 `keyword_terms` fed into `StageOneQuery` are produced from the effective query by: split on every run of non-alphanumeric characters (`!char::is_alphanumeric`), drop empty fragments, lowercase each remaining fragment, then de-duplicate keeping first-seen order. These candidate terms feed C1's `recall_text` BM25 analyzer, which performs the real tokenisation and matching; `keyword_terms` only supplies the candidate set, so it deliberately does not replicate the analyzer's class tokeniser, stemming, or stop-word handling.

#### Error Table

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| `query` empty, over 4096 chars, `result_cap` outside `[1, RECALL_RESULT_CAP_MAX]`, or an undecodable `cursor` (step 1) | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"recall request field out of range","correlation_id":"<uuid>"}}` |
| Embedding provider call times out (step 3) — fail fast, no degradation | 504 | PROVIDER_TIMEOUT | `{"error":{"code":"PROVIDER_TIMEOUT","message":"embedding provider timed out","correlation_id":"<uuid>"}}` |
| Embedding or reranker provider returns non-2xx / malformed body (steps 3, 5) | 502 | PROVIDER_ERROR | `{"error":{"code":"PROVIDER_ERROR","message":"upstream model provider error","correlation_id":"<uuid>"}}` |
| Memory Store stage-1 recall times out (step 4) | 504 | STORE_TIMEOUT | `{"error":{"code":"STORE_TIMEOUT","message":"memory store timed out","correlation_id":"<uuid>"}}` |
| Memory Store unavailable (step 4) | 503 | STORE_UNAVAILABLE | `{"error":{"code":"STORE_UNAVAILABLE","message":"memory store unavailable","correlation_id":"<uuid>"}}` |
| Query-vector length ≠ `RECALL_EMBED_DIM` (step 3), or cursor serialisation failure (step 10) | 500 | INTERNAL | `{"error":{"code":"INTERNAL","message":"internal error","correlation_id":"<uuid>"}}` |

Note: reranker timeout (step 5) is **not** an error row — it degrades to stage-1 order and still returns `200`. Provenance attach (step 9) is best-effort — a `get_source` miss/error leaves that fact's `source` as `None` and still returns `200`, so it is not an error row either. C8 owns the `AppError` → status/code mapping; the codes above are the Phase 4 registry values C6's variants map to.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Retrieval Engine

  Scenario: Happy path — ranked facts within budget
    Given an authenticated read scope (tenant "acme", user "u-7", teams ["platform"])
    And a recall request with query "who owns the orders table" and result_cap 3
    And the Memory Store returns 12 candidates and the reranker scores them
    When recall is invoked
    Then the response contains at most 3 RankedFacts ordered by final score descending
    And each RankedFact carries a fact and a score in [0,1]
    And meta.abstained is absent
    And a next_cursor is present when more than 3 candidates survive gating

  Scenario: Edge case — abstain when nothing is relevant
    Given an authenticated read scope
    And the top reranked candidate's final score is 0.14
    And RECALL_ABSTAIN_THRESHOLD is 0.2
    When recall is invoked
    Then the response facts list is empty
    And meta.abstained is true
    And no next_cursor is present

  Scenario: Edge case — reranker timeout degrades to stage-1 order
    Given an authenticated read scope returning 8 stage-1 candidates
    And the reranker call exceeds its 60 ms sub-budget
    When recall is invoked
    Then the response is returned with status 200
    And facts are ordered by their stage-1 semantic score
    And the rerank_skipped metric is incremented

  Scenario: Provenance attached only when requested
    Given an authenticated read scope returning a sourced fact that clears the gate
    When recall is invoked with include_provenance true
    Then each returned sourced fact carries source.origin_ref and source.modification_marker
    When recall is invoked with include_provenance false
    Then no returned fact carries a source object

  Scenario: Error path — embedding provider times out
    Given an authenticated read scope
    And the embedding provider call exceeds its 60 ms sub-budget
    When recall is invoked
    Then recall fails fast
    And the caller receives status 504 with code PROVIDER_TIMEOUT

  Scenario: Error path — result_cap out of range
    Given an authenticated read scope
    And a recall request with result_cap 200
    When recall is invoked
    Then the caller receives status 400 with code VAL_OUT_OF_RANGE

  Scenario: Error path — memory store unavailable
    Given an authenticated read scope
    And the Memory Store stage-1 recall fails with Unavailable
    When recall is invoked
    Then recall fails fast
    And the caller receives status 503 with code STORE_UNAVAILABLE
```

#### Performance, Security, Observability

- **Performance targets.** Whole read path p95 ≤ 200 ms excluding caller network time (NFR-P2). SA-LAT-01 sub-budget table, summing within NFR-P2:

  | Step | Sub-budget | Breach behaviour |
  |---|---|---|
  | Query embed (step 3) | ≤ 60 ms | Fail fast (no query vector ⇒ no semantic recall) |
  | Stage-1 ANN recall (step 4) | ≤ 50 ms (NFR-P3) | Fail fast with typed `Store` error |
  | Cross-encoder rerank (step 5) | ≤ 60 ms | Skip rerank, return stage-1 order |
  | Provenance attach (step 9, only if `include_provenance`) | bounded store read per cited source | Best-effort; a miss/error leaves that fact's `source` `None` |
  | In-process overhead (steps 1, 2, 6, 7, 8, 10) | ≤ 5 ms | n/a (no external call) |

  Response stays within the bounded token budget (NFR-P5) because `result_cap ≤ RECALL_RESULT_CAP_MAX = 50`. No LLM call on this path (NFR-P1); exactly two read-path model inferences (embed, rerank) per ADR-012.
- **Security.** The component never trusts a scope value from `req`; scope comes only from `ctx` (built by C3), which is passed to `MemoryStore::recall` so the store applies the read-filter rule (tenant isolation is structural — a different tenant is a different namespace). Cross-tenant retrieval is structurally impossible. Provider API keys (`RECALL_EMBED_API_KEY`, `RECALL_RERANK_API_KEY`) are read from env only and never logged. Fact `content` is never logged at `info` or below; only counts and scores are logged. Input is validated at the boundary (step 1). Authentication, authorisation, and rate limiting are enforced by C3/C8 before `recall` is called; C6 assumes `ctx.allowed_ops.read == true`.
- **Observability.** Per-stage latency histograms (milliseconds): `recall_embed_ms`, `recall_stage1_ms`, `recall_rerank_ms`, `recall_total_ms` — each labelled `{tenant}`. Counters: `recall_abstain_total{tenant}` (the abstain-rate numerator; abstain rate = `recall_abstain_total / recall_total` over the metrics window), `recall_rerank_skipped_total{tenant}`, `recall_provenance_attached_total{tenant}`. Trace span `retrieval.recall` with child spans `retrieval.embed`, `retrieval.stage1`, `retrieval.rerank`, all carrying `correlation_id` from `ctx`. Log fields per the per-step `Logged` notes; structured logs carry `correlation_id`, never fact `content` or provider keys.

#### Gaps

None. *(Resolved in the Phase 3 reconciliation pass and the RFC-01 / ADR-014 amendment:*
- *Freshness is agent-side (ADR-014): C6 runs no source-change check and the `FreshnessChecker`/`BrokerClient`/`Currency` types are removed. C6 instead attaches per-fact provenance on request — it loads each cited `Source` via `MemoryStore::get_source` and returns `SourceProvenance { origin_ref, modification_marker }`; facts with `source_id == None`, and `include_provenance == false`, carry no `source` object.*
- *`StageOneQuery`/`Candidate` field sets match the authoritative C1 Memory Store spec verbatim (per-signal `semantic_score`/`keyword_score`/`graph_score`, not a single fused score).*
- *`RECALL_REFORMULATION_ENABLED` (bool, default `false`, owner C6) is now in Phase 2D.*
- *Keyword tokenisation follows C1's `recall_text` BM25 analyzer (class tokeniser, lowercase + ascii filters) — a documented assumption, not an open gap.)*
