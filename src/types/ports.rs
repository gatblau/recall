//! Provider & infrastructure traits (§2C.6) — dependency-injected seams that keep providers
//! swappable (OQ-MODELS/OQ-QUEUE/OQ-STORE).
//!
//! The traits are DECLARED here; component logic that implements them lands in later phases.
//! `StoreError`/`Candidate`/`StageOneQuery`/`AuditEntry` are owned by the C1 spec; `QueueError`
//! by C2; `ExtractedFact`/`EntityMention` by C4. (recall is LLM-free — ADR-015 — so there is no
//! `LlmClient` and no `InsightCandidate`.) Each component phase refines its owned type in place.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::api::DeletionProof;
use crate::types::domain::{Entity, Fact, MemoryClass, Relationship, Source, Visibility};
use crate::types::job::{JobKind, WorkJob};
use crate::types::scope::{ScopeContext, ScopeRef};

// --- Store-owned supporting types (C1) — canonical, per the C1 Memory Store spec (Phase 2) ---

/// Typed error for every `MemoryStore` operation. Maps to `AppError::Store` in §2C.7; the
/// `StoreError` -> error-code mapping is owned by the Phase 4 X1 registry (`src/error.rs`).
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    /// Record failed domain validation. `VAL_INVALID_BODY` for structural failures,
    /// `VAL_OUT_OF_RANGE` for score-range failures (-> 400).
    #[error("validation: {0}")]
    Validation(String),
    /// Target id absent or excluded by the read filter (-> 404 NOT_FOUND).
    #[error("not found")]
    NotFound,
    /// A `put_*` record's `owner.tenant` differs from the active namespace (-> 403 SCOPE_FORBIDDEN).
    #[error("scope forbidden")]
    ScopeForbidden,
    /// Store connection lost / engine not reachable (-> 503 STORE_UNAVAILABLE).
    #[error("store unavailable: {0}")]
    Unavailable(String),
    /// Per-statement timeout (-> 504 STORE_TIMEOUT).
    #[error("store timeout")]
    Timeout,
    /// `hard_delete` verification found a collected record still present (-> 500 INTERNAL).
    #[error("partial delete: {removed} of {expected} removed")]
    PartialDelete { removed: u32, expected: u32 },
    /// Unhandled internal invariant violation (-> 500 INTERNAL).
    #[error("internal: {0}")]
    Internal(String),
}

/// One stage-1 retrieval candidate: a fact id, the fact itself, and the per-signal scores that
/// contributed to its selection. The Retrieval Engine (C6) fuses and reranks these.
#[derive(Clone)]
pub struct Candidate {
    /// "fact:<uuidv7>".
    pub fact_id: String,
    /// The resolved fact, already read-filtered.
    pub fact: Fact,
    /// Cosine similarity from the HNSW vector index, [0,1]; 0.0 if not a vector hit.
    pub semantic_score: f64,
    /// BM25 score, normalised to [0,1]; 0.0 if not a keyword hit.
    pub keyword_score: f64,
    /// Graph-proximity score, [0,1]; 0.0 if not reached via traversal.
    pub graph_score: f64,
}

/// The stage-1 multi-signal query the Retrieval Engine (C6) submits to `recall`.
#[derive(Clone)]
pub struct StageOneQuery {
    /// `dim == RECALL_EMBED_DIM`; empty disables the vector signal.
    pub query_vector: Vec<f32>,
    /// BM25 terms; empty disables the keyword signal.
    pub keyword_terms: Vec<String>,
    /// Metadata filters (memory_class, visibility, entity, valid_at).
    pub filters: RecallFilters,
    /// The authenticated scope; the read filter is applied to every signal.
    pub scope: ScopeContext,
    /// Max candidates to return (SA-RERANK-01, default `RECALL_STAGE1_K` = 50).
    pub stage1_k: u16,
}

/// Metadata filters for `recall`. The §2C.4 type re-stated with `Clone, Default` so `StageOneQuery`
/// is self-contained for C1; C6/C8 use the §2C.4 definition (`crate::types::api::RecallFilters`),
/// which is field-identical.
#[derive(Clone, Default)]
pub struct RecallFilters {
    pub memory_class: Option<MemoryClass>,
    pub visibility: Option<Visibility>,
    /// Restrict to facts touching this entity id.
    pub entity: Option<String>,
    /// As-of query into the bi-temporal history.
    pub valid_at: Option<DateTime<Utc>>,
}

/// Append-only audit record (SA-AUDIT-01), persisted to the per-tenant `audit_log` table. Canonical
/// definition owned by the C1 spec (Phase 2).
#[derive(Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// "audit_log:<uuidv7>".
    pub id: String,
    /// Selects the namespace; never stored cross-tenant.
    pub tenant: String,
    /// `ctx.user` (OIDC subject).
    pub subject: String,
    /// "recall" | "remember" | "forget" | "get_fact" | "freshness_check".
    pub operation: String,
    /// The scope the operation targeted.
    pub scope: ScopeRef,
    /// "ok" | "denied" | "error:<CODE>".
    pub outcome: String,
    /// `ctx.token_jti` — never the token itself, never PII.
    pub token_jti: String,
    pub correlation_id: String,
    /// Server-set, RFC3339 ms.
    pub at: DateTime<Utc>,
}

// --- Queue-owned supporting type (C2; minimal Phase-1 form) ---

/// Work-queue failure modes. Canonical definition owned by the C2 spec (Phase 3). Every variant maps
/// through `AppError::Queue` to HTTP `503 QUEUE_UNAVAILABLE` (X1 registry); the `Display` detail is
/// logged at `error`, never returned to a client.
#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    /// Store/NATS connection or statement failure -> 503.
    #[error("queue backend unavailable: {0}")]
    BackendUnavailable(String),
    /// `complete`/`fail` referenced an unknown job id.
    #[error("job not found: {0}")]
    JobNotFound(String),
    /// `complete`/`fail` on a job not in `Leased` status.
    #[error("job not leased: {0}")]
    NotLeased(String),
    /// `enqueue` payload failed structural validation.
    #[error("invalid job: {0}")]
    InvalidJob(String),
    /// `RECALL_QUEUE_BACKEND=nats` without `RECALL_QUEUE_NATS_URL`.
    #[error("backend misconfigured: {0}")]
    Misconfigured(String),
}

// --- Write-pipeline supporting types (C4) — canonical, per the C4 Write Pipeline spec (Phase 5) ---

/// The LLM extractor's output shape, pre-persistence. One per asserted fact. This is NOT a stored
/// `Fact`: it carries no id, no scores, no validity, no scope — those are assigned by later pipeline
/// steps (C4 *Public Interface*).
#[derive(Clone, Serialize, Deserialize)]
pub struct ExtractedFact {
    /// Structured assertion (JSON object), not free text.
    pub content: serde_json::Value,
    /// Raw entity references to resolve (>=1). (Spec name: `entity_mentions`.)
    pub entities: Vec<EntityMention>,
    /// Proposed memory class; `procedural` is impossible (enum). (Spec name: `memory_class`.)
    pub memory_class: crate::types::domain::MemoryClass,
    /// [0,1] — the LLM's own confidence in this extraction. (Spec name: `extractor_confidence`.)
    pub confidence: f64,
}

/// A mention of an entity within extracted content (C4 *Public Interface*).
#[derive(Clone, Serialize, Deserialize)]
pub struct EntityMention {
    /// Surface form as it appeared in the content (1..=512 chars). (Spec name: `surface_form`.)
    pub surface: String,
    /// Optional coarse type hint, e.g. "person", "team". (Spec name: `mention_type`.)
    pub canonical_name: Option<String>,
}

// (InsightCandidate removed by ADR-015 — server-side consolidation is dropped; the agent writes
// consolidated insights itself as agent-stated facts.)

// --- Provider-shared error/result types (§2C.6) ---

/// Shared by EmbeddingClient/RerankClient/PiiDetector.
#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    /// -> 504 PROVIDER_TIMEOUT.
    #[error("provider timeout")]
    Timeout,
    /// -> 502 PROVIDER_ERROR.
    #[error("provider status {0}")]
    Status(u16),
    /// -> 502 PROVIDER_ERROR.
    #[error("provider transport: {0}")]
    Transport(String),
    /// -> 502 PROVIDER_ERROR.
    #[error("provider malformed: {0}")]
    Malformed(String),
}

/// A PII span returned by `PiiDetector::scan` (C4 *Public Interface*). Produced by the injected
/// `PiiDetector` trait; consumed only by the write pipeline.
#[derive(Clone, Serialize, Deserialize)]
pub struct PiiSpan {
    /// RFC 6901 pointer into `content` locating the string value containing the span.
    pub json_pointer: String,
    /// Byte offset within the located string value (inclusive start).
    pub start: u32,
    /// Exclusive byte offset; `end > start`.
    pub end: u32,
    /// e.g. "email", "person", "phone", "national_id".
    pub pii_type: String,
    /// [0,1].
    pub confidence: f64,
}

// --- Infrastructure traits ---

#[async_trait]
pub trait MemoryStore: Send + Sync {
    // --- Fact CRUD + bi-temporal ---
    /// Upsert; embedding passed alongside.
    async fn put_fact(&self, f: &Fact) -> Result<(), StoreError>;
    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError>;
    /// Multi-signal stage-1.
    async fn recall(
        &self,
        ctx: &ScopeContext,
        q: &StageOneQuery,
    ) -> Result<Vec<Candidate>, StoreError>;
    async fn end_validity(
        &self,
        ctx: &ScopeContext,
        id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError>;
    async fn supersede(
        &self,
        ctx: &ScopeContext,
        old_id: &str,
        new_id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError>;
    async fn hard_delete(
        &self,
        ctx: &ScopeContext,
        id: &str,
    ) -> Result<DeletionProof, StoreError>;
    // --- Entity CRUD + resolution support ---
    /// Upsert.
    async fn put_entity(&self, e: &Entity) -> Result<(), StoreError>;
    async fn get_entity(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Entity>, StoreError>;
    async fn find_entity_by_name(
        &self,
        ctx: &ScopeContext,
        name: &str,
    ) -> Result<Vec<Entity>, StoreError>;
    async fn merge_entities(
        &self,
        ctx: &ScopeContext,
        keep_id: &str,
        merge_id: &str,
    ) -> Result<(), StoreError>;
    // --- Relationship CRUD ---
    async fn put_relationship(&self, r: &Relationship) -> Result<(), StoreError>;
    async fn get_relationship(
        &self,
        ctx: &ScopeContext,
        id: &str,
    ) -> Result<Option<Relationship>, StoreError>;
    async fn end_relationship_validity(
        &self,
        ctx: &ScopeContext,
        id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError>;
    // --- Source CRUD ---
    /// Upsert.
    async fn put_source(&self, s: &Source) -> Result<(), StoreError>;
    async fn get_source(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Source>, StoreError>;
    // --- Audit (SA-AUDIT-01) ---
    /// Append-only, synchronous.
    async fn append_audit(&self, e: &AuditEntry) -> Result<(), StoreError>;
    // --- Maintenance surface consumed by C7 (all take ctx; ctx.tenant selects the namespace) ---
    /// Admin op, no ctx.
    async fn list_tenants(&self) -> Result<Vec<String>, StoreError>;
    async fn scan_recent_episodes(
        &self,
        ctx: &ScopeContext,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError>;
    async fn scan_contradiction_candidates(
        &self,
        ctx: &ScopeContext,
        limit: u32,
    ) -> Result<Vec<(Fact, Fact)>, StoreError>;
    /// Coarse prefilter; C7 owns the decay maths.
    async fn scan_decay_candidates(
        &self,
        ctx: &ScopeContext,
        salience_floor: f64,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError>;
    async fn scan_reembed_candidates(
        &self,
        ctx: &ScopeContext,
        current_model_version: &str,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError>;
    async fn update_fact_maintenance_fields(
        &self,
        ctx: &ScopeContext,
        f: &Fact,
    ) -> Result<(), StoreError>;
    async fn set_fact_embedding(
        &self,
        ctx: &ScopeContext,
        fact_id: &str,
        vector: &[f32],
        model_version: &str,
    ) -> Result<(), StoreError>;
    // --- Lifecycle / tenancy ---
    /// Idempotent.
    async fn ensure_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError>;
    /// ADR-011 erasure.
    async fn drop_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError>;
    /// Connection live + vector-index dim == RECALL_EMBED_DIM.
    async fn ready(&self) -> Result<(), StoreError>;
}

// FreshnessChecker removed by ADR-014 — freshness is agent-side; recall runs no read-path check.

#[async_trait]
pub trait WorkQueue: Send + Sync {
    /// Returns job id.
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>;
    async fn claim(
        &self,
        kinds: &[JobKind],
        lease: Duration,
    ) -> Result<Option<WorkJob>, QueueError>;
    async fn complete(&self, job_id: &str) -> Result<(), QueueError>;
    async fn fail(&self, job_id: &str, retryable: bool) -> Result<(), QueueError>;
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync {
    /// dim = config.
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, ProviderError>;
}

#[async_trait]
pub trait RerankClient: Send + Sync {
    async fn rerank(&self, query: &str, docs: &[String]) -> Result<Vec<f64>, ProviderError>;
}

// LlmClient removed by ADR-015 — recall is LLM-free: the agent extracts facts and consolidates
// insights, and submits structured agent-asserted content. The write pipeline wraps it directly.

// BrokerClient removed by ADR-014 — recall makes no outbound broker call.

#[async_trait]
pub trait PiiDetector: Send + Sync {
    async fn scan(&self, content: &serde_json::Value) -> Result<Vec<PiiSpan>, ProviderError>;
}
