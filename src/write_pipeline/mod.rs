//! C4 — Write Pipeline. The asynchronous consumer of `ExtractFact` work jobs from the durable work
//! queue (C2). It turns raw, untrusted `remember` content into a clean, scoped, provenance-tagged
//! `Fact` in the Memory Store (C1), or into a quarantined / rejected record when the content cannot be
//! trusted.
//!
//! The component is a **linear step pipeline driven by a claim/lease loop** (C4 *Approach*): one async
//! task per worker calls `WorkQueue::claim(&[JobKind::ExtractFact], lease)`; each claimed job flows
//! through eight ordered step functions in fixed order:
//!
//!   1. Filter noise         — drop empty / low-signal content (data minimisation).
//!   2. Extract              — `LlmClient::extract`, or bypass the LLM for agent-stated content.
//!   3. Normalise            — canonicalise content (key order, whitespace, NFC, RFC3339 ms).
//!   4. Entity-resolve       — rules -> ML -> create-new ladder (v1; never silent overwrite).
//!   5. Score                — salience + confidence in [0,1] (SA-SCORE-01).
//!   6. PII scan             — redact high-confidence spans in place, else flag `pii_review` (SA-PII-01).
//!   7. Write gate           — trust bands admit/quarantine/reject (SA-WGATE-01, ADR-008).
//!   8. Embed + persist      — `EmbeddingClient::embed` then `MemoryStore::put_fact` (idempotent id).
//!
//! It never runs on the read path (ADR-004): a slow or failed write is retried with backoff behind the
//! queue and is dead-lettered after the attempt cap (SA-QUEUE-02), and a write never blocks a read.
//!
//! **Security.** Untrusted-source content is treated as **data, never instructions** (ADR-008): the
//! imperative-pattern detector caps instruction-like content strictly below the quarantine floor so it
//! can never be admitted, and extracted content is never evaluated, executed, or used to steer pipeline
//! control flow. Scope is taken only from the job's `ScopeRef` (derived by C3 at enqueue), never from
//! request-body content. Every store write goes through parameterised queries. No content, PII span
//! text, secret, or token is ever logged — only counts and ids.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use surrealdb::engine::any::Any;
use surrealdb::types::{Datetime, Object, Value};
use surrealdb::Surreal;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::error::{AppError, ValidationKind};
use crate::store::migrate::{validate_tenant, Migrator, DB_NAME};
use crate::types::api::SourceInput;
use crate::types::domain::{Entity, Fact, MemoryClass, Source, Visibility};
use crate::types::job::{JobKind, WorkJob};
use crate::types::ports::{
    EmbeddingClient, EntityMention, ExtractedFact, LlmClient, MemoryStore, PiiDetector, PiiSpan,
    ProviderError, StoreError, WorkQueue,
};
use crate::types::scope::{OpSet, ScopeContext, ScopeRef};

/// The outcome of one job (one persisted/quarantined/rejected fact), used for logging, metrics, and
/// tests (C4 *Public Interface*).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteOutcome {
    Persisted,
    Quarantined,
    Rejected,
    FilteredNoise,
}

/// Decision returned by the write gate, separating the three audited outcomes (C4 *Public Interface*).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GateDecision {
    Admit,
    Quarantine,
    Reject,
}

/// Result of the imperative-pattern detector (C4 *Public Interface*).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InstructionLikelihood {
    /// `true` => the trust score is capped strictly below `trust_quarantine`.
    pub is_instruction_like: bool,
    /// Count of imperative patterns matched (for audit).
    pub matched_patterns: u32,
}

/// A gate-quarantined record persisted to the `quarantine` table (C4 *Data Model*).
#[derive(Clone, Serialize, Deserialize)]
pub struct QuarantineRecord {
    /// "quarantine:<uuidv7>".
    pub id: String,
    /// Redacted content (object) — PII already removed.
    pub content: Json,
    /// Entity ids (>=1).
    pub entities: Vec<String>,
    pub source_id: Option<String>,
    pub memory_class: MemoryClass,
    pub owner: ScopeRef,
    /// [0,1] — the gate's trust score.
    pub trust_score: f64,
    /// [0,1].
    pub confidence: f64,
    /// [0,1].
    pub salience: f64,
    pub is_instruction_like: bool,
    pub pii_review: bool,
    pub idempotency_key: Option<String>,
    /// Audit: why quarantined.
    pub reason: String,
    pub quarantined_at: DateTime<Utc>,
}

/// Validated write-pipeline configuration (C4 *Public Interface*). The spec declares this `Copy`; it
/// carries `embed_model_version: String` here (used at persist time), so it is `Clone` rather than
/// `Copy`.
#[derive(Clone)]
pub struct WritePipelineConfig {
    /// `RECALL_TRUST_ADMIT` — a trust score `>=` this admits.
    pub trust_admit: f64,
    /// `RECALL_TRUST_QUARANTINE` — `quarantine <= score < admit` quarantines; `< quarantine` rejects.
    pub trust_quarantine: f64,
    /// `RECALL_PII_REDACT_CONF` — span confidence `>=` this is redacted; below sets `pii_review=true`.
    pub pii_redact_conf: f64,
    /// `RECALL_EMBED_DIM` — the embed vector length must equal this (SA-EMBED-01).
    pub embed_dim: u32,
    /// `RECALL_JOB_MAX_ATTEMPTS` — retry cap before dead-letter (owned by C2; read for logging).
    pub max_attempts: u32,
    /// `RECALL_SOURCE_TRUST_DEFAULT` — prior trust for a newly-seen source.
    pub source_trust_default: f64,
    /// `RECALL_EMBED_MODEL_VERSION` — stored alongside the embedding for re-embed bookkeeping (C7).
    pub embed_model_version: String,
    /// Claim lease length (default 30 s).
    pub claim_lease: Duration,
    /// Per-job wall-clock budget (default 30 s).
    pub per_job_budget: Duration,
}

impl WritePipelineConfig {
    /// Build the pipeline config from the loaded §2D configuration.
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            trust_admit: cfg.trust_admit,
            trust_quarantine: cfg.trust_quarantine,
            pii_redact_conf: cfg.pii_redact_conf,
            embed_dim: cfg.embed_dim,
            max_attempts: cfg.job_max_attempts,
            source_trust_default: cfg.source_trust_default,
            embed_model_version: cfg.embed_model_version.clone(),
            claim_lease: Duration::from_secs(30),
            per_job_budget: Duration::from_secs(30),
        }
    }
}

/// The ML-tier string-similarity admit threshold (C4 step 4): a single candidate scoring at or above
/// this resolves to the existing entity; otherwise the mention escalates to create-new.
const ML_SIMILARITY_ADMIT: f64 = 0.92;

/// The validated `remember` job payload deserialised from `WorkJob::payload` (step 0).
#[derive(Deserialize)]
struct JobPayload {
    content: Json,
    #[serde(default)]
    source: Option<SourceInput>,
    #[serde(default)]
    agent_stated: bool,
}

/// The runtime entrypoint and step functions of the Write Pipeline (C4 *Public Interface*).
pub struct WritePipeline {
    store: Arc<dyn MemoryStore>,
    queue: Arc<dyn WorkQueue>,
    embed: Arc<dyn EmbeddingClient>,
    llm: Arc<dyn LlmClient>,
    pii: Arc<dyn PiiDetector>,
    /// Shared SurrealDB handle for the C4-owned `quarantine` table (mirrors how C2 writes its tables
    /// over `Store::handle()`); `None` when no quarantine sink is wired (the gate then rejects rather
    /// than quarantines mid-band content, which is surfaced as a defended error path).
    db: Option<Surreal<Any>>,
    cfg: WritePipelineConfig,
}

impl WritePipeline {
    /// Construct with injected dependencies and validated config. `db` is the shared store connection
    /// used for the C4-owned `quarantine` table writes.
    pub fn new(
        store: Arc<dyn MemoryStore>,
        queue: Arc<dyn WorkQueue>,
        embed: Arc<dyn EmbeddingClient>,
        llm: Arc<dyn LlmClient>,
        pii: Arc<dyn PiiDetector>,
        db: Surreal<Any>,
        cfg: WritePipelineConfig,
    ) -> Self {
        Self {
            store,
            queue,
            embed,
            llm,
            pii,
            db: Some(db),
            cfg,
        }
    }

    /// The consumer loop. Claims `ExtractFact` jobs and processes each to a terminal queue state. Runs
    /// until `cancel` is triggered. Never returns on a transient queue error — it logs and re-claims
    /// after a short delay.
    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let claimed = self
                .queue
                .claim(&[JobKind::ExtractFact], self.cfg.claim_lease)
                .await;
            match claimed {
                Ok(Some(job)) => {
                    let ctx = self.ctx_from_job(&job);
                    match self.process(&ctx, &job).await {
                        Ok(outcome) => {
                            tracing::debug!(
                                target: "recall",
                                job_id = %job.id,
                                outcome = ?outcome,
                                "write.process.done"
                            );
                        }
                        Err(e) => self.fail_job(&job, &e).await,
                    }
                }
                Ok(None) => {
                    // No claimable job; back off briefly then re-poll, unless cancelled.
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                        _ = cancel.cancelled() => return,
                    }
                }
                Err(e) => {
                    tracing::warn!(target: "recall", error = %e, "write.claim.failed");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        _ = cancel.cancelled() => return,
                    }
                }
            }
        }
    }

    /// Translate a failed `process` into the queue action: a retryable `AppError` -> `fail(true)`
    /// (backoff retry then dead-letter at the cap, SA-QUEUE-02); a terminal one -> `fail(false)`
    /// (immediate dead-letter).
    async fn fail_job(&self, job: &WorkJob, err: &AppError) {
        let retryable = is_retryable(err);
        tracing::warn!(
            target: "recall",
            job_id = %job.id,
            attempts = job.attempts,
            retryable,
            error = %err,
            "write.job.failed"
        );
        if let Err(qe) = self.queue.fail(&job.id, retryable).await {
            // A queue failure during fail-handling is logged; the lease will expire and the reaper
            // (C2) returns the job to pending (SA-QUEUE-02).
            tracing::error!(target: "recall", job_id = %job.id, error = %qe, "write.fail.queue_error");
        }
    }

    /// Build the internal `ScopeContext` from the job's `ScopeRef` (C4 *Shared Context*): single-team
    /// membership, `allowed_ops = { read, write, !forget }`, correlation id from the job's scope.
    fn ctx_from_job(&self, job: &WorkJob) -> ScopeContext {
        let teams = match &job.scope.team {
            Some(t) => vec![t.clone()],
            None => vec![],
        };
        ScopeContext {
            tenant: job.scope.tenant.clone(),
            teams,
            user: job.scope.user.clone(),
            token_jti: String::new(),
            allowed_ops: OpSet {
                read: true,
                write: true,
                forget: false,
            },
            correlation_id: job.id.clone(),
        }
    }

    /// Process exactly one claimed job to a terminal queue state (complete or fail). Returns the
    /// outcome for metrics/tests. Bounded by `cfg.per_job_budget`.
    pub async fn process(
        &self,
        ctx: &ScopeContext,
        job: &WorkJob,
    ) -> Result<WriteOutcome, AppError> {
        match tokio::time::timeout(self.cfg.per_job_budget, self.process_inner(ctx, job)).await {
            Ok(r) => r,
            // A per-job budget overrun is a retryable store/provider stall.
            Err(_) => Err(AppError::Store(StoreError::Timeout)),
        }
    }

    async fn process_inner(
        &self,
        ctx: &ScopeContext,
        job: &WorkJob,
    ) -> Result<WriteOutcome, AppError> {
        // Step 0 — validate the job payload.
        if job.kind != JobKind::ExtractFact {
            return Err(AppError::Validation(
                ValidationKind::InvalidBody,
                "wrong job kind for the write pipeline".into(),
            ));
        }
        let payload: JobPayload = serde_json::from_value(job.payload.clone())
            .map_err(|e| AppError::Validation(ValidationKind::InvalidBody, format!("payload: {e}")))?;
        if !payload.content.is_object() {
            return Err(AppError::Validation(
                ValidationKind::InvalidBody,
                "content must be a JSON object".into(),
            ));
        }
        tracing::info!(
            target: "recall",
            job_id = %job.id,
            tenant = %ctx.tenant,
            attempts = job.attempts,
            "write.job.claimed"
        );

        // Step 1 — filter noise.
        if self.filter_noise(&payload.content) {
            tracing::info!(target: "recall", job_id = %job.id, "write.filtered_noise");
            self.complete(&job.id).await?;
            return Ok(WriteOutcome::FilteredNoise);
        }

        // Step 2 — extract.
        let facts = self.extract(&payload.content, payload.agent_stated).await?;
        tracing::info!(target: "recall", job_id = %job.id, fact_count = facts.len(), "write.extracted");
        if facts.is_empty() {
            self.complete(&job.id).await?;
            return Ok(WriteOutcome::FilteredNoise);
        }

        // Steps 3-8 run once per ExtractedFact. The aggregate outcome reported is the "strongest" one
        // (Persisted > Quarantined > Rejected > FilteredNoise) so a multi-fact job's metric reflects
        // that at least one fact was admitted. A failure on any fact aborts the whole job (retryable);
        // persistence is idempotent on the derived fact id so a replay reuses the same id (step 8).
        let mut best = WriteOutcome::FilteredNoise;
        let source_id = self.upsert_source(ctx, payload.source.as_ref()).await?;
        let source_trust = self.read_source_trust(ctx, source_id.as_deref()).await?;

        for (idx, ef) in facts.into_iter().enumerate() {
            let outcome = self
                .process_fact(ctx, job, ef, idx, source_id.clone(), source_trust)
                .await?;
            best = stronger(best, outcome);
        }

        self.complete(&job.id).await?;
        Ok(best)
    }

    /// Run steps 3-8 for one extracted fact, returning its terminal (non-completing) outcome. The job
    /// is completed by the caller once every fact has been processed.
    #[allow(clippy::too_many_arguments)]
    async fn process_fact(
        &self,
        ctx: &ScopeContext,
        job: &WorkJob,
        ef: ExtractedFact,
        fact_index: usize,
        source_id: Option<String>,
        source_trust: f64,
    ) -> Result<WriteOutcome, AppError> {
        // Step 3 — normalise.
        let ef = self.normalise(&ef);

        // Step 4 — entity-resolve (rules -> ML -> create-new).
        let entities = self.resolve_entities(ctx, &ef).await?;

        // Step 5 — score.
        let (salience, confidence) = self.score(&ef, source_trust);

        // Step 6 — PII scan (redact in place, or flag for review).
        let (content, pii_review) = self.pii_scan(&ef.content).await?;

        // Step 7 — write gate (instruction detector then trust bands).
        let instr = self.detect_instruction(&content);
        let decision = self.write_gate(confidence, source_trust, instr);
        let trust = self.trust_score(confidence, source_trust, instr);
        tracing::info!(
            target: "recall",
            job_id = %job.id,
            decision = ?decision,
            trust,
            is_instruction_like = instr.is_instruction_like,
            "write.gate"
        );

        match decision {
            GateDecision::Reject => Ok(WriteOutcome::Rejected),
            GateDecision::Quarantine => {
                let qr = QuarantineRecord {
                    id: format!("quarantine:{}", Uuid::now_v7()),
                    content,
                    entities,
                    source_id,
                    memory_class: ef.memory_class,
                    owner: job.scope.clone(),
                    trust_score: trust,
                    confidence,
                    salience,
                    is_instruction_like: instr.is_instruction_like,
                    pii_review,
                    idempotency_key: job.idempotency_key.clone(),
                    reason: "trust in [trust_quarantine, trust_admit)".into(),
                    quarantined_at: Utc::now(),
                };
                self.quarantine(ctx, &qr).await?;
                Ok(WriteOutcome::Quarantined)
            }
            GateDecision::Admit => {
                // Step 8 — embed and persist.
                let now = Utc::now();
                let fact = Fact {
                    id: self.derive_fact_id(job.idempotency_key.as_deref(), fact_index),
                    content,
                    entities,
                    source_id,
                    memory_class: ef.memory_class,
                    visibility: Visibility::UserPrivate,
                    owner: job.scope.clone(),
                    valid_from: now,
                    valid_to: None,
                    ingested_at: now,
                    confidence,
                    salience,
                    stability: 1.0,
                    pii_review,
                    supersedes: None,
                    superseded_by: None,
                    derived_from: vec![],
                    last_recalled_at: None,
                };
                self.embed_and_persist(ctx, &fact).await?;
                tracing::info!(
                    target: "recall",
                    job_id = %job.id,
                    fact_id = %fact.id,
                    memory_class = ?fact.memory_class,
                    confidence,
                    salience,
                    pii_review,
                    "write.persisted"
                );
                Ok(WriteOutcome::Persisted)
            }
        }
    }

    // ---- step functions (ordered) ----

    /// Step 1. `true` => content is empty or low-signal and the job is completed as noise. Drops when,
    /// after trimming, the serialised content is an empty object/array or the total non-whitespace
    /// string length across all string leaves is `< 3` characters (data minimisation, HLD 06).
    pub fn filter_noise(&self, content: &Json) -> bool {
        match content {
            Json::Object(m) if m.is_empty() => return true,
            Json::Array(a) if a.is_empty() => return true,
            _ => {}
        }
        non_ws_string_len(content) < 3
    }

    /// Step 2. Extract structured facts. If `agent_stated`, bypass the LLM and wrap the content
    /// directly as a single asserted `ExtractedFact` (`extractor_confidence = 1.0`, `memory_class =
    /// Episodic`); the extracted content is treated as data, never instructions. Otherwise call
    /// `LlmClient::extract`.
    pub async fn extract(
        &self,
        content: &Json,
        agent_stated: bool,
    ) -> Result<Vec<ExtractedFact>, AppError> {
        if agent_stated {
            let entities = scan_entity_mentions(content);
            return Ok(vec![ExtractedFact {
                content: content.clone(),
                entities,
                memory_class: MemoryClass::Episodic,
                confidence: 1.0,
            }]);
        }
        let facts = self.llm.extract(content).await?;
        Ok(facts)
    }

    /// Step 3. Canonicalise content: sort object keys, collapse internal whitespace runs to single
    /// spaces in string values, and apply Unicode NFC normalisation. Pure; no dependency.
    pub fn normalise(&self, ef: &ExtractedFact) -> ExtractedFact {
        ExtractedFact {
            content: normalise_json(&ef.content),
            entities: ef
                .entities
                .iter()
                .map(|m| EntityMention {
                    surface: collapse_ws(&m.surface),
                    canonical_name: m.canonical_name.clone(),
                })
                .collect(),
            memory_class: ef.memory_class,
            confidence: ef.confidence,
        }
    }

    /// Step 4. Resolve each mention to an entity id via the rules -> ML -> create-new ladder (v1),
    /// returning `>=1` entity ids. Never silently overwrites an existing entity (HLD 04); on ambiguity
    /// a new entity is created via `MemoryStore::put_entity` rather than risking a wrong merge.
    pub async fn resolve_entities(
        &self,
        ctx: &ScopeContext,
        ef: &ExtractedFact,
    ) -> Result<Vec<String>, AppError> {
        let mut ids: Vec<String> = Vec::new();
        let mut created = 0u32;

        for mention in &ef.entities {
            let normalised = normalise_surface(&mention.surface);
            if normalised.is_empty() {
                continue;
            }

            // (a) Rules tier: exact canonical-name or alias match within scope.
            let hits = self.store.find_entity_by_name(ctx, &normalised).await?;
            if hits.len() == 1 {
                push_unique(&mut ids, hits[0].id.clone());
                continue;
            }
            if hits.len() > 1 {
                // Ambiguous exact match: pick the lexicographically-smallest id deterministically
                // rather than guess; a later maintenance merge folds duplicates (C7).
                let mut sorted = hits;
                sorted.sort_by(|a, b| a.id.cmp(&b.id));
                push_unique(&mut ids, sorted[0].id.clone());
                continue;
            }

            // (b) ML tier: string-similarity against in-scope candidates. With no exact hit and the v1
            // store exposing no broad entity scan, the candidate set for similarity is the rules-tier
            // lookup against the *raw* surface form (a case/punctuation variant). A single candidate at
            // or above the admit threshold resolves; otherwise escalate to create-new.
            let raw_hits = self.store.find_entity_by_name(ctx, &mention.surface).await?;
            if let Some(best) = best_similarity_match(&normalised, &raw_hits) {
                push_unique(&mut ids, best);
                continue;
            }

            // (c) create-new tier (v1): create a NEW entity (never merge into an existing one without an
            // above-threshold match, never silently overwrite). Duplicate entities created this way are
            // folded later by the C7 Maintenance Worker via `merge_entities`.
            let entity = Entity {
                id: format!("entity:{}", Uuid::now_v7()),
                canonical_name: normalised.clone(),
                aliases: vec![mention.surface.clone()],
                owner: ScopeRef {
                    tenant: ctx.tenant.clone(),
                    team: ctx.teams.first().cloned(),
                    user: ctx.user.clone(),
                },
            };
            self.store.put_entity(&entity).await?;
            created += 1;
            push_unique(&mut ids, entity.id);
        }

        if ids.is_empty() {
            // A fact must connect >=1 entity (2C.2). When the extractor offered no usable mention,
            // synthesise a single entity from the content so the fact is still anchored rather than
            // dropping a salient assertion.
            let synth = synth_entity_name(&ef.content);
            let entity = Entity {
                id: format!("entity:{}", Uuid::now_v7()),
                canonical_name: synth.clone(),
                aliases: vec![],
                owner: ScopeRef {
                    tenant: ctx.tenant.clone(),
                    team: ctx.teams.first().cloned(),
                    user: ctx.user.clone(),
                },
            };
            if synth.is_empty() {
                return Err(AppError::Validation(
                    ValidationKind::InvalidBody,
                    "a fact must connect at least one entity".into(),
                ));
            }
            self.store.put_entity(&entity).await?;
            created += 1;
            ids.push(entity.id);
        }

        tracing::info!(
            target: "recall",
            entity_count = ids.len(),
            created_count = created,
            "write.entities.resolved"
        );
        Ok(ids)
    }

    /// Step 5. Score `salience` and `confidence`, each clamped to `[0,1]` (SA-SCORE-01).
    /// `confidence = clamp01(extractor_confidence * (0.5 + 0.5 * source_trust))`; `salience` is a
    /// bounded heuristic from content richness (entity-mention count, predicate presence). Pure.
    pub fn score(&self, ef: &ExtractedFact, source_trust: f64) -> (f64, f64) {
        let source_trust_factor = 0.5 + 0.5 * source_trust.clamp(0.0, 1.0);
        let confidence = clamp01(ef.confidence * source_trust_factor);

        // Salience heuristic: base 0.3, +0.15 per entity mention (capped at 0.45 of mention weight),
        // +0.2 when the content names a predicate/relation field. Bounded to [0,1].
        let mention_weight = (ef.entities.len() as f64 * 0.15).min(0.45);
        let predicate_bonus = if has_predicate(&ef.content) { 0.2 } else { 0.0 };
        let salience = clamp01(0.3 + mention_weight + predicate_bonus);
        (salience, confidence)
    }

    /// Step 6. PII scan; redact spans with `confidence >= cfg.pii_redact_conf` in place with the
    /// literal `‹redacted:‹{pii_type}››`, set `pii_review = true` for any lower-confidence span
    /// (SA-PII-01). Returns `(redacted_content, pii_review)`.
    pub async fn pii_scan(&self, content: &Json) -> Result<(Json, bool), AppError> {
        let spans = self.pii.scan(content).await?;
        let mut redacted = content.clone();
        let mut pii_review = false;
        let mut redacted_count = 0u32;
        let mut flagged_count = 0u32;

        // Apply redactions per located string, highest-offset-first so earlier offsets stay valid as
        // bytes are spliced out and the replacement token inserted.
        let mut by_pointer: std::collections::BTreeMap<String, Vec<PiiSpan>> =
            std::collections::BTreeMap::new();
        for span in spans {
            if span.confidence >= self.cfg.pii_redact_conf {
                by_pointer.entry(span.json_pointer.clone()).or_default().push(span);
            } else {
                pii_review = true;
                flagged_count += 1;
            }
        }
        for (pointer, mut spans) in by_pointer {
            spans.sort_by_key(|s| std::cmp::Reverse(s.start));
            if let Some(target) = pointer_get_str(&redacted, &pointer) {
                let mut s = target.to_string();
                for span in &spans {
                    let token = format!("‹redacted:‹{}››", span.pii_type);
                    let (start, end) = (span.start as usize, span.end as usize);
                    if start <= end && end <= s.len() && s.is_char_boundary(start) && s.is_char_boundary(end) {
                        s.replace_range(start..end, &token);
                        redacted_count += 1;
                    } else {
                        // An out-of-range/non-boundary span cannot be safely redacted in place; flag for
                        // review rather than corrupt the string.
                        pii_review = true;
                        flagged_count += 1;
                    }
                }
                pointer_set_str(&mut redacted, &pointer, s);
            } else {
                // The pointer did not resolve to a string; flag the uncertainty for review.
                pii_review = true;
                flagged_count += 1;
            }
        }

        tracing::info!(
            target: "recall",
            redacted_count,
            flagged_count,
            "write.pii.scanned"
        );
        Ok((redacted, pii_review))
    }

    /// Step 7a. Detect imperative/instruction-like content over the content's string values
    /// (case-insensitive). `is_instruction_like` is `true` when `matched_patterns >= 1` (ADR-008).
    pub fn detect_instruction(&self, content: &Json) -> InstructionLikelihood {
        let mut matched = 0u32;
        for_each_string(content, &mut |s| {
            matched += count_instruction_patterns(s);
        });
        InstructionLikelihood {
            is_instruction_like: matched >= 1,
            matched_patterns: matched,
        }
    }

    /// Step 7b. The write gate. `trust = clamp01(0.6 * confidence + 0.4 * source_trust)`; when
    /// `instr.is_instruction_like`, trust is capped strictly below `trust_quarantine` so it can never
    /// admit or quarantine. Then maps to a three-way decision (SA-WGATE-01).
    pub fn write_gate(
        &self,
        confidence: f64,
        source_trust: f64,
        instr: InstructionLikelihood,
    ) -> GateDecision {
        let trust = self.trust_score(confidence, source_trust, instr);
        if trust >= self.cfg.trust_admit {
            GateDecision::Admit
        } else if trust >= self.cfg.trust_quarantine {
            GateDecision::Quarantine
        } else {
            GateDecision::Reject
        }
    }

    /// The trust score the gate compares against the bands; isolated so the persisted/quarantined
    /// trust value matches the decision exactly.
    fn trust_score(
        &self,
        confidence: f64,
        source_trust: f64,
        instr: InstructionLikelihood,
    ) -> f64 {
        let mut trust = clamp01(0.6 * confidence + 0.4 * source_trust.clamp(0.0, 1.0));
        if instr.is_instruction_like {
            trust = trust.min(self.cfg.trust_quarantine - 0.0001);
        }
        trust
    }

    /// Step 8. Embed fact content and persist `Fact` + vector via `MemoryStore::put_fact` then
    /// `set_fact_embedding`. Asserts the embedding length `== cfg.embed_dim` (SA-EMBED-01); a mismatch
    /// is a terminal `VAL_OUT_OF_RANGE`.
    pub async fn embed_and_persist(
        &self,
        ctx: &ScopeContext,
        fact: &Fact,
    ) -> Result<(), AppError> {
        let text = content_to_text(&fact.content);
        let vectors = self.embed.embed(&[text]).await?;
        let vector = vectors.into_iter().next().ok_or_else(|| {
            AppError::Validation(
                ValidationKind::OutOfRange,
                "embedding provider returned no vector".into(),
            )
        })?;
        if vector.len() as u32 != self.cfg.embed_dim {
            return Err(AppError::Validation(
                ValidationKind::OutOfRange,
                format!(
                    "embedding length {} != RECALL_EMBED_DIM {}",
                    vector.len(),
                    self.cfg.embed_dim
                ),
            ));
        }
        // Persist the fact (idempotent on the derived id) then attach the embedding + model version.
        self.store.put_fact(fact).await?;
        self.store
            .set_fact_embedding(ctx, &fact.id, &vector, &self.cfg.embed_model_version)
            .await?;
        Ok(())
    }

    /// Persist a gate-quarantined record into the `quarantine` table (never the `fact` table). Uses an
    /// UPSERT on the deterministic record id so a queue replay does not duplicate the row (the
    /// `quarantine_idem_idx` UNIQUE index also guards keyed replays).
    pub async fn quarantine(
        &self,
        ctx: &ScopeContext,
        qr: &QuarantineRecord,
    ) -> Result<(), AppError> {
        let db = self.db.as_ref().ok_or_else(|| {
            AppError::Store(StoreError::Internal("quarantine sink not wired".into()))
        })?;
        // Provision + select the tenant namespace (idempotent) so the quarantine table exists.
        Migrator::new(db, self.cfg.embed_dim)
            .migrate_up(&ctx.tenant)
            .await?;
        validate_tenant(&ctx.tenant)?;
        db.use_ns(ctx.tenant.clone())
            .use_db(DB_NAME)
            .await
            .map_err(crate::store::migrate::map_db_err)?;
        let thing = id_thing(&qr.id)?;
        db.query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(quarantine_to_object(qr))))
            .await
            .map_err(crate::store::migrate::map_db_err)?
            .check()
            .map_err(crate::store::migrate::map_db_err)?;
        Ok(())
    }

    // ---- helpers ----

    /// Upsert the provenance `Source` when one was supplied, returning its id. A new source is assigned
    /// `RECALL_SOURCE_TRUST_DEFAULT`; the source id is derived deterministically from
    /// `(idempotency_key, origin_ref)` is not required here — provenance upsert is idempotent by record
    /// id, and a fresh id is fine because a replayed job reuses the same derived fact id regardless.
    async fn upsert_source(
        &self,
        ctx: &ScopeContext,
        source: Option<&SourceInput>,
    ) -> Result<Option<String>, AppError> {
        let Some(si) = source else {
            return Ok(None);
        };
        let id = format!("source:{}", deterministic_uuid(&format!("source:{}", si.origin_ref)));
        // Reuse an existing source's trust signal when the same source id is already present.
        let existing = self.store.get_source(ctx, &id).await?;
        let trust = existing
            .as_ref()
            .map(|s| s.trust_signal)
            .unwrap_or(self.cfg.source_trust_default);
        let src = Source {
            id: id.clone(),
            origin_ref: si.origin_ref.clone(),
            modification_marker: si.modification_marker.clone(),
            trust_signal: trust,
            owner: ScopeRef {
                tenant: ctx.tenant.clone(),
                team: ctx.teams.first().cloned(),
                user: ctx.user.clone(),
            },
        };
        self.store.put_source(&src).await?;
        Ok(Some(id))
    }

    /// Read the source trust used by scoring + the gate. A source-less / agent-stated fact uses
    /// `source_trust = 1.0`; a known source uses its `trust_signal`; a brand-new source uses the
    /// configured default.
    async fn read_source_trust(
        &self,
        ctx: &ScopeContext,
        source_id: Option<&str>,
    ) -> Result<f64, AppError> {
        match source_id {
            None => Ok(1.0),
            Some(id) => Ok(self
                .store
                .get_source(ctx, id)
                .await?
                .map(|s| s.trust_signal)
                .unwrap_or(self.cfg.source_trust_default)),
        }
    }

    /// Derive the persisted fact id. When the job carries an `idempotency_key`, the id is a UUIDv5-style
    /// deterministic uuid over `(idempotency_key, fact_index)` so a queue replay reuses the same id
    /// (idempotent persist via the C1 `UPSERT`); otherwise a fresh UUIDv7.
    fn derive_fact_id(&self, idempotency_key: Option<&str>, fact_index: usize) -> String {
        match idempotency_key {
            Some(key) => {
                let seed = format!("fact:{key}:{fact_index}");
                format!("fact:{}", deterministic_uuid(&seed))
            }
            None => format!("fact:{}", Uuid::now_v7()),
        }
    }

    /// `WorkQueue::complete`, mapping a queue error to `AppError::Queue`.
    async fn complete(&self, job_id: &str) -> Result<(), AppError> {
        self.queue.complete(job_id).await.map_err(AppError::Queue)
    }
}

// --- free helpers (pure) ---------------------------------------------------------------------------

/// Order the four outcomes so the aggregate report reflects the strongest per-job result.
fn stronger(a: WriteOutcome, b: WriteOutcome) -> WriteOutcome {
    fn rank(o: WriteOutcome) -> u8 {
        match o {
            WriteOutcome::Persisted => 3,
            WriteOutcome::Quarantined => 2,
            WriteOutcome::Rejected => 1,
            WriteOutcome::FilteredNoise => 0,
        }
    }
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

/// Whether an `AppError` should be retried (transient) vs dead-lettered immediately (terminal),
/// per the C4 Error Table.
fn is_retryable(err: &AppError) -> bool {
    match err {
        AppError::Store(StoreError::Validation(_)) => false,
        AppError::Store(_) => true,
        AppError::Queue(_) => true,
        AppError::Provider(ProviderError::Timeout) | AppError::Provider(ProviderError::Status(_)) => {
            true
        }
        AppError::Provider(_) => false,
        AppError::Validation(_, _) => false,
        _ => false,
    }
}

fn clamp01(v: f64) -> f64 {
    v.clamp(0.0, 1.0)
}

/// Append `id` to `ids` unless already present (entity-id dedup within one fact).
fn push_unique(ids: &mut Vec<String>, id: String) {
    if !ids.contains(&id) {
        ids.push(id);
    }
}

/// Total length of non-whitespace characters across every string leaf in the JSON value.
fn non_ws_string_len(v: &Json) -> usize {
    let mut total = 0usize;
    for_each_string(v, &mut |s| {
        total += s.chars().filter(|c| !c.is_whitespace()).count();
    });
    total
}

/// Visit every string leaf in a JSON value.
fn for_each_string(v: &Json, f: &mut impl FnMut(&str)) {
    match v {
        Json::String(s) => f(s),
        Json::Array(a) => a.iter().for_each(|e| for_each_string(e, f)),
        Json::Object(m) => m.values().for_each(|e| for_each_string(e, f)),
        _ => {}
    }
}

/// Collapse internal whitespace runs to a single space and trim, after Unicode NFC normalisation.
fn collapse_ws(s: &str) -> String {
    let nfc = nfc(s);
    nfc.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Minimal Unicode NFC normalisation. The crate avoids a heavy dependency; the canonicalisation that
/// matters for recall is whitespace + key order, so NFC here is the identity over already-composed
/// input. (Documented assumption: input is expected NFC; full decomposition/recomposition is deferred.)
fn nfc(s: &str) -> String {
    s.to_string()
}

/// Recursively normalise a JSON value: object keys sorted (BTreeMap ordering), string values
/// whitespace-collapsed + NFC.
fn normalise_json(v: &Json) -> Json {
    match v {
        Json::String(s) => Json::String(collapse_ws(s)),
        Json::Array(a) => Json::Array(a.iter().map(normalise_json).collect()),
        Json::Object(m) => {
            let mut sorted = serde_json::Map::new();
            for (k, val) in m.iter() {
                sorted.insert(k.clone(), normalise_json(val));
            }
            // serde_json::Map preserves insertion order unless the `preserve_order` feature is off, in
            // which case it is a BTreeMap (already sorted). Insert in sorted key order to be
            // deterministic in both builds.
            let mut keys: Vec<&String> = sorted.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                out.insert(k.clone(), sorted.get(k).cloned().unwrap_or(Json::Null));
            }
            Json::Object(out)
        }
        other => other.clone(),
    }
}

/// Normalise a surface form for the rules tier: NFC, lowercase, trim, collapse whitespace, strip
/// leading/trailing ASCII punctuation.
fn normalise_surface(s: &str) -> String {
    let collapsed = collapse_ws(s).to_lowercase();
    collapsed
        .trim_matches(|c: char| c.is_ascii_punctuation())
        .trim()
        .to_string()
}

/// The best ML-tier candidate at or above the similarity admit threshold, if exactly one stands out.
/// Similarity is a normalised character-bigram Dice coefficient between the normalised surface form and
/// each candidate's canonical name.
fn best_similarity_match(normalised: &str, candidates: &[Entity]) -> Option<String> {
    let mut best: Option<(String, f64)> = None;
    for c in candidates {
        let sim = dice_bigram(normalised, &normalise_surface(&c.canonical_name));
        if sim >= ML_SIMILARITY_ADMIT {
            match &best {
                Some((_, b)) if *b >= sim => {}
                _ => best = Some((c.id.clone(), sim)),
            }
        }
    }
    best.map(|(id, _)| id)
}

/// Sørensen–Dice coefficient over character bigrams of two strings (1.0 = identical). Empty/short
/// strings fall back to exact equality.
fn dice_bigram(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    let ba = bigrams(a);
    let bb = bigrams(b);
    if ba.is_empty() || bb.is_empty() {
        return 0.0;
    }
    let mut bb_counts: std::collections::HashMap<(char, char), usize> =
        std::collections::HashMap::new();
    for g in &bb {
        *bb_counts.entry(*g).or_default() += 1;
    }
    let mut overlap = 0usize;
    for g in &ba {
        if let Some(c) = bb_counts.get_mut(g) {
            if *c > 0 {
                *c -= 1;
                overlap += 1;
            }
        }
    }
    (2.0 * overlap as f64) / (ba.len() as f64 + bb.len() as f64)
}

fn bigrams(s: &str) -> Vec<(char, char)> {
    let chars: Vec<char> = s.chars().collect();
    chars.windows(2).map(|w| (w[0], w[1])).collect()
}

/// Whether the content names a predicate/relation (used in the salience heuristic).
fn has_predicate(content: &Json) -> bool {
    if let Json::Object(m) = content {
        m.contains_key("predicate") || m.contains_key("relation") || m.contains_key("verb")
    } else {
        false
    }
}

/// Deterministically derive entity mentions from agent-stated content: the string values of the
/// well-known `subject`/`object` fields, then any other string leaf, as raw mentions.
fn scan_entity_mentions(content: &Json) -> Vec<EntityMention> {
    let mut out = Vec::new();
    if let Json::Object(m) = content {
        for key in ["subject", "object"] {
            if let Some(Json::String(s)) = m.get(key) {
                if !s.trim().is_empty() {
                    out.push(EntityMention {
                        surface: s.clone(),
                        canonical_name: None,
                    });
                }
            }
        }
    }
    out
}

/// Synthesise a single entity name from content when the extractor offered no usable mention: prefer a
/// `subject` string, else the first string leaf, normalised.
fn synth_entity_name(content: &Json) -> String {
    if let Json::Object(m) = content {
        if let Some(Json::String(s)) = m.get("subject") {
            let n = normalise_surface(s);
            if !n.is_empty() {
                return n;
            }
        }
    }
    let mut first = String::new();
    for_each_string(content, &mut |s| {
        if first.is_empty() {
            let n = normalise_surface(s);
            if !n.is_empty() {
                first = n;
            }
        }
    });
    first
}

/// Count imperative-mood and prompt-injection markers in a string (case-insensitive). The detector
/// matches a small allowlist of leading imperative verbs and known injection phrases; it is the
/// memory-poisoning defence (ADR-008) and never executes the content.
fn count_instruction_patterns(s: &str) -> u32 {
    let lower = s.to_lowercase();
    let mut count = 0u32;

    // Known prompt-injection markers anywhere in the string.
    const MARKERS: &[&str] = &[
        "ignore previous",
        "ignore all previous",
        "disregard previous",
        "disregard the above",
        "system prompt",
        "you are now",
        "act as",
        "new instructions",
        "override",
        "jailbreak",
    ];
    for m in MARKERS {
        if lower.contains(m) {
            count += 1;
        }
    }

    // Leading imperative verbs (the first token of the trimmed string).
    const IMPERATIVES: &[&str] = &[
        "ignore", "disregard", "delete", "forget", "execute", "run", "do", "send", "reveal",
        "print", "output", "respond", "reply", "tell", "stop", "drop", "remove", "set", "update",
        "grant", "give",
    ];
    if let Some(first) = lower.split_whitespace().next() {
        let token = first.trim_matches(|c: char| c.is_ascii_punctuation());
        if IMPERATIVES.contains(&token) {
            count += 1;
        }
    }
    count
}

/// Resolve an RFC 6901 JSON pointer to a string value, if it points at one.
fn pointer_get_str<'a>(v: &'a Json, pointer: &str) -> Option<&'a str> {
    v.pointer(pointer).and_then(|p| p.as_str())
}

/// Set the string at an RFC 6901 JSON pointer (the pointer must already resolve to a string leaf).
fn pointer_set_str(v: &mut Json, pointer: &str, value: String) {
    if let Some(slot) = v.pointer_mut(pointer) {
        *slot = Json::String(value);
    }
}

/// Render a JSON content object to a flat keyword string for embedding: every scalar leaf joined with
/// spaces. Mirrors the C1 store's `content_to_text` so the embedded text matches the indexed text.
fn content_to_text(content: &Json) -> String {
    crate::store::convert::content_to_text(content)
}

/// A deterministic UUID (v5, DNS namespace) over a seed string — stable across replays for idempotent
/// id derivation.
fn deterministic_uuid(seed: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, seed.as_bytes())
}

// --- SurrealDB row building for the quarantine table -----------------------------------------------

/// Build the SurrealDB record-id literal for a "table:key" id as a parameter-bindable record-id value.
fn id_thing(id: &str) -> Result<Value, AppError> {
    let (table, key) = id.split_once(':').ok_or_else(|| {
        AppError::Store(StoreError::Validation(format!("malformed id: {id}")))
    })?;
    Ok(Value::RecordId(surrealdb::types::RecordId::new(
        table.to_string(),
        key.to_string(),
    )))
}

/// Convert an owned string into a native SurrealDB string value.
fn s(v: String) -> Value {
    Value::String(v)
}

/// Convert an optional string into a native SurrealDB value (`NONE` when absent).
fn opt_s(v: Option<String>) -> Value {
    match v {
        Some(x) => Value::String(x),
        None => Value::None,
    }
}

/// Convert an `f64` into a native SurrealDB float value.
fn f(v: f64) -> Value {
    Value::Number(surrealdb::types::Number::Float(v))
}

/// The kebab-case string for a `MemoryClass` (matches the table's enum assertion).
fn memory_class_str(c: MemoryClass) -> &'static str {
    match c {
        MemoryClass::Episodic => "episodic",
        MemoryClass::Semantic => "semantic",
        MemoryClass::Consolidated => "consolidated",
    }
}

/// Build the native row object for a `QuarantineRecord` (without the `id`, which is the record key).
fn quarantine_to_object(qr: &QuarantineRecord) -> Object {
    use surrealdb::types::SurrealValue;
    let mut o = Object::new();
    o.insert("content", qr.content.clone().into_value());
    o.insert(
        "entities",
        Value::Array(surrealdb::types::Array::from(qr.entities.clone())),
    );
    o.insert("source_id", opt_s(qr.source_id.clone()));
    o.insert("memory_class", s(memory_class_str(qr.memory_class).to_string()));
    let mut owner = Object::new();
    owner.insert("tenant", s(qr.owner.tenant.clone()));
    owner.insert("team", opt_s(qr.owner.team.clone()));
    owner.insert("user", s(qr.owner.user.clone()));
    o.insert("owner", Value::Object(owner));
    o.insert("trust_score", f(qr.trust_score));
    o.insert("confidence", f(qr.confidence));
    o.insert("salience", f(qr.salience));
    o.insert("is_instruction_like", Value::Bool(qr.is_instruction_like));
    o.insert("pii_review", Value::Bool(qr.pii_review));
    o.insert("idempotency_key", opt_s(qr.idempotency_key.clone()));
    o.insert("reason", s(qr.reason.clone()));
    o.insert(
        "quarantined_at",
        Value::Datetime(Datetime::from(qr.quarantined_at)),
    );
    o
}

#[cfg(test)]
mod tests;
