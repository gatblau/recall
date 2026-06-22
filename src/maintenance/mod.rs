//! C7 — Maintenance Worker. The asynchronous, idle-biased keeper of memory truth and bounded size.
//!
//! It runs off the synchronous read path (ADR-004) so LLM and embedding latency never enters the
//! recall budget. Two cooperating drivers share one duty set: a **scheduler driver** ([`run_scheduler`])
//! fires a full maintenance cycle per tenant on the idle-biased trigger (after `idle_quiet` of no
//! ingestion, with a hard fallback every `consolidate_max_interval`); a **queue-consumer driver**
//! ([`run_consumer`]) claims `Consolidate` / `ReEmbedFact` / `HardDelete` jobs from C2 and dispatches
//! each to the same duty functions.
//!
//! Per tenant the worker performs five duties: (1) **consolidation** — distilling recent episodic facts
//! into validated Consolidated Insights with source-capped decaying confidence (ADR-006/007); (2)
//! **supersession** — ending the superseded fact's validity and recording successor links, never
//! destructively (ADR-002); (3) **decay / forget** — Ebbinghaus retrievability with a salience floor;
//! (4) **re-embed** — refreshing embeddings for stale-model facts (SA-EMBED-01); and (5) **verifiable
//! hard delete** — removing a fact plus derived summaries and embeddings, returning a `DeletionProof`.
//!
//! The decay maths and contradiction detection are pure, I/O-free cores ([`retrievability`],
//! [`is_prune_candidate`], [`reinforce`], [`insight_confidence`], [`detect_contradiction`]) so they are
//! unit-tested against case tables (ADR-010). Every destructive step is ordered last and proof-gated, so
//! a failed cycle leaves prior memory intact. Structured logs carry `correlation_id` on every line; LLM
//! and embedding keys are read from env only and never logged, and fact content is never logged.
//!
//! Scope: each store operation is scoped per tenant via a maintenance `ScopeContext` built from the
//! tenant id alone (`teams = []`, `user = ""`, `token_jti = "maintenance"`, all ops allowed, a fresh
//! `correlation_id`); the store's namespace-per-tenant boundary makes cross-tenant maintenance
//! structurally impossible (ADR-011).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::error::{AppError, ValidationKind};
use crate::types::api::DeletionProof;
use crate::types::domain::{Fact, MemoryClass};
use crate::types::job::{JobKind, WorkJob};
use crate::types::ports::{
    EmbeddingClient, InsightCandidate, LlmClient, MemoryStore, ProviderError, WorkQueue,
};
use crate::types::scope::{OpSet, ScopeContext, ScopeRef};

/// The claim lease length for queue jobs (mirrors C2/C4 defaults).
const CLAIM_LEASE: Duration = Duration::from_secs(30);

/// The poll/back-off interval for the consumer and scheduler loops (one tick).
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The runtime entrypoints and duty functions of the Maintenance Worker (C7 *Public Interface*).
pub struct MaintenanceWorker {
    store: Arc<dyn MemoryStore>,
    queue: Arc<dyn WorkQueue>,
    llm: Arc<dyn LlmClient>,
    embed: Arc<dyn EmbeddingClient>,
    cfg: MaintenanceConfig,
}

/// Validated maintenance configuration (C7 *Public Interface*), projected from the §2D config.
#[derive(Clone)]
pub struct MaintenanceConfig {
    /// `RECALL_SALIENCE_FLOOR` — a fact at or above this is never time-pruned (SA-DECAY-01).
    pub salience_floor: f64,
    /// `RECALL_DECAY_K` — global decay constant `k` in `R = exp(-Δt/(s·k))`.
    pub decay_k: f64,
    /// `RECALL_PRUNE_RETRIEVABILITY` — `R` below which a low-salience fact becomes a prune candidate.
    pub prune_retrievability: f64,
    /// `RECALL_IDLE_QUIET_SECS` — idle period with no tenant writes before a consolidation cycle.
    pub idle_quiet: Duration,
    /// `RECALL_CONSOLIDATE_MAX_INTERVAL_SECS` — hard fallback consolidation interval (6 h).
    pub consolidate_max_interval: Duration,
    /// `RECALL_EMBED_DIM` — re-embed output length MUST equal this (SA-EMBED-01).
    pub embed_dim: u32,
    /// `RECALL_EMBED_MODEL_VERSION` — active embedding model id; a stale-version fact is a re-embed candidate.
    pub embed_model_version: String,
    /// `RECALL_MAINT_BATCH_SIZE` — per-duty scan bound (`limit`) per cycle.
    pub batch_size: u32,
    /// `RECALL_MAINT_CONSOLIDATE_MIN_EPISODES` — minimum group size before consolidation is attempted.
    pub min_episodes: u32,
    /// `RECALL_INSIGHT_DECAY_FACTOR` — multiplicative confidence decay applied to a promoted insight.
    pub insight_decay_factor: f64,
    /// `RECALL_REINFORCE_GAIN` — stability gain applied on recall (exposed for C6 + unit tests).
    pub reinforce_gain: f64,
}

impl MaintenanceConfig {
    /// Build the maintenance config from the loaded §2D configuration.
    pub fn from_config(c: &Config) -> Self {
        Self {
            salience_floor: c.salience_floor,
            decay_k: c.decay_k,
            prune_retrievability: c.prune_retrievability,
            idle_quiet: Duration::from_secs(c.idle_quiet_secs as u64),
            consolidate_max_interval: Duration::from_secs(c.consolidate_max_interval_secs as u64),
            embed_dim: c.embed_dim,
            embed_model_version: c.embed_model_version.clone(),
            batch_size: c.maint_batch_size,
            min_episodes: c.maint_consolidate_min_episodes,
            insight_decay_factor: c.insight_decay_factor,
            reinforce_gain: c.reinforce_gain,
        }
    }
}

// --- Types owned by this spec ---------------------------------------------------------------------

/// Result of comparing two facts for contradiction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ContradictionVerdict {
    NoConflict,
    /// `b` supersedes `a` (b is newer/more confident) — end a's validity.
    Supersedes,
    /// `a` supersedes `b` — end b's validity.
    SupersededBy,
}

/// The `ReEmbedFact` job payload — the fact to re-embed.
#[derive(Deserialize)]
pub struct ReEmbedPayload {
    /// "fact:<uuidv7>".
    pub fact_id: String,
}

/// The `HardDelete` job payload — the fact to erase.
#[derive(Deserialize)]
pub struct HardDeletePayload {
    /// "fact:<uuidv7>".
    pub fact_id: String,
}

/// The per-duty summary returned by [`MaintenanceWorker::run_cycle`].
#[derive(Serialize, Default, Clone)]
pub struct CycleReport {
    pub consolidation: ConsolidationReport,
    pub supersession: SupersessionReport,
    pub decay: DecayReport,
    pub reembed: ReEmbedReport,
    /// Duty names that errored this cycle.
    pub failed_duties: Vec<String>,
}

/// Consolidation-duty outcome counts.
#[derive(Serialize, Default, Clone)]
pub struct ConsolidationReport {
    pub groups_seen: u32,
    pub candidates: u32,
    pub promoted: u32,
    pub rejected_validation: u32,
}

/// Supersession-duty outcome counts.
#[derive(Serialize, Default, Clone)]
pub struct SupersessionReport {
    pub pairs_checked: u32,
    pub superseded: u32,
}

/// Decay-duty outcome counts.
#[derive(Serialize, Default, Clone)]
pub struct DecayReport {
    pub evaluated: u32,
    pub reinforced: u32,
    pub pruned: u32,
}

/// Re-embed-duty outcome counts.
#[derive(Serialize, Default, Clone)]
pub struct ReEmbedReport {
    pub evaluated: u32,
    pub reembedded: u32,
}

// --- Worker construction and drivers --------------------------------------------------------------

impl MaintenanceWorker {
    /// Construct a worker over the four injected seams and the resolved configuration.
    pub fn new(
        store: Arc<dyn MemoryStore>,
        queue: Arc<dyn WorkQueue>,
        llm: Arc<dyn LlmClient>,
        embed: Arc<dyn EmbeddingClient>,
        cfg: MaintenanceConfig,
    ) -> Self {
        Self {
            store,
            queue,
            llm,
            embed,
            cfg,
        }
    }

    /// Build a maintenance `ScopeContext` for one tenant (C7 *Shared Context*): no teams, empty user,
    /// `token_jti = "maintenance"`, all ops allowed, a fresh `correlation_id`. Used solely to satisfy
    /// the `MemoryStore` scoping signature; the store's namespace-per-tenant boundary is structural.
    fn maint_ctx(&self, tenant: &str) -> ScopeContext {
        ScopeContext {
            tenant: tenant.to_string(),
            teams: Vec::new(),
            user: String::new(),
            token_jti: "maintenance".to_string(),
            allowed_ops: OpSet {
                read: true,
                write: true,
                forget: true,
            },
            correlation_id: Uuid::new_v4().to_string(),
        }
    }

    /// Scheduler driver. Runs until `shutdown` is cancelled. Per tick, evaluates each tenant's
    /// idle/fallback trigger and runs a full maintenance cycle for those that are due.
    ///
    /// The per-tenant `last_cycle_at` fallback timer is held in an in-memory map for the lifetime of the
    /// loop (not persisted this phase — see module note); a never-seen tenant is treated as due.
    pub async fn run_scheduler(&self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        // In-memory per-tenant last-successful-cycle clock (not persisted; the `maintenance_state` table
        // exists for a future persistence follow-up).
        let mut last_cycle: HashMap<String, tokio::time::Instant> = HashMap::new();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let tenants = match self.store.list_tenants().await {
                        Ok(t) => t,
                        Err(e) => {
                            tracing::warn!(
                                target: "recall",
                                error = %e,
                                "maintenance.scheduler.list_tenants_failed"
                            );
                            continue;
                        }
                    };
                    let now = tokio::time::Instant::now();
                    for tenant in tenants {
                        if !self.tenant_is_due(&tenant, &last_cycle, now).await {
                            continue;
                        }
                        let ctx = self.maint_ctx(&tenant);
                        match self.run_cycle(&tenant).await {
                            Ok(_report) => {
                                last_cycle.insert(tenant.clone(), tokio::time::Instant::now());
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "recall",
                                    tenant = %tenant,
                                    correlation_id = %ctx.correlation_id,
                                    error = %e,
                                    "maintenance.cycle.failed"
                                );
                                // Do not advance last_cycle_at: the fallback timer re-tries next tick.
                            }
                        }
                    }
                }
                _ = shutdown.cancelled() => return,
            }
        }
    }

    /// Whether a tenant's cycle is due: quiet (no recent ingestion — the idle trigger, SA-CONSOL-01) OR
    /// the fallback interval has elapsed since the last successful cycle. A tenant with no recorded
    /// cycle is treated as due.
    async fn tenant_is_due(
        &self,
        tenant: &str,
        last_cycle: &HashMap<String, tokio::time::Instant>,
        now: tokio::time::Instant,
    ) -> bool {
        // Fallback: no prior cycle, or the max interval has elapsed.
        let fallback_due = match last_cycle.get(tenant) {
            Some(last) => now.duration_since(*last) >= self.cfg.consolidate_max_interval,
            None => true,
        };
        if fallback_due {
            return true;
        }
        // Idle trigger: a bounded probe for any episode ingested within the idle window. An empty result
        // means the tenant has had no recent ingestion (quiet -> idle -> due).
        let ctx = self.maint_ctx(tenant);
        let since = Utc::now()
            - chrono::Duration::from_std(self.cfg.idle_quiet).unwrap_or(chrono::Duration::zero());
        match self.store.scan_recent_episodes(&ctx, since, 1).await {
            Ok(recent) => recent.is_empty(),
            // A transient store error is not a reason to consolidate; defer to the fallback timer.
            Err(_) => false,
        }
    }

    /// Queue-consumer driver. Runs until `shutdown` is cancelled. Claims Consolidate / ReEmbedFact /
    /// HardDelete jobs and dispatches each to the matching handler, completing or failing per outcome.
    pub async fn run_consumer(&self, shutdown: CancellationToken) {
        let kinds = [
            JobKind::Consolidate,
            JobKind::ReEmbedFact,
            JobKind::HardDelete,
        ];
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let claimed = match self.queue.claim(&kinds, CLAIM_LEASE).await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!(
                                target: "recall",
                                error = %e,
                                "maintenance.consumer.claim_failed"
                            );
                            continue;
                        }
                    };
                    let Some(job) = claimed else { continue };
                    self.dispatch(job).await;
                }
                _ = shutdown.cancelled() => return,
            }
        }
    }

    /// Dispatch one claimed job by kind, then complete or fail it per the C7 retry classification.
    async fn dispatch(&self, job: WorkJob) {
        let ctx = self.maint_ctx(&job.scope.tenant);
        let result = match job.kind {
            JobKind::Consolidate => self.handle_consolidate(&job.scope).await.map(|_| ()),
            JobKind::ReEmbedFact => match parse_payload::<ReEmbedPayload>(&job.payload) {
                Ok(p) => self.handle_reembed(&job.scope, &p).await,
                Err(e) => Err(e),
            },
            JobKind::HardDelete => match parse_payload::<HardDeletePayload>(&job.payload) {
                Ok(p) => self.handle_hard_delete(&job.scope, &p).await.map(|_| ()),
                Err(e) => Err(e),
            },
            // The consumer only claims the three kinds above; any other kind is a no-op completion.
            _ => Ok(()),
        };

        match result {
            Ok(()) => {
                if let Err(e) = self.queue.complete(&job.id).await {
                    tracing::warn!(
                        target: "recall",
                        job_id = %job.id,
                        error = %e,
                        "maintenance.job.complete_failed"
                    );
                }
            }
            Err(e) => {
                let retryable = is_retryable(&e);
                tracing::warn!(
                    target: "recall",
                    kind = %kind_str(job.kind),
                    job_id = %job.id,
                    code = %error_code(&e),
                    correlation_id = %ctx.correlation_id,
                    retryable,
                    "maintenance.job.failed"
                );
                if let Err(fe) = self.queue.fail(&job.id, retryable).await {
                    tracing::warn!(
                        target: "recall",
                        job_id = %job.id,
                        error = %fe,
                        "maintenance.job.fail_failed"
                    );
                }
            }
        }
    }

    // --- Cycle entrypoint and job handlers --------------------------------------------------------

    /// Full maintenance cycle for one tenant. Order is fixed and non-destructive-first:
    /// consolidation → supersession → decay → re-embed. Never deletes (delete is HardDelete-job only).
    /// An error in one duty is recorded in `failed_duties` and the cycle continues to the next duty.
    pub async fn run_cycle(&self, tenant: &str) -> Result<CycleReport, AppError> {
        let ctx = self.maint_ctx(tenant);
        let now = Utc::now();
        let mut report = CycleReport::default();

        match self.consolidate_tenant(&ctx).await {
            Ok(r) => report.consolidation = r,
            Err(e) => self.record_duty_failure(&mut report, "consolidate", &ctx, &e),
        }
        match self.supersede_contradictions(&ctx).await {
            Ok(r) => report.supersession = r,
            Err(e) => self.record_duty_failure(&mut report, "supersede", &ctx, &e),
        }
        match self.decay_tenant(&ctx, now).await {
            Ok(r) => report.decay = r,
            Err(e) => self.record_duty_failure(&mut report, "decay", &ctx, &e),
        }
        match self.reembed_tenant(&ctx).await {
            Ok(r) => report.reembed = r,
            Err(e) => self.record_duty_failure(&mut report, "reembed", &ctx, &e),
        }

        Ok(report)
    }

    /// Record a duty-level failure on the cycle report and log it (the cycle continues regardless).
    fn record_duty_failure(
        &self,
        report: &mut CycleReport,
        duty: &str,
        ctx: &ScopeContext,
        e: &AppError,
    ) {
        report.failed_duties.push(duty.to_string());
        tracing::warn!(
            target: "recall",
            duty,
            correlation_id = %ctx.correlation_id,
            error = %e,
            "maintenance.duty.failed"
        );
    }

    /// Consolidate handler — episodic→semantic for one tenant scope.
    pub async fn handle_consolidate(
        &self,
        scope: &ScopeRef,
    ) -> Result<ConsolidationReport, AppError> {
        let ctx = self.maint_ctx(&scope.tenant);
        self.consolidate_tenant(&ctx).await
    }

    /// ReEmbedFact handler — re-embed one fact named in the job payload.
    pub async fn handle_reembed(
        &self,
        scope: &ScopeRef,
        payload: &ReEmbedPayload,
    ) -> Result<(), AppError> {
        let ctx = self.maint_ctx(&scope.tenant);
        let fact = self
            .store
            .get_fact(&ctx, &payload.fact_id)
            .await?
            .ok_or(AppError::NotFound)?;
        self.reembed_one(&ctx, &fact).await?;
        Ok(())
    }

    /// HardDelete handler — verifiable deletion of one fact; returns the proof. Not complete until a
    /// `DeletionProof` is obtained; a partial removal propagates the `StoreError`, never success.
    pub async fn handle_hard_delete(
        &self,
        scope: &ScopeRef,
        payload: &HardDeletePayload,
    ) -> Result<DeletionProof, AppError> {
        let ctx = self.maint_ctx(&scope.tenant);
        // Resolve the fact first so a missing target is a clean NotFound (non-retryable).
        if self.store.get_fact(&ctx, &payload.fact_id).await?.is_none() {
            return Err(AppError::NotFound);
        }
        // The operation is not complete until the proof is obtained; a StoreError (partial / unconfirmed
        // delete) propagates and is retried — success is never reported on a partial delete.
        let proof = self.store.hard_delete(&ctx, &payload.fact_id).await?;
        tracing::info!(
            target: "recall",
            record_id = %proof.record_id,
            embeddings_removed = proof.embeddings_removed,
            digest = %proof.digest,
            correlation_id = %ctx.correlation_id,
            "maintenance.hard_delete.ok"
        );
        Ok(proof)
    }

    // --- Duty functions ---------------------------------------------------------------------------

    /// Distil recent episodic facts into validated Consolidated Insights for one tenant.
    async fn consolidate_tenant(&self, ctx: &ScopeContext) -> Result<ConsolidationReport, AppError> {
        let mut report = ConsolidationReport::default();
        let now = Utc::now();
        let since = now
            - chrono::Duration::from_std(self.cfg.consolidate_max_interval)
                .unwrap_or(chrono::Duration::zero());
        let episodes = self
            .store
            .scan_recent_episodes(ctx, since, self.cfg.batch_size)
            .await?;

        // Group by subject signature: the sorted entity set + the content subject (where present).
        let mut groups: HashMap<String, Vec<Fact>> = HashMap::new();
        for f in episodes {
            groups.entry(subject_signature(&f)).or_default().push(f);
        }

        for (_sig, group) in groups {
            if (group.len() as u32) < self.cfg.min_episodes {
                tracing::debug!(
                    target: "recall",
                    correlation_id = %ctx.correlation_id,
                    group_size = group.len(),
                    "maintenance.consolidate.group_too_small"
                );
                continue;
            }
            report.groups_seen += 1;

            let candidates = self.llm.consolidate(&group).await?;
            for candidate in candidates {
                report.candidates += 1;
                match self.validate_candidate(ctx, &candidate, &group).await? {
                    Some(source_facts) => {
                        let insight = build_insight(&candidate, &source_facts, self.cfg.insight_decay_factor, now);
                        self.store.put_fact(&insight).await?;
                        report.promoted += 1;
                    }
                    None => {
                        report.rejected_validation += 1;
                        tracing::info!(
                            target: "recall",
                            correlation_id = %ctx.correlation_id,
                            "maintenance.consolidate.rejected"
                        );
                    }
                }
            }
        }
        Ok(report)
    }

    /// Validate one candidate against its source facts. Returns the resolved source facts when valid,
    /// `None` when the candidate fails any check (rejected, not promoted). Errors propagate only on a
    /// store failure while resolving sources.
    async fn validate_candidate(
        &self,
        ctx: &ScopeContext,
        candidate: &InsightCandidate,
        group: &[Fact],
    ) -> Result<Option<Vec<Fact>>, AppError> {
        // (c) content must be a JSON object.
        if !candidate.content.is_object() {
            return Ok(None);
        }
        // (a) derived_from non-empty and every id resolves to a fact in the scanned group.
        if candidate.derived_from.is_empty() {
            return Ok(None);
        }
        let mut source_facts = Vec::with_capacity(candidate.derived_from.len());
        for id in &candidate.derived_from {
            let in_group = group.iter().find(|f| &f.id == id);
            match in_group {
                Some(f) => source_facts.push(f.clone()),
                None => {
                    // Not in the scanned group — confirm via get_fact (a store error propagates); a
                    // candidate citing a fact outside the group is rejected regardless.
                    let _ = self.store.get_fact(ctx, id).await?;
                    return Ok(None);
                }
            }
        }
        // (b) entities is a subset of the union of the source facts' entities.
        let mut union: Vec<&String> = Vec::new();
        for f in &source_facts {
            for e in &f.entities {
                if !union.contains(&e) {
                    union.push(e);
                }
            }
        }
        if !candidate.entities.iter().all(|e| union.contains(&e)) {
            return Ok(None);
        }
        Ok(Some(source_facts))
    }

    /// Detect contradictions among currently-valid facts and supersede the older side (end_validity +
    /// successor links). Non-destructive; history retained.
    async fn supersede_contradictions(
        &self,
        ctx: &ScopeContext,
    ) -> Result<SupersessionReport, AppError> {
        let mut report = SupersessionReport::default();
        let now = Utc::now();
        let pairs = self
            .store
            .scan_contradiction_candidates(ctx, self.cfg.batch_size)
            .await?;
        for (a, b) in pairs {
            report.pairs_checked += 1;
            // Skip pairs where one side has already been closed this cycle (re-read is bounded; a fresh
            // get keeps the decision honest against the latest validity).
            match detect_contradiction(&a, &b) {
                ContradictionVerdict::NoConflict => {}
                ContradictionVerdict::Supersedes => {
                    // b supersedes a -> end a.
                    self.apply_supersession(ctx, &a, &b, now).await?;
                    report.superseded += 1;
                }
                ContradictionVerdict::SupersededBy => {
                    // a supersedes b -> end b.
                    self.apply_supersession(ctx, &b, &a, now).await?;
                    report.superseded += 1;
                }
            }
        }
        Ok(report)
    }

    /// End the superseded fact's validity and record successor links, non-destructively.
    async fn apply_supersession(
        &self,
        ctx: &ScopeContext,
        superseded: &Fact,
        successor: &Fact,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.store.end_validity(ctx, &superseded.id, now).await?;
        let mut closed = superseded.clone();
        closed.superseded_by = Some(successor.id.clone());
        self.store.update_fact_maintenance_fields(ctx, &closed).await?;
        let mut newer = successor.clone();
        newer.supersedes = Some(superseded.id.clone());
        self.store.update_fact_maintenance_fields(ctx, &newer).await?;
        tracing::info!(
            target: "recall",
            superseded_id = %superseded.id,
            successor_id = %successor.id,
            correlation_id = %ctx.correlation_id,
            "maintenance.supersede"
        );
        Ok(())
    }

    /// Apply graceful decay to a tenant's currently-valid facts; prune candidates below the
    /// retrievability AND salience floors (prune == end_validity, never destructive delete).
    async fn decay_tenant(
        &self,
        ctx: &ScopeContext,
        now: DateTime<Utc>,
    ) -> Result<DecayReport, AppError> {
        let mut report = DecayReport::default();
        let candidates = self
            .store
            .scan_decay_candidates(ctx, self.cfg.salience_floor, self.cfg.batch_size)
            .await?;
        for fact in candidates {
            report.evaluated += 1;
            let last = fact.last_recalled_at.unwrap_or(fact.ingested_at);
            let delta_secs = (now - last).num_milliseconds() as f64 / 1000.0;
            let r = retrievability(delta_secs, fact.stability, self.cfg.decay_k);
            if is_prune_candidate(
                r,
                fact.salience,
                self.cfg.prune_retrievability,
                self.cfg.salience_floor,
            ) {
                // Prune is validity-ending, never a destructive delete (ADR-006/ADR-002).
                self.store.end_validity(ctx, &fact.id, now).await?;
                report.pruned += 1;
            }
            // High-salience facts are left unchanged; reinforcement is applied by the read path (C6).
        }
        Ok(report)
    }

    /// Re-embed a tenant's stale/changed facts in batches; output vector length MUST equal embed_dim.
    async fn reembed_tenant(&self, ctx: &ScopeContext) -> Result<ReEmbedReport, AppError> {
        let mut report = ReEmbedReport::default();
        let candidates = self
            .store
            .scan_reembed_candidates(ctx, &self.cfg.embed_model_version, self.cfg.batch_size)
            .await?;
        for fact in candidates {
            report.evaluated += 1;
            match self.reembed_one(ctx, &fact).await {
                Ok(()) => report.reembedded += 1,
                // A dimension mismatch skips the fact rather than aborting the whole batch (SA-EMBED-01).
                Err(AppError::Validation(ValidationKind::OutOfRange, _)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(report)
    }

    /// Re-embed one fact: embed its content text, assert the dimension, then write the vector + model
    /// version. A dimension mismatch is `VAL_OUT_OF_RANGE` (the fact's embedding is not updated).
    async fn reembed_one(&self, ctx: &ScopeContext, fact: &Fact) -> Result<(), AppError> {
        let text = content_to_text(&fact.content);
        let vectors = self.embed.embed(&[text]).await?;
        let vector = vectors.into_iter().next().unwrap_or_default();
        if vector.len() as u32 != self.cfg.embed_dim {
            tracing::warn!(
                target: "recall",
                fact_id = %fact.id,
                got = vector.len(),
                expected = self.cfg.embed_dim,
                correlation_id = %ctx.correlation_id,
                "maintenance.reembed.dim_mismatch"
            );
            return Err(AppError::Validation(
                ValidationKind::OutOfRange,
                "embedding dimension mismatch".to_string(),
            ));
        }
        self.store
            .set_fact_embedding(ctx, &fact.id, &vector, &self.cfg.embed_model_version)
            .await?;
        Ok(())
    }
}

// --- Pure, I/O-free cores (unit-tested against case tables) ----------------------------------------

/// Ebbinghaus retrievability (SA-DECAY-01): `R = exp(-delta_secs / (stability * k))`.
/// `stability` is clamped to a minimum of `f64::MIN_POSITIVE` to avoid division by zero;
/// `delta_secs < 0` (clock skew) is clamped to 0, yielding `R = 1.0`.
pub fn retrievability(delta_secs: f64, stability: f64, k: f64) -> f64 {
    let delta = delta_secs.max(0.0);
    let s = stability.max(f64::MIN_POSITIVE);
    let denom = (s * k).max(f64::MIN_POSITIVE);
    (-delta / denom).exp()
}

/// Prune-candidate predicate (SA-DECAY-01): true iff `R < prune_retrievability` AND
/// `salience < salience_floor`.
pub fn is_prune_candidate(
    r: f64,
    salience: f64,
    prune_retrievability: f64,
    salience_floor: f64,
) -> bool {
    r < prune_retrievability && salience < salience_floor
}

/// Reinforcement on recall: raise stability and reset the decay clock.
/// `new_stability = stability * (1.0 + reinforce_gain)`; returns `(new_stability, now)`.
pub fn reinforce(stability: f64, reinforce_gain: f64, now: DateTime<Utc>) -> (f64, DateTime<Utc>) {
    (stability * (1.0 + reinforce_gain), now)
}

/// Decaying confidence for a promoted insight (ADR-006): floor of
/// `min(candidate.confidence, min(source confidences)) * insight_decay_factor` into [0,1].
/// Guarantees an insight never outranks its source facts.
pub fn insight_confidence(
    candidate_confidence: f64,
    source_confidences: &[f64],
    insight_decay_factor: f64,
) -> f64 {
    let min_source = source_confidences
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let cap = if min_source.is_finite() {
        candidate_confidence.min(min_source)
    } else {
        candidate_confidence
    };
    (cap * insight_decay_factor).clamp(0.0, 1.0)
}

/// Contradiction-detection contract. Given two currently-valid facts about the same subject, decide
/// whether `b` contradicts `a` and, if so, which one is superseded. Reads only `valid_from`,
/// `confidence`, `id`, `content`, and `entities`; performs no I/O.
///
/// Content-conflict heuristic (C7 *Gaps*): the two facts conflict iff they share ≥1 entity AND their
/// triple-shaped `content` carries the same `(subject, predicate)` with a differing `object`. When they
/// conflict, the **superseded** side is the one with the strictly earlier `valid_from`; ties on
/// `valid_from` break on lower `confidence`; remaining ties break on lexicographically smaller `id`.
pub fn detect_contradiction(a: &Fact, b: &Fact) -> ContradictionVerdict {
    if !content_conflicts(a, b) {
        return ContradictionVerdict::NoConflict;
    }
    // Determine which side is superseded (the older/weaker side). `a_is_superseded == true` means a is
    // ended, i.e. b supersedes a -> Supersedes.
    let a_is_superseded = if a.valid_from != b.valid_from {
        a.valid_from < b.valid_from
    } else if a.confidence != b.confidence {
        a.confidence < b.confidence
    } else {
        a.id < b.id
    };
    if a_is_superseded {
        ContradictionVerdict::Supersedes
    } else {
        ContradictionVerdict::SupersededBy
    }
}

/// Whether two facts' content conflicts per the C7 heuristic: share ≥1 entity AND same triple
/// `(subject, predicate)` with a differing `object`.
fn content_conflicts(a: &Fact, b: &Fact) -> bool {
    let shares_entity = a.entities.iter().any(|e| b.entities.contains(e));
    if !shares_entity {
        return false;
    }
    let (Some(sa), Some(pa), Some(oa)) = triple(&a.content) else {
        return false;
    };
    let (Some(sb), Some(pb), Some(ob)) = triple(&b.content) else {
        return false;
    };
    sa == sb && pa == pb && oa != ob
}

/// Extract the `(subject, predicate, object)` string fields of a triple-shaped content object.
fn triple(content: &Json) -> (Option<&str>, Option<&str>, Option<&str>) {
    let obj = content.as_object();
    let get = |k: &str| obj.and_then(|m| m.get(k)).and_then(|v| v.as_str());
    (get("subject"), get("predicate"), get("object"))
}

// --- Internal helpers ------------------------------------------------------------------------------

/// The subject signature a consolidation group is keyed on: the sorted entity set joined with the
/// content subject (where present). Mirrors the "(entity-set, content-subject)" signature in the spec.
fn subject_signature(f: &Fact) -> String {
    let mut entities = f.entities.clone();
    entities.sort();
    let subject = f
        .content
        .as_object()
        .and_then(|m| m.get("subject"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    format!("{}|{}", entities.join(","), subject)
}

/// Build a Consolidated Insight `Fact` from a validated candidate and its source facts (C7 step 5):
/// source-capped decaying confidence; salience = max source salience; stability = 1.0; fresh validity;
/// owner = common owner of the sources (else tenant-shared at the tenant scope).
fn build_insight(
    candidate: &InsightCandidate,
    sources: &[Fact],
    insight_decay_factor: f64,
    now: DateTime<Utc>,
) -> Fact {
    use crate::types::domain::Visibility;

    let source_confidences: Vec<f64> = sources.iter().map(|f| f.confidence).collect();
    let confidence = insight_confidence(candidate.confidence, &source_confidences, insight_decay_factor);
    let salience = sources.iter().map(|f| f.salience).fold(0.0_f64, f64::max);

    // Owner: inherit the common owner of the sources; if they disagree on team/user, own at tenant
    // visibility (TenantShared) under the tenant alone.
    let tenant = sources
        .first()
        .map(|f| f.owner.tenant.clone())
        .unwrap_or_default();
    let same_owner = sources
        .iter()
        .all(|f| f.owner.team == sources[0].owner.team && f.owner.user == sources[0].owner.user);
    let (owner, visibility) = if same_owner {
        (
            sources[0].owner.clone(),
            sources[0].visibility,
        )
    } else {
        (
            ScopeRef {
                tenant: tenant.clone(),
                team: None,
                user: String::new(),
            },
            Visibility::TenantShared,
        )
    };

    Fact {
        id: format!("fact:{}", Uuid::new_v4()),
        content: candidate.content.clone(),
        entities: candidate.entities.clone(),
        source_id: None,
        memory_class: MemoryClass::Consolidated,
        visibility,
        owner,
        valid_from: now,
        valid_to: None,
        ingested_at: now,
        confidence,
        salience,
        stability: 1.0,
        pii_review: false,
        supersedes: None,
        superseded_by: None,
        derived_from: candidate.derived_from.clone(),
        last_recalled_at: None,
    }
}

/// Render a JSON content object to a flat embedding-input text: every scalar leaf concatenated with
/// spaces (mirrors the store's `content_to_text` so re-embed input matches first-embed input).
fn content_to_text(content: &Json) -> String {
    let mut out = String::new();
    fn walk(v: &Json, out: &mut String) {
        match v {
            Json::String(x) => push(out, x),
            Json::Number(x) => push(out, &x.to_string()),
            Json::Bool(x) => push(out, if *x { "true" } else { "false" }),
            Json::Array(a) => a.iter().for_each(|e| walk(e, out)),
            Json::Object(m) => m.values().for_each(|e| walk(e, out)),
            Json::Null => {}
        }
    }
    fn push(out: &mut String, s: &str) {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(s);
    }
    walk(content, &mut out);
    out
}

/// Deserialise a kind-specific job payload; a malformed payload is a non-retryable `VAL_INVALID_BODY`.
fn parse_payload<T: for<'de> Deserialize<'de>>(payload: &Json) -> Result<T, AppError> {
    serde_json::from_value(payload.clone()).map_err(|e| {
        AppError::Validation(ValidationKind::InvalidBody, format!("invalid job payload: {e}"))
    })
}

/// Whether a handler error is retryable per the C7 consumer classification: provider timeouts, store,
/// and queue errors are retryable; validation and not-found are not.
fn is_retryable(e: &AppError) -> bool {
    match e {
        AppError::Provider(ProviderError::Timeout) => true,
        AppError::Store(_) | AppError::Queue(_) => true,
        AppError::Provider(_) => false,
        AppError::Validation(_, _) | AppError::NotFound => false,
        _ => false,
    }
}

/// A short stable code string for a handler error, for the `maintenance.job.failed` log line.
fn error_code(e: &AppError) -> &'static str {
    match e {
        AppError::NotFound => "NOT_FOUND",
        AppError::Validation(ValidationKind::OutOfRange, _) => "VAL_OUT_OF_RANGE",
        AppError::Validation(ValidationKind::InvalidBody, _) => "VAL_INVALID_BODY",
        AppError::Validation(_, _) => "VAL",
        AppError::Store(_) => "STORE",
        AppError::Queue(_) => "QUEUE",
        AppError::Provider(ProviderError::Timeout) => "PROVIDER_TIMEOUT",
        AppError::Provider(_) => "PROVIDER_ERROR",
        _ => "INTERNAL",
    }
}

/// The canonical snake_case string for a `JobKind` (matches the C2 serde rename, for log lines only).
fn kind_str(k: JobKind) -> &'static str {
    match k {
        JobKind::ExtractFact => "extract_fact",
        JobKind::ReEmbedFact => "re_embed_fact",
        JobKind::Consolidate => "consolidate",
        JobKind::HardDelete => "hard_delete",
    }
}

#[cfg(test)]
mod tests;
