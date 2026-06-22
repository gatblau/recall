//! C6 — Retrieval Engine. The synchronous read path of `recall` (ADR-004: no LLM call, NFR-P1).
//!
//! A fixed pipeline turns an authenticated, scoped [`RecallRequest`] into a bounded, ranked set of
//! facts with confidence, an opaque pagination cursor, and — when the request opts in via
//! `include_provenance` — each sourced fact's `origin_ref` + `modification_marker` so the agent can
//! check source freshness itself (ADR-014) — or an explicit abstention. It performs exactly two
//! read-path model inferences (query embedding and a cross-encoder rerank, ADR-012/ADR-005), each with
//! its own SA-LAT-01 sub-budget inside NFR-P2 (whole-path p95 ≤ 200 ms). It owns no persistence:
//! candidate retrieval, scope/`valid_at` filtering, and source loading are delegated to the Memory
//! Store (C1). `recall` performs no source-change check (ADR-014). This component is pure orchestration
//! plus the ranking, gating, and cursor arithmetic.
//!
//! Pipeline (`recall`): input-guard + cursor-decode → (optional reformulation) → embed → stage-1
//! multi-signal recall → rerank → recency weight → gate/abstain → pagination window → conditional
//! provenance attach → build outcome. Each external step degrades per the spec rather than blocking the
//! response: embed and stage-1 fail fast (a typed error); rerank degrades to stage-1 order; provenance
//! attach is best-effort (a source-load miss leaves that fact's `source` unset). Scope is taken only
//! from `ctx` (built by C3), never from the request body;
//! tenant isolation is structural (a different tenant is a different namespace). Provider keys are
//! env-only and never logged; fact `content` is never logged — only counts and scores.

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::{AppError, ValidationKind};
use crate::types::api::{RankedFact, RecallRequest, RecallResponse, SourceProvenance};
use crate::types::domain::Fact;
use crate::types::ports::{
    EmbeddingClient, MemoryStore, RecallFilters as StoreFilters, RerankClient, StageOneQuery,
};
use crate::types::scope::ScopeContext;

/// The resolved C6 configuration (Phase 2D keys owned by C6 unless noted).
#[derive(Clone, Copy)]
pub struct RetrievalConfig {
    /// `RECALL_EMBED_DIM` — query-vector length must equal this (and the store's index dim).
    pub embed_dim: u32,
    /// `RECALL_STAGE1_K` — stage-1 candidate fan-out fed to rerank (SA-RERANK-01).
    pub stage1_k: u16,
    /// `RECALL_RESULT_CAP_MAX` — hard upper bound on `result_cap` (SA-CAP-01).
    pub result_cap_max: u8,
    /// `RECALL_ABSTAIN_THRESHOLD` — top-final score below which recall abstains (SA-GATE-01).
    pub abstain_threshold: f64,
    /// `RECALL_RECENCY_WEIGHT` — recency boost weight `w` (SA-RECENCY-01).
    pub recency_weight: f64,
    /// `RECALL_RECENCY_TAU_DAYS` — recency decay constant `τ` in days.
    pub recency_tau_days: f64,
    /// `RECALL_REFORMULATION_ENABLED` — A/B flag gating the optional reformulation step (default off).
    pub reformulation_enabled: bool,
}

impl RetrievalConfig {
    /// Project the resolved [`Config`] onto the C6 subset.
    pub fn from_config(c: &Config) -> Self {
        Self {
            embed_dim: c.embed_dim,
            stage1_k: c.stage1_k,
            result_cap_max: c.result_cap_max,
            abstain_threshold: c.abstain_threshold,
            recency_weight: c.recency_weight,
            recency_tau_days: c.recency_tau_days,
            reformulation_enabled: c.reformulation_enabled,
        }
    }
}

/// What C6 returns to C8: the response payload plus the `Meta` fields C8 serialises.
pub struct RecallOutcome {
    /// `{ facts: Vec<RankedFact> }`.
    pub response: RecallResponse,
    /// Opaque cursor; `None` on the last page or on abstain.
    pub next_cursor: Option<String>,
    /// `true` => `facts` is empty by gating, not by absence.
    pub abstained: bool,
}

/// The opaque pagination cursor payload — `(final_score, fact_id)` of the last emitted fact
/// (SA-PAGE-01). Encoded as base64url(JSON); never persisted.
#[derive(Serialize, Deserialize, Debug)]
struct Cursor {
    /// Final score of the last emitted fact.
    s: f64,
    /// Id of the last emitted fact.
    id: String,
}

impl Cursor {
    fn encode(&self) -> Result<String, AppError> {
        let json = serde_json::to_vec(self).map_err(|_| AppError::Internal)?;
        Ok(URL_SAFE_NO_PAD.encode(json))
    }

    fn decode(raw: &str) -> Result<Cursor, AppError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(raw.as_bytes())
            .map_err(|_| AppError::Validation(ValidationKind::OutOfRange, "undecodable cursor".into()))?;
        serde_json::from_slice(&bytes)
            .map_err(|_| AppError::Validation(ValidationKind::OutOfRange, "malformed cursor".into()))
    }
}

/// A candidate carried through ranking: the fact and the final score assigned in the recency step.
struct Scored {
    fact: Fact,
    final_score: f64,
}

/// The synchronous read path. Holds the injected store/provider seams and the resolved config;
/// construct once and share via `Arc`. Freshness is agent-side (ADR-014): no freshness seam.
pub struct RetrievalEngine {
    store: Arc<dyn MemoryStore>,
    embedder: Arc<dyn EmbeddingClient>,
    reranker: Arc<dyn RerankClient>,
    config: RetrievalConfig,
}

impl RetrievalEngine {
    pub fn new(
        store: Arc<dyn MemoryStore>,
        embedder: Arc<dyn EmbeddingClient>,
        reranker: Arc<dyn RerankClient>,
        config: RetrievalConfig,
    ) -> Self {
        Self {
            store,
            embedder,
            reranker,
            config,
        }
    }

    /// The synchronous read path. `ctx` is the authenticated scope (never derived from `req`); `req`
    /// is the validated recall request. Returns the ranked facts plus the `Meta` fields C8 serialises.
    pub async fn recall(
        &self,
        ctx: &ScopeContext,
        req: &RecallRequest,
    ) -> Result<RecallOutcome, AppError> {
        // Step 1 — input guard and pagination decode.
        let query = req.query.trim();
        if query.is_empty() || req.query.chars().count() > 4096 {
            return Err(AppError::Validation(
                ValidationKind::OutOfRange,
                "query length out of range".into(),
            ));
        }
        if req.result_cap == 0 || req.result_cap > self.config.result_cap_max {
            return Err(AppError::Validation(
                ValidationKind::OutOfRange,
                format!("result_cap={}", req.result_cap),
            ));
        }
        let effective_cap = req.result_cap.min(self.config.result_cap_max) as usize;
        let cursor = match &req.cursor {
            Some(raw) => Some(Cursor::decode(raw)?),
            None => None,
        };

        // Step 2 — optional query reformulation (A/B-gated, off by default per ADR-012 / good-mem §7.3).
        let effective_query = req.query.clone();
        tracing::debug!(
            target: "recall",
            correlation_id = %ctx.correlation_id,
            query_len = query.len(),
            result_cap_effective = effective_cap,
            cursor_present = cursor.is_some(),
            reformulation_enabled = self.config.reformulation_enabled,
            "retrieval.input"
        );

        // Step 3 — embed the query (read-path inference #1). Fail fast on any provider error.
        let mut embeddings = self.embedder.embed(std::slice::from_ref(&effective_query)).await?;
        let query_vector = if embeddings.is_empty() {
            return Err(AppError::Provider(
                crate::types::ports::ProviderError::Malformed("empty embedding response".into()),
            ));
        } else {
            embeddings.swap_remove(0)
        };
        if query_vector.len() != self.config.embed_dim as usize {
            // A dim mismatch is a misconfiguration startup should have caught (SA-EMBED-01).
            return Err(AppError::Internal);
        }

        // Step 4 — stage-1 multi-signal recall (store applies the read filter using ctx). Fail fast.
        let stage_query = StageOneQuery {
            query_vector,
            keyword_terms: keyword_terms(&effective_query),
            filters: StoreFilters {
                memory_class: req.filters.memory_class,
                visibility: req.filters.visibility,
                entity: req.filters.entity.clone(),
                valid_at: req.filters.valid_at,
            },
            scope: ctx.clone(),
            stage1_k: self.config.stage1_k,
        };
        let candidates = self.store.recall(ctx, &stage_query).await?;
        tracing::debug!(
            target: "recall",
            correlation_id = %ctx.correlation_id,
            stage1_candidates = candidates.len(),
            "retrieval.stage1"
        );
        if candidates.is_empty() {
            // No candidates -> abstain with an empty page (step 7 outcome, reached early).
            return Ok(RecallOutcome {
                response: RecallResponse { facts: vec![] },
                next_cursor: None,
                abstained: true,
            });
        }

        // Step 5 — cross-encoder rerank (inference #2). Degrade to stage-1 order on any provider error.
        let docs: Vec<String> = candidates
            .iter()
            .map(|c| c.fact.content.to_string())
            .collect();
        let rerank_scores = match self.reranker.rerank(&effective_query, &docs).await {
            Ok(scores) => Some(scores),
            Err(e) => {
                tracing::warn!(
                    target: "recall",
                    correlation_id = %ctx.correlation_id,
                    rerank_skipped = true,
                    detail = %e,
                    "retrieval.rerank_degraded"
                );
                None
            }
        };

        // Step 6 — recency weighting; assign the final score and sort (final desc, id asc).
        let now = Utc::now();
        let mut scored: Vec<Scored> = candidates
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                let rerank_score = match &rerank_scores {
                    Some(scores) => scores.get(i).copied().unwrap_or(c.semantic_score).clamp(0.0, 1.0),
                    None => c.semantic_score,
                };
                let age_days = ((now - c.fact.ingested_at).num_seconds() as f64 / 86_400.0).max(0.0);
                let final_score = recency_final_score(
                    rerank_score,
                    age_days,
                    self.config.recency_weight,
                    self.config.recency_tau_days,
                );
                Scored {
                    fact: c.fact,
                    final_score,
                }
            })
            .collect();
        scored.sort_by(|a, b| {
            b.final_score
                .partial_cmp(&a.final_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.fact.id.cmp(&b.fact.id))
        });

        // Step 7 — retrieval gating / abstain.
        let top_final = scored.first().map(|s| s.final_score).unwrap_or(0.0);
        if top_final < self.config.abstain_threshold {
            tracing::debug!(
                target: "recall",
                correlation_id = %ctx.correlation_id,
                top_final_score = top_final,
                abstained = true,
                "retrieval.gate"
            );
            return Ok(RecallOutcome {
                response: RecallResponse { facts: vec![] },
                next_cursor: None,
                abstained: true,
            });
        }

        // Step 8 — pagination window: drop everything not strictly after the cursor, then truncate.
        let surviving_after_cursor: Vec<Scored> = match &cursor {
            Some(cur) => scored
                .into_iter()
                .filter(|s| {
                    s.final_score < cur.s || (s.final_score == cur.s && s.fact.id > cur.id)
                })
                .collect(),
            None => scored,
        };
        let remaining = surviving_after_cursor.len();
        let page: Vec<Scored> = surviving_after_cursor.into_iter().take(effective_cap).collect();
        let has_more = remaining > page.len();

        // Step 9 — conditional provenance attach (SA-PROV-01, ADR-014). When the caller opts in,
        // surface each sourced fact's origin_ref + modification_marker so the agent can run its own
        // source-freshness check. Best-effort: a get_source miss/error simply omits that fact's source.
        let mut provenance: std::collections::HashMap<String, SourceProvenance> =
            std::collections::HashMap::new();
        if req.include_provenance {
            for s in &page {
                if let Some(sid) = &s.fact.source_id {
                    if let Ok(Some(src)) = self.store.get_source(ctx, sid).await {
                        provenance.insert(
                            s.fact.id.clone(),
                            SourceProvenance {
                                origin_ref: src.origin_ref,
                                modification_marker: src.modification_marker,
                            },
                        );
                    }
                }
            }
        }

        // Step 10 — build the outcome.
        let next_cursor = if has_more {
            page.last()
                .map(|last| {
                    Cursor {
                        s: last.final_score,
                        id: last.fact.id.clone(),
                    }
                    .encode()
                })
                .transpose()?
        } else {
            None
        };
        let facts: Vec<RankedFact> = page
            .into_iter()
            .map(|s| {
                let source = provenance.remove(&s.fact.id);
                RankedFact {
                    fact: s.fact,
                    score: s.final_score.clamp(0.0, 1.0),
                    source,
                }
            })
            .collect();
        tracing::debug!(
            target: "recall",
            correlation_id = %ctx.correlation_id,
            facts_returned = facts.len(),
            next_cursor_present = next_cursor.is_some(),
            "retrieval.outcome"
        );
        Ok(RecallOutcome {
            response: RecallResponse { facts },
            next_cursor,
            abstained: false,
        })
    }

}

/// Apply the SA-RECENCY-01 recency boost: `final = rerank · (1 + w·exp(−age_days/τ))`. `age_days` is
/// floored at 0 by the caller. Pure arithmetic, factored out for table-driven testing.
fn recency_final_score(rerank_score: f64, age_days: f64, w: f64, tau: f64) -> f64 {
    let recency_boost = w * (-age_days / tau).exp();
    rerank_score * (1.0 + recency_boost)
}

/// Extract BM25 keyword terms from the query: split on non-alphanumerics, lowercase, drop empties,
/// de-duplicate (order-preserving). The store's `recall_text` analyzer does the real matching; this
/// only feeds candidate terms (C6 *Gaps* — keyword tokenisation follows C1's analyzer).
fn keyword_terms(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .filter(|t| seen.insert(t.clone()))
        .collect()
}

#[cfg(test)]
mod tests;
