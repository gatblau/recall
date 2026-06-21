//! C5 — Freshness Checker. The read-path conditional source-change check (ADR-013).
//!
//! Called in-process by the Retrieval Engine (C6) with the candidate facts that cite a source. For
//! each distinct cited source it performs **one** cheap conditional change-check against the Faraday
//! broker (sending the source's `modification_marker` as an `If-Modified-Since`-style precondition,
//! issued **as the authenticated user** so the broker enforces source access rights), then maps the
//! answer to a per-fact [`Currency`]:
//!
//! * unchanged source -> `Current`.
//! * changed source -> `StalePendingRefresh`, plus one idempotent `ReReadSource` job enqueued on C2.
//! * unreachable broker / provider error / per-call timeout / batch-deadline breach ->
//!   `UnverifiedCurrency` (the stored fact is kept; the read is never blocked or failed).
//!
//! The component is a **bounded-fan-out with a single wall-clock deadline** (C5 *Approach*): distinct
//! sources are checked concurrently under one batch deadline derived from the SA-LAT-01 freshness
//! sub-budget (`RECALL_FRESHNESS_BUDGET_MS`, default 25 ms); each single round-trip is bounded by
//! `RECALL_FRESHNESS_PER_CALL_MS` (default 20 ms). Any check still outstanding when the batch deadline
//! elapses is treated as `UnverifiedCurrency`, so the freshness step degrades to "skip -> flag
//! unverified-currency" rather than overrunning the read-path budget.
//!
//! **The function never errors.** Every failure mode (broker error, queue error, per-call timeout,
//! batch-deadline breach) is mapped to a `Currency` value; the read path is never blocked or failed by
//! freshness. It owns no persistence and reads no store — C6 supplies the resolved `Source` for each
//! fact. No fact content leaves the process: only source ids, the absorbed-error class, and counts are
//! logged, all carrying the request `correlation_id`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::types::api::Currency;
use crate::types::domain::{Fact, Source};
use crate::types::job::{JobKind, JobStatus, WorkJob};
use crate::types::ports::{
    BrokerClient, FreshnessChecker, ProviderError, SourceState, WorkQueue,
};
use crate::types::scope::{ScopeContext, ScopeRef};

/// The resolved currency of one distinct source after its check (internal; mapped to [`Currency`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SourceOutcome {
    /// Broker reported the source unmodified.
    Current,
    /// Broker reported the source changed; a `ReReadSource` job is enqueued.
    Changed,
    /// Unreachable broker, provider error, per-call timeout, or batch-deadline breach.
    Unverified,
}

impl SourceOutcome {
    fn to_currency(self) -> Currency {
        match self {
            SourceOutcome::Current => Currency::Current,
            SourceOutcome::Changed => Currency::StalePendingRefresh,
            SourceOutcome::Unverified => Currency::UnverifiedCurrency,
        }
    }
}

/// Stateless broker-backed freshness checker. Holds the two injected seams (broker + queue) and the
/// resolved SA-LAT-01 budgets. Construct once and share via `Arc`; `check` borrows `&self`.
pub struct BrokerFreshnessChecker {
    broker: Arc<dyn BrokerClient>,
    queue: Arc<dyn WorkQueue>,
    /// From `RECALL_FRESHNESS_BUDGET_MS` (default 25 ms): wall-clock deadline for the whole batch.
    budget: Duration,
    /// From `RECALL_FRESHNESS_PER_CALL_MS` (default 20 ms): timeout for a single broker round-trip.
    per_call: Duration,
}

impl BrokerFreshnessChecker {
    /// Build a checker over the injected broker + queue seams and the resolved budgets.
    pub fn new(
        broker: Arc<dyn BrokerClient>,
        queue: Arc<dyn WorkQueue>,
        budget: Duration,
        per_call: Duration,
    ) -> Self {
        Self {
            broker,
            queue,
            budget,
            per_call,
        }
    }

    /// Enqueue one idempotent `ReReadSource` job for a changed source. Best-effort: an enqueue failure
    /// is logged at `warn` and swallowed — the change has already been detected, so the fact is still
    /// returned `StalePendingRefresh` (C5 *Internal Logic* step 7; error-table QUEUE_UNAVAILABLE row).
    async fn enqueue_reread(&self, ctx: &ScopeContext, src: &Source) {
        let scope = ScopeRef {
            tenant: ctx.tenant.clone(),
            team: ctx.teams.first().cloned(),
            user: ctx.user.clone(),
        };
        let job = WorkJob {
            id: format!("work_job:{}", Uuid::now_v7()),
            kind: JobKind::ReReadSource,
            payload: json!({
                "source_id": src.id,
                "origin_ref": src.origin_ref,
                "modification_marker": src.modification_marker,
            }),
            scope,
            // Idempotent on the source id so duplicate change detections within the C2 idempotency
            // window do not pile up duplicate re-read jobs.
            idempotency_key: Some(format!("re-read-source:{}", src.id)),
            attempts: 0,
            status: JobStatus::Pending,
            not_before: Utc::now(),
            created_at: Utc::now(),
            leased_until: None,
        };
        match self.queue.enqueue(job).await {
            Ok(_) => tracing::info!(
                target: "recall",
                correlation_id = %ctx.correlation_id,
                source_id = %src.id,
                "freshness.reread_enqueued"
            ),
            // QUEUE_UNAVAILABLE absorbed: log and continue; the fact stays StalePendingRefresh.
            Err(e) => tracing::warn!(
                target: "recall",
                correlation_id = %ctx.correlation_id,
                source_id = %src.id,
                error_class = "QUEUE_UNAVAILABLE",
                detail = %e,
                "freshness.reread_enqueue_failed"
            ),
        }
    }
}

/// Map one source's completed check (or per-call timeout) to a [`SourceOutcome`]. `Changed` is handled
/// by the caller (it must enqueue first); every absorbed failure becomes `Unverified` with a `warn`.
fn classify(
    correlation_id: &str,
    source_id: &str,
    check: Result<Result<SourceState, ProviderError>, tokio::time::error::Elapsed>,
) -> SourceOutcome {
    match check {
        Ok(Ok(SourceState::Unchanged)) => SourceOutcome::Current,
        Ok(Ok(SourceState::Changed)) => SourceOutcome::Changed,
        Ok(Ok(SourceState::Unreachable)) | Ok(Err(_)) => {
            tracing::warn!(
                target: "recall",
                correlation_id = %correlation_id,
                source_id = %source_id,
                error_class = "PROVIDER_ERROR",
                "freshness.source_unverified"
            );
            SourceOutcome::Unverified
        }
        Err(_elapsed) => {
            tracing::warn!(
                target: "recall",
                correlation_id = %correlation_id,
                source_id = %source_id,
                error_class = "PROVIDER_TIMEOUT",
                "freshness.source_timeout"
            );
            SourceOutcome::Unverified
        }
    }
}

#[async_trait]
impl FreshnessChecker for BrokerFreshnessChecker {
    async fn check(
        &self,
        ctx: &ScopeContext,
        facts: &[(Fact, Source)],
    ) -> Vec<(String, Currency)> {
        // Step 1 — empty / fast exit: no work, no broker contact, no log.
        if facts.is_empty() {
            return Vec::new();
        }

        // Step 2 — start the single batch deadline shared by every broker round-trip below.
        let deadline = tokio::time::Instant::now() + self.budget;
        let cid = ctx.correlation_id.clone();

        // Step 3 — group by distinct source id (first-seen order), so multiple facts citing the same
        // source share one broker round-trip and, on a change, one re-read job.
        let mut distinct: Vec<Source> = Vec::new();
        let mut seen: HashMap<String, ()> = HashMap::new();
        for (_f, src) in facts {
            if seen.insert(src.id.clone(), ()).is_none() {
                distinct.push(src.clone());
            }
        }

        // Step 4 — fan out one conditional check per distinct source, each under the per-call timeout.
        let mut set: tokio::task::JoinSet<(
            String,
            Result<Result<SourceState, ProviderError>, tokio::time::error::Elapsed>,
        )> = tokio::task::JoinSet::new();
        for src in &distinct {
            let broker = self.broker.clone();
            let ctx_cloned = ctx.clone();
            let src_cloned = src.clone();
            let per_call = self.per_call;
            let sid = src.id.clone();
            set.spawn(async move {
                let res =
                    tokio::time::timeout(per_call, broker.check_source(&ctx_cloned, &src_cloned))
                        .await;
                (sid, res)
            });
        }

        // Step 5 — await the fan-out bounded by the batch deadline; anything still outstanding when the
        // deadline elapses is left absent from `by_source` and defaults to Unverified in step 8.
        let mut by_source: HashMap<String, SourceOutcome> = HashMap::new();
        loop {
            match tokio::time::timeout_at(deadline, set.join_next()).await {
                Ok(Some(Ok((sid, res)))) => {
                    let outcome = classify(&cid, &sid, res);
                    by_source.insert(sid, outcome);
                }
                // A spawned task panicked: absorb as Unverified (leave absent -> default).
                Ok(Some(Err(_join_err))) => {}
                // All checks resolved before the deadline.
                Ok(None) => break,
                // Batch-deadline breach: stop awaiting; the rest stay Unverified.
                Err(_elapsed) => {
                    let skipped = set.len();
                    if skipped > 0 {
                        tracing::warn!(
                            target: "recall",
                            correlation_id = %cid,
                            error_class = "PROVIDER_TIMEOUT",
                            skipped_sources = skipped,
                            "freshness.deadline_breach"
                        );
                    }
                    break;
                }
            }
        }
        set.abort_all();

        // Steps 6 & 7 — enqueue a re-read for each changed source (idempotent, best-effort).
        for src in &distinct {
            if by_source.get(&src.id) == Some(&SourceOutcome::Changed) {
                self.enqueue_reread(ctx, src).await;
            }
        }

        // Step 8 — expand per-source outcomes back to per-fact results in original input order.
        facts
            .iter()
            .map(|(f, src)| {
                let outcome = by_source
                    .get(&src.id)
                    .copied()
                    .unwrap_or(SourceOutcome::Unverified);
                (f.id.clone(), outcome.to_currency())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
