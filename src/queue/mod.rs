//! C2 — Durable Work Queue. A store-backed implementation of the `WorkQueue` trait (Phase 2C.6) over
//! the same embedded SurrealDB engine the C1 Memory Store owns (SA-QUEUE-01), plus a lease-reaper that
//! reclaims a crashed worker's job.
//!
//! Each tenant namespace (ADR-011) holds a `work_job` table governed by a claim/lease protocol: a
//! claim is a single conditional `UPDATE … RETURN AFTER LIMIT 1` whose `WHERE` guard makes the flip
//! to `Leased` atomic, so two workers polling concurrently can never both win the same job. Retry uses
//! exponential backoff with jitter (SA-QUEUE-02); an exhausted or non-retryable job is parked in a
//! `dead_letter` table. The default backend is store-backed; a `nats` backend sits behind the same
//! trait so producers and consumers never change (OQ-QUEUE).
//!
//! All caller input (scope, idempotency key, payload, ids) is bound as query parameters — never
//! string-interpolated — per the sql-safety layer rule. Tenant identifiers, which select the namespace
//! and cannot be parameter-bound in DDL, are validated by `crate::store::migrate::validate_tenant`.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::Rng;
use serde_json::Value as Json;
use surrealdb::engine::any::Any;
use surrealdb::types::{Datetime, Object, Value};
use surrealdb::Surreal;
use tokio_util::sync::CancellationToken;

use crate::store::migrate::{validate_tenant, Migrator, DB_NAME};
use crate::types::job::{JobKind, JobStatus, WorkJob};
use crate::types::ports::{QueueError, StoreError, WorkQueue};
use crate::types::scope::ScopeRef;

/// Store-backed `WorkQueue` implementation (`RECALL_QUEUE_BACKEND=store`). Built over a shared handle
/// to the C1 store's embedded SurrealDB engine (see `Store::handle`), so its `work_job`/`dead_letter`
/// tables live inside the same single-binary database.
pub struct StoreWorkQueue {
    /// Shared SurrealDB connection (the engine handle is internally `Arc`-backed).
    db: Surreal<Any>,
    /// `RECALL_EMBED_DIM`, required to run the C1+C2 migration set when provisioning a namespace.
    embed_dim: u32,
    /// `RECALL_JOB_MAX_ATTEMPTS` (default 5) — retry cap before dead-letter (SA-QUEUE-02).
    max_attempts: u32,
    /// `RECALL_JOB_BACKOFF_BASE_MS` (default 2000) — backoff base for job retries.
    backoff_base_ms: u32,
}

impl StoreWorkQueue {
    /// Build a queue over a shared store connection and the C2 retry configuration.
    pub fn new(db: Surreal<Any>, embed_dim: u32, max_attempts: u32, backoff_base_ms: u32) -> Self {
        Self {
            db,
            embed_dim,
            max_attempts,
            backoff_base_ms,
        }
    }

    /// Open an in-memory queue (the real engine, in-process) on its own SurrealDB connection for tests
    /// and ephemeral runs. Mirrors `Store::new_in_memory`.
    pub async fn new_in_memory(
        embed_dim: u32,
        max_attempts: u32,
        backoff_base_ms: u32,
    ) -> Result<Self, QueueError> {
        let db = surrealdb::engine::any::connect("mem://")
            .await
            .map_err(|e| QueueError::BackendUnavailable(e.to_string()))?;
        Ok(Self::new(db, embed_dim, max_attempts, backoff_base_ms))
    }

    /// Select the tenant namespace + `recall` database on the shared connection, provisioning the
    /// schema (the squashed 0001_init) idempotently first so a never-seen tenant's `work_job` table
    /// exists before the first enqueue.
    async fn ensure_and_use(&self, tenant: &str) -> Result<(), QueueError> {
        Migrator::new(&self.db, self.embed_dim)
            .migrate_up(tenant)
            .await
            .map_err(store_to_queue)?;
        validate_tenant(tenant).map_err(store_to_queue)?;
        self.db
            .use_ns(tenant.to_string())
            .use_db(DB_NAME)
            .await
            .map_err(db_to_queue)?;
        Ok(())
    }

    /// Build a `LeaseReaper` over the same shared SurrealDB connection and embedding dimension as this
    /// queue, so it reaps the very `work_job` tables this queue writes.
    pub fn reaper(&self, interval: Duration) -> LeaseReaper {
        LeaseReaper::new(self.db.clone(), self.embed_dim, interval)
    }

    /// Compute the retry backoff for attempt `n`: `backoff_base_ms · 2^n` with a uniform jitter factor
    /// in `[0.8, 1.2]` (SA-QUEUE-02). Saturates the shift so a high attempt count cannot overflow.
    fn backoff(&self, n: u32) -> Duration {
        let base = self.backoff_base_ms as u64;
        let factor = 1u64.checked_shl(n).unwrap_or(u64::MAX);
        let raw = base.saturating_mul(factor);
        let jitter: f64 = rand::thread_rng().gen_range(0.8..=1.2);
        let ms = (raw as f64 * jitter) as u64;
        Duration::from_millis(ms)
    }
}

#[async_trait]
impl WorkQueue for StoreWorkQueue {
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError> {
        validate_job(&job)?;
        self.ensure_and_use(&job.scope.tenant).await?;

        // Idempotent enqueue: if a key is present and a row with the same (scope, idempotency_key)
        // already exists in any status, return its id without inserting (SA-IDEM-01 at the queue
        // layer).
        if let Some(key) = job.idempotency_key.as_ref() {
            if let Some(existing) = self.find_by_idem(&job.scope, key).await? {
                tracing::debug!(target: "recall", job_kind = %kind_str(job.kind), dedup = true, "queue.enqueue");
                return Ok(existing);
            }
        }

        let now = Utc::now();
        let row = job_to_object(&job, now);
        let thing = id_thing(&job.id)?;
        let insert = self
            .db
            .query("CREATE $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(row)))
            .await
            .map_err(db_to_queue)?
            .check();
        if let Err(e) = insert {
            // The UNIQUE index on (scope, idempotency_key) is a second-line guard against a concurrent
            // duplicate racing past the dedup select; on a unique violation, re-run the select and
            // return the winning id rather than surfacing the error.
            if let Some(key) = job.idempotency_key.as_ref() {
                if let Some(existing) = self.find_by_idem(&job.scope, key).await? {
                    return Ok(existing);
                }
            }
            return Err(db_to_queue(e));
        }
        tracing::info!(target: "recall", job_id = %job.id, kind = %kind_str(job.kind), "queue.enqueue");
        Ok(job.id)
    }

    async fn claim(
        &self,
        kinds: &[JobKind],
        lease: Duration,
    ) -> Result<Option<WorkJob>, QueueError> {
        if kinds.is_empty() {
            return Ok(None);
        }
        // The active namespace is whichever tenant the worker is polling; a worker claims within a
        // tenant it has already provisioned. Mirror the store: select the tenant of the first kind's
        // caller is not available here, so the caller must have set the namespace via a prior enqueue
        // on this connection. We do not switch namespace in claim (cross-tenant claim is impossible —
        // the active namespace handle is the tenant boundary, Security note).
        let kind_strs: Vec<String> = kinds.iter().map(|k| kind_str(*k).to_string()).collect();
        let now = Utc::now();

        // Atomic single-winner claim. SurrealDB 3.x `UPDATE` does not accept a `LIMIT`, so a single
        // claimable id is chosen by an inner `SELECT … LIMIT 1` and the outer `UPDATE` targets exactly
        // that record while *re-checking the claimable predicate in its own WHERE*. The conditional
        // flip is evaluated atomically per record by the storage transaction: two workers that pick
        // the same id race on the outer UPDATE, but only the first satisfies the predicate
        // (status='pending' OR expired lease); the second sees status='leased' AND leased_until >= now
        // and updates nothing — so the job is granted to exactly one worker. `RETURN AFTER` yields the
        // leased row to the winner and an empty result to the loser.
        //
        // Under true concurrency the SurrealKV transaction layer may reject the loser's write with a
        // "transaction conflict" rather than silently no-op it (optimistic concurrency control). That
        // conflict is the engine enforcing single-winner; the loser retries a bounded number of times
        // (a later pass observes the row already leased and returns no row) and, if every attempt
        // conflicts, returns Ok(None) — exactly one worker still ends up with the job.
        const MAX_CLAIM_ATTEMPTS: u32 = 5;
        let mut last_conflict = false;
        for attempt in 0..MAX_CLAIM_ATTEMPTS {
            let now = if attempt == 0 { now } else { Utc::now() };
            let lease_until =
                now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::zero());
            let result = self
                .db
                .query(
                    "UPDATE work_job SET status = 'leased', leased_until = $lease_until \
                     WHERE id IN (SELECT VALUE id FROM work_job \
                         WHERE kind IN $kinds AND not_before <= $now \
                           AND (status = 'pending' OR (status = 'leased' AND leased_until < $now)) \
                         LIMIT 1) \
                       AND kind IN $kinds AND not_before <= $now \
                       AND (status = 'pending' OR (status = 'leased' AND leased_until < $now)) \
                     RETURN AFTER",
                )
                .bind(("kinds", kind_strs.clone()))
                .bind(("now", dt(now)))
                .bind(("lease_until", dt(lease_until)))
                .await
                .and_then(|mut resp| resp.take::<Vec<Json>>(0));

            match result {
                Ok(rows) => match rows.into_iter().next() {
                    Some(row) => {
                        let job = row_to_job(row)?;
                        tracing::debug!(target: "recall", claimed = true, job_id = %job.id, "queue.claim");
                        return Ok(Some(job));
                    }
                    None => {
                        tracing::debug!(target: "recall", claimed = false, "queue.claim");
                        return Ok(None);
                    }
                },
                Err(e) if is_conflict(&e) => {
                    last_conflict = true;
                    continue;
                }
                Err(e) => return Err(db_to_queue(e)),
            }
        }
        // Every attempt conflicted: another worker won the contended row. Treat as no claim this round.
        if last_conflict {
            tracing::debug!(target: "recall", claimed = false, conflict = true, "queue.claim");
        }
        Ok(None)
    }

    async fn complete(&self, job_id: &str) -> Result<(), QueueError> {
        let thing = id_thing(job_id)?;
        let mut resp = self
            .db
            .query("UPDATE $id SET status = 'done', leased_until = NONE WHERE status = 'leased' RETURN BEFORE")
            .bind(("id", thing))
            .await
            .map_err(db_to_queue)?;
        let updated: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("complete: {e}")))?;
        if updated.is_empty() {
            return Err(self.diagnose_missing(job_id).await);
        }
        tracing::info!(target: "recall", job_id = %job_id, "queue.complete");
        Ok(())
    }

    async fn fail(&self, job_id: &str, retryable: bool) -> Result<(), QueueError> {
        let current = match self.load_job(job_id).await? {
            Some(j) => j,
            None => return Err(QueueError::JobNotFound(job_id.to_string())),
        };
        if current.status != JobStatus::Leased {
            return Err(QueueError::NotLeased(job_id.to_string()));
        }

        let now = Utc::now();
        let thing = id_thing(job_id)?;
        if retryable && current.attempts < self.max_attempts {
            let next = current.attempts + 1;
            let delay = self.backoff(next);
            let not_before =
                now + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
            self.db
                .query(
                    "UPDATE $id SET attempts = $next, status = 'pending', leased_until = NONE, \
                     not_before = $not_before",
                )
                .bind(("id", thing))
                .bind(("next", next as i64))
                .bind(("not_before", dt(not_before)))
                .await
                .map_err(db_to_queue)?
                .check()
                .map_err(db_to_queue)?;
            tracing::warn!(
                target: "recall",
                job_id = %job_id,
                attempts = next,
                delay_ms = delay.as_millis() as u64,
                "queue.fail.retry"
            );
            return Ok(());
        }

        // Not retryable, or attempts exhausted: dead-letter and park a copy.
        self.db
            .query("UPDATE $id SET status = 'dead_letter', leased_until = NONE")
            .bind(("id", thing))
            .await
            .map_err(db_to_queue)?
            .check()
            .map_err(db_to_queue)?;
        let dl = dead_letter_object(&current, now);
        self.db
            .query("CREATE dead_letter CONTENT $rec")
            .bind(("rec", Value::Object(dl)))
            .await
            .map_err(db_to_queue)?
            .check()
            .map_err(db_to_queue)?;
        let reason = if retryable {
            "attempts_exhausted"
        } else {
            "non_retryable"
        };
        tracing::error!(
            target: "recall",
            job_id = %job_id,
            attempts = current.attempts,
            reason,
            "queue.fail.dead_letter"
        );
        Ok(())
    }
}

impl StoreWorkQueue {
    /// Select an existing job id for `(scope, idempotency_key)` in the active namespace, if one exists.
    async fn find_by_idem(
        &self,
        scope: &ScopeRef,
        key: &str,
    ) -> Result<Option<String>, QueueError> {
        // Match the scope sub-fields individually rather than comparing whole objects: SurrealDB may
        // store an optional `team = NONE` field by omitting it, so an object-equality comparison
        // against a `$scope` that carries an explicit `team: NONE` would not match. `team` is compared
        // with an explicit NONE-aware predicate so a None team only matches a None team.
        let team_pred = match &scope.team {
            Some(_) => "scope.team = $team",
            None => "scope.team IS NONE",
        };
        let sql = format!(
            "SELECT id FROM work_job WHERE scope.tenant = $tenant AND scope.user = $user \
             AND {team_pred} AND idempotency_key = $key LIMIT 1"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("tenant", scope.tenant.clone()))
            .bind(("user", scope.user.clone()))
            .bind(("key", key.to_string()));
        if let Some(team) = &scope.team {
            q = q.bind(("team", team.clone()));
        }
        let mut resp = q.await.map_err(db_to_queue)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("dedup select: {e}")))?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.get("id").and_then(normalise_id)))
    }

    /// Load a single job row by id from the active namespace.
    async fn load_job(&self, job_id: &str) -> Result<Option<WorkJob>, QueueError> {
        let thing = id_thing(job_id)?;
        let mut resp = self
            .db
            .query("SELECT * FROM $id")
            .bind(("id", thing))
            .await
            .map_err(db_to_queue)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("load job: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(row_to_job(row)?)),
            None => Ok(None),
        }
    }

    /// Read a single job by id from the active namespace (test-support introspection; the queue
    /// otherwise exposes only the trait surface + reaper). Returns `None` when absent.
    #[doc(hidden)]
    pub async fn peek(&self, job_id: &str) -> Result<Option<WorkJob>, QueueError> {
        self.load_job(job_id).await
    }

    /// Count jobs in the active namespace's `work_job` table (test-support introspection).
    #[doc(hidden)]
    pub async fn count_jobs(&self) -> Result<u64, QueueError> {
        let mut resp = self
            .db
            .query("SELECT count() AS c FROM work_job GROUP ALL")
            .await
            .map_err(db_to_queue)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("count: {e}")))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// Count dead-letter rows whose original job id matches (test-support introspection).
    #[doc(hidden)]
    pub async fn count_dead_letter(&self, original_job_id: &str) -> Result<u64, QueueError> {
        let mut resp = self
            .db
            .query("SELECT count() AS c FROM dead_letter WHERE original_job_id = $oid GROUP ALL")
            .bind(("oid", original_job_id.to_string()))
            .await
            .map_err(db_to_queue)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("count dl: {e}")))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0))
    }

    /// Force a leased job's `leased_until` to a past instant (test-support: simulate a crashed worker
    /// whose lease has expired, so the reaper can reclaim it).
    #[doc(hidden)]
    pub async fn expire_lease(&self, job_id: &str) -> Result<(), QueueError> {
        let thing = id_thing(job_id)?;
        let past = Utc::now() - chrono::Duration::seconds(5);
        self.db
            .query("UPDATE $id SET leased_until = $past WHERE status = 'leased'")
            .bind(("id", thing))
            .bind(("past", dt(past)))
            .await
            .map_err(db_to_queue)?
            .check()
            .map_err(db_to_queue)?;
        Ok(())
    }

    /// Force a pending job's `not_before` to now (test-support: skip a retry backoff window so a test
    /// can re-claim immediately instead of sleeping for real).
    #[doc(hidden)]
    pub async fn fast_forward(&self, job_id: &str) -> Result<(), QueueError> {
        let thing = id_thing(job_id)?;
        self.db
            .query("UPDATE $id SET not_before = $now")
            .bind(("id", thing))
            .bind(("now", dt(Utc::now())))
            .await
            .map_err(db_to_queue)?
            .check()
            .map_err(db_to_queue)?;
        Ok(())
    }

    /// Distinguish a missing-row from a wrong-status outcome after a guarded `complete` updated nothing.
    async fn diagnose_missing(&self, job_id: &str) -> QueueError {
        match self.load_job(job_id).await {
            Ok(Some(_)) => QueueError::NotLeased(job_id.to_string()),
            Ok(None) => QueueError::JobNotFound(job_id.to_string()),
            Err(e) => e,
        }
    }
}

/// Lease-reaper: a long-running task that periodically reverts expired leases to `Pending` across all
/// tenant namespaces so a crashed worker's job is reclaimed. Runs until the cancellation token fires.
pub struct LeaseReaper {
    db: Surreal<Any>,
    embed_dim: u32,
    /// Reaper sweep cadence (`RECALL_QUEUE_REAPER_SECS`, default 30 s).
    interval: Duration,
}

impl LeaseReaper {
    /// Build a reaper bound to a shared store connection.
    pub fn new(db: Surreal<Any>, embed_dim: u32, interval: Duration) -> Self {
        Self {
            db,
            embed_dim,
            interval,
        }
    }

    /// One sweep across every tenant namespace: revert `status='leased' AND leased_until < now` back
    /// to `Pending` (leased_until NONE). A per-namespace store failure is logged and skipped so one
    /// slow tenant does not stall the sweep; only an all-namespaces failure surfaces an error. Returns
    /// the total count reclaimed.
    pub async fn reap_once(&self) -> Result<u64, QueueError> {
        let tenants = self.list_tenants().await?;
        let mut reclaimed: u64 = 0;
        let mut failures = 0usize;
        let total = tenants.len();
        let now = Utc::now();
        for tenant in &tenants {
            if validate_tenant(tenant).is_err() {
                continue;
            }
            // Provision (idempotent) and select the namespace before the sweep so a tenant whose queue
            // table does not yet exist is skipped cleanly rather than erroring.
            if let Err(e) = Migrator::new(&self.db, self.embed_dim).migrate_up(tenant).await {
                tracing::warn!(target: "recall", tenant = %tenant, error = %e, "queue.reap.skip");
                failures += 1;
                continue;
            }
            if self
                .db
                .use_ns(tenant.clone())
                .use_db(DB_NAME)
                .await
                .is_err()
            {
                failures += 1;
                continue;
            }
            let swept = self
                .db
                .query(
                    "UPDATE work_job SET status = 'pending', leased_until = NONE \
                     WHERE status = 'leased' AND leased_until < $now RETURN BEFORE",
                )
                .bind(("now", dt(now)))
                .await;
            match swept {
                Ok(mut resp) => {
                    let rows: Vec<Json> = resp.take(0).unwrap_or_default();
                    let n = rows.len() as u64;
                    if n > 0 {
                        tracing::info!(target: "recall", tenant = %tenant, reclaimed = n, "queue.reap");
                    }
                    reclaimed += n;
                }
                Err(e) => {
                    tracing::warn!(target: "recall", tenant = %tenant, error = %e, "queue.reap.skip");
                    failures += 1;
                }
            }
        }
        if total > 0 && failures == total {
            return Err(QueueError::BackendUnavailable(
                "lease-reaper: all tenant namespaces failed".into(),
            ));
        }
        Ok(reclaimed)
    }

    /// Sweep loop: every `interval`, run `reap_once` (swallowing per-namespace failures, already
    /// logged); stop when the cancellation token fires.
    pub async fn run(self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.reap_once().await {
                        tracing::warn!(target: "recall", error = %e, "queue.reap.sweep_failed");
                    }
                }
                _ = cancel.cancelled() => return,
            }
        }
    }

    /// Enumerate tenant namespaces known to the shared connection (mirrors `Store::list_tenants`).
    async fn list_tenants(&self) -> Result<Vec<String>, QueueError> {
        let mut resp = self
            .db
            .query("INFO FOR ROOT")
            .await
            .map_err(db_to_queue)?;
        let info: Vec<Json> = resp
            .take(0)
            .map_err(|e| QueueError::BackendUnavailable(format!("list tenants: {e}")))?;
        let mut out = Vec::new();
        if let Some(ns) = info
            .first()
            .and_then(|v| v.get("namespaces"))
            .and_then(|v| v.as_object())
        {
            out.extend(ns.keys().cloned());
        }
        Ok(out)
    }
}

// --- Validation ------------------------------------------------------------------------------------

/// Validate a job structurally before enqueue: `id` matches `work_job:<key>`, `payload` is a JSON
/// value (always true for `serde_json::Value`), and `scope.tenant`/`scope.user` are non-empty. The
/// `kind` is a known `JobKind` by construction (it is an enum). On failure returns
/// `QueueError::InvalidJob`.
fn validate_job(job: &WorkJob) -> Result<(), QueueError> {
    let (table, key) = job
        .id
        .split_once(':')
        .ok_or_else(|| QueueError::InvalidJob(format!("malformed id: {}", job.id)))?;
    if table != "work_job" || key.is_empty() {
        return Err(QueueError::InvalidJob(format!("id must be work_job:<key>: {}", job.id)));
    }
    if job.scope.tenant.trim().is_empty() {
        return Err(QueueError::InvalidJob("scope.tenant is empty".into()));
    }
    if job.scope.user.trim().is_empty() {
        return Err(QueueError::InvalidJob("scope.user is empty".into()));
    }
    Ok(())
}

// --- Row <-> domain conversion ---------------------------------------------------------------------

/// The canonical snake_case string for a `JobKind` (matches the serde rename used on the wire and the
/// `work_job.kind` assertion in the 0001_init migration).
fn kind_str(k: JobKind) -> &'static str {
    match k {
        JobKind::ExtractFact => "extract_fact",
        JobKind::ReEmbedFact => "re_embed_fact",
        JobKind::Consolidate => "consolidate",
        JobKind::HardDelete => "hard_delete",
    }
}

/// The canonical snake_case string for a `JobStatus`.
fn status_str(s: JobStatus) -> &'static str {
    match s {
        JobStatus::Pending => "pending",
        JobStatus::Leased => "leased",
        JobStatus::Done => "done",
        JobStatus::DeadLetter => "dead_letter",
    }
}

/// Convert a chrono UTC timestamp into a native SurrealDB datetime value.
fn dt(at: DateTime<Utc>) -> Value {
    Value::Datetime(Datetime::from(at))
}

/// Convert an `i64` into a native SurrealDB integer value.
fn int(v: i64) -> Value {
    Value::Number(surrealdb::types::Number::Int(v))
}

/// Build the native SurrealDB object for a `ScopeRef`.
fn scope_obj(scope: &ScopeRef) -> Object {
    let mut o = Object::new();
    o.insert("tenant", Value::String(scope.tenant.clone()));
    o.insert(
        "team",
        match &scope.team {
            Some(t) => Value::String(t.clone()),
            None => Value::None,
        },
    );
    o.insert("user", Value::String(scope.user.clone()));
    o
}

/// Build the native row object for a `WorkJob` on enqueue (without the `id`, which is the record key):
/// status Pending, attempts 0, not_before = created_at = now, leased_until NONE.
fn job_to_object(job: &WorkJob, now: DateTime<Utc>) -> Object {
    use surrealdb::types::SurrealValue;
    let mut o = Object::new();
    o.insert("kind", Value::String(kind_str(job.kind).to_string()));
    o.insert("payload", job.payload.clone().into_value());
    o.insert("scope", Value::Object(scope_obj(&job.scope)));
    o.insert(
        "idempotency_key",
        match &job.idempotency_key {
            Some(k) => Value::String(k.clone()),
            None => Value::None,
        },
    );
    o.insert("attempts", int(0));
    o.insert("status", Value::String(status_str(JobStatus::Pending).to_string()));
    o.insert("not_before", dt(now));
    o.insert("created_at", dt(now));
    o.insert("leased_until", Value::None);
    o
}

/// Build the native row object for a `dead_letter` entry copied from an exhausted/non-retryable job.
fn dead_letter_object(job: &WorkJob, now: DateTime<Utc>) -> Object {
    use surrealdb::types::SurrealValue;
    let mut o = Object::new();
    o.insert("kind", Value::String(kind_str(job.kind).to_string()));
    o.insert("payload", job.payload.clone().into_value());
    o.insert("scope", Value::Object(scope_obj(&job.scope)));
    o.insert(
        "idempotency_key",
        match &job.idempotency_key {
            Some(k) => Value::String(k.clone()),
            None => Value::None,
        },
    );
    o.insert("attempts", int(job.attempts as i64));
    o.insert("original_job_id", Value::String(job.id.clone()));
    o.insert("created_at", dt(job.created_at));
    o.insert("dead_lettered_at", dt(now));
    o
}

/// Deserialise a `work_job` row (taken as JSON) into a domain `WorkJob`. SurrealDB renders record ids
/// to "table:key" strings and datetimes to RFC3339, which serde + chrono parse natively.
fn row_to_job(mut row: Json) -> Result<WorkJob, QueueError> {
    fix_id(&mut row);
    serde_json::from_value(row).map_err(|e| QueueError::BackendUnavailable(format!("decode job: {e}")))
}

/// Build the SurrealDB record-id literal for a "table:key" id as a parameter-bindable record-id value.
/// The id is split on the first ':' so an arbitrary uuid key (which may contain '-') is bound as the
/// record key, never interpolated.
fn id_thing(id: &str) -> Result<Value, QueueError> {
    let (table, key) = id
        .split_once(':')
        .ok_or_else(|| QueueError::InvalidJob(format!("malformed id: {id}")))?;
    Ok(Value::RecordId(surrealdb::types::RecordId::new(
        table.to_string(),
        key.to_string(),
    )))
}

/// Normalise a row's `id` JSON value to the canonical "table:key" string, stripping any angle brackets
/// or backticks the SQL form may add.
fn normalise_id(raw: &Json) -> Option<String> {
    raw.as_str()
        .map(|s| s.replace(['⟨', '⟩', '`'], "").trim().to_string())
}

/// Normalise the `id` field of a row in place to a clean "table:key" string.
fn fix_id(row: &mut Json) {
    if let Some(obj) = row.as_object_mut() {
        if let Some(id) = obj.get("id") {
            if let Some(norm) = normalise_id(id) {
                obj.insert("id".to_string(), Json::String(norm));
            }
        }
    }
}

// --- Error mapping ---------------------------------------------------------------------------------

/// Map a raw SurrealDB error to a `QueueError::BackendUnavailable` (X1: -> 503 QUEUE_UNAVAILABLE).
fn db_to_queue(e: surrealdb::Error) -> QueueError {
    QueueError::BackendUnavailable(e.to_string())
}

/// Detect a SurrealDB optimistic-concurrency write conflict, which the claim path treats as "another
/// worker won this row" rather than a backend failure.
fn is_conflict(e: &surrealdb::Error) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("conflict")
}

/// Map a `StoreError` raised by the shared Migrator to a `QueueError`. A migration failure means the
/// backend could not be provisioned (-> 503).
fn store_to_queue(e: StoreError) -> QueueError {
    QueueError::BackendUnavailable(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::scope::ScopeRef;

    fn scope(tenant: &str, user: &str) -> ScopeRef {
        ScopeRef {
            tenant: tenant.into(),
            team: None,
            user: user.into(),
        }
    }

    fn job(id: &str, kind: JobKind, tenant: &str, user: &str, key: Option<&str>) -> WorkJob {
        WorkJob {
            id: id.into(),
            kind,
            payload: serde_json::json!({"k": "v"}),
            scope: scope(tenant, user),
            idempotency_key: key.map(|s| s.to_string()),
            attempts: 0,
            status: JobStatus::Pending,
            not_before: Utc::now(),
            created_at: Utc::now(),
            leased_until: None,
        }
    }

    #[test]
    fn validate_job_rejects_bad_id() {
        let mut j = job("bad-id", JobKind::ExtractFact, "acme", "u-1", None);
        assert!(matches!(validate_job(&j), Err(QueueError::InvalidJob(_))));
        j.id = "work_job:".into();
        assert!(matches!(validate_job(&j), Err(QueueError::InvalidJob(_))));
        j.id = "fact:abc".into();
        assert!(matches!(validate_job(&j), Err(QueueError::InvalidJob(_))));
    }

    #[test]
    fn validate_job_rejects_empty_scope() {
        let mut j = job("work_job:abc", JobKind::ExtractFact, "", "u-1", None);
        assert!(matches!(validate_job(&j), Err(QueueError::InvalidJob(_))));
        j.scope.tenant = "acme".into();
        j.scope.user = "".into();
        assert!(matches!(validate_job(&j), Err(QueueError::InvalidJob(_))));
    }

    #[test]
    fn validate_job_accepts_well_formed() {
        let j = job("work_job:abc", JobKind::ExtractFact, "acme", "u-1", Some("k-1"));
        assert!(validate_job(&j).is_ok());
    }

    #[tokio::test]
    async fn backoff_grows_and_respects_jitter_bounds() {
        let q = StoreWorkQueue::new_in_memory(8, 5, 10).await.expect("queue");
        // backoff(n) ~= 10ms * 2^n, jittered to [0.8,1.2]. For n=1: base 20ms -> [16,24].
        for _ in 0..50 {
            let d = q.backoff(1).as_millis();
            assert!((16..=24).contains(&d), "n=1 backoff {d}ms outside [16,24]");
        }
        for _ in 0..50 {
            let d = q.backoff(3).as_millis();
            // base 10 * 2^3 = 80 -> [64,96].
            assert!((64..=96).contains(&d), "n=3 backoff {d}ms outside [64,96]");
        }
    }

    #[tokio::test]
    async fn enqueue_claim_complete_round_trip() {
        let q = StoreWorkQueue::new_in_memory(8, 5, 10).await.expect("queue");
        let j = job("work_job:rt-1", JobKind::ExtractFact, "acme", "u-1", None);
        let id = q.enqueue(j).await.expect("enqueue");
        assert_eq!(id, "work_job:rt-1");
        let claimed = q
            .claim(&[JobKind::ExtractFact], Duration::from_secs(30))
            .await
            .expect("claim")
            .expect("a job");
        assert_eq!(claimed.id, "work_job:rt-1");
        assert_eq!(claimed.status, JobStatus::Leased);
        assert!(claimed.leased_until.is_some());
        q.complete("work_job:rt-1").await.expect("complete");
        let after = q.load_job("work_job:rt-1").await.expect("load").expect("present");
        assert_eq!(after.status, JobStatus::Done);
        assert!(after.leased_until.is_none());
    }

    #[tokio::test]
    async fn idempotent_enqueue_returns_same_id() {
        let q = StoreWorkQueue::new_in_memory(8, 5, 10).await.expect("queue");
        let j1 = job("work_job:idem-1", JobKind::ExtractFact, "acme", "u-1", Some("k-1"));
        let j2 = job("work_job:idem-2", JobKind::ExtractFact, "acme", "u-1", Some("k-1"));
        let id1 = q.enqueue(j1).await.expect("enqueue 1");
        let id2 = q.enqueue(j2).await.expect("enqueue 2");
        assert_eq!(id1, "work_job:idem-1");
        assert_eq!(id2, "work_job:idem-1", "second enqueue must dedup to the first id");
    }
}
