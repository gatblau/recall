### SPEC: Durable Work Queue

**File:** `src/queue` | **Package:** `recall::queue` | **Phase:** 2 | **Dependencies:** C1 Memory Store

> **Mode:** greenfield
> **derivedFromHld:** 0.6.0

#### Purpose

The Durable Work Queue decouples the asynchronous write and maintenance path from the synchronous request, so a slow or failed write never blocks a read (ADR-004) and every unit of background work is retry-safe. It is the single hand-off seam between producers (HTTP API Edge enqueuing `ExtractFact`/`HardDelete`) and consumers (Write Pipeline claiming `ExtractFact`, Maintenance Worker claiming `ReEmbedFact`/`HardDelete`). It implements the `WorkQueue` trait (Phase 2C.6) plus a lease-reaper. The default backend is store-backed — a SurrealDB `work_job` table inside each tenant namespace governed by a claim/lease protocol (SA-QUEUE-01) — keeping the single-binary deployment intact; a `RECALL_QUEUE_BACKEND=nats` alternative sits behind the same trait so producers and consumers never change.

#### Approach

The default implementation is a **store-backed queue** layered on the same embedded SurrealDB engine the C1 Memory Store owns: each tenant namespace gets a `work_job` table, and claiming is a conditional (optimistic) `UPDATE` whose target id is chosen by an inner `SELECT … LIMIT 1` subquery and whose outer `WHERE` re-checks the claimable predicate, so SurrealKV optimistic concurrency grants exactly one matching `Pending`/expired-lease row to a single worker and two workers polling concurrently cannot both win the same job. This was chosen over (a) a dedicated broker (rejected for the default — adds a second process and breaks the single-binary story, SA-QUEUE-01; still reachable via the `nats` backend behind this same trait) and (b) an in-memory channel (rejected — loses jobs on crash, violating the retry-safe requirement of ADR-004). Retry uses exponential backoff with jitter and a dead-letter parking table (SA-QUEUE-02). The component owns its own state (the two tables) rather than delegating scheduling to an external timer, because the claim/lease/reaper protocol is self-contained inside the store.

#### Shared Context

The following are duplicated verbatim from Phase 2C so this spec is implementable alone.

**Work-queue types (Phase 2C.5, `src/types/job.rs`):**

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct WorkJob {
    pub id: String,                          // "work_job:<uuidv7>"
    pub kind: JobKind,
    pub payload: serde_json::Value,          // kind-specific, validated by the consumer
    pub scope: ScopeRef,
    pub idempotency_key: Option<String>,     // present for API-originated writes
    pub attempts: u32,
    pub status: JobStatus,
    pub not_before: DateTime<Utc>,           // backoff schedule (SA-QUEUE-02)
    pub created_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>, // claim lease; null when unclaimed
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind { ExtractFact, ReEmbedFact, HardDelete }  // ReReadSource removed by ADR-014; Consolidate removed by ADR-015

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus { Pending, Leased, Done, DeadLetter }
```

**Scope reference (Phase 2C.3, `src/types/scope.rs`):**

```rust
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef {
    pub tenant: String,            // tenant id -> SurrealDB namespace (ADR-011)
    pub team: Option<String>,      // team id, null for user-only facts
    pub user: String,              // user id, bound to the OIDC subject claim
}
```

**Work-queue trait (Phase 2C.6, `src/types/ports.rs`) — the contract this component implements:**

```rust
#[async_trait]
pub trait WorkQueue: Send + Sync {                    // impl: store-backed (C2)
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>; // returns job id
    async fn claim(&self, kinds: &[JobKind], lease: Duration)
        -> Result<Option<WorkJob>, QueueError>;
    async fn complete(&self, job_id: &str) -> Result<(), QueueError>;
    async fn fail(&self, job_id: &str, retryable: bool) -> Result<(), QueueError>;
}
```

**Typed application error mapping (Phase 2C.7) — `QueueError` maps into `AppError::Queue` and surfaces as HTTP `503 QUEUE_UNAVAILABLE`:**

```rust
#[error("queue: {0}")] Queue(#[from] QueueError),   // -> 503 QUEUE_UNAVAILABLE
```

**Configuration keys (Phase 2D) owned or consumed by this component:**

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_QUEUE_BACKEND` | enum `store\|nats` | `store` | no | Work-queue backend (SA-QUEUE-01). |
| `RECALL_QUEUE_NATS_URL` | url | _(unset)_ | conditional | Required iff `RECALL_QUEUE_BACKEND=nats`. |
| `RECALL_JOB_MAX_ATTEMPTS` | u32 | `5` | no | Retry cap before dead-letter (SA-QUEUE-02). |
| `RECALL_JOB_BACKOFF_BASE_MS` | u32 | `2000` | no | Backoff base for job retries. |

This spec additionally introduces two configuration keys for the reaper and claim polling that the Phase 2D table does not yet carry; they are listed under **Gaps**.

**Binding retry contract (SA-QUEUE-02):** a failed retryable job is retried with exponential backoff (base `RECALL_JOB_BACKOFF_BASE_MS`, factor 2, jitter ±20 %, cap `RECALL_JOB_MAX_ATTEMPTS` = 5 attempts), then moved to a `dead_letter` table for manual reprocessing.

#### Public Interface

This component exposes the `WorkQueue` trait implementation `StoreWorkQueue` and a standalone reaper task. The four trait methods plus the reaper are the full surface.

```rust
/// Store-backed WorkQueue implementation (RECALL_QUEUE_BACKEND=store). Built over a shared
/// `Surreal<Db>` handle to the same embedded SurrealDB engine the C1 Memory Store owns, rather than
/// over the `MemoryStore` trait object — the trait exposes no raw DB handle, and the claim/lease
/// protocol needs to issue its own SurrealQL against the namespaced connection.
pub struct StoreWorkQueue {
    db: Surreal<Db>,                    // shared connection (the engine handle is Arc-backed)
    embed_dim: u32,                     // RECALL_EMBED_DIM — needed to run the 0001_init migration
    max_attempts: u32,                  // RECALL_JOB_MAX_ATTEMPTS (default 5)
    backoff_base_ms: u32,               // RECALL_JOB_BACKOFF_BASE_MS (default 2000)
}

impl StoreWorkQueue {
    /// Build a queue over a shared store connection and the C2 retry configuration.
    pub fn new(db: Surreal<Db>, embed_dim: u32, max_attempts: u32, backoff_base_ms: u32) -> Self;
    /// Open an in-memory queue (the real engine, in-process) on its own SurrealDB connection for
    /// tests and ephemeral runs. Mirrors `Store::new_in_memory`.
    pub async fn new_in_memory(embed_dim: u32, max_attempts: u32, backoff_base_ms: u32)
        -> Result<Self, QueueError>;
    /// Build a `LeaseReaper` over the same shared connection and embedding dimension as this queue.
    pub fn reaper(&self, interval: Duration) -> LeaseReaper;
}

#[async_trait]
impl WorkQueue for StoreWorkQueue {
    /// Persist a WorkJob: status Pending, not_before = now, attempts = 0, leased_until = None.
    /// If job.idempotency_key is Some, deduplicate on the scope sub-fields plus the key: if a row
    /// already exists with the same scope.tenant, scope.user, scope.team and idempotency_key in any
    /// status, return that row's id without inserting (idempotent enqueue, SA-IDEM-01 at the queue
    /// layer). Returns the job id.
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>;

    /// Atomically claim one job: pick a single claimable id via an inner SELECT … LIMIT 1 subquery
    /// (kind ∈ kinds, not_before <= now, and (status = Pending OR (status = Leased AND
    /// leased_until < now))), then UPDATE exactly that record — re-checking the same predicate in
    /// the outer WHERE — to set status = Leased, leased_until = now + lease, and return it. Single
    /// winner is enforced by SurrealKV optimistic concurrency. Returns Ok(None) when no job is
    /// claimable.
    async fn claim(&self, kinds: &[JobKind], lease: Duration)
        -> Result<Option<WorkJob>, QueueError>;

    /// Mark a leased job Done (status = Done, leased_until = None).
    async fn complete(&self, job_id: &str) -> Result<(), QueueError>;

    /// Resolve a failed claim. If retryable AND attempts < max_attempts: attempts += 1,
    /// status = Pending, leased_until = None, not_before = now + backoff(attempts). Otherwise:
    /// status = DeadLetter, leased_until = None, and copy the row into the dead_letter table.
    async fn fail(&self, job_id: &str, retryable: bool) -> Result<(), QueueError>;
}

/// Lease-reaper: a long-running task that periodically reverts expired leases to Pending so a
/// crashed worker's job is reclaimed. Runs until the cancellation token fires.
pub struct LeaseReaper {
    db: Surreal<Db>,                    // shared connection — the same engine the queue writes
    embed_dim: u32,                     // RECALL_EMBED_DIM — to provision a namespace before sweeping
    interval: Duration,                 // reaper sweep cadence (RECALL_QUEUE_REAPER_SECS, default 30 s)
}

impl LeaseReaper {
    pub fn new(db: Surreal<Db>, embed_dim: u32, interval: Duration) -> Self;
    /// Sweep loop: every `interval`, run reap_once across all tenant namespaces; stop on cancel.
    pub async fn run(self, cancel: CancellationToken);
    /// One sweep: UPDATE work_job SET status=Pending, leased_until=NONE
    ///   WHERE status='leased' AND leased_until < now. Returns the count reclaimed.
    pub async fn reap_once(&self) -> Result<u64, QueueError>;
}

/// Typed errors produced by this component. Maps to AppError::Queue -> 503 QUEUE_UNAVAILABLE.
#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    #[error("queue backend unavailable: {0}")]
    BackendUnavailable(String),            // store/NATS connection or statement failure -> 503
    #[error("job not found: {0}")]
    JobNotFound(String),                   // complete/fail referenced an unknown job id -> 503-class internal
    #[error("job not leased: {0}")]
    NotLeased(String),                     // complete/fail on a job not in Leased status
    #[error("invalid job: {0}")]
    InvalidJob(String),                    // enqueue payload failed structural validation
    #[error("backend misconfigured: {0}")]
    Misconfigured(String),                 // RECALL_QUEUE_BACKEND=nats without RECALL_QUEUE_NATS_URL
}
```

The exact SurrealQL claim pattern (optimistic single-winner claim) is:

```sql
-- claim: SurrealDB 3.x rejects a LIMIT on UPDATE, so a single claimable id is chosen by an inner
-- SELECT … LIMIT 1 subquery and the outer UPDATE targets exactly that record, re-checking the same
-- claimable predicate in its own WHERE. Single winner is enforced by SurrealKV optimistic
-- concurrency: two workers that pick the same id race on the outer UPDATE, only the first satisfies
-- the predicate, and the loser either updates nothing or is rejected with a transaction conflict.
UPDATE work_job
  SET status = 'leased', leased_until = $lease_until
  WHERE id IN (SELECT VALUE id FROM work_job
      WHERE kind IN $kinds AND not_before <= $now
        AND (status = 'pending' OR (status = 'leased' AND leased_until < $now))
      LIMIT 1)
    AND kind IN $kinds AND not_before <= $now
    AND (status = 'pending' OR (status = 'leased' AND leased_until < $now))
  RETURN AFTER;
```

`$lease_until` is computed in the caller as `now + lease`. The loser of a transaction conflict
retries a bounded number of times (`MAX_CLAIM_ATTEMPTS = 5`); if every attempt conflicts, `claim`
returns `Ok(None)` so exactly one worker still ends up with the job.

#### Example

Enqueue an extraction job, then a worker claims, fails it once (retryable), and finally completes it.

Input to `enqueue`:

```json
{
  "id": "work_job:018f4c2a-7b10-7e3a-9c11-7a2d9f0b1234",
  "kind": "extract_fact",
  "payload": {"content": {"subject": "Team Alpha", "predicate": "owns", "object": "orders table"}, "source": null},
  "scope": {"tenant": "acme", "team": "platform", "user": "u-42"},
  "idempotency_key": "remember-2026-06-20-abc",
  "attempts": 0,
  "status": "pending",
  "not_before": "2026-06-20T10:00:00.000Z",
  "created_at": "2026-06-20T10:00:00.000Z",
  "leased_until": null
}
```

`enqueue` returns `"work_job:018f4c2a-7b10-7e3a-9c11-7a2d9f0b1234"`.

`claim(&[JobKind::ExtractFact], Duration::from_secs(30))` at `2026-06-20T10:00:01.000Z` returns the job with `status = "leased"`, `leased_until = "2026-06-20T10:00:31.000Z"`.

`fail("work_job:018f4c2a-…", true)` (provider timeout, retryable) → `attempts = 1`, `status = "pending"`, `not_before = "2026-06-20T10:00:04.000Z"` (base 2000 ms · 2^1 = 4000 ms, jitter ±20 % puts it in `[10:00:04.200, 10:00:05.800]` after the +1 s claim moment; computed from `now`).

A later `claim` re-leases it; `complete("work_job:018f4c2a-…")` sets `status = "done"`.

#### Internal Logic

**`enqueue(job)`**

1. Validate the job structurally: `id` matches `work_job:<uuid>`, `kind` is a known `JobKind`, `payload` is a JSON value, `scope.tenant` and `scope.user` are non-empty. On failure return `QueueError::InvalidJob`. Logged at `warn` with `job_kind`, `correlation_id`.
2. If `job.idempotency_key` is `Some(key)`: run a scoped select matching the scope sub-fields individually rather than comparing whole objects (SurrealDB may store an optional `team = NONE` by omitting it, so an object-equality comparison against an explicit `team: NONE` would not match): `SELECT id FROM work_job WHERE scope.tenant = $tenant AND scope.user = $user AND <team_pred> AND idempotency_key = $key LIMIT 1`, where `<team_pred>` is `scope.team = $team` when `scope.team` is `Some` and `scope.team IS NONE` when it is `None`. If a row exists, return its id unchanged (no insert, no side effect) — idempotent enqueue (SA-IDEM-01 at the queue layer). Logged at `debug` with `dedup = true`.
3. Otherwise normalise the row to insert: `status = Pending`, `attempts = 0`, `not_before = now`, `created_at = now`, `leased_until = None`. `now` is the server clock (UTC, ms precision, SA-TIME-01).
4. Insert via the shared store connection, binding the record id and row as parameters: `CREATE $id CONTENT $rec` (the id is split on the first `:` and bound as a record-id value, never interpolated). On a store/connection failure return `QueueError::BackendUnavailable`. The unique index on `(scope, idempotency_key)` (Data Model) is a second-line guard against a concurrent duplicate insert racing past step 2; on a unique-violation, re-run the step-2 select and return the winning id.
5. Return the job id. Emit metric `recall_queue_enqueued_total{kind}` and log at `info` with `job_id`, `kind`, `correlation_id`.

**`claim(kinds, lease)`**

1. If `kinds` is empty, return `Ok(None)` (nothing requested). The claim runs against whichever tenant namespace the worker has already selected on this connection (set by a prior enqueue); `claim` does not switch namespace, so cross-tenant claim is impossible — the active namespace handle is the tenant boundary.
2. Run the conditional inner-SELECT/outer-UPDATE statement shown above against the active tenant namespace, binding `$kinds`, `$now`, `$lease_until`. The outer WHERE re-checks `(status = 'pending' OR (status = 'leased' AND leased_until < $now))` so the transition is conditional per record: a second worker evaluating the same row after the first has flipped it sees `status = 'leased' AND leased_until >= now` and updates nothing. Under true concurrency SurrealKV may instead reject the loser's write with a transaction conflict; that conflict is the engine enforcing single-winner. On a conflict, retry up to `MAX_CLAIM_ATTEMPTS = 5` (recomputing `now`/`$lease_until` each retry); if every attempt conflicts, return `Ok(None)`. On any other store failure return `QueueError::BackendUnavailable`.
3. If the statement returns a row, deserialise it into `WorkJob` and return `Ok(Some(job))`; emit `recall_queue_claimed_total{kind}`. If it returns nothing, return `Ok(None)`.
4. Logged at `debug` with `claimed = <bool>`, `job_id` when present, `conflict = true` when every attempt conflicted.

**`complete(job_id)`**

1. Run `UPDATE $job_id SET status = 'done', leased_until = NONE WHERE status = 'leased' RETURN BEFORE`. On a store failure return `QueueError::BackendUnavailable`.
2. If no row was updated, determine why: a `SELECT` shows the row absent → `QueueError::JobNotFound`; present but not `Leased` → `QueueError::NotLeased`.
3. Emit `recall_queue_completed_total{kind}`; log at `info` with `job_id`.

**`fail(job_id, retryable)`**

1. Load the job row by id (scoped to the active namespace). Absent → `QueueError::JobNotFound`. Present but `status != Leased` → `QueueError::NotLeased`. On a store failure → `QueueError::BackendUnavailable`.
2. If `retryable` **and** `job.attempts < max_attempts`: compute `next = job.attempts + 1`; `delay_ms = backoff(next)` where `backoff(n) = backoff_base_ms · 2^n` then a uniform jitter factor in `[0.8, 1.2]` is applied (SA-QUEUE-02); `not_before = now + delay_ms`. Run `UPDATE $job_id SET attempts = $next, status = 'pending', leased_until = NONE, not_before = $not_before`. Emit `recall_queue_retried_total{kind}`; log at `warn` with `job_id`, `attempts = next`, `delay_ms`.
3. Else (not retryable, or attempts exhausted): run `UPDATE $job_id SET status = 'dead_letter', leased_until = NONE`, then `CREATE dead_letter CONTENT $rec` with a copy of the row (`kind`, `payload`, `scope`, `idempotency_key`, `attempts`, `original_job_id = job.id`, `created_at = job.created_at`, `dead_lettered_at = now`). Emit `recall_queue_dead_lettered_total{kind}`; log at `error` with `job_id`, `attempts`, `reason = retryable ? "attempts_exhausted" : "non_retryable"`.

**`LeaseReaper::reap_once()`**

1. For each tenant namespace known to C1, run `UPDATE work_job SET status = 'pending', leased_until = NONE WHERE status = 'leased' AND leased_until < $now RETURN BEFORE`. On a store failure for a namespace, log at `warn` with `tenant` and continue to the next namespace (one slow tenant does not stall the sweep); accumulate the failure into the returned result only if every namespace fails, in which case return `QueueError::BackendUnavailable`.
2. Sum the reclaimed row counts. Emit `recall_queue_reaped_total{tenant}`; log at `info` with `reclaimed` when `> 0`.

**`LeaseReaper::run(cancel)`**

1. Loop: await either the `interval` tick or the `cancel` token. On tick, call `reap_once` and swallow per-namespace failures (already logged). On cancel, return.

#### Data Model

Two tables owned by this component, created inside **each tenant namespace** (ADR-011) so a tenant's queue is dropped with its namespace and never commingles with another tenant's jobs. The C2 tables are defined in the single squashed initial migration `migrations/0001_init.up.surql` (version 1), under its `C2 — Durable Work Queue` section — the previously separate `0002_queue` migration was folded into one initial migration before first release (FU-018, greenfield, no shipped DB). Every statement uses `IF NOT EXISTS` so the migration is idempotent. The DDL below is the as-built schemafull C2 slice of that migration; nested-JSON fields use SurrealDB 3.x's `TYPE object FLEXIBLE` form (3.x requires `FLEXIBLE` after `TYPE` and rejects un-enumerated nested keys on a bare `TYPE object`).

```sql
-- Durable work queue table (one per tenant namespace).
DEFINE TABLE IF NOT EXISTS work_job SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS kind            ON work_job TYPE string
  ASSERT $value IN ['extract_fact','re_embed_fact','re_read_source','consolidate','hard_delete'];
DEFINE FIELD IF NOT EXISTS payload         ON work_job TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS scope           ON work_job TYPE object;
DEFINE FIELD IF NOT EXISTS scope.tenant    ON work_job TYPE string;
DEFINE FIELD IF NOT EXISTS scope.team      ON work_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS scope.user      ON work_job TYPE string;
DEFINE FIELD IF NOT EXISTS idempotency_key ON work_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS attempts        ON work_job TYPE int    DEFAULT 0 ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS status          ON work_job TYPE string
  ASSERT $value IN ['pending','leased','done','dead_letter'];
DEFINE FIELD IF NOT EXISTS not_before      ON work_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS created_at      ON work_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS leased_until    ON work_job TYPE option<datetime>;

-- Claim-path index: the claim WHERE filters on status, kind, not_before; this composite index
-- serves the optimistic-claim scan so a polling worker reads a small slice, not the whole table.
DEFINE INDEX IF NOT EXISTS work_job_claim_idx ON work_job FIELDS status, kind, not_before;

-- Dedup index: idempotent enqueue requires (scope, idempotency_key) to be unique when the key
-- is present. SurrealDB allows multiple rows whose key is NONE; uniqueness binds only set keys.
DEFINE INDEX IF NOT EXISTS work_job_idem_idx ON work_job
  FIELDS scope, idempotency_key UNIQUE;

-- Dead-letter parking table (one per tenant namespace): jobs that exhausted retries (SA-QUEUE-02),
-- kept for manual reprocessing. Carries the full original row plus the time it was parked.
DEFINE TABLE IF NOT EXISTS dead_letter SCHEMAFULL;

DEFINE FIELD IF NOT EXISTS kind             ON dead_letter TYPE string;
DEFINE FIELD IF NOT EXISTS payload          ON dead_letter TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS scope            ON dead_letter TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS idempotency_key  ON dead_letter TYPE option<string>;
DEFINE FIELD IF NOT EXISTS attempts         ON dead_letter TYPE int;
DEFINE FIELD IF NOT EXISTS original_job_id  ON dead_letter TYPE string;
DEFINE FIELD IF NOT EXISTS created_at       ON dead_letter TYPE datetime;
DEFINE FIELD IF NOT EXISTS dead_lettered_at ON dead_letter TYPE datetime;

DEFINE INDEX IF NOT EXISTS dead_letter_scope_idx ON dead_letter FIELDS scope.tenant, dead_lettered_at;
```

**Rollback considerations (sql-safety layer):** the two C2 tables are dropped by the squashed down migration `migrations/0001_init.down.surql` (which reverts the whole initial schema, not the C2 slice alone), via `REMOVE TABLE work_job;` and `REMOVE TABLE dead_letter;`, which is **destructive** — dropping `work_job` discards every queued and leased job (uncommitted background work is lost) and dropping `dead_letter` discards parked jobs awaiting manual reprocessing. The down migration must therefore run only against an idle queue (no in-flight leases) and is a user action against a shared store (dry-run first, SA-MIG-01). `REMOVE INDEX work_job_idem_idx` is non-destructive to data but removes the dedup guard, so a concurrent duplicate enqueue could insert a second row until the index is restored; restore the index before re-enabling producers. Tightening `work_job` from schemaless to schemafull rejects any pre-existing row that violates the field assertions — verify no malformed rows exist before applying (SA-MIG-01).

#### Error Table

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| Store/NATS backend connection or statement failure during enqueue/claim/complete/fail/reap | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"work queue unavailable","correlation_id":"<uuid>"}}` |
| `RECALL_QUEUE_BACKEND=nats` with `RECALL_QUEUE_NATS_URL` unset (startup) | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"queue backend misconfigured","correlation_id":"<uuid>"}}` |
| `enqueue` job fails structural validation (bad id/kind/empty scope) | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"invalid work job","correlation_id":"<uuid>"}}` |
| `complete`/`fail` references an unknown `job_id` | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"work job not found","correlation_id":"<uuid>"}}` |
| `complete`/`fail` references a job that is not in `Leased` status | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"work job not leased","correlation_id":"<uuid>"}}` |

All `QueueError` variants map through `AppError::Queue` to HTTP `503 QUEUE_UNAVAILABLE` per the Phase 2C.7 mapping and the canonical error registry. This component is in-process infrastructure (no HTTP surface of its own); the status/code/body columns describe how its errors surface when a producing path (C8) maps them. The `message` field never leaks internal store state (rule C11): the human text is one of the five fixed strings above; the underlying `QueueError` `Display` (which may carry a store detail) is logged at `error`, never returned.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Durable Work Queue

  Scenario: Happy path — enqueue, claim, complete
    Given an empty work_job table in tenant namespace "acme"
    When a producer enqueues an ExtractFact job with scope {tenant:acme, team:platform, user:u-42} and no idempotency key
    Then enqueue returns a "work_job:<uuid>" id and the row has status Pending, attempts 0, leased_until null
    When a worker calls claim([ExtractFact], 30s)
    Then it receives that job with status Leased and leased_until = now + 30s
    When the worker calls complete on that job id
    Then the row has status Done and leased_until null

  Scenario: Edge case — idempotent enqueue deduplicates on (scope, idempotency_key)
    Given a Pending ExtractFact job already exists with scope {tenant:acme, user:u-42} and idempotency_key "k-1"
    When a producer enqueues a second job with the same scope and idempotency_key "k-1"
    Then no new row is inserted
    And enqueue returns the existing job's id

  Scenario: Edge case — concurrent claim grants the job to exactly one worker
    Given one Pending ExtractFact job whose not_before <= now
    When two workers call claim([ExtractFact], 30s) concurrently
    Then exactly one worker receives Some(job) and the other receives None
    And the job row ends with status Leased and a single leased_until value

  Scenario: Edge case — lease-reaper reclaims a crashed worker's job
    Given a Leased ReEmbedFact job whose leased_until is 5 seconds in the past
    When the lease-reaper runs a sweep
    Then the job row reverts to status Pending with leased_until null
    And a subsequent claim([ReEmbedFact], 30s) returns that job

  Scenario: Error path — retryable failure backs off then dead-letters at the attempt cap
    Given a Leased ExtractFact job with RECALL_JOB_MAX_ATTEMPTS = 5
    When the worker calls fail(job_id, retryable=true) and attempts is below 5
    Then attempts increments by 1, status becomes Pending, and not_before = now + backoff_base_ms * 2^attempts with +/-20% jitter
    When fail(job_id, retryable=true) is called with attempts already at 5
    Then status becomes DeadLetter, leased_until is null, and a copy is written to the dead_letter table

  Scenario: Error path — backend unavailable surfaces QUEUE_UNAVAILABLE
    Given the store backend is unreachable
    When a producer calls enqueue
    Then it returns QueueError::BackendUnavailable
    And the producing HTTP path maps it to status 503 with code QUEUE_UNAVAILABLE
```

#### Performance, Security, Observability

- **Performance targets:** `claim` is on the worker polling loop, so claim latency is the hot metric — target **p95 ≤ 20 ms** for the inner-SELECT/outer-UPDATE conditional claim, served by the `work_job_claim_idx` composite index on `(status, kind, not_before)`. `enqueue` target **p95 ≤ 15 ms** (one indexed dedup select + one insert). The reaper sweep is off the request path; cadence default 30 s (see Gaps: `RECALL_QUEUE_REAPER_SECS`), bounded so a crashed worker's lease is reclaimed within `lease + 30 s`. Memory: the component holds no in-RAM queue — state lives in the store — so its footprint is the C1 store handle plus per-call bindings.
- **Security:** every table lives inside the tenant namespace (ADR-011), so a tenant's jobs are structurally isolated and erased with the tenant; cross-tenant claim is impossible because `claim` runs against the active namespace handle. `scope` is carried on every job and never derived from a payload. Payloads may contain content destined for the write gate (untrusted, ADR-008) but are opaque to the queue — the queue neither parses nor logs `payload`. `idempotency_key` is treated as an opaque string (1–255 chars per SA-IDEM-01) and never logged. No secrets are read by this component except `RECALL_QUEUE_NATS_URL` when the `nats` backend is selected (env only, never logged).
- **Observability:** metrics `recall_queue_enqueued_total{kind}`, `recall_queue_claimed_total{kind}`, `recall_queue_completed_total{kind}`, `recall_queue_retried_total{kind}`, `recall_queue_dead_lettered_total{kind}`, `recall_queue_reaped_total{tenant}`, and gauge `recall_queue_depth{tenant,kind,status}`. Log fields on every operation: `job_id`, `kind`, `tenant`, `correlation_id`; on retry add `attempts`, `delay_ms`; on dead-letter add `reason`. Trace span names: `queue.enqueue`, `queue.claim`, `queue.complete`, `queue.fail`, `queue.reap`, each carrying `job.kind` and `job.id` attributes. Context (`context`/correlation id) propagates from the producing call into the span.

#### Gaps

None. *(All three drafting gaps were resolved in the Phase 3 reconciliation pass: `RECALL_QUEUE_REAPER_SECS` and `RECALL_QUEUE_POLL_MS` are now defined in Phase 2D, owner C2; the lease-reaper enumerates tenant namespaces over its own shared `Surreal<Db>` handle via an `INFO FOR ROOT` query — reading the `namespaces` map — mirroring `Store::list_tenants` rather than calling through a trait method.)*
