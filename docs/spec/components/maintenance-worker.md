### SPEC: Maintenance Worker
**File:** `src/maintenance` | **Package:** `recall::maintenance` | **Phase:** 4 | **Dependencies:** C1 (Memory Store), C2 (Durable Work Queue)

> **Mode:** greenfield
> **derivedFromHld:** 0.6.0

#### Purpose

The Maintenance Worker is the asynchronous, idle-biased keeper of memory truth and bounded size. It runs off the synchronous read path so that embedding latency never enters the budget a recall caller waits on (ADR-004). Per tenant it performs four duties: (1) **supersession** — detecting contradicting facts and ending the superseded fact's validity while recording successor links, never destructively (ADR-002); (2) **decay / forget** — applying the Ebbinghaus retrievability model with a salience floor so high-importance facts survive disuse and stale low-salience facts become prune candidates (SA-DECAY-01, ADR-006); (3) **re-embed** — refreshing embeddings for facts whose content changed or whose embedding-model version is stale (SA-EMBED-01); and (4) **verifiable hard delete** — removing a fact plus its derived summaries and embeddings and returning a `DeletionProof`, supporting the right to erasure (SA-DELETE-01, HLD 06). It exists because memory left unmaintained drowns retrieval, accumulates contradictions, and grows without bound. Recall holds no LLM (ADR-015), so server-side consolidation of episodic facts into semantic insights is not a maintenance duty; the agent writes consolidated insights itself as agent-stated facts (`MemoryClass::Consolidated`, `derived_from`).

#### Approach

The worker is structured as two cooperating drivers over a single shared duty set. A **scheduler driver** fires a full maintenance cycle per tenant on the idle-biased trigger (SA-MAINT-CADENCE-01): after `RECALL_IDLE_QUIET_SECS` of no writes for a tenant, with a hard fallback every `RECALL_MAINT_MAX_INTERVAL_SECS`. A **queue-consumer driver** claims `ReEmbedFact` and `HardDelete` jobs from the `WorkQueue` (C2) and dispatches each to the same duty functions the scheduler uses. Two alternatives were rejected: a write-path-synchronous maintenance pass (rejected — puts embedding latency on the read or write path, violating ADR-004); and a single global timer with no per-tenant idle signal (rejected — wastes store load on quiet tenants and starves busy ones, contradicting SA-MAINT-CADENCE-01). The decay maths and contradiction detection are pure functions isolated from I/O so they are unit-tested against case tables (ADR-007 auditability, ADR-010 algorithmic-core unit tests). Every destructive step is ordered last and gated on proof, so a failed cycle leaves prior memory intact.

#### Shared Context

Types, traits, config keys, and env vars this spec depends on, duplicated from Phase 2C / 2D so this section is implementable alone.

##### Domain types (from 2C.2)

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Fact {
    pub id: String,                       // "fact:<uuidv7>" (SA-ID-01)
    pub content: serde_json::Value,       // structured assertion (object), not free text
    pub entities: Vec<String>,            // entity ids this fact connects (>=1)
    pub source_id: Option<String>,        // "source:<uuidv7>" provenance, null for agent-stated
    pub memory_class: MemoryClass,
    pub visibility: Visibility,
    pub owner: ScopeRef,                  // owning (tenant, team, user)
    pub valid_from: DateTime<Utc>,        // RFC3339 ms (SA-TIME-01)
    pub valid_to: Option<DateTime<Utc>>,  // null = currently true (open interval)
    pub ingested_at: DateTime<Utc>,       // server-set
    pub confidence: f64,                  // [0,1] (SA-SCORE-01)
    pub salience: f64,                    // [0,1]
    pub stability: f64,                   // decay stability `s` (SA-DECAY-01), >=0.0
    pub supersedes: Option<String>,       // fact id this one replaces
    pub superseded_by: Option<String>,    // fact id replacing this one
    pub derived_from: Vec<String>,        // source fact ids (consolidated insights only)
    pub last_recalled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryClass { Episodic, Semantic, Consolidated }   // procedural rejected (SA-CLASS-01)

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Visibility { UserPrivate, TeamShared, TenantShared }
```

##### Scope (from 2C.3)

```rust
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef { pub tenant: String, pub team: Option<String>, pub user: String }

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

The worker is not driven by an inbound HTTP token. It constructs a **maintenance `ScopeContext`** per tenant from the tenant id alone: `teams = []`, `user = ""`, `token_jti = "maintenance"`, `allowed_ops = { read: true, write: true, forget: true }`, and a freshly-generated `correlation_id` per cycle or per job (built by `maint_ctx(tenant)`).

C1 recognises this shape as the maintenance/admin scope: `is_maintenance_scope(ctx)` returns true iff `ctx.token_jti == "maintenance"` and `ctx.user.is_empty()`. For such a scope, C1's `fact_read_filter(ctx)` reduces the per-user/visibility `WHERE` fragment to `"true"`, so maintenance reads (and the writes derived from them — decay-prune, supersession, and the hard-delete derived-collection) operate on the **whole tenant**: user-private and team-shared facts as well as tenant-shared. This resolves RISK-009 — without it, an empty-`user` scope would match only tenant-shared facts under the normal per-user filter, and decay/supersession/hard-delete would silently skip user-private and team-shared memory. Tenant isolation stays **structural**: every store method runs `ensure_and_use(ctx.tenant)` against the per-tenant namespace first, so a maintenance scope for one tenant can never read or mutate another tenant's facts.

##### Verifiable-deletion type (from 2C.4)

```rust
#[derive(Serialize)]
pub struct DeletionProof {
    pub deleted_at: DateTime<Utc>,
    pub record_id: String,
    pub derived_removed: Vec<String>,        // ids of removed derived summaries
    pub embeddings_removed: u32,
    pub digest: String,                      // sha256 hex over sorted removed ids (SA-DELETE-01)
}
```

##### Work-queue types (from 2C.5)

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct WorkJob {
    pub id: String,                          // "work_job:<uuidv7>"
    pub kind: JobKind,
    pub payload: serde_json::Value,          // kind-specific, validated by the consumer
    pub scope: ScopeRef,
    pub idempotency_key: Option<String>,
    pub attempts: u32,
    pub status: JobStatus,
    pub not_before: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind { ExtractFact, ReEmbedFact, HardDelete }  // ReReadSource removed by ADR-014; Consolidate removed by ADR-015

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus { Pending, Leased, Done, DeadLetter }
```

##### Traits consumed (from 2C.6) — exact signatures

```rust
#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn put_fact(&self, f: &Fact) -> Result<(), StoreError>;
    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError>;
    async fn end_validity(&self, ctx: &ScopeContext, id: &str, at: DateTime<Utc>)
        -> Result<(), StoreError>;
    async fn hard_delete(&self, ctx: &ScopeContext, id: &str)
        -> Result<DeletionProof, StoreError>;
    // ...plus the maintenance-scan surface this spec relies on (see "MemoryStore surface
    // required by C7" below). The full trait surface is owned by the C1 spec.
}

#[async_trait]
pub trait WorkQueue: Send + Sync {
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>;
    async fn claim(&self, kinds: &[JobKind], lease: Duration)
        -> Result<Option<WorkJob>, QueueError>;
    async fn complete(&self, job_id: &str) -> Result<(), QueueError>;
    async fn fail(&self, job_id: &str, retryable: bool) -> Result<(), QueueError>;
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, ProviderError>; // dim = RECALL_EMBED_DIM
}
```

`StoreError`, `QueueError`, `ProviderError` are owned by their respective component specs (C1, C2) and referenced here by name. Recall holds no LLM (ADR-015): server-side consolidation is removed, so this component consumes no `LlmClient` and defines no consolidation candidate or wire contract.

##### MemoryStore surface required by C7 (extension of 2C.6 owned by C1)

The worker needs read scans and field-update operations beyond the four signatures quoted above. These are part of the C1-owned `MemoryStore` trait surface (2C.6 states "entity/relationship/source CRUD, maintenance scans — full surface in C1 spec"). This spec depends on the following signatures existing exactly as written; any that C1 does not provide is recorded in *Gaps*.

```rust
// Read recent episodic facts for a tenant, newest-first, bounded by `limit`.
async fn scan_recent_episodes(&self, ctx: &ScopeContext, since: DateTime<Utc>, limit: u32)
    -> Result<Vec<Fact>, StoreError>;

// Read candidate fact pairs that may contradict, for a tenant. The store returns pairs of
// currently-valid (valid_to == null) facts sharing >=1 entity; the worker adjudicates the
// contradiction in-process via `detect_contradiction`.
async fn scan_contradiction_candidates(&self, ctx: &ScopeContext, limit: u32)
    -> Result<Vec<(Fact, Fact)>, StoreError>;

// Coarse decay prefilter: currently-valid facts with `salience < salience_floor`, oldest-touched
// first, bounded. The worker computes Ebbinghaus retrievability and the final prune decision.
async fn scan_decay_candidates(&self, ctx: &ScopeContext, salience_floor: f64, limit: u32)
    -> Result<Vec<Fact>, StoreError>;

// Read facts whose stored embedding-model version differs from `current_model_version`,
// or whose content changed since last embed, bounded.
async fn scan_reembed_candidates(&self, ctx: &ScopeContext, current_model_version: &str, limit: u32)
    -> Result<Vec<Fact>, StoreError>;

// Update mutable maintenance fields of a fact in place (no new record, no validity change):
// confidence, salience, stability, last_recalled_at, supersedes, superseded_by.
async fn update_fact_maintenance_fields(&self, ctx: &ScopeContext, f: &Fact)
    -> Result<(), StoreError>;

// Replace the stored embedding vector + record the embedding-model version for a fact.
async fn set_fact_embedding(&self, ctx: &ScopeContext, fact_id: &str, vector: &[f32], model_version: &str)
    -> Result<(), StoreError>;

// List every tenant namespace the worker must visit on a scheduled cycle.
async fn list_tenants(&self) -> Result<Vec<String>, StoreError>;
```

##### Config & env vars (from 2D, owner C7 unless noted)

| Variable | Type | Default | Description |
|---|---|---|---|
| `RECALL_SALIENCE_FLOOR` | f64 | `0.3` | Decay salience floor; a fact at or above this is never time-pruned (SA-DECAY-01). |
| `RECALL_DECAY_K` | f64 | `10.0` | Global decay constant `k` in `R = exp(-Δt/(s·k))`. |
| `RECALL_PRUNE_RETRIEVABILITY` | f64 | `0.05` | Retrievability `R` below which a low-salience fact becomes a prune candidate. |
| `RECALL_IDLE_QUIET_SECS` | u32 | `300` | Idle period with no tenant writes before a maintenance cycle (SA-MAINT-CADENCE-01). |
| `RECALL_MAINT_MAX_INTERVAL_SECS` | u32 | `21600` | Hard fallback maintenance interval (6 h). |
| `RECALL_EMBED_DIM` | u32 | `1024` | Embedding dimension; re-embed output length MUST equal this (SA-EMBED-01). |
| `RECALL_EMBED_MODEL_VERSION` | string | _(none)_ | **[LLD]** Identifier of the active embedding model; a fact embedded under a different version is a re-embed candidate. Recorded in *Gaps* (not present in 2D). |
| `RECALL_EMBED_URL` | url | _(none, required)_ | Embedding provider endpoint. |
| `RECALL_EMBED_API_KEY` | secret | _(none, required)_ | Embedding provider key (env only). |
| `RECALL_MAINT_BATCH_SIZE` | u32 | `200` | **[LLD]** Per-duty scan bound (`limit`) per cycle, capping embedding/store load. Recorded in *Gaps* (not present in 2D). |
| `RECALL_LOG_LEVEL` | enum | `info` | Structured-log verbosity. |

#### Public Interface

All entry points and the duty functions, with exact signatures. The decay maths and contradiction detection are pure (no I/O) so they are unit-testable in isolation.

##### Worker construction and drivers

```rust
pub struct MaintenanceWorker {
    store: Arc<dyn MemoryStore>,
    queue: Arc<dyn WorkQueue>,
    embed: Arc<dyn EmbeddingClient>,
    cfg: MaintenanceConfig,
}

#[derive(Clone)]
pub struct MaintenanceConfig {
    pub salience_floor: f64,        // RECALL_SALIENCE_FLOOR
    pub decay_k: f64,               // RECALL_DECAY_K
    pub prune_retrievability: f64,  // RECALL_PRUNE_RETRIEVABILITY
    pub idle_quiet: Duration,       // RECALL_IDLE_QUIET_SECS
    pub maint_max_interval: Duration, // RECALL_MAINT_MAX_INTERVAL_SECS
    pub embed_dim: u32,             // RECALL_EMBED_DIM
    pub embed_model_version: String,// RECALL_EMBED_MODEL_VERSION
    pub batch_size: u32,            // RECALL_MAINT_BATCH_SIZE
    pub reinforce_gain: f64,        // RECALL_REINFORCE_GAIN
}

impl MaintenanceWorker {
    pub fn new(
        store: Arc<dyn MemoryStore>,
        queue: Arc<dyn WorkQueue>,
        embed: Arc<dyn EmbeddingClient>,
        cfg: MaintenanceConfig,
    ) -> Self;

    /// Scheduler driver. Runs until `shutdown` is cancelled. Per tick, evaluates each tenant's
    /// idle/fallback trigger and runs a full maintenance cycle for those that are due.
    pub async fn run_scheduler(&self, shutdown: CancellationToken);

    /// Queue-consumer driver. Runs until `shutdown` is cancelled. Claims ReEmbedFact /
    /// HardDelete jobs and dispatches each to the matching handler.
    pub async fn run_consumer(&self, shutdown: CancellationToken);
}
```

##### Cycle entrypoint and job handlers

```rust
/// Full maintenance cycle for one tenant. Order is fixed and non-destructive-first:
/// supersession → decay → re-embed. Never deletes (delete is HardDelete-job only).
/// Returns a per-duty summary; an error in one duty is recorded in the summary and the cycle
/// continues to the next non-dependent duty (prior memory stays intact).
pub async fn run_cycle(&self, tenant: &str) -> Result<CycleReport, AppError>;

/// ReEmbedFact handler — re-embed one fact named in the job payload.
pub async fn handle_reembed(&self, scope: &ScopeRef, payload: &ReEmbedPayload)
    -> Result<(), AppError>;

/// HardDelete handler — verifiable deletion of one fact; returns the proof.
/// Not complete until a DeletionProof is obtained; a partial removal returns an error, never success.
pub async fn handle_hard_delete(&self, scope: &ScopeRef, payload: &HardDeletePayload)
    -> Result<DeletionProof, AppError>;
```

##### Duty functions (exact signatures)

```rust
/// Detect contradictions among currently-valid facts and supersede the older side
/// (end_validity + successor links). Non-destructive; history retained.
async fn supersede_contradictions(&self, ctx: &ScopeContext) -> Result<SupersessionReport, AppError>;

/// Apply graceful decay to a tenant's currently-valid facts; prune candidates that fall below the
/// retrievability AND salience floors (prune == end_validity, never destructive delete).
async fn decay_tenant(&self, ctx: &ScopeContext, now: DateTime<Utc>)
    -> Result<DecayReport, AppError>;

/// Re-embed a tenant's stale/changed facts in batches; output vector length MUST equal embed_dim.
async fn reembed_tenant(&self, ctx: &ScopeContext) -> Result<ReEmbedReport, AppError>;

// ---- Pure, I/O-free cores (unit-tested against case tables) ----

/// Ebbinghaus retrievability (SA-DECAY-01): R = exp(-delta_secs / (stability * k)).
/// `stability` is clamped to a minimum of `f64::MIN_POSITIVE` to avoid division by zero;
/// `delta_secs < 0` (clock skew) is clamped to 0, yielding R = 1.0.
pub fn retrievability(delta_secs: f64, stability: f64, k: f64) -> f64;

/// Prune-candidate predicate (SA-DECAY-01): true iff R < prune_retrievability AND salience < salience_floor.
pub fn is_prune_candidate(r: f64, salience: f64, prune_retrievability: f64, salience_floor: f64) -> bool;

/// Reinforcement on recall: raise stability and reset the decay clock.
/// new_stability = stability * (1.0 + reinforce_gain); returns (new_stability, last_recalled_at = now).
pub fn reinforce(stability: f64, reinforce_gain: f64, now: DateTime<Utc>)
    -> (f64, DateTime<Utc>);

/// Contradiction-detection contract. Given two currently-valid facts about the same subject,
/// decide whether `b` contradicts `a` and, if so, which one is superseded.
pub fn detect_contradiction(a: &Fact, b: &Fact) -> ContradictionVerdict;
```

##### Types owned by this spec

```rust
/// Result of comparing two facts for contradiction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ContradictionVerdict {
    NoConflict,
    Supersedes,   // `b` supersedes `a` (b is newer/more confident) — end a's validity
    SupersededBy, // `a` supersedes `b` — end b's validity
}

#[derive(Deserialize)]
pub struct ReEmbedPayload { pub fact_id: String }   // "fact:<uuidv7>"

#[derive(Deserialize)]
pub struct HardDeletePayload { pub fact_id: String } // "fact:<uuidv7>"

#[derive(Serialize, Default, Clone)]
pub struct CycleReport {
    pub supersession: SupersessionReport,
    pub decay: DecayReport,
    pub reembed: ReEmbedReport,
    pub failed_duties: Vec<String>,   // duty names that errored this cycle
}

#[derive(Serialize, Default, Clone)]
pub struct SupersessionReport { pub pairs_checked: u32, pub superseded: u32 }
#[derive(Serialize, Default, Clone)]
pub struct DecayReport { pub evaluated: u32, pub reinforced: u32, pub pruned: u32 }
#[derive(Serialize, Default, Clone)]
pub struct ReEmbedReport { pub evaluated: u32, pub reembedded: u32 }
```

##### Example

Verifiable hard delete (queue-driven). Job claimed by `run_consumer`:

```json
{
  "id": "work_job:018f...e1",
  "kind": "hard_delete",
  "payload": { "fact_id": "fact:018f3a2b-0000-7000-8000-000000000abc" },
  "scope": { "tenant": "acme", "team": null, "user": "u-42" },
  "status": "leased"
}
```

`handle_hard_delete` calls `MemoryStore::hard_delete(ctx, "fact:018f...abc")` and returns:

```json
{
  "deleted_at": "2026-06-20T12:00:00.000Z",
  "record_id": "fact:018f3a2b-0000-7000-8000-000000000abc",
  "derived_removed": ["fact:018f3a2b-0000-7000-8000-000000000def"],
  "embeddings_removed": 2,
  "digest": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

Decay maths example (pure core): `retrievability(delta_secs = 864000.0 /*10 days*/, stability = 1.0, k = 10.0)` → `exp(-864000 / (1.0 * 10.0))` underflows to `~0.0`; with `salience = 0.1` and floors `(0.05, 0.3)`, `is_prune_candidate` → `true`. The same fact with `salience = 0.9` → `false` (survives disuse, ADR-006).

#### Internal Logic

##### `run_scheduler`

As built, the per-tenant `last_cycle_at` fallback clock is held in an in-memory `HashMap<String, tokio::time::Instant>` for the lifetime of the loop. The `maintenance_state` table (see *Data Model*) is a future-persistence seam the worker does not yet read or write; v1 cycle bookkeeping is in-memory and resets on restart, which forces every tenant to be treated as due on the next tick after a restart (a benign re-run, not data loss). This is an intentional as-built decision: the worker holds only the four injected seams (`store`, `queue`, `llm`, `embed`) and depends on no `maintenance_state` read.

1. Loop until `shutdown` fires. Each iteration: call `MemoryStore::list_tenants()`. On `StoreError`, log `maintenance.scheduler.list_tenants_failed`, back off one tick, continue (the scheduler never aborts on a transient store error).
2. For each tenant, decide whether its cycle is **due** via `tenant_is_due`: the **fallback** trigger fires when there is no in-memory `last_cycle_at` for the tenant OR `now - last_cycle_at >= maint_max_interval`; otherwise the **idle** trigger fires when `scan_recent_episodes(ctx, since = now - idle_quiet, limit = 1)` returns an **empty** result (no recent ingestion → quiet → idle, SA-MAINT-CADENCE-01). A transient store error on the idle probe returns `false` (not a reason to run a cycle; the fallback timer governs progress). This avoids any cross-component write coupling — C7 does not depend on C4 stamping a field.
3. For each due tenant, build the maintenance `ScopeContext` and call `run_cycle(tenant)`. On `Ok`, insert `now` into the in-memory `last_cycle` map. On `Err`, log `maintenance.cycle.failed` with the tenant and `correlation_id`; do **not** advance the in-memory `last_cycle_at` (the fallback timer re-tries the tenant next tick). Possible errors: `AppError::Store`, `AppError::Provider`.

##### `run_consumer`

1. Loop until `shutdown` fires. Each iteration: call `WorkQueue::claim(&[JobKind::ReEmbedFact, JobKind::HardDelete], lease)`. On `QueueError`, log `maintenance.consumer.claim_failed`, back off one tick, continue.
2. On `Ok(None)` (no job), sleep one poll interval and continue.
3. On `Ok(Some(job))`, build a maintenance `ScopeContext` from `job.scope.tenant` (per *Shared Context*). Dispatch by `job.kind`: `ReEmbedFact` → `handle_reembed(&job.scope, &serde_from(job.payload)?)`; `HardDelete` → `handle_hard_delete(&job.scope, &serde_from(job.payload)?)`. A payload that fails to deserialise is `AppError::Validation` → `WorkQueue::fail(job_id, retryable=false)` (the job is malformed and will never succeed) → moves toward dead-letter (SA-QUEUE-02).
4. On handler `Ok`, call `WorkQueue::complete(job_id)`. On handler `Err`: classify — `AppError::Provider(ProviderError::Timeout)` and `AppError::Store`/`AppError::Queue` are retryable → `WorkQueue::fail(job_id, retryable=true)`; `AppError::Validation`/`AppError::NotFound` are non-retryable → `WorkQueue::fail(job_id, retryable=false)`. Log `maintenance.job.failed` with `job.kind`, `job.id`, error code, `correlation_id`.

##### `run_cycle`

1. Build the maintenance `ScopeContext` for `tenant`. Initialise an empty `CycleReport`.
2. Run duties in fixed, non-destructive-first order: `supersede_contradictions` → `decay_tenant(now)` → `reembed_tenant`. For each duty, on `Err` record the duty name in `report.failed_duties`, log `maintenance.duty.failed` with the duty name and `correlation_id`, and continue to the next duty. No duty deletes any record; the destructive HardDelete path is reached only via the queue handler. This realises the HLD error posture: a failed maintenance cycle leaves prior memory intact (03-sequences, "Maintain").
3. Return `Ok(report)` if no duty raised an unrecoverable error; the duty-level failures are carried in `failed_duties` rather than aborting the cycle.

##### `supersede_contradictions`

1. `scan_contradiction_candidates(ctx, limit = batch_size)`. On `StoreError` → `AppError::Store`.
2. Group candidates by subject signature. Within each group, compare pairs `(a, b)` of currently-valid facts (`valid_to == None`) with `detect_contradiction(a, b)`.
3. `detect_contradiction` contract (pure): returns `NoConflict` when the facts assert compatible content; otherwise the **superseded side is the one with the strictly earlier `valid_from`**; ties on `valid_from` break on **lower `confidence`**; remaining ties break on **lexicographically smaller `id`** (deterministic). The newer/stronger side is the successor. The function reads only `valid_from`, `confidence`, `id`, and a content-conflict predicate over `content` + `entities`; it performs no I/O.
4. For each superseding verdict, end the superseded fact's validity: `MemoryStore::end_validity(ctx, superseded_id, at = now)`, then `update_fact_maintenance_fields` to set `superseded.superseded_by = successor_id` and `successor.supersedes = superseded_id`. **Never destructive** — the superseded fact is retained with a closed validity interval (ADR-002). On `StoreError` → `AppError::Store`. `superseded += 1`. Log `maintenance.supersede` with both ids.

##### `decay_tenant`

1. `scan_decay_candidates(ctx, salience_floor = cfg.salience_floor, limit = batch_size)`. On `StoreError` → `AppError::Store`.
2. For each currently-valid fact, compute `delta_secs = now - last_recalled_at.unwrap_or(ingested_at)` and `r = retrievability(delta_secs, fact.stability, decay_k)`.
3. If `is_prune_candidate(r, fact.salience, prune_retrievability, salience_floor)` is `true`, prune the fact via `MemoryStore::end_validity(ctx, fact.id, at = now)` — **prune is validity-ending, never a destructive delete** (ADR-006: graceful decay; ADR-002: non-destructive). Long-tail TTL is realised by the retrievability term alone (an untouched low-salience fact's `R` falls below the floor over time). `pruned += 1`.
4. High-salience facts (`salience >= salience_floor`) are never pruned regardless of `R` — they survive disuse (ADR-006). They are left unchanged this cycle.
5. Reinforcement is applied by the read path, not here: when the Retrieval Engine (C6) recalls a fact it raises stability and resets the decay clock; this duty only reads `last_recalled_at`/`stability` and prunes. The pure `reinforce` core is exposed for C6 to call and for unit tests. Return the `DecayReport`.

##### `reembed_tenant` / `handle_reembed`

1. `scan_reembed_candidates(ctx, current_model_version = cfg.embed_model_version, limit = batch_size)` (cycle path), or `get_fact(ctx, payload.fact_id)` (job path). On `StoreError` → `AppError::Store`; a job-path missing fact → `AppError::NotFound`.
2. Serialise each fact's `content` to the embedding input text and call `EmbeddingClient::embed(&[text])`. On `ProviderError::Timeout` → `AppError::Provider` (retryable). 
3. Assert `vector.len() == cfg.embed_dim as usize`; a mismatch is `AppError::Validation` (SA-EMBED-01 — a wrong-dimension vector must never enter the index) and the fact is skipped (logged `maintenance.reembed.dim_mismatch`).
4. `MemoryStore::set_fact_embedding(ctx, fact.id, &vector, &cfg.embed_model_version)`. On `StoreError` → `AppError::Store`. `reembedded += 1`.

##### `handle_hard_delete`

1. Build the maintenance `ScopeContext` from `scope.tenant`. Resolve the fact: `get_fact(ctx, payload.fact_id)`; absent → `AppError::NotFound` (logged, non-retryable).
2. Call `MemoryStore::hard_delete(ctx, payload.fact_id)`. The store removes the fact, its derived summaries, and its embeddings atomically and returns a `DeletionProof` whose `digest` is the SHA-256 over the sorted removed ids (SA-DELETE-01).
3. **The operation is not complete until the proof is obtained.** On `StoreError` (the delete could not be confirmed atomically) → `AppError::Store`; the job is retried (retryable) and **success is never reported on a partial delete** (03-sequences Forget error posture, SA-DELETE-01). On success, log `maintenance.hard_delete.ok` with `record_id`, `embeddings_removed`, `digest`, `correlation_id`; return the `DeletionProof`.

#### Data Model

The worker **mutates C1-owned tables**; it does not own the `fact` table. The fields it mutates on `fact` (within the tenant namespace, via the signatures above) are:

- `valid_to` — set on supersession (`end_validity`) and on decay-prune (`end_validity`).
- `supersedes` / `superseded_by` — set on supersession to record successor links.
- `confidence`, `salience`, `stability`, `last_recalled_at` — set via `update_fact_maintenance_fields` (decay/supersession bookkeeping; reinforcement values are computed by C6 and written via the same call).
- the fact embedding vector + its embedding-model version — set via `set_fact_embedding` on re-embed.

The worker no longer inserts `Consolidated` `fact` rows: recall holds no LLM (ADR-015), so server-side consolidation is removed. The agent writes consolidated insights itself as agent-stated facts through the C4 write path (`MemoryClass::Consolidated`, `derived_from`); C7 then maintains them like any other fact.

The worker **owns one small table**, `maintenance_state`, for per-tenant cycle bookkeeping. As built, the table is a **future-persistence seam**: it is defined in the schema but the v1 worker does not read or write it — `run_scheduler` keeps `last_cycle_at` in an in-memory `HashMap` instead (see *Internal Logic*), so cycle bookkeeping resets on restart (a benign re-run). The table exists so that cross-restart idle/fallback accuracy can be added later without a schema change.

The table lives in the squashed single initial migration `migrations/0001_init.{up,down}.surql` (version 1), within each tenant namespace (per ADR-011 erasure cleanliness — dropping the namespace removes it). The five originally numbered migrations (init, queue, quarantine, maintenance, idempotency) were squashed into this one `0001_init` before first release (greenfield, no shipped DB); there is no separate `0004_maintenance` migration. The as-built DDL is `IF NOT EXISTS`-guarded so the single migration is idempotent:

```sql
-- From migrations/0001_init.up.surql (SurrealDB; schemafull). One row per tenant within the tenant namespace.
DEFINE TABLE IF NOT EXISTS maintenance_state SCHEMAFULL PERMISSIONS NONE;  -- maintenance-only; not reachable by user-scoped record permissions

DEFINE FIELD IF NOT EXISTS tenant              ON maintenance_state TYPE string ASSERT $value != "";
DEFINE FIELD IF NOT EXISTS last_cycle_at       ON maintenance_state TYPE option<datetime>;  -- last successful run_cycle
DEFINE FIELD IF NOT EXISTS embed_model_version ON maintenance_state TYPE option<string>;    -- active embed model seen
DEFINE INDEX IF NOT EXISTS maintenance_state_tenant ON maintenance_state FIELDS tenant UNIQUE;
```

```sql
-- From migrations/0001_init.down.surql (rollback). Non-destructive to fact/entity/relationship/source data:
-- maintenance_state is bookkeeping only, so dropping it loses no memory.
REMOVE TABLE maintenance_state;
```

**Rollback considerations (sql-safety layer rule).** `REMOVE TABLE maintenance_state` is non-destructive with respect to memory: every fact, entity, relationship, and source is untouched. Because the v1 worker never reads the table, dropping it has no runtime effect at all (the in-memory clock is the live source). No `fact`-table column is dropped or narrowed by this component (the maintenance-mutated columns are defined and owned by the C1 migration). The verifiable-delete path is destructive **by design and by explicit user intent** (right to erasure); that destruction is performed inside `MemoryStore::hard_delete` (C1), proof-gated, and out of scope for this table's rollback.

#### Error Table

`AppError` variants map to HTTP `(status, code)` only when surfaced through the API (the HardDelete proof is returned to C8 → broker via the Forget sequence). The scheduler and consumer drivers consume errors internally (log + queue retry/dead-letter) and surface nothing to a synchronous caller.

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| HardDelete job names a fact that does not exist / not owned by scope | 404 | NOT_FOUND | `{"error":{"code":"NOT_FOUND","message":"fact not found","correlation_id":"<uuid>"}}` |
| Re-embed produced a vector whose length != `RECALL_EMBED_DIM` (SA-EMBED-01) | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"embedding dimension mismatch","correlation_id":"<uuid>"}}` |
| Job payload fails to deserialise (malformed `ReEmbedPayload` / `HardDeletePayload`) | 400 | VAL_INVALID_BODY | `{"error":{"code":"VAL_INVALID_BODY","message":"invalid job payload","correlation_id":"<uuid>"}}` |
| Memory Store unreachable during a scan/update/delete | 503 | STORE_UNAVAILABLE | `{"error":{"code":"STORE_UNAVAILABLE","message":"memory store unavailable","correlation_id":"<uuid>"}}` |
| Memory Store operation exceeded its deadline | 504 | STORE_TIMEOUT | `{"error":{"code":"STORE_TIMEOUT","message":"memory store timeout","correlation_id":"<uuid>"}}` |
| Work queue unreachable during claim/complete/fail | 503 | QUEUE_UNAVAILABLE | `{"error":{"code":"QUEUE_UNAVAILABLE","message":"work queue unavailable","correlation_id":"<uuid>"}}` |
| Consolidation / re-embed provider exceeded its deadline | 504 | PROVIDER_TIMEOUT | `{"error":{"code":"PROVIDER_TIMEOUT","message":"provider timeout","correlation_id":"<uuid>"}}` |
| Consolidation / re-embed provider returned a non-timeout error | 502 | PROVIDER_ERROR | `{"error":{"code":"PROVIDER_ERROR","message":"provider error","correlation_id":"<uuid>"}}` |
| HardDelete could not be confirmed atomically (partial removal) | 503 | STORE_UNAVAILABLE | `{"error":{"code":"STORE_UNAVAILABLE","message":"deletion not confirmed; not reported complete","correlation_id":"<uuid>"}}` |

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Maintenance Worker

  Scenario: Happy path — idle-biased maintenance cycle runs supersession, decay, and re-embed
    Given tenant "acme" has had no writes for longer than RECALL_IDLE_QUIET_SECS
    When run_cycle("acme") runs
    Then the duties run in order supersede_contradictions, decay_tenant, reembed_tenant
    And no duty calls any LLM (recall holds no LLM, ADR-015)
    And the CycleReport carries the per-duty outcome

  Scenario: Edge case — high-salience fact survives disuse
    Given a currently-valid fact untouched for 10 days with stability 1.0 and salience 0.9
    And RECALL_PRUNE_RETRIEVABILITY is 0.05 and RECALL_SALIENCE_FLOOR is 0.3
    When the decay duty evaluates the fact
    Then retrievability is below the prune threshold
    But is_prune_candidate returns false because salience is at or above the floor
    And the fact's valid_to remains null

  Scenario: Edge case — low-salience stale fact is pruned non-destructively
    Given a currently-valid fact untouched for 10 days with stability 1.0 and salience 0.1
    When the decay duty evaluates the fact
    Then is_prune_candidate returns true
    And the fact is pruned by ending its validity, not by deletion
    And the fact record is retained with a non-null valid_to

  Scenario: Edge case — supersession ends the older fact's validity and links successor
    Given two currently-valid contradicting facts about the same subject with different valid_from
    When the supersession duty runs detect_contradiction on the pair
    Then the fact with the earlier valid_from has its validity ended
    And superseded_by points to the successor and supersedes points back
    And neither fact is deleted

  Scenario: Error path — partial hard delete is never reported as success
    Given a HardDelete job for "fact:018f...abc"
    When MemoryStore::hard_delete fails before returning a DeletionProof
    Then handle_hard_delete returns an error and returns 503 with code STORE_UNAVAILABLE
    And the job is retried as a retryable failure
    And no DeletionProof is emitted

  Scenario: Error path — re-embed dimension mismatch is rejected
    Given RECALL_EMBED_DIM is 1024
    And EmbeddingClient::embed returns a vector of length 768 for a fact
    When the re-embed duty checks the vector length
    Then the fact's embedding is not updated and returns 400 with code VAL_OUT_OF_RANGE
```

#### Performance, Security, Observability

- **Performance targets:** The worker runs **off the synchronous read path**; no maintenance operation may breach the read path's availability (NFR-AV1). Each duty is bounded by `RECALL_MAINT_BATCH_SIZE` (default 200) records per cycle so a single cycle's store load is capped and cannot starve read-path store access. Idle-biased scheduling (SA-MAINT-CADENCE-01) confines embedding/store cost to quiet windows, with a 6 h fallback guaranteeing progress under continuous load. There is no synchronous latency budget for the cycle itself; the only caller-visible latency is the HardDelete proof returned through the Forget sequence, bounded by the C1 `hard_delete` deadline. Memory ceiling: a cycle holds at most `batch_size` facts in memory at a time.
- **Security:** Every store operation is **scoped per tenant** via the maintenance `ScopeContext`; the store's namespace-per-tenant boundary makes cross-tenant maintenance structurally impossible (ADR-011, enforced by `ensure_and_use(ctx.tenant)` on every method). Within a tenant, the maintenance scope (`token_jti = "maintenance"`, empty `user`) is recognised by C1's `is_maintenance_scope` and its read filter (`fact_read_filter`) widens to the whole tenant, so decay, supersession, and verifiable hard delete reach user-private and team-shared facts as well as tenant-shared (RISK-009 resolution) — necessary for these duties and the right to erasure to operate over all of a tenant's memory, while tenant isolation remains structural. The worker holds no end-user credentials and reads no source documents. `maintenance_state` carries `PERMISSIONS NONE` so it is unreachable through user-scoped record permissions. Verifiable hard delete (proof-gated, including derived summaries and embeddings) is the mechanism that satisfies the right to erasure (HLD 06). The embedding API key is read from env only and never logged; fact content is never written to operational logs.
- **Observability:** Structured logs with `correlation_id` on every line: `maintenance.cycle.failed{tenant}`, `maintenance.duty.failed{duty}`, `maintenance.supersede{superseded_id,successor_id}`, `maintenance.reembed.dim_mismatch{fact_id}`, `maintenance.hard_delete.ok{record_id,embeddings_removed,digest}`, `maintenance.job.failed{kind,job_id,code}`. Metrics (per tenant where noted): `maintenance_cycle_total`, `maintenance_cycle_failed_total`, `maintenance_facts_superseded_total`, `maintenance_facts_pruned_total`, `maintenance_facts_reembedded_total`, `maintenance_hard_delete_total`, `maintenance_duty_duration_seconds{duty}`. Trace spans: `maintenance.run_cycle`, `maintenance.supersede_contradictions`, `maintenance.decay_tenant`, `maintenance.reembed_tenant`, `maintenance.handle_hard_delete`.

#### Gaps

None. *(Resolved in the Phase 3 reconciliation pass. Detail of how each was closed:)*

- **Config keys — now in Phase 2D.** `RECALL_EMBED_MODEL_VERSION` (string, default `default`),
  `RECALL_MAINT_BATCH_SIZE` (u32, 500), and `RECALL_REINFORCE_GAIN` (f64, 0.5 — the `reinforce_gain`)
  are all defined in Phase 2D, owner C7. The consolidation-only keys
  (`RECALL_MAINT_CONSOLIDATE_MIN_EPISODES`, `RECALL_INSIGHT_DECAY_FACTOR`) were removed by ADR-015
  along with server-side consolidation.
- **`MemoryStore` maintenance surface — now canonical.** `scan_recent_episodes`,
  `scan_contradiction_candidates` (returns `Vec<(Fact, Fact)>`), `scan_decay_candidates(ctx,
  salience_floor, limit)`, `scan_reembed_candidates`, `update_fact_maintenance_fields`,
  `set_fact_embedding`, and `list_tenants` are all present in the Phase 2C.6 trait surface and the C1
  spec with the signatures this component depends on (the C7 *required surface* block was aligned to
  match).
- **Contradiction-content predicate — pinned (assumption, `confidence: reduced`).** v1 uses a concrete
  heuristic: two currently-valid facts contradict iff they share ≥1 entity **and** their `content`
  carries the same `(subject, predicate)` pair (for triple-shaped content) with a differing `object`.
  The superseded *side* is
  chosen deterministically (later `valid_from` wins → higher `confidence` → lexicographically greater
  `id`). This is a component-internal rule (ADR-002/ADR-007); it is tunable and does not change
  architecture, so it is recorded as an assumption rather than an open question.
- **Idle trigger — no C4 dependency (assumption).** The idle trigger derives "quiet" from store
  activity the worker can read itself — the most-recent `ingested_at` across the tenant's facts
  (via a bounded `scan_recent_episodes(ctx, since = now - idle_quiet, limit = 1)` probe) and the
  presence of Pending jobs for the tenant — rather than a `last_write_at` field written by C4. The
  6 h fallback timer (`RECALL_MAINT_MAX_INTERVAL_SECS`) guarantees progress regardless.
