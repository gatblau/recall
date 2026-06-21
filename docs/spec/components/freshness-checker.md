### SPEC: Freshness Checker
**File:** `src/freshness` | **Package:** `recall::freshness` | **Phase:** 3 | **Dependencies:** C2 (Durable Work Queue)

> **Mode:** greenfield
> **derivedFromHld:** 0.4.1

#### Purpose

The Freshness Checker verifies the currency of source-cited facts on the synchronous read path **without** performing a synchronous source re-read (ADR-013). It is called in-process by the Retrieval Engine (C6) with the candidate facts that cite a `source_id`. For each such fact it performs one cheap conditional source-change check against the Faraday broker (a `GET {RECALL_BROKER_URL}/sources/{origin_ref}/freshness` carrying the source's `modification_marker` as an `If-None-Match` precondition, issued as the authenticated user), and maps the broker's answer to a `Currency` value: an unchanged source yields `Current`; a changed source yields `StalePendingRefresh` and enqueues an asynchronous `ReReadSource` work job (idempotent on the source id); an unreachable broker or per-call timeout yields `UnverifiedCurrency` and the stored fact is kept. The whole batch is bounded to the SA-LAT-01 freshness sub-budget (≤ 25 ms total); on budget breach the remaining facts are returned without a check rather than overrunning. The component returns a per-fact `Currency` to C6; it never re-ranks, drops, or mutates facts.

#### Approach

The component is a stateless batch function over an injected `BrokerClient` and `WorkQueue`. It owns no persistence; the `ReReadSource` job is enqueued through C2 and the actual re-read/re-extraction runs off the read path. The governing pattern is a **bounded-fan-out with a wall-clock deadline**: distinct source ids are checked concurrently under a single batch deadline derived from SA-LAT-01, and any check still outstanding when the deadline elapses is treated as `UnverifiedCurrency` (the same outcome as an unreachable broker), so the freshness step degrades to "skip → flag unverified-currency" rather than blocking the read path. One conditional check is issued **per distinct source id**, not per fact, so multiple facts citing the same source share one broker round-trip and one (idempotent) re-read job. Considered and rejected: (a) one check per fact — rejected, it multiplies broker round-trips and re-read jobs for facts sharing a source and makes the 25 ms batch budget unreachable; (b) a synchronous source re-read on a detected change — rejected by ADR-013, source-fetch latency would blow NFR-P2.

#### Shared Context

Duplicated from Phase 2C (the reader implements from this section alone). Field order, names, and types are the contract.

```rust
// from src/types/domain.rs (2C.2)
#[derive(Serialize, Deserialize, Clone)]
pub struct Fact {
    pub id: String,                       // "fact:<uuidv7>"
    pub content: serde_json::Value,
    pub entities: Vec<String>,
    pub source_id: Option<String>,        // "source:<uuidv7>", null for agent-stated
    pub memory_class: MemoryClass,
    pub visibility: Visibility,
    pub owner: ScopeRef,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub ingested_at: DateTime<Utc>,
    pub confidence: f64,
    pub salience: f64,
    pub stability: f64,
    pub supersedes: Option<String>,
    pub superseded_by: Option<String>,
    pub derived_from: Vec<String>,
    pub last_recalled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Source {
    pub id: String,                          // "source:<uuidv7>"
    pub origin_ref: String,                  // document/system handle, opaque to recall
    pub modification_marker: Option<String>, // ETag / Last-Modified token for freshness
    pub trust_signal: f64,                   // [0,1]
    pub owner: ScopeRef,
}

// from src/types/scope.rs (2C.3)
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

// from src/types/api.rs (2C.4) — the per-fact freshness flag this component returns
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Currency { Current, StalePendingRefresh, UnverifiedCurrency }

// from src/types/job.rs (2C.5) — the job enqueued on a detected change
#[derive(Serialize, Deserialize, Clone)]
pub struct WorkJob {
    pub id: String,                          // "work_job:<uuidv7>"
    pub kind: JobKind,
    pub payload: serde_json::Value,          // kind-specific, validated by the consumer
    pub scope: ScopeRef,
    pub idempotency_key: Option<String>,     // present for API-originated writes
    pub attempts: u32,
    pub status: JobStatus,
    pub not_before: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind { ExtractFact, ReEmbedFact, ReReadSource, Consolidate, HardDelete }

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus { Pending, Leased, Done, DeadLetter }

// from src/types/ports.rs (2C.6) — the two injected seams this component depends on
#[async_trait]
pub trait BrokerClient: Send + Sync {
    async fn check_source(&self, ctx: &ScopeContext, src: &Source)
        -> Result<SourceState, ProviderError>;       // Unchanged | Changed | Unreachable
}

#[async_trait]
pub trait WorkQueue: Send + Sync {
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError>; // returns job id
    // ...claim/complete/fail — owned by C2; not used by this component.
}
```

`QueueError` is the typed error owned by C2 (Durable Work Queue) and referenced here by name; this component matches on `Result::Err(QueueError)` from `enqueue` but does not construct it.

`SourceState` and `ProviderError` are **shared provider types defined in §2C.6 (`src/types/ports.rs`)**, consumed here by the broker trait (the broker adapter is a thin HTTP trait impl under `src/providers/`, `HttpBrokerClient`, with no standalone spec — Phase 2B). Their as-built shapes are restated in the Data Model section below.

**Broker wire contract (as built in `src/providers/mod.rs`, `HttpBrokerClient::check_source`).** The injected `BrokerClient` issues:

```text
GET {RECALL_BROKER_URL}/sources/{origin_ref}/freshness
  If-None-Match: <modification_marker>   (sent only when src.modification_marker.is_some())
  X-Recall-Tenant:  <ctx.tenant>
  X-Recall-User:    <ctx.user>
  X-Correlation-Id: <ctx.correlation_id>
```

The path segment is the source's `origin_ref`; `base_url` is trimmed of a trailing `/`. The broker authorises the user from the identity headers; `recall` holds no source credentials and sends no fact or source content — the `modification_marker` is the only source datum on the wire. Response mapping:

| Broker response | `check_source` result |
|---|---|
| `304 Not Modified` | `Ok(SourceState::Unchanged)` — source unchanged |
| `200 OK` | `Ok(SourceState::Changed)` — source changed since the marker |
| any other status | `Err(ProviderError::Status(status))` |
| transport failure / timeout | `Err(ProviderError::Transport(..))` / `Err(ProviderError::Timeout)` |

This component (C5) absorbs every `Err(ProviderError)` and the per-call timeout into `Currency::UnverifiedCurrency` (Internal Logic step 6), so a broker fault never blocks or fails the read path. Note the as-built broker reports a change via `200`/`304` status only and returns `ProviderError` on any other status; it does not return a distinct `SourceState::Unreachable` from the HTTP path — an unreachable broker surfaces as a `ProviderError`, which C5 maps to the same `UnverifiedCurrency` outcome.

Configuration this component reads (from Phase 2D):

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_BROKER_URL` | url | _(none)_ | **yes** | Faraday broker base URL for the conditional source-change check. Consumed by the injected `BrokerClient` HTTP adapter, not read directly by `recall::freshness`. |
| `RECALL_FRESHNESS_BUDGET_MS` | u32 | `25` | no | Wall-clock deadline for the whole `check` batch (SA-LAT-01 freshness sub-budget). On breach, unchecked facts are returned `UnverifiedCurrency`. |
| `RECALL_FRESHNESS_PER_CALL_MS` | u32 | `20` | no | Per-broker-call timeout; bounds a single `check_source` round-trip so one slow source cannot consume the whole batch budget. Must be `<= RECALL_FRESHNESS_BUDGET_MS`. |

#### Public Interface

```rust
// src/freshness/mod.rs

/// The §2C.6 read-path freshness trait this component implements (duplicated here, binding).
#[async_trait]
pub trait FreshnessChecker: Send + Sync {
    async fn check(&self, ctx: &ScopeContext, facts: &[(Fact, Source)]) -> Vec<(String, Currency)>;
}

/// Stateless broker-backed freshness checker. Holds the two injected seams and the resolved budgets.
pub struct BrokerFreshnessChecker {
    broker: Arc<dyn BrokerClient>,
    queue: Arc<dyn WorkQueue>,
    budget: Duration,        // from RECALL_FRESHNESS_BUDGET_MS (default 25 ms)
    per_call: Duration,      // from RECALL_FRESHNESS_PER_CALL_MS (default 20 ms)
}

impl BrokerFreshnessChecker {
    pub fn new(
        broker: Arc<dyn BrokerClient>,
        queue: Arc<dyn WorkQueue>,
        budget: Duration,
        per_call: Duration,
    ) -> Self;
}

#[async_trait]
impl FreshnessChecker for BrokerFreshnessChecker {
    /// Batch freshness check over candidate facts that cite a source.
    ///
    /// `facts` is the candidate set from the Retrieval Engine (C6). Each input is the fact paired
    /// with the resolved `Source` record it cites; C6 supplies the `Source` (already loaded under
    /// the caller's scope) so this component performs no store read.
    ///
    /// Returns one `(fact_id, Currency)` per input fact, in input order. The function never errors:
    /// every failure mode (broker error, queue error, per-call timeout, batch-deadline breach) is
    /// mapped to a `Currency` value, so the read path is never blocked or failed by freshness.
    async fn check(
        &self,
        ctx: &ScopeContext,
        facts: &[(Fact, Source)],
    ) -> Vec<(String, Currency)>;
}
```

**Invocation from C6 (Retrieval Engine).** After rerank, recency weighting, and gating, C6 partitions its surviving ranked facts: facts with `source_id == None` are immediately assigned `Currency::Current` (an agent-stated fact has no source to check). For each fact with `source_id == Some(_)`, C6 loads the cited `Source` under the request `ScopeContext`, forms the `(Fact, Source)` pair, and passes the batch to `FreshnessChecker::check(ctx, &facts)`. C6 then joins the returned `(fact_id, Currency)` back onto each `RankedFact.currency` field by `fact_id`. C6 does not re-rank or drop based on the returned `Currency`.

##### Example

Input — three candidate facts, two citing source `source:01J...A` (which the broker reports changed) and one citing `source:01J...B` (unchanged):

```text
ctx   = ScopeContext { tenant: "acme", teams: ["platform"], user: "u-42",
                       token_jti: "jti-9", allowed_ops: {read:true,write:false,forget:false},
                       correlation_id: "c-7" }
facts = [
  (Fact { id: "fact:01J...1", source_id: Some("source:01J...A"), .. },
   Source { id: "source:01J...A", modification_marker: Some("W/\"abc\""), .. }),
  (Fact { id: "fact:01J...2", source_id: Some("source:01J...A"), .. },
   Source { id: "source:01J...A", modification_marker: Some("W/\"abc\""), .. }),
  (Fact { id: "fact:01J...3", source_id: Some("source:01J...B"), .. },
   Source { id: "source:01J...B", modification_marker: Some("W/\"def\""), .. }),
]
```

Output — both facts on the changed source are `stale-pending-refresh` (one `ReReadSource` job is enqueued, idempotency-keyed on `source:01J...A`, not two); the fact on the unchanged source is `current`:

```text
[
  ("fact:01J...1", StalePendingRefresh),
  ("fact:01J...2", StalePendingRefresh),
  ("fact:01J...3", Current),
]
```

#### Internal Logic

1. **Empty / fast exit.** If `facts` is empty, return an empty vector without contacting the broker. Log nothing (no work performed). [No dependency call.]
2. **Start the batch deadline.** Capture `deadline = Instant::now() + self.budget`. All broker work below shares this single wall-clock deadline; this is the mechanism that bounds the batch to the SA-LAT-01 freshness sub-budget. [No dependency call.]
3. **Group by source id.** Build the set of distinct `source.id` values across the input pairs (every input pair carries a `Source`, so each fact has exactly one source to check). One conditional check is issued per distinct source id; multiple facts citing the same source share its result. [No dependency call.]
4. **Fan out one conditional check per distinct source, under per-call timeout.** Each distinct source is checked on its own `tokio::task::JoinSet` task that calls `self.broker.check_source(ctx, &source)` wrapped in `tokio::time::timeout(self.per_call, …)`; the task is spawned with cloned copies of the broker handle, `ScopeContext`, and `Source`, and yields `(source_id, timeout_result)`. `check_source` issues `GET {RECALL_BROKER_URL}/sources/{origin_ref}/freshness` with the source's `modification_marker` as the `If-None-Match` precondition and `X-Recall-Tenant`/`X-Recall-User`/`X-Correlation-Id` identity headers, called as the authenticated user (the broker enforces the user's source access rights; `recall` never reads the source directly). Possible outcomes per source: `Ok(SourceState::Unchanged)`, `Ok(SourceState::Changed)`, `Ok(SourceState::Unreachable)`, `Err(ProviderError)`, or the per-call `timeout` elapsing. [Dependency call: `BrokerClient::check_source`.]
5. **Await all checks bounded by the batch deadline.** Drain the `JoinSet` in a loop, each iteration awaiting `tokio::time::timeout_at(deadline, set.join_next())`. A source whose check completes before the deadline is classified and recorded in a `by_source: HashMap<String, SourceOutcome>` map; a spawned task that panicked (`Ok(Some(Err(join_err)))`) is absorbed and left absent from the map; `Ok(None)` ends the loop (all checks resolved before the deadline); on the deadline elapsing (`Err(Elapsed)`) the loop breaks, logging `freshness.deadline_breach` at `warn` with the count of still-outstanding tasks (`set.len()`) when that count is non-zero, and `set.abort_all()` cancels the remaining tasks. Any source left absent from `by_source` — outstanding at the deadline, or its task panicked — defaults to `UnverifiedCurrency` in step 8 (the freshness step degrades to "skip → flag unverified-currency" rather than overrunning the read-path budget — SA-LAT-01). [No new dependency call.]
6. **Map each source's outcome to a per-source `Currency`:**
   - `Ok(SourceState::Unchanged)` → `Currency::Current`.
   - `Ok(SourceState::Changed)` → enqueue one `ReReadSource` job for this source (step 7), then `Currency::StalePendingRefresh`. If the enqueue fails, the source is still marked `StalePendingRefresh` (the change was detected; the refresh job is best-effort and the failure is logged at `warn` — never block or fail the read for a queue error).
   - `Ok(SourceState::Unreachable)`, `Err(ProviderError)`, per-call timeout, or batch-deadline breach (step 5) → `Currency::UnverifiedCurrency`. The stored fact is returned unchanged; never block.
   [No dependency call in this step except the enqueue delegated to step 7.]
7. **Enqueue an idempotent `ReReadSource` job (changed sources only).** Construct one `WorkJob` with `kind = JobKind::ReReadSource`, `scope = ctx`'s `(tenant, teams[0]-or-none, user)` projected to `ScopeRef`, `payload = {"source_id": <source.id>, "origin_ref": <source.origin_ref>, "modification_marker": <source.modification_marker>}`, and `idempotency_key = Some(format!("re-read-source:{}", source.id))` so duplicate change detections for the same source within the C2 idempotency window do not pile up duplicate jobs. Call `self.queue.enqueue(job)`. On `Ok(_)` log at `info`; on `Err(QueueError)` log at `warn` and continue (see step 6). [Dependency call: `WorkQueue::enqueue`.]
8. **Expand per-source results back to per-fact results.** For each input fact in original input order, look up its source's resolved `Currency` (from step 6) and emit `(fact.id.clone(), currency)`. Two facts citing the same changed source both receive `StalePendingRefresh` from the single check and single job. Return the vector. [No dependency call.]

**Degradation rule (binding).** On any path that cannot complete a source's check within `RECALL_FRESHNESS_BUDGET_MS` (batch) or `RECALL_FRESHNESS_PER_CALL_MS` (single call) — including an unreachable broker or a provider error — that source's facts are returned `UnverifiedCurrency`, and on a confirmed change the facts are returned `StalePendingRefresh` even if the re-read enqueue fails. The function returns `Current`/`StalePendingRefresh`/`UnverifiedCurrency` and **never** overruns the budget or propagates an error to C6.

#### Data Model

`N/A — owns no tables; enqueues `ReReadSource` jobs to C2 (Durable Work Queue) and reads no store.`

This component consumes two **in-memory** helper types (no persistence), both defined in §2C.6 (`src/types/ports.rs`) and shared across the provider traits. `SourceState` is the broker check result; `ProviderError` is the shared provider error returned by every 2C.6 provider trait (broker, embedding, rerank, llm, pii). Their as-built shapes:

```rust
// src/types/ports.rs (§2C.6) — in-memory only, no DDL.

/// Result of one conditional source-change check, returned by `BrokerClient::check_source` (C5).
/// As built it carries no derives.
pub enum SourceState {
    Unchanged,
    Changed,
    Unreachable,
}

/// Shared by EmbeddingClient/RerankClient/LlmClient/BrokerClient/PiiDetector.
#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    /// -> 504 PROVIDER_TIMEOUT.
    #[error("provider timeout")]            Timeout,
    /// -> 502 PROVIDER_ERROR.
    #[error("provider status {0}")]         Status(u16),
    /// -> 502 PROVIDER_ERROR.
    #[error("provider transport: {0}")]     Transport(String),
    /// -> 502 PROVIDER_ERROR.
    #[error("provider malformed: {0}")]     Malformed(String),
}
```

The as-built `HttpBrokerClient` returns `SourceState::Unchanged` (`304`) or `SourceState::Changed` (`200`) and never constructs `SourceState::Unreachable` from the HTTP path: a broker that cannot be reached surfaces as `ProviderError::Transport`/`ProviderError::Timeout`, and any non-`200`/`304` status as `ProviderError::Status(status)`. C5 maps all three of those error cases to the same `UnverifiedCurrency` outcome (Internal Logic step 6).

#### Error Table

This component is invoked in-process by C6 (no HTTP surface of its own) and **returns no error to its caller** — every failure is mapped to a `Currency` value so the read path is never blocked. The table records each failure mode, the internal disposition, and the read-path-facing `Currency`. The Status / Code columns reference the canonical registry only to show the *upstream* error this component absorbs; this component itself emits none of them to a handler.

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| Broker round-trip exceeds `RECALL_FRESHNESS_PER_CALL_MS` (single source slow) | n/a (absorbed) | PROVIDER_TIMEOUT | none — source's facts mapped to `Currency::UnverifiedCurrency`; logged `warn`; read path continues |
| Whole batch exceeds `RECALL_FRESHNESS_BUDGET_MS` (deadline breach) | n/a (absorbed) | PROVIDER_TIMEOUT | none — unchecked facts mapped to `Currency::UnverifiedCurrency`; logged `warn` with count of skipped sources; read path continues |
| Broker returns `SourceState::Unreachable` or `ProviderError` (broker/source down) | n/a (absorbed) | PROVIDER_ERROR | none — source's facts mapped to `Currency::UnverifiedCurrency`; logged `warn`; read path continues |
| `WorkQueue::enqueue` fails on a detected change (`QueueError`) | n/a (absorbed) | QUEUE_UNAVAILABLE | none — facts still mapped to `Currency::StalePendingRefresh` (change detected); re-read job not enqueued; logged `warn` |

`PROVIDER_TIMEOUT (504)`, `PROVIDER_ERROR (502)`, and `QUEUE_UNAVAILABLE (503)` are registry codes; they appear here as the upstream conditions C5 absorbs. C5 never surfaces a status code — its only output is a per-fact `Currency`.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Freshness Checker

  Scenario: Happy path — unchanged source marks the fact current
    Given a candidate fact citing source "source:A" with modification_marker "W/\"abc\""
    And the broker reports SourceState::Unchanged for "source:A" within 20 ms
    When check is called with that one (fact, source) pair
    Then the result is [("fact:1", Current)]
    And no ReReadSource job is enqueued

  Scenario: Changed source — enqueues one idempotent job and flags stale-pending-refresh
    Given two candidate facts "fact:1" and "fact:2" both citing source "source:A"
    And the broker reports SourceState::Changed for "source:A"
    When check is called with both (fact, source) pairs
    Then both facts are returned with Currency StalePendingRefresh
    And exactly one ReReadSource WorkJob is enqueued with idempotency_key "re-read-source:source:A"
    And the job payload contains "source_id" = "source:A"

  Scenario: Edge case — facts with no source skip the broker entirely
    Given C6 has already assigned Current to facts whose source_id is None
    And one candidate fact "fact:3" cites source "source:B" reported Unchanged
    When check is called with only the (fact:3, source:B) pair
    Then exactly one broker check_source call is made
    And the result is [("fact:3", Current)]

  Scenario: Error path — broker unreachable returns unverified-currency without blocking
    Given a candidate fact "fact:4" citing source "source:C"
    And the broker returns SourceState::Unreachable for "source:C"
    When check is called with that pair
    Then the result is [("fact:4", UnverifiedCurrency)]
    And the function returns Ok (no error propagated to C6)
    And a warn log records the absorbed PROVIDER_ERROR condition

  Scenario: Error path — batch deadline breach degrades to unverified-currency
    Given three candidate facts citing three distinct sources
    And each broker check_source call would take 30 ms
    And RECALL_FRESHNESS_BUDGET_MS is 25
    When check is called with the three pairs
    Then the batch returns within approximately 25 ms
    And every unchecked fact is returned with Currency UnverifiedCurrency

  Scenario: Error path — queue failure on a changed source still flags stale-pending-refresh
    Given a candidate fact "fact:5" citing a source the broker reports Changed
    And WorkQueue::enqueue fails with QueueError
    When check is called with that pair
    Then the result is [("fact:5", StalePendingRefresh)]
    And a warn log records the absorbed QUEUE_UNAVAILABLE condition
```

#### Performance, Security, Observability

- **Performance targets:** the whole `check` batch fits the SA-LAT-01 freshness sub-budget of **≤ 25 ms** (`RECALL_FRESHNESS_BUDGET_MS`). Distinct sources are checked concurrently; a single `check_source` round-trip is bounded by `RECALL_FRESHNESS_PER_CALL_MS` (default 20 ms). **On budget breach the function returns `Current`/`StalePendingRefresh`/`UnverifiedCurrency` for already-resolved sources and `UnverifiedCurrency` for the rest — it never overruns the budget and never blocks the read path.** Memory is bounded by the candidate set size (≤ `RECALL_STAGE1_K` survivors, default 50) and the distinct-source count; no buffering beyond the result vector.
- **Security:** the conditional check is issued **as the authenticated user** — `check_source` receives the request `ScopeContext` so the broker enforces the user's source access rights; `recall` never reads source systems directly and holds no source credentials. The `modification_marker` is the only source datum sent; no fact content leaves the process. The `ReReadSource` job is scoped with the caller's `ScopeRef` so the async re-read runs under the same tenant/user boundary. No request-body value influences scope. No PII is logged (only source ids, the absorbed-error class, and counts).
- **Observability:** structured logs carry `correlation_id` (from `ctx.correlation_id`) on every line. Metrics: `recall_freshness_checks_total{outcome="current|stale_pending_refresh|unverified_currency"}` (counter), `recall_freshness_batch_duration_ms` (histogram, asserted against the 25 ms budget), `recall_freshness_deadline_breach_total` (counter), `recall_freshness_reread_enqueued_total{result="ok|queue_error"}` (counter). Trace span `freshness.check` with attributes `fact_count`, `distinct_source_count`, `budget_ms`; child span `freshness.check_source` per distinct source with attribute `source_id` and `outcome`.

#### Gaps

None. *(Both drafting gaps were resolved in the Phase 3 reconciliation pass: `RECALL_FRESHNESS_BUDGET_MS` and `RECALL_FRESHNESS_PER_CALL_MS` are now defined in Phase 2D, owner C5; `SourceState`, `ProviderError`, and `PiiSpan` now have a single canonical home in Phase 2C.6 (`src/types/ports.rs`), shared by every provider trait, and C5's public `check` method is the canonical `FreshnessChecker` trait in Phase 2C.6.)*
