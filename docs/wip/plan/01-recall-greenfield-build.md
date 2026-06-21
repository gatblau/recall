# Plan 01 — recall greenfield build

> **Move:** CSPA 3 (Plan increments) · **Input:** `docs/spec/phase-5-playbook.md` (Approved, Phase 6 PASS) · **Authored:** 2026-06-20
> **Test stack (chosen — no Practice Pack):** integration via **cucumber-rs** outside-in BDD; real services via **testcontainers-rs** (SurrealDB server + Dex); SurrealDB **in-memory** backend for the inner loop; external providers + Faraday broker via **wiremock-rs** stubs honouring each wire contract.

## Anchor (from input)

Build the `recall` agentic-memory service from the approved LLD spec set, following the Phase 5
Generation Playbook's dependency-ordered build (`docs/spec/phase-5-playbook.md`, Steps 0–7). Evidence:
the Playbook DAG `C1 → {C2,C3} → {C4,C5} → {C6,C7} → C8` with Phase-0 cross-cutting foundations
(X1–X13), and the Phase 6 audit verdict **PASS** that promoted the spec set to *Approved*.

## Verified context

- `docs/spec/phase-5-playbook.md` — build order Steps 0–7 (the spine of this plan).
- `docs/spec/phase-6-audit.md` — `Verdict: PASS`, all 21 items `[x]`, all component *Gaps* `None.`
- `docs/spec/phase-2-architecture.md` — §2C shared types (canonical `MemoryStore`/`WorkQueue`/`FreshnessChecker` + provider traits), §2D 50 env vars.
- `docs/spec/phase-4-cross-cutting.md` — X1 canonical error-code registry + X2–X13.
- `docs/spec/components/{memory-store,work-queue,auth-scope,write-pipeline,freshness-checker,retrieval-engine,maintenance-worker,http-api-edge}.md` — 8 component specs, each *Gaps: None*.
- Repo is **greenfield**: no `Cargo.toml`, no `src/`, no `*.rs`. Every source path below is **to be created** (not an open assumption — greenfield is expected here).

## Open assumptions

- **OQ-STORE outcome (BLOCKING for ADR-009, gated as Phase 0).** Embedded SurrealDB must meet
  NFR-P2 (p95 ≤ 200 ms) / NFR-P3 (ANN ≤ 50 ms) for graph + vector + keyword over bi-temporal edges.
  Phase 0 is the spike; a failure reopens ADR-003 **and** ADR-009 and returns the work to `/design`.
  Do not start Phase 1 (Rust scaffolding) until Phase 0 passes.
- **Container image pins (non-blocking).** SurrealDB `surrealdb/surrealdb:v2.x` and Dex
  `dexidp/dex:v2.x` tags are fixed at scaffolding (Phase 1) and recorded in the test harness; the
  exact patch tags are chosen then, not assumed here.
- **External provider wire contracts (non-blocking — OQ-MODELS/OQ-IDP).** The embedding/reranker/LLM
  provider and Faraday broker HTTP shapes are stubbed in tests via wiremock to the contracts the specs
  assume (embedding returns `RECALL_EMBED_DIM` `f32` vectors; broker honours `If-Modified-Since`
  200/304). Real-provider conformance is a deployment-time check (see Risks).

## Scope

- **In scope:** the full single-binary `recall` service — all 8 components, all 13 cross-cutting
  concerns, migrations, the OQ-STORE spike, and the outside-in BDD integration suite.
- **Out of scope (deferred — see Follow-ups):** NATS work-queue backend (OQ-QUEUE; store-backed ships
  first behind the `WorkQueue` trait); local/in-process embedder (ADR-012 latency mitigation); query
  reformulation A/B path (off by default, SA-RECONFIG); LLM entity adjudication (HLD-impact deferral);
  team→database physical-isolation escape hatch (ADR-011 residual); semantic answer caching (HLD
  non-goal). Faraday broker, sandbox, and Faraday-side audit are owned by Faraday, not `recall`.

---

## Phases

### Phase 0 — OQ-STORE spike (precondition gate, ADR-009)

- **Goal:** prove embedded SurrealDB meets the latency/scale targets before any Rust scaffolding.
- **Changes:** a throwaway spike crate `spike/` (not part of the shipped binary) loading representative
  facts and running ANN + BM25 + 2-hop graph queries against embedded SurrealKV and a remote
  SurrealDB/TiKV instance through one engine abstraction.
- **Rationale:** ADR-009 (Rust + embedded SurrealDB) is justified *because* SurrealDB embeds in-process;
  a spike failure invalidates the language choice, so this must run first (HLD 10 risk; Playbook Step 0).
- **Exit criteria:**
  - Compiles: `cargo build -p spike`.
  - Behavioural gate: a spike report shows ANN-only p95 ≤ 50 ms and a representative recall p95 ≤ 200 ms
    at the target corpus, for both embedded and remote modes.
  - **Escalation, not a test gate:** if the targets are missed, STOP — reopen ADR-003/ADR-009 and return
    to `/design`; do not proceed to Phase 1.
- **Risks / rollback:** the spike is throwaway; deleting `spike/` undoes it. Failure is the load-bearing
  risk (RISK-001).

### Phase 1 — Scaffolding & cross-cutting foundations (X1, X3, X4, X5, X6 + shared types + test harness)

- **Goal:** a bootable skeleton binary with the Phase-0 foundations and the integration-test harness wired.
- **Changes:**
  - `Cargo.toml` (crate `recall`), module tree under `src/`: `types/`, `config.rs`, `error.rs`,
    `obs/`, `providers/`, plus empty `store/ queue/ auth/ write_pipeline/ freshness/ retrieval/
    maintenance/ api/ shutdown.rs`.
  - `src/error.rs`, `src/api/envelope.rs` — **X1**: `AppError`, canonical error-code registry, `Success`/`ErrorEnvelope`, `map_error`, panic-recovery layer.
  - `src/config.rs` — **X6**: typed `Config::load()` (env > file > default), startup validation incl. the embedding-dim check.
  - `src/obs/` — **X3/X4/X5**: JSON `tracing` logging + redaction, metric catalogue, OTLP tracing.
  - `src/types/*` — §2C.1–§2C.6 shared types + the `MemoryStore`/`WorkQueue`/`FreshnessChecker`/provider traits + `ProviderError`/`SourceState`/`PiiSpan`.
  - `src/providers/*` — thin HTTP adapters (skeleton) for the five provider traits.
  - `tests/bdd/` — cucumber-rs harness; `tests/support/` — testcontainers-rs fixtures (SurrealDB, Dex) + wiremock provider stubs; pin the image tags here.
- **Rationale:** every later phase consumes these foundations; the harness must exist before any component's Gherkin can be made executable.
- **Exit criteria:**
  - Compiles + lint: `cargo build` and `cargo clippy -- -D warnings`.
  - Config unit tests: `cargo test --lib config` (env-override, missing-required, NATS-conditional).
  - **Integration (cucumber-rs):** `cargo test --test bdd -- config_and_boot` brings the binary up and asserts a Phase-1 smoke feature: process boots with a minimal valid env, `GET /healthz` → 200, an unknown route → the X1 error envelope with a `correlation_id`. Services: none required yet (wiremock + testcontainers fixtures compile and start but are unused this phase).
  - Behavioural check: `curl -s localhost:$PORT/healthz` returns `{"data":{"status":"live"}}`.
- **Risks / rollback:** foundational only; revert the commit to undo. Image-pin churn is low risk.

### Phase 2 — C1 Memory Store + X7 migrations

- **Goal:** the persistence layer implementing the full `MemoryStore` trait over embedded SurrealDB with per-tenant namespaces and the three retrieval signals.
- **Changes:** `src/store/mod.rs` (trait impl), `src/store/migrate.rs` (**X7** `Migrator`), `migrations/0001_init.{up,down}.surql`, `src/store/types.rs` (`StoreError`, `Candidate`, `StageOneQuery`, `AuditEntry`).
- **Rationale:** Phase 1 (foundations) only; every other component depends on C1. First real persistence.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Schema/migration (`schema/**`/`*.sql` touched): `migrate_up` applies `0001` cleanly on a fresh namespace and is idempotent on re-run; `dry_run` returns the statements without executing; the `0001` down path is verified destructive-on-empty-only (visually confirmed in `migrations/0001_init.down.surql`; rollback note per sql-safety).
  - **Integration (cucumber-rs + testcontainers-rs):** `cargo test --test bdd -- memory_store`. Services: **real SurrealDB** via testcontainers-rs (`surrealdb/surrealdb:v2.x`); the fast inner loop uses the SurrealDB **in-memory** backend. Scenarios map C1's Gherkin: put→recall by vector+keyword; **cross-tenant isolation** (a `globex` caller never sees `acme` facts); supersession ends validity without deleting; verifiable `hard_delete` returns a `DeletionProof` with correct digest; score-out-of-range write rejected.
  - Behavioural check: a scripted put_fact→recall round-trip returns the fact with `semantic_score`/`keyword_score` in `[0,1]`.
- **Risks / rollback:** destructive DDL only on the throwaway test namespace; the embedded store file is disposable. Cross-tenant isolation is the critical assertion (RISK-002).

### Phase 3 — C2 Durable Work Queue

- **Goal:** store-backed durable queue with atomic claim/lease, idempotent enqueue, backoff→dead-letter, and a lease-reaper.
- **Changes:** `src/queue/mod.rs` (`WorkQueue` impl + `QueueError`), `migrations/0002_queue.{up,down}.surql` (`work_job`, `dead_letter`).
- **Rationale:** depends on C1 (store-backed). Required before any async producer/consumer (C4/C5/C7/C8).
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Schema: `0002` applies + down path verified; `(scope, idempotency_key)` UNIQUE + `(status,kind,not_before)` indexes present.
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB):** `cargo test --test bdd -- work_queue`. Scenarios: enqueue→claim→complete; idempotent dedup on `(scope, idempotency_key)`; **concurrent claim grants to exactly one worker** (two claimers, one job); lease-reaper reverts a crashed worker's expired lease to Pending; retryable fail backs off then dead-letters at `RECALL_JOB_MAX_ATTEMPTS`.
  - Behavioural check: a job enqueued twice with the same key yields one `work_job` row and `AlreadyAccepted`-equivalent dedup.
- **Risks / rollback:** isolated table; drop `work_job`/`dead_letter` to undo. Concurrent-claim correctness is the key assertion.

### Phase 4 — C3 Auth & Scope (X2 policy)

- **Goal:** OIDC bearer-JWT validation and `ScopeContext` derivation; the reusable read-filter helper and `authorise`.
- **Changes:** `src/auth/mod.rs` (validation, JWKS cache/refresh, `ScopeContext`, `authorise`, read-filter helper).
- **Rationale:** independent of C2 (shared types only); required by C8. Built here so the edge can wire it in Phase 9.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - **Integration (cucumber-rs + testcontainers-rs Dex):** `cargo test --test bdd -- auth_scope`. Services: **real Dex** OIDC issuer via testcontainers-rs (`dexidp/dex:v2.x`) minting tokens; no provider stubs needed. Scenarios map C3's Gherkin: valid token → `ScopeContext` with mapped tenant/teams/user/ops; expired/wrong-audience/alg=none → `AUTH_INVALID_TOKEN`; missing header → `AUTH_MISSING_TOKEN`; token lacking the op scope → `AUTH_INSUFFICIENT_SCOPE`; read-filter helper admits own + team-shared + tenant-shared, denies cross-tenant.
  - Behavioural check: a Dex-minted token validates in < 5 ms against a warm JWKS cache (assert no network call on the second validation).
- **Risks / rollback:** isolated module. Security-critical — the read-filter helper is exercised here and again end-to-end in Phase 10.

### Phase 5 — C4 Write Pipeline

- **Goal:** the asynchronous 8-step ingestion pipeline (filter→extract→normalise→entity-resolve→score→PII→write-gate→embed+persist).
- **Changes:** `src/write_pipeline/mod.rs` (`ExtractedFact`, `EntityMention`, the step functions, `QuarantineRecord`), `migrations/0003_quarantine.{up,down}.surql`.
- **Rationale:** depends on C1 (persist/entity/source) + C2 (consume `ExtractFact`). First real write path.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Schema: `0003_quarantine` applies + down path verified.
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB + wiremock-rs):** `cargo test --test bdd -- write_pipeline`. Services: real SurrealDB; **wiremock** stubs for `LlmClient::extract`, `EmbeddingClient::embed` (returns a `RECALL_EMBED_DIM` vector), `PiiDetector::scan`. Scenarios map C4's Gherkin: trusted content → persisted `Fact` (visibility `user-private`, embedding set); instruction-like content → rejected; mid-trust → quarantined; high-confidence PII span redacted; low-confidence PII → `pii_review=true`; entity rules→ML→create-new resolution; queue replay with same key persists once (idempotent derived id).
  - Behavioural check: a `remember` job for booby-trapped instruction text lands in `quarantine`, not `fact`.
- **Risks / rollback:** isolated; drop `quarantine`. Write-gate threshold calibration is a tuning risk (RISK-003).

### Phase 6 — C5 Freshness Checker

- **Goal:** the read-path conditional source-change check (`FreshnessChecker`/`BrokerFreshnessChecker`) with async re-read enqueue and deadline degradation.
- **Changes:** `src/freshness/mod.rs` (`BrokerFreshnessChecker` impl of the `FreshnessChecker` trait).
- **Rationale:** depends on C2 (enqueue `ReReadSource`); consumed by C6. Built before C6.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB + wiremock-rs):** `cargo test --test bdd -- freshness`. Services: real SurrealDB (queue); **wiremock** stub for the Faraday broker honouring `If-Modified-Since` (200 changed / 304 unchanged / connection-drop unreachable). Scenarios map C5's Gherkin: unchanged→`Current`; changed→`StalePendingRefresh` + exactly one idempotent `ReReadSource` job; broker unreachable→`UnverifiedCurrency` (no error); batch-deadline breach→remaining facts `UnverifiedCurrency`; queue-enqueue failure on a change still yields `StalePendingRefresh`.
  - Behavioural check: a changed-source fact returns `StalePendingRefresh` and a single `work_job` of kind `ReReadSource` appears.
- **Risks / rollback:** isolated module; no schema. Broker-contract drift is a deployment risk (RISK-004).

### Phase 7 — C6 Retrieval Engine

- **Goal:** the synchronous read pipeline (embed→stage-1→rerank→recency→gate/abstain→freshness-tag→cursor) within the SA-LAT-01 budgets.
- **Changes:** `src/retrieval/mod.rs` (the `recall` orchestration, ranking + gating arithmetic, cursor codec).
- **Rationale:** depends on C1 (`recall`/`get_source`) + C5 (`FreshnessChecker`). The read path's core.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Unit: `cargo test --lib retrieval` — recency-weight + gating arithmetic, cursor encode/decode (table-driven).
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB + wiremock-rs):** `cargo test --test bdd -- retrieval`. Services: real SurrealDB; **wiremock** stubs for embedding (query vector) + reranker (scores). Scenarios map C6's Gherkin: happy ranked page with `Currency`; abstain when top score < `RECALL_ABSTAIN_THRESHOLD`; reranker timeout → degrade to stage-1 order (still 200); embed timeout → fail fast `PROVIDER_TIMEOUT`; `result_cap` out-of-range → `VAL_OUT_OF_RANGE`; cursor resumes after the prior page; freshness-unreachable → page flagged `UnverifiedCurrency`.
  - Behavioural check: a recall over seeded facts returns ≤ `result_cap` ranked facts with a stable `next_cursor`, p95 asserted ≤ 200 ms in the suite.
- **Risks / rollback:** isolated; no schema. Provider latency threatening NFR-P2 is the standing risk (RISK-004).

### Phase 8 — C7 Maintenance Worker

- **Goal:** the idle-biased async keeper — consolidation, supersession, decay, re-embed, verifiable hard delete.
- **Changes:** `src/maintenance/mod.rs` (`InsightCandidate`, `ContradictionVerdict`, decay/consolidation pure cores, job handlers, scheduler), `migrations/0004_maintenance.{up,down}.surql` (`maintenance_state`).
- **Rationale:** depends on C1 (scans/mutate/hard_delete) + C2 (consume Consolidate/ReEmbed/HardDelete). Builds on the read path so reinforcement bookkeeping is consistent.
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Schema: `0004_maintenance` applies + down path (non-destructive drop) verified.
  - Unit: `cargo test --lib maintenance` — Ebbinghaus decay/prune predicate, insight decaying-confidence, contradiction side-selection (table-driven, ADR-007/010).
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB + wiremock-rs):** `cargo test --test bdd -- maintenance`. Services: real SurrealDB; **wiremock** stubs for `LlmClient::consolidate` + `EmbeddingClient::embed`. Scenarios map C7's Gherkin: idle-biased consolidation promotes a validated insight (insight never outranks sources); a failed consolidation cycle leaves prior memory intact; contradicting facts → superseded (validity ended, history retained); low-salience stale fact pruned (validity-ended) while a high-salience one survives; verifiable `HardDelete` job returns a `DeletionProof`; re-embed dim-mismatch skips the fact.
  - Behavioural check: ten similar episodes consolidate into one `consolidated` insight with decaying confidence; a contradiction supersedes the older fact without deleting it.
- **Risks / rollback:** isolated; non-destructive maintenance (hard delete only via explicit job). Self-reinforcing error is mitigated by validate-before-promote (RISK-005).

### Phase 9 — C8 HTTP API Edge + remaining cross-cutting (X8, X9, X10, X11, X12, X13)

- **Goal:** the public HTTP surface, full middleware stack, and the operational endpoints — the assembled service.
- **Changes:** `src/api/mod.rs` (axum router, all `/v1` + operational routes, generated OpenAPI), `src/api/middleware/{auth,ratelimit,cors,validate}.rs`, `src/api/{health,pagination}.rs`, `src/shutdown.rs`, `migrations/0005_idempotency.{up,down}.surql` (`idempotency_record`).
- **Rationale:** top of the DAG; depends on C1, C2, C3, C6. Wires every component + cross-cutting concern into the single binary. The async write path means C8 enqueues on C2 (no synchronous C8→C4 edge).
- **Exit criteria:**
  - Compiles + lint: `cargo build` / `cargo clippy -- -D warnings`.
  - Schema: `0005_idempotency` applies + down path verified.
  - **Integration (cucumber-rs + testcontainers-rs SurrealDB + Dex + wiremock-rs):** `cargo test --test bdd -- api_edge`. Full stack up. Scenarios map C8's Gherkin + the cross-cutting specs: each route happy/edge/error; middleware order (body-limit→auth→rate-limit→idempotency→handler→audit); idempotent write replay returns the original ack; `429` with `RateLimit-*`/`Retry-After`; opaque-cursor pagination; `VAL_BODY_TOO_LARGE` before parse; `/readyz` 503 on dim-mismatch; an audit row written per authenticated call (never the token); SIGTERM drains in-flight + reverts an interrupted job's lease (X13).
  - Behavioural check: `POST /v1/memories` → (async) → `POST /v1/recall` returns the remembered fact; `DELETE /v1/memories/{id}` returns a `DeletionProof`.
- **Risks / rollback:** the integration point; revert the phase to fall back to Phase-8 state. Eventual-consistency surprise on the write→recall round-trip (RISK-006).

### Phase 10 — Integration hardening & whole-system verification gate (Playbook Step 7)

- **Goal:** prove the assembled system end-to-end and meet the shipping gates.
- **Changes:** complete the cross-component BDD features under `tests/bdd/features/`; add the secscan/coverage CI wiring.
- **Rationale:** the final gate before `/sync-check`; exercises flows no single-component phase covers.
- **Exit criteria:**
  - **Whole-system BDD:** `cargo test --test bdd` (all features) green with the full stack (testcontainers SurrealDB + Dex, wiremock providers/broker). Cross-component flows: auth→scope→retrieval; async write→store→recall (eventual consistency); supersession over time; recall freshness branch (changed/unreachable); verifiable delete proof; **cross-tenant isolation (NFR-PR1)** as a first-class scenario.
  - **Coverage gate:** `cargo llvm-cov --workspace --fail-under-lines 70` (C3 rule ≥ 70%).
  - **Security gate:** `secscan` clean (run the `secscan` skill) + `cargo audit` clean — no hardcoded secrets, parameterised SurrealQL only, no token/PII/content in logs.
  - **Smoke:** single binary boots with embedded store, `/readyz` green, a remember→recall round-trip and a forget→deletion-proof round-trip succeed.
- **Risks / rollback:** verification-only; failures route back to the offending component phase.

---

## Cross-cutting validation

Final black-box gate before the change is considered complete (the gate `/sync-check` relies on):

```
cargo build && cargo clippy -- -D warnings
cargo test --test bdd                       # all cucumber features, full stack via testcontainers + wiremock
cargo llvm-cov --workspace --fail-under-lines 70
cargo audit && <secscan skill>
```

All integrating services brought up real-via-testcontainers (SurrealDB `surrealdb/surrealdb:v2.x`, Dex
`dexidp/dex:v2.x`) or stubbed-to-contract via wiremock-rs (embedding/reranker/LLM providers + Faraday
broker). After this gate is clean, run `/sync-check` to confirm Design ↔ Spec ↔ Code alignment
(*no exit without sync*).

---

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Add NATS work-queue backend behind the WorkQueue trait
  why: store-backed queue ships first to keep the single-binary story (SA-QUEUE-01); NATS is the scale-out option (OQ-QUEUE), non-architectural.
  source: planning
  suggested-command: /breakdown FU-001 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-002
  title: Add local/in-process embedder option for the read path
  why: ADR-012 names a local embedder as the NFR-P2/P3 latency mitigation if the remote query-embed hop is too slow; not needed for v1 correctness.
  source: planning
  suggested-command: /breakdown FU-002 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-003
  title: Implement and A/B-gate query reformulation in the Retrieval Engine
  why: off by default (good-mem §7.3 reports it can underperform dense retrieval); RECALL_REFORMULATION_ENABLED is wired but the reformulation logic is deferred.
  source: planning
  suggested-command: /breakdown FU-003 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-004
  title: LLM entity adjudication tier in the Write Pipeline
  why: v1 uses rules→ML→create-new (HLD-impact-pass deferral, 10-risks); add the LLM adjudication tier once duplication rates are measured. blocked-by data from FU-005.
  source: planning
  suggested-command: /breakdown FU-004 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  blocked-by: [FU-005]
  added: 2026-06-20
- id: FU-005
  title: Instrument entity-duplication rate metric
  why: needed to decide whether FU-004 (LLM adjudication) is worth the cost; the maintenance merge_entities pass folds duplicates in the meantime.
  source: planning
  suggested-command: /breakdown FU-005 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-006
  title: Team→database physical-isolation escape hatch
  why: ADR-011 residual — promote a team to its own database when it needs physical isolation; v1 uses record-level team scoping only.
  source: planning
  suggested-command: /breakdown FU-006 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-007
  title: Complete OQ-STORE remote leg + confirm at real OQ-VOLUMES scale
  why: Phase 0 passed the embedded leg provisionally (ANN p95 9.46ms, stage-1 15.60ms at 25k/namespace, indexed) but the remote SurrealDB/TiKV leg could not run here (no docker daemon) and the real target corpus is still OQ-VOLUMES-TBC. Run the remote leg on real infra and re-confirm once OQ-VOLUMES is pinned. Note the indexed-vs-unindexed finding — the `ent`/scope index is load-bearing for the graph leg (49ms→2ms).
  source: apply-phase-0
  suggested-command: /breakdown FU-007 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: FU-008
  title: Reconcile the C1 spec DDL/SurrealQL to the as-built SurrealDB 3.x implementation
  why: Phase 2 had to adapt the C1 spec's illustrative 2.x SurrealQL to SurrealDB 3.x — BM25 index is `FIELDS content_text FULLTEXT ANALYZER recall_text BM25` (not `SEARCH ANALYZER`) over a derived `content_text` projection; HNSW `DIMENSION` is a `<dim>` placeholder substituted from RECALL_EMBED_DIM; a `fact_ent` index was added (load-bearing for the graph leg); relationship endpoints stored as `from_ent`/`to_ent` (reserved-word clash); namespace/database DDL moved into the Migrator; `schema_migrations` created outside the numbered set. These are REALITY-DRIFT vs `docs/spec/components/memory-store.md`; reconcile via /sync-spec (code→spec) so /sync-check passes.
  source: apply-phase-2
  suggested-command: /sync-spec docs/spec/components/memory-store.md
  status: open
  added: 2026-06-21
- id: FU-009
  title: Unify embedded + remote Store behind one type
  why: Phase 2's `Store` is typed over the local engine, so `connect_remote` returns Unavailable; the remote engine seam (NFR-MA1, ADR-009 scale-out) is currently exercised only by the testcontainers test. Unify embedded/remote connection behind the one `Store` type so remote deployment works without a code change. Relates to FU-007.
  source: apply-phase-2
  suggested-command: /breakdown FU-009 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-21
- id: FU-010
  title: Reconcile the C2 work-queue spec to the as-built SurrealDB 3.x implementation
  why: Phase 3 adapted the C2 spec to 3.x — `TYPE object FLEXIBLE` (not `FLEXIBLE TYPE object`) for `work_job.payload`/`dead_letter.payload`/`dead_letter.scope`; the atomic claim's `UPDATE … LIMIT 1` became an inner `SELECT … LIMIT 1` subquery (UPDATE-level LIMIT unsupported), relying on SurrealKV optimistic-concurrency for single-winner; dedup matches `scope.tenant`/`user`/`team` sub-fields rather than whole-object equality. Also the spec's `StoreWorkQueue::new(store: Arc<dyn MemoryStore>, …)` was built over a shared `Surreal<Db>` handle (the `MemoryStore` trait exposes no raw DB handle) per the plan's instruction. REALITY-DRIFT vs `docs/spec/components/work-queue.md`; reconcile via /sync-spec so /sync-check passes.
  source: apply-phase-3
  suggested-command: /sync-spec docs/spec/components/work-queue.md
  status: open
  added: 2026-06-21
- id: FU-011
  title: Pin the production OIDC tenant/jti/groups claims (OQ-IDP) + Dex test-coverage note
  why: Phase 4's C3 validation is exercised end-to-end, but real Dex (password connector) emits only iss/sub/aud/exp/iat/email — not a custom `tenant` claim, nor `jti`, nor `groups` for static users. The integration split is honest (real Dex covers crypto/discovery/JWKS/standard claims; a real-crypto local issuer covers the custom `tenant`/`groups`/`scope`/`jti` mapping and negative paths), but the PRODUCTION IdP must be configured to emit `tenant` (RECALL_OIDC_TENANT_CLAIM), `groups` (RECALL_OIDC_TEAMS_CLAIM) and a `jti` (SA-AUDIT-01) — resolve OQ-IDP with the chosen provider and add a deployment OIDC-config check. Also additive (non-drifting) C3 helpers `AuthConfig::from_config()` / `cached_key_count()` and the `jsonwebtoken = 10` pin to note in spec reconciliation.
  source: apply-phase-4
  suggested-command: /breakdown FU-011 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-21
- id: FU-012
  title: Reconcile the C4 write-pipeline spec to the as-built implementation + provider wire contracts
  why: Phase 5 adapted the C4 spec — `quarantine.content` is `TYPE object FLEXIBLE`; `entities`/`source_id` stored as `array<string>`/`option<string>` (ids are "table:key" strings, matching C1); NFC normalisation is currently identity (documented assumption); `ExtractedFact` gained `memory_class`, `PiiSpan` gained `start`/`end`. It also DEFINED concrete provider wire contracts not previously in the spec: embed `POST /embeddings {model,input[]}→{embeddings[][]}`, LLM extract `POST /extract {content}→{facts[]}`, PII `POST /pii/scan {content}→{spans[]}`. Reconcile these into `docs/spec/components/write-pipeline.md` (and a provider-contract note) via /sync-spec so /sync-check passes.
  source: apply-phase-5
  suggested-command: /sync-spec docs/spec/components/write-pipeline.md
  status: open
  added: 2026-06-21
- id: FU-013
  title: Reconcile the C5 freshness spec to the as-built broker wire contract
  why: Phase 6 DEFINED the concrete Faraday-broker conditional-check wire contract the C5 spec left to the (spec-less) broker adapter — `GET {RECALL_BROKER_URL}/sources/{origin_ref}/freshness` with `If-None-Match: <modification_marker>` and `X-Recall-Tenant`/`X-Recall-User`/`X-Correlation-Id` headers; `304`->unchanged, `200`->changed, any other status->absorbed ProviderError. It also added `Debug`/`PartialEq`/`Eq` derives to `Currency` (additive, non-drifting). Reconcile the wire contract into `docs/spec/components/freshness-checker.md` (and the shared provider-contract note alongside FU-012's) via /sync-spec so /sync-check passes.
  source: apply-phase-6
  suggested-command: /sync-spec docs/spec/components/freshness-checker.md
  status: open
  added: 2026-06-21
- id: FU-014
  title: Reconcile the C6 retrieval spec + the recall→HNSW-KNN change + the rerank wire contract
  why: Phase 7 implemented C6 and resolved RISK-008. Spec reconciliation needed for (a) C1 — `MemoryStore::recall`'s vector signal changed from the spec's illustrative brute-force `vector::similarity::cosine(...) ORDER BY` to the HNSW KNN operator `embedding <|knn_k,ef|> $qv` + `vector::distance::knn()` with 4× over-fetch-then-filter (relates to FU-008); (b) C6 — the cursor is concretely base64url(JSON `{s,id}`), keyword tokenisation is a simple non-alphanumeric/lowercase/dedupe split feeding C1's `recall_text` BM25 analyzer, reformulation is a no-op pass-through, and `Cursor`/`RetrievalConfig`/`RecallOutcome` are the as-built types; (c) the cross-encoder rerank wire contract was DEFINED here — `POST {RECALL_RERANK_URL}/rerank {query,documents[]}→{scores[]}` (positionally aligned) — add it to the provider-contract note alongside FU-012/FU-013. Reconcile via /sync-spec so /sync-check passes.
  source: apply-phase-7
  suggested-command: /sync-spec docs/spec/components/retrieval-engine.md
  status: open
  added: 2026-06-21
- id: FU-015
  title: Reconcile the C7 maintenance spec to the as-built (consolidate wire contract, InsightCandidate fields, maintenance_state persistence)
  why: Phase 8 implemented C7 with three as-built decisions to reconcile into `docs/spec/components/maintenance-worker.md`/C1: (a) the LLM consolidate wire contract was DEFINED here — `POST {RECALL_LLM_URL}/consolidate {episodes:[<Fact>]}→{insights:[{content,entities,derived_from,confidence,support_count}]}` (add to the provider-contract note alongside FU-012/FU-013/FU-014); (b) `InsightCandidate` gained `entities`/`support_count` in `src/types/ports.rs` to match the C7 spec's owned type; (c) `run_scheduler`'s per-tenant `last_cycle_at` is held in-memory (not persisted) — the `maintenance_state` table (migration 0004) exists but the worker does not yet read/write it (the worker holds only the four spec deps), so cycle bookkeeping resets on restart (benign re-run). Reconcile via /sync-spec; implement maintenance_state persistence if cross-restart idle/fallback accuracy is required. blocked-by/relates-to RISK-009.
  source: apply-phase-8
  suggested-command: /sync-spec docs/spec/components/maintenance-worker.md
  status: open
  added: 2026-06-21
- id: FU-016
  title: Reconcile the C8 HTTP API Edge spec to the as-built implementation
  why: Phase 9 implemented C8 with several as-built decisions to reconcile into `docs/spec/components/http-api-edge.md` (and the C1 0001 migration). (a) `DELETE /v1/memories/{id}` calls `store.hard_delete` DIRECTLY (synchronous, proof-gated) instead of enqueuing a C2 HardDelete job and awaiting a proof — no job-result store exists and C7's `handle_hard_delete` itself just calls `store.hard_delete`, so this is functionally identical and preserves SA-DELETE-01; the spec's Forget sequence (enqueue+await) should be reconciled to the direct call (or a job-result store added if the async-worker ownership is required). (b) C6 returns `RecallOutcome { response, next_cursor, abstained }`, not the spec's `(RecallResponse, RecallMeta)` tuple — mapped to `Meta`. (c) OpenAPI is a hand-built `serde_json` 3.1 document, not auto-derived (SA-VER-01). (d) idempotency get/put are concrete `Store` methods (not on the `MemoryStore` trait); the idempotency_record id is a uuidv5 over (tenant,user,route,key). (e) `audit_log.scope` was changed to `TYPE object FLEXIBLE` in the C1-owned `migrations/0001_init.up.surql` so the audit-write contract works on 3.x (non-destructive; relates to FU-008). (f) `/healthz` keeps `{"data":{"status":"live"},...}` (binding boot.feature) rather than the spec's `{"status":"ok"}`. (g) rate-limit burst stays the spec-fixed 40/10 with a `TokenBucket::empty()` test seam. (h) `build_state` is now async and wires the full stack via `Store::connect` (SurrealKV at RECALL_STORE_PATH; relates to FU-009). Reconcile via /sync-spec so /sync-check passes.
  source: apply-phase-9
  suggested-command: /sync-spec docs/spec/components/http-api-edge.md
  status: open
  added: 2026-06-21
- id: FU-017
  title: Run the cargo-audit dependency-CVE gate (tool absent in the build env)
  why: Phase 10's security gate names `cargo audit` alongside secscan, but `cargo-audit` is not installed in this environment, so the dependency-CVE scan could not run (the four mandatory framework gates — build, lint, integration, secscan — all passed; coverage passed at 77.73% ≥ 70%). secscan (static) is clean repo-wide. Install and run before shipping.
  source: apply-phase-10
  suggested-command: cargo install cargo-audit && cargo audit
  status: open
  added: 2026-06-21
- id: FU-018
  title: Reconcile the spec migration story to the squashed single 0001_init migration
  why: The five numbered migrations (0001 init, 0002 queue, 0003 quarantine, 0004 maintenance, 0005 idempotency) were squashed into a single `migrations/0001_init.{up,down}.surql` before first release (greenfield, no shipped DB). `src/store/migrate.rs` now registers one `version: 1, slug: "init"` entry and the migration-count test asserts `LATEST = 1`. The per-component spec Data Model sections (C1/C2/C4/C7/C8) and the X7 migrations cross-cutting spec still describe separate numbered migrations — fold this into the FU-008/010/012/015/016 /sync-spec reconciliations so the specs describe one initial migration.
  source: sync-check-resolution
  suggested-command: /sync-spec docs/spec/components/memory-store.md
  status: open
  added: 2026-06-21
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: Embedded SurrealDB misses the latency/scale targets
  why: ADR-009 (Rust+embedded) rests on the store embedding in-process; a Phase-0 spike failure reopens ADR-003 and ADR-009 — a rearchitecture, not a config swap.
  source: planning
  likelihood: medium
  impact: high
  mitigation: Phase 0 spike gates the build before any Rust scaffolding; keep the engine abstraction tested against the remote path too.
  status: open
  added: 2026-06-20
- id: RISK-002
  title: Cross-tenant data leakage
  why: a read-filter or namespace-selection bug could surface one tenant's facts to another; the graph/vector neighbour paths are the subtle cases.
  source: planning
  likelihood: low
  impact: high
  mitigation: namespace-per-tenant is structural (ADR-011); cross-tenant isolation is a first-class BDD scenario in Phases 2 and 10 (NFR-PR1).
  status: open
  added: 2026-06-20
- id: RISK-003
  title: Write-gate trust-threshold miscalibration
  why: thresholds too strict reject good facts; too loose admit poison (ADR-008). Defaults are unvalidated against labelled data.
  source: planning
  likelihood: medium
  impact: medium
  mitigation: quarantine (not hard-reject) the uncertain; monitor writes_rejected_total/writes_quarantined_total; tune post-launch.
  status: open
  added: 2026-06-20
- id: RISK-004
  title: External provider/broker latency or contract drift threatens NFR-P2
  why: query-embed + rerank are on the read path (ADR-012); a slow or contract-divergent provider/broker blows the 200 ms budget or breaks freshness.
  source: planning
  likelihood: medium
  impact: medium
  mitigation: per-inference sub-budgets with degradation (SA-LAT-01); wiremock contract tests in CI; FU-002 local embedder as the latency escape hatch.
  suggested-command: /breakdown FU-002 from docs/wip/plan/01-recall-greenfield-build.md
  status: open
  added: 2026-06-20
- id: RISK-005
  title: Self-reinforcing consolidation error
  why: a wrong consolidated insight could become permanent knowledge and outrank its sources.
  source: planning
  likelihood: low
  impact: medium
  mitigation: validate insights against sources before promotion; decaying confidence; an insight never outranks its sources (ADR-006); Phase-8 BDD asserts this.
  status: open
  added: 2026-06-20
- id: RISK-006
  title: Eventual-consistency surprise on write→recall
  why: the async write path means a just-remembered fact is not immediately recallable; callers may expect read-after-write.
  source: planning
  likelihood: medium
  impact: low
  mitigation: document the contract; idempotent acks; the Phase-9/10 round-trip scenario asserts eventual (not immediate) visibility.
  status: open
  added: 2026-06-20
- id: RISK-007
  title: HNSW ANN p99 latency tail under embedded SurrealDB
  why: the OQ-STORE spike showed ANN p95 well under 50ms but occasional p99 outliers of 78–135ms at 25k/namespace; if the tail worsens at real scale it threatens an NFR-P2 p99 SLO.
  source: apply-phase-0
  likelihood: medium
  impact: low
  mitigation: tune HNSW EF/M params and warm the index; assert p95 (not just mean) in the Phase 7 retrieval integration tests; revisit under FU-007 at real scale.
  status: open
  added: 2026-06-20
- id: RISK-008
  title: recall() does a brute-force cosine scan, not an HNSW ANN lookup
  why: the C1 `recall` vector query (src/store/mod.rs ~L241) is `vector::similarity::cosine(embedding,$qv) ... ORDER BY s DESC LIMIT $k`, which does NOT use the `fact_hnsw` index — SurrealDB only uses HNSW via the `<|K,EF|>` KNN operator. So recall is O(N) per query and will breach NFR-P3 (ANN ≤ 50ms) at the target corpus (the spike showed `<|50,40|>` = 4–10ms vs a scan). The C1 spec's own DDL example wrote the same brute-force form, so spec and code agree but both contradict NFR-P3. Functionally correct; latency-incorrect.
  source: apply-phase-2
  likelihood: high
  impact: high
  mitigation: switch recall to the `<|K,EF|>` KNN operator and reconcile the scope read-filter (KNN-then-filter, or candidate over-fetch then filter); MUST be resolved in/before Phase 7 (whose exit gate asserts p95 ≤ 200ms) and the C1 spec DDL updated to match. RESOLVED in apply-phase-7: `src/store/mod.rs` recall now uses `embedding <|knn_k,ef|> $qv` with `vector::distance::knn()` (knn_k = 4× stage1_k over-fetch, then KNN-then-filter on the scope/validity/meta predicates, ORDER BY dist ASC LIMIT stage1_k); similarity recovered as `1 - cosine_distance`. Validated against the real in-memory engine (recall + cross-tenant tests green). The C1 spec DDL/query reconciliation rides on FU-008/FU-014.
  suggested-command: /breakdown RISK-008 from docs/wip/plan/01-recall-greenfield-build.md
  status: mitigated
  added: 2026-06-21
- id: RISK-009
  title: Maintenance worker mutations silently no-op on non-tenant-shared facts
  why: C7's maintenance ScopeContext sets user="" and teams=[] (per the C7 spec Shared Context — scoping "solely to satisfy the MemoryStore signature", tenant boundary structural). But C1's `get_fact`/`end_validity`/`update_fact_maintenance_fields`/`hard_delete` apply the per-user read filter (`owner.user=$cuser OR team-shared∈teams OR tenant-shared`), so with an empty user/teams they admit ONLY tenant-shared facts. The duty scans (`scan_*`) read the whole namespace, but the subsequent mutations return NotFound for user-private and team-shared facts. Consequence in production: decay-prune, supersession, and verifiable hard-delete silently do nothing for the bulk of memory (most facts are user-private) — memory grows unbounded, contradictions persist, and right-to-erasure fails for user-private facts. Functionally hidden at test scale because the Phase-8 BDD seeds tenant-shared facts; analogous to RISK-008 (correct-at-test-scale, wrong in prod).
  source: apply-phase-8
  likelihood: high
  impact: high
  mitigation: add a maintenance/admin store read path that selects the tenant namespace WITHOUT the per-user read filter (the tenant boundary stays structural via the namespace), used when the ScopeContext is the maintenance context; or have the C1 trait accept a maintenance scope that bypasses the per-user filter. MUST be resolved before C7 ships to production; reconcile C1 + C7 specs to match. Tracked alongside FU-015. RESOLVED in the /sync-check resolution pass: `src/store/mod.rs` now has `is_maintenance_scope(ctx)` (jti == "maintenance" + empty user) and `fact_read_filter(ctx)`; `get_fact` and `hard_delete`'s derived-collection query use the maintenance-aware filter, so decay/supersession/hard-delete reach user-private + team-shared facts. Tenant isolation stays structural (every method `ensure_and_use(ctx.tenant)` first). Covered by `store::tests::maintenance_scope_reaches_user_private_facts_within_the_tenant_only` (reads a user-private fact + ends its validity; a cross-tenant maintenance scope still sees nothing). C1/C7 spec reconciliation rides on FU-015.
  suggested-command: /breakdown RISK-009 from docs/wip/plan/01-recall-greenfield-build.md
  status: mitigated
  added: 2026-06-21
```
