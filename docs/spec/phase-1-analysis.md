# Phase 1 — Analysis & Ambiguity Resolution

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.5.0 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20 · **Amended:** 2026-06-22 (RFC 01, ADR-014)

## Table of contents

- [1A — Assumptions Register](#1a--assumptions-register)
- [1B — Open Questions (blocking only)](#1b--open-questions-blocking-only)
- [1C — Glossary](#1c--glossary)

This phase turns the agentic-memory HLD (revision 0.4.0) into the analysis substrate the
component and cross-cutting specs are built on. Per rule S6 (*assumptions over questions when
possible*), every place where the HLD is silent but a sane default exists is recorded as an
assumption rather than escalated as a question. Every assumption is auditable here.

---

## 1A — Assumptions Register

The HLD's own assumptions (ASSUMP-*) and decisions (ADR-*) are binding inputs; they are restated in
the Glossary and the architecture phase. The rows below are **LLD-level assumptions** — concrete
defaults the spec picks to make components implementable in a single prompt. Each is the value the
spec assumes unless the corresponding HLD Open Question is later resolved differently.

| ID | Area | Assumption | Rationale | Impact if wrong |
|---|---|---|---|---|
| SA-ID-01 | Identity format | Every stored record (Fact, Entity, Relationship, Source) is keyed by a server-generated **UUIDv7** rendered as a 36-char lowercase string; the SurrealDB record id is `<table>:⟨uuid⟩`. | UUIDv7 is time-ordered (good index locality for bi-temporal scans) and globally unique without coordination across tenant namespaces. | Switching to ULID/UUIDv4 is a string-format change confined to the Memory Store id helper; no contract field changes type. |
| SA-ENV-01 | Error envelope | Every error response is the single shape `{"error": {"code": "<MACHINE_CODE>", "message": "<human text>", "correlation_id": "<uuid>"}}`; every success response is `{"data": <payload>, "meta": {...}}`. | HLD 07 mandates *one* success and *one* error envelope with machine `code` + human `message`; correlation id makes every response traceable. | A shape change touches the shared envelope type and every handler; caught early by the Phase 6 "documented on both sides" audit. |
| SA-ENV-02 | Error code registry | Error `code` values are SCREAMING_SNAKE_CASE drawn from one central registry (Phase 4 Error Handling spec); no handler invents an ad-hoc code. | A closed registry lets agents branch on `code` without parsing prose. | An unregistered code leaks; the Phase 4 spec is the single source and the audit checks every error-table row against it. |
| SA-SCORE-01 | Score types | `confidence` and `salience` are `f64` constrained to the closed interval `[0.0, 1.0]`; persisted as SurrealDB `float`; validated at write. | The concept notes treat both as probabilities/importance in [0,1]; one type avoids per-call-site clamping bugs. | A range change is a validation-rule edit in the Write Pipeline and Memory Store; stored values stay valid under a widened range. |
| SA-TIME-01 | Temporal model | All timestamps are UTC RFC 3339 with millisecond precision. A Fact carries `valid_from` (required), `valid_to` (nullable — open interval = currently true), and `ingested_at` (required, set server-side). | Bi-temporal model (ADR-002) needs two timelines; nullable `valid_to` encodes "still true". | A precision/zone change is confined to the time helper; stored values remain parseable. |
| SA-VIS-01 | Visibility enum | Fact `visibility` is one of exactly `user-private` (default), `team-shared`, `tenant-shared`. No other value is accepted. | HLD 04/07 name exactly these three levels under ADR-011. | Adding a level is an enum extension plus an authorisation-rule row; existing data defaults to `user-private`. |
| SA-CLASS-01 | Memory class enum | Fact `memory_class` is one of exactly `episodic`, `semantic`, `consolidated`. `procedural` is rejected with `VAL_UNSUPPORTED_CLASS` (HLD non-goal). | HLD 00 defers procedural memory explicitly; rejecting it keeps the door closed without silent acceptance. | Re-enabling procedural is an enum extension; no stored value becomes invalid. |
| SA-PAGE-01 | Pagination | Recall and any list response use **opaque cursor** pagination (`meta.next_cursor`, nullable); the cursor encodes (score, record-id) for a stable total order. Default page size 20, max 100. | HLD 07 mandates bounded, capped, paginated, token-efficient results; cursors are stable under concurrent writes where offsets are not. | Cursor encoding is internal to the Retrieval Engine; changing it does not change the response contract (cursor is opaque to callers). |
| SA-CAP-01 | Result cap | `POST /v1/recall` accepts `result_cap` in `[1, 50]`, default 10; the response never exceeds it. | Keeps a recall response inside the single-digit-thousands token budget (NFR-P5). | The cap is a config-backed constant; raising it risks NFR-P5 and is a config change, not a contract change. |
| SA-IDEM-01 | Idempotency | Every write (`remember`, `retire`, `delete`) requires an `Idempotency-Key` header (1–255 chars). The (scope, key) pair is stored with the original outcome for **24 h**; a replay inside the window returns the original result with no new side effect. | HLD 03/08 mandate idempotent acks; a bounded retention window stops unbounded key growth. | Window length is a config value; too short risks duplicate writes on slow retries, caught by the Remember Gherkin replay scenario. |
| SA-JWKS-01 | OIDC discovery cache | `recall` fetches `.well-known/openid-configuration` and the JWKS at startup and refreshes the JWKS every **3600 s**, plus an on-demand refresh (rate-limited to once per 60 s) when a token presents an unknown `kid`. | Standard JWKS rotation handling; avoids a network fetch per request while tolerating key rotation. | Cache TTL is config; too long risks rejecting freshly-rotated keys until the on-demand refresh, which mitigates it. |
| SA-QUEUE-01 | Work queue | The durable work queue is **store-backed** (a SurrealDB `work_job` table with a claim/lease protocol) by default, preserving the single-binary story; the queue is accessed through a `WorkQueue` trait so a NATS backend can replace it without touching producers/consumers (OQ-QUEUE). | ASSUMP-QUEUE + HLD 05 prefer a store-backed queue to keep one binary; the trait keeps the product swappable. | A backend swap is a trait-impl change; job payloads and the producer/consumer contract are unchanged. |
| SA-QUEUE-02 | Job retry | A failed job is retried with exponential backoff (base 2 s, factor 2, jitter ±20 %, max 5 attempts), then moved to a `dead_letter` table for manual reprocessing. | HLD 03 Remember error posture mandates bounded retry-with-backoff then dead-letter. | Backoff numbers are config; tuning them does not change the job contract. |
| SA-DECAY-01 | Decay model | Salience-adjusted retrievability follows an Ebbinghaus form `R = exp(-Δt / (s · k))` where `s` is per-fact stability (raised on each recall/reinforcement) and `k` a global constant; a Fact is a prune candidate when `R < 0.05` **and** `salience < salience_floor` (default floor 0.3). | ADR-006 commits graceful, reinforcement-resettable decay with a salience floor; the exponential form is the standard Ebbinghaus model from the concept notes. | The curve and floor are config; the model is isolated in the Maintenance Worker decay function and unit-tested against a table of cases. |
| SA-RECENCY-01 | Recency weighting | Final read-path ranking is `final = rerank_score · (1 + recency_boost)` where `recency_boost = w · exp(-age_days / τ)` (default `w=0.15`, `τ=30`). | ADR-005 names recency weighting after rerank; an explicit small bounded boost keeps it from dominating relevance. | Weights are config; isolated in the Retrieval Engine ranking function, unit-tested. |
| SA-GATE-01 | Retrieval gating (abstain) | A recall abstains (returns zero facts with `meta.abstained=true`) when the top reranked candidate's score `< abstain_threshold` (default 0.2). | HLD 03 mandates "abstain rather than pad"; an explicit threshold makes the behaviour testable. | Threshold is config; too high over-abstains, caught by the abstain Gherkin scenario. |
| SA-WGATE-01 | Write gate | The write gate scores trust in `[0,1]`; `≥ trust_admit` (default 0.7) → persist; `trust_quarantine ≤ score < trust_admit` (default 0.4) → quarantine table; `< trust_quarantine` → reject. Instruction-like content (imperative-pattern detector) is capped below `trust_quarantine`. | ADR-008 commits quarantine-not-hard-reject for the uncertain; thresholds make the three-way outcome concrete. | Thresholds are config tuned against labelled data; isolated in the Write Pipeline gate function. |
| SA-PII-01 | PII handling | On the write path a PII detector runs; a span flagged with confidence `≥ pii_redact` (default 0.9) is redacted in place (replaced by `‹redacted:‹type››`); a lower-confidence flag stores the fact with `pii_review=true` for later review. | HLD 06 commits "redact when flagged with high confidence, flag-for-review otherwise". | Thresholds are config; the detector is an injected trait so its implementation is swappable. |
| SA-EMBED-01 | Embedding contract | The embedding provider returns a fixed-dimension `f32` vector; dimension is read from config (`RECALL_EMBED_DIM`, default 1024) and must match the SurrealDB vector index dimension at startup (readiness fails on mismatch). | OQ-MODELS leaves the provider open; pinning dimension to config with a startup check prevents silent index corruption. | A dimension change requires a re-embed migration (Maintenance Worker) and an index rebuild; the startup check makes the mismatch loud. |
| SA-RERANK-01 | Rerank candidate set | Stage-1 recall returns up to `stage1_k` candidates (default 50) to the cross-encoder; the reranker scores all of them; the top `result_cap` survive gating. | ADR-005/012 bound the rerank set; 50 keeps rerank inside its NFR-P2 sub-budget. | `stage1_k` is config; larger sets cost read-path latency, smaller sets cost recall. |
| SA-LAT-01 | Read-path latency sub-budgets | Within NFR-P2 (p95 ≤ 200 ms): query-embed ≤ 60 ms, stage-1 ANN ≤ 50 ms, cross-encoder rerank ≤ 60 ms, in-process overhead ≤ 5 ms. On sub-budget breach the step degrades (skip rerank → stage-1 order) rather than blocking. The read path performs no freshness check (ADR-014). | ADR-012 requires explicit per-inference sub-budgets summing within NFR-P2; degradation keeps the path inside budget. | Sub-budgets are config; they are asserted in integration tests against the target corpus (OQ-VOLUMES pins absolute numbers). |
| SA-FRESH-01 | Freshness placement | Freshness is **agent-side** (ADR-014). `recall` performs no source-change check, makes no outbound broker call, enqueues no re-read job, and returns no `currency`; the agent (with its co-located broker) runs ask→check→update and uses the existing write/supersede endpoints for the refresh. | The broker is a per-agent local component a central `recall` cannot reach (ADR-014, superseding ADR-013). | Re-introducing a recall-side check reopens ADR-014 and adds an unreachable outbound dependency. |
| SA-PROV-01 | Recall provenance opt-in | `POST /v1/recall` accepts `include_provenance: bool` (default `false`). When `true`, each returned `RankedFact` with a source carries a `source: { origin_ref, modification_marker }` object; when `false` the field is omitted (lean response). | The agent needs the document handle + version token to check freshness itself; making it opt-in keeps the common response lean (SA-FRESH-01). | Field is additive and request-gated; a default-on flip only enlarges responses, never breaks parsing. |
| SA-AUDIT-01 | Audit trail storage | The audit trail is a per-tenant append-only SurrealDB table `audit_log` inside the tenant namespace, written synchronously on every authenticated call before the response is returned; it stores subject, operation, scope, outcome, token `jti`, correlation id, and timestamp — never the token, never fact content. | HLD 06/07 mandate a dedicated, append-only, per-tenant audit trail distinct from operational logs, erased with the tenant. | Moving the trail out of the namespace would break per-tenant erasure (ADR-011); kept in-namespace by design. |
| SA-RATE-01 | Rate limiting | A per-(subject, operation-class) **token-bucket** limiter: read class default 120 req/min burst 40; write class default 30 req/min burst 10; returns `429 RATE_LIMITED` with `RateLimit-Limit`/`RateLimit-Remaining`/`RateLimit-Reset` and `Retry-After`. | HLD 07 mandates agent-aware tiers with standard headers; token bucket gives smooth burst tolerance. | Limits are config; per-tenant overrides are a config map, not a contract change. |
| SA-MIG-01 | Migrations | Store schema evolves via numbered, ordered migration files applied through a `Migrator`; SurrealDB starts **schemaless** for `fact`/`entity`/`relationship` and tightens to **schemafull** as the model stabilises; every migration ships an explicit down path; applies to a shared store are a user action (dry-run first). | HLD 07 migrations stance + sql-safety layer rule (destructive changes surface rollback). | A missing down path blocks the migration at review; enforced by the Phase 4 Migrations spec. |
| SA-DELETE-01 | Verifiable deletion proof | A hard delete returns a `deletion_proof` = `{deleted_at, record_id, derived_removed: [...], embeddings_removed: N, digest}` where `digest` is a SHA-256 over the removed record ids; the operation is not reported complete until every derived summary and embedding is removed. | HLD 03/06 require verifiable deletion *including derived summaries and embeddings* with proof. | A partial delete must not report success; enforced by the Forget Gherkin error scenario. |
| SA-CONSOL-01 | Consolidation cadence | The Maintenance Worker runs **idle-biased**: triggered after `idle_quiet_period` (default 300 s) with no writes for a tenant, with a hard fallback timer every `consolidation_max_interval` (default 6 h). | OQ-CONSOLIDATE-CADENCE default is idle-biased; a fallback timer guarantees progress under continuous load. | Cadence is config; affects semantic-fact freshness vs background cost, not correctness. |
| SA-VER-01 | API versioning | The HTTP surface is path-versioned under `/v1`; a breaking change takes `/v2`. The OpenAPI document at `/openapi.json` is generated from the handler types, not hand-maintained. | HLD 08 commits path versioning and a machine-readable contract for runtime codegen. | Hand-drift between code and contract is prevented by generation; a generation gap is caught by the Phase 4 API spec. |

### Inherited HLD assumptions (binding, not re-derived)

These remain authoritative as written in HLD `09-decisions.md`; the spec implements them and does not
restate their rationale: **ASSUMP-IDENTITY**, **ASSUMP-OIDC**, **ASSUMP-TENANCY**, **ASSUMP-STORE-CAP**,
**ASSUMP-ASYNC**, **ASSUMP-QUEUE**, **ASSUMP-MODELS**. ASSUMP-LANG is superseded by ADR-009 (Rust +
embedded SurrealDB).

---

## 1B — Open Questions (blocking only)

**No question blocks spec authoring.** Every HLD Open Question has a defensible default (recorded as
an assumption in 1A or inherited from the HLD), so Phases 2–6 proceed end-to-end. The table below
records each HLD Open Question, the default the spec adopts, and — for one row — a non-spec blocker
the user must act on before *building*, not before *specifying*.

| ID | Question | Default adopted by this spec | Blocking for `/spec`? | Note |
|---|---|---|---|---|
| OQ-STORE | Does embedded SurrealDB meet latency/scale targets? | Assume yes; build against the embedded engine behind the `MemoryStore` trait (SA-EMBED-01, ADR-003/009). | **No for authoring.** | **Load-bearing for the Rust/embedded commitment (ADR-009).** Run the OQ-STORE spike *before* Rust scaffolding — a spike failure reopens ADR-003 and ADR-009. This is a build-order precondition, surfaced in Phase 5, not a spec blocker. |
| OQ-VOLUMES | Target corpus size, tenant counts, SLOs? | Latency NFRs use the SA-LAT-01 sub-budgets; absolute corpus/SLO numbers stay TBC. | No | Pins absolute scale numbers; the sub-budget *shape* holds regardless. |
| OQ-IDP | Which OIDC provider and exact claims? | IdP-agnostic; issuer/audience/claim names are config (Phase 2D); Dex used in tests (ADR-010). | No | Production values are deployment config. |
| OQ-CONSOLIDATE-CADENCE | When does consolidation run? | Idle-biased with a 6 h fallback timer (SA-CONSOL-01). | No | Config-tunable. |
| OQ-QUEUE | Which durable-queue product? | Store-backed default behind a `WorkQueue` trait (SA-QUEUE-01). | No | Swappable to NATS without contract change. |
| OQ-MODELS | Which embedding/reranker/LLM providers, and hosting? | External providers behind traits, configured per Phase 2D; local/in-process embedder is the read-path latency mitigation (ADR-012). | No | Read-path hosting is a latency decision, not a contract one. |

Residual HLD sub-question carried forward (non-blocking): the **team→database promotion threshold**
(the physical-isolation escape hatch under ADR-011) is left to deployment policy; the spec models
both record-level team scoping and database-per-team, so either is reachable by configuration.

---

## 1C — Glossary

Terms used across the spec set. Every term is present in the HLD glossary (HLD `00-overview.md`)
unless marked **[LLD]**, meaning it was introduced at spec time; any **[LLD]** term that names a
domain concept is a candidate for the Phase 3.5 HLD-impact-pass (glossary bubble-up, D-HLD-1).

| Term | Definition | Example |
|---|---|---|
| Fact | A structured assertion stored in memory, with provenance, validity, confidence and salience. | "Team Alpha owns the `orders` table" (valid from 2026-01-04, confidence 0.9). |
| Entity | A node in the memory graph — a person, team, system, or thing a fact refers to. | "Team Alpha", "Sarah", "`orders` table". |
| Relationship (edge) | A typed connection between entities, carrying its own properties (timestamps, confidence, source). | "owns", "is-lead-of", "supersedes". |
| Episodic memory | A record of a specific thing that happened, in sequence. | "On 2026-06-12 the user asked to audit the schema." |
| Semantic memory | General, time-independent knowledge distilled from many episodes. | "Schema audits usually surface missing indexes." |
| Consolidated Insight | A semantic Fact distilled from many episodes; carries derived-from links and decaying confidence. | Promoting ten "standup at 9am" episodes into one durable fact. |
| Consolidation | The background process that distils recurring episodes into semantic facts. | The Maintenance Worker's episodic→semantic pass. |
| Bi-temporal model | Storing two timelines per fact: validity in the world, and when the system learned it. | A "lead engineer" edge with validity 2026-01→2026-03 plus ingestion date. |
| Supersession | Ending a superseded fact's validity and recording its successor, instead of deleting. | "Sarah is lead" ended; "Tom is lead" added; both retained. |
| Salience | A fact's importance score, used to protect it from forgetting. | A production-region fact has high salience. |
| Confidence | How sure `recall` is that a fact is true; lower confidence decays faster. | An inferred fact starts at 0.5; a verified one at 0.95. |
| Salience floor | A minimum-importance threshold below which time-based forgetting may not prune. | High-salience facts survive disuse (default 0.3). |
| Provenance | The source and ingestion date attached to every fact, supporting citation and trust. | "from document X, ingested 2026-06-12". |
| Freshness loop | Ask → `recall` returns the note plus its source + version → the **agent** checks whether the source changed → re-reads and writes a fresh superseding note if stale. Runs at the agent, not in `recall` (ADR-014). | The agent re-reading a changed document and writing an updated note. |
| Write gate | The trust check on the write path that may quarantine or reject untrusted content. | Rejecting an instruction-like "fact" from a booby-trapped document. |
| Scope | The ownership/access boundary of a fact, modelled as Tenant → Team → User. | User U's facts within Tenant T. |
| Tenant | An organisation — the hard isolation boundary; one SurrealDB namespace per tenant. | "Acme Corp". |
| Team | A group within a tenant that may share memory. | Acme's Platform team. |
| Visibility | Whether a fact is private to its user, shared to a team, or shared across the tenant. | A team-shared runbook fact. |
| Faraday broker | The trusted component that authenticates as the user and calls `recall` on their behalf. | Injects the OIDC bearer token. |
| Source (provenance) | Where a Fact came from — origin reference, modification marker, trust signal. | "document X, last-modified marker `W/\"abc\"`". |
| Stage-1 recall | Multi-signal candidate retrieval (semantic + keyword + graph) before reranking. | Returns up to 50 candidates for the cross-encoder. |
| Cross-encoder rerank | A read-path discriminative model inference that reorders stage-1 candidates by relevance — not an LLM call. | Reordering 50 candidates to surface the 10 most relevant. |
| Retrieval gating / abstain | Returning no facts when no candidate clears the relevance threshold, rather than padding. | A novel query with no relevant memory returns `abstained=true`. |
| Recency weighting | A bounded boost favouring newer facts after rerank. | A fact from yesterday outranks an equally-relevant one from last year. |
| Source provenance (response) **[LLD]** | The `{ origin_ref, modification_marker }` object returned per sourced fact when `include_provenance` is set, so the agent can check source freshness itself (SA-PROV-01, ADR-014). | `{ "origin_ref": "wiki-page-7", "modification_marker": "W/\"abc\"" }`. |
| Idempotency-Key | A caller-supplied header making a write retry-safe within the retention window. | Replaying a `remember` with the same key returns the original ack. |
| Work job **[LLD]** | A unit of asynchronous work on the durable queue (extract, re-embed, consolidate, delete). | An enqueued "extract fact from content" job. |
| Dead letter **[LLD]** | A job that exhausted its retries, parked for manual reprocessing. | A write whose extraction provider stayed down past 5 attempts. |
| Deletion proof | The verifiable record returned by a hard delete (removed ids, embedding count, digest). | `{deleted_at, record_id, embeddings_removed: 3, digest: "…"}`. |
| Quarantine | A holding state for write-gate-uncertain content, neither admitted nor rejected. | An ambiguous fact held for review at trust score 0.55. |
| Trust score **[LLD]** | The write gate's `[0,1]` trust rating that drives admit/quarantine/reject. | An instruction-like payload scored 0.2 → rejected. |
| Scope partition key **[LLD]** | The derived (tenant, team(s), user) key used to filter every store query. | Built from token claims, never from the request body. |
</content>
