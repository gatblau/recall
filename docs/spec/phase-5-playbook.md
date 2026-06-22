# Phase 5 — Generation Playbook

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.5.0 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20 · **Amended:** 2026-06-22 (RFC 01, ADR-014)

The ordered build checklist `codegen` follows, one Playbook step per invocation. Steps respect the
Phase 2B dependency DAG: every component depends only on lower-numbered steps. The cross-cutting
concerns (X1–X13) are Phase-0 foundations built into scaffolding and consumed everywhere.

**Toolchain:** Rust + Cargo (ADR-009) plus the active **Practice Pack** for exact build/test/lint
commands (HLD 05; `codegen`/`testgen` read them from the Pack — this Playbook names *what* to run, not
the literal command strings). Test strategy is outside-in BDD with testcontainers + Dex for OIDC and
SurrealDB's in-memory backend for the fast inner loop (ADR-010).

---

## Step 0 — OQ-STORE spike (precondition, before any Rust scaffolding)

**Load-bearing for ADR-009.** Validate that embedded SurrealDB meets the latency/scale targets
(NFR-P2 p95 ≤ 200 ms, NFR-P3 ANN ≤ 50 ms) for graph + vector (HNSW) + keyword (BM25) over rich
bi-temporal edges, **and** exercise the remote SurrealDB/TiKV path through the same engine abstraction.

- [ ] Build a throwaway spike: load representative facts, run ANN + BM25 + 2-hop graph queries, measure p95.
- [ ] Exercise both embedded (SurrealKV) and remote modes behind one abstraction.
- [ ] **Gate:** if the spike fails, **stop** — ADR-003 *and* ADR-009 reopen (the Rust/embedded rationale collapses); return to `/design`. Do not scaffold Rust until this passes (HLD 10 risk, OQ-STORE).

## Step 1 — Scaffolding & Phase-0 foundations

- [ ] Cargo project for crate `recall`; module skeleton: `store`, `queue`, `auth`, `write_pipeline`, `retrieval`, `maintenance`, `api`, plus `types/`, `config.rs`, `error.rs`, `obs/`, `providers/`, `shutdown.rs`. (No `freshness` module — C5 retired by ADR-014.)
- [ ] **X1 Error Handling** — `AppError` (§2C.7), the canonical error-code registry, envelopes (§2C.1), `map_error`, panic-recovery layer.
- [ ] **X6 Configuration** — typed `Config` from env > file > default with startup validation (§2D, 40+ keys); fail-fast on missing required keys and the embedding-dimension check (SA-EMBED-01).
- [ ] **X3/X4/X5 Observability** — `tracing` JSON logging with correlation-id + redaction, the metric catalogue, OTLP tracing with async context propagation.
- [ ] Shared types (§2C.1–§2C.6): envelopes, domain entities (incl. `Fact.pii_review`), scope + read-filter, API payloads (incl. `RecallRequest.include_provenance`, `RankedFact.source: Option<SourceProvenance>`), work-queue types, the `MemoryStore`/`WorkQueue` and provider traits, `ProviderError`/`PiiSpan`.
- [ ] Provider adapters (`src/providers/`): thin HTTP impls of `EmbeddingClient`, `RerankClient`, `LlmClient`, `PiiDetector` with timeouts + bounded retry; test stand-ins. (No `BrokerClient` — ADR-014.)
- [ ] CI wiring per the Practice Pack: build, test, lint, coverage gate (≥70%), `secscan`.

## Step 2 — C1 Memory Store (Phase 1)

- [ ] Implement the full `MemoryStore` trait (`components/memory-store.md`) over embedded SurrealDB; namespace-per-tenant (ADR-011); HNSW + BM25 + graph signals; bi-temporal CRUD; `supersede`; verifiable `hard_delete` with `DeletionProof`; maintenance scans; `append_audit`; `list_tenants`.
- [ ] **X7 Migrations** — the `Migrator`, `0001_init` up/down per tenant (schemaless fact/entity/relationship/source + schemafull append-only `audit_log`), `schema_migrations`, dry-run.
- [ ] Tests: unit for read-filter + bi-temporal logic; integration (SurrealDB in-memory + container) for every error-table row and Gherkin scenario, incl. cross-tenant isolation and verifiable-delete proof.

## Step 3 — C2 Work Queue · C3 Auth & Scope (Phase 2, independent — may build in parallel)

- [ ] **C2** (`components/work-queue.md`): store-backed `work_job` + `dead_letter`, atomic claim/lease, idempotent enqueue dedup, backoff+jitter→dead-letter, lease-reaper over `list_tenants`. Tests for concurrent-claim safety + reaper.
- [ ] **C3** (`components/auth-scope.md`): OIDC discovery + JWKS cache/refresh, full token validation (alg-allowlist, iss/aud/exp/nbf), `ScopeContext` construction, `authorise`, read-filter helper. Tests use **Dex** (ADR-010): valid/expired/wrong-aud/missing-scope.

## Step 4 — C4 Write Pipeline (Phase 3)

- [ ] **C4** (`components/write-pipeline.md`): the 8-step pipeline (filter→extract→normalise→entity-resolve(rules→ML→create-new)→score→PII scan→write gate→embed+persist), `quarantine` table, idempotent persist. Tests for admit/quarantine/reject, PII redaction/flag, replay idempotency.
- [ ] **C5 Freshness Checker — RETIRED (ADR-014):** not built. Freshness is agent-side; `recall` performs no source-change check and makes no outbound broker call.

## Step 5 — C6 Retrieval Engine · C7 Maintenance Worker (Phase 4)

- [ ] **C6** (`components/retrieval-engine.md`): the read pipeline (embed→stage-1 multi-signal→rerank→recency→gate/abstain→cursor→provenance-attach (conditional, ADR-014)), SA-LAT-01 sub-budgets + degradation. Tests for happy/abstain/rerank-timeout-degrade/store-timeout/provenance-on-off + the p95 budget assertion.
- [ ] **C7** (`components/maintenance-worker.md`): scheduler (idle via activity probe + fallback timer) + queue-consumer; consolidation (validate-before-promote, decaying confidence), supersession, decay (Ebbinghaus + salience floor), re-embed, verifiable hard delete; `maintenance_state`. Unit-test the decay/consolidation pure cores against case tables.

## Step 6 — C8 HTTP API Edge (Phase 5)

- [ ] **C8** (`components/http-api-edge.md`): axum router for all HLD-08 routes; middleware stack in fixed order — correlation-id → **X12** body-size/validation → **X2** auth(C3) → **X9** rate limit → idempotency (SA-IDEM-01) → handler → audit-emit (SA-AUDIT-01).
- [ ] **X8 Health** (`/healthz`, `/readyz`), **X10 Pagination** (opaque cursor), **X11 CORS** (off by default), generated `GET /openapi.json` (SA-VER-01), `GET /metrics`.
- [ ] **X13 Graceful Shutdown** — signal handling, drain, worker lease release, store flush.
- [ ] Tests: per-route happy/edge/error, idempotent replay, rate-limit headers, audit emission, auth/authz matrix.

## Step 7 — Integration & verification (whole-system gate)

- [ ] **Outside-in BDD suite** (ADR-010): drive the public API end-to-end through Dex + containerised SurrealDB + queue + provider stand-ins. Map each component's Gherkin to a scenario.
- [ ] Cross-component flows: auth→scope→retrieval; async write→store→retrieval (eventual consistency); supersession over time; recall provenance branch (include_provenance on/off); the agent-side freshness loop (recall returns provenance → agent writes a fresh superseding note → next recall returns it); verifiable delete proof; cross-tenant isolation (NFR-PR1) as a first-class case.
- [ ] **Unit cores**: decay maths, ranking fusion + recency, entity-resolution rules, write-gate thresholds — table-driven.
- [ ] **Gates (blocking):** build clean; lint clean; **coverage ≥ 70%** (C3 rule); `secscan` clean (no hardcoded secrets, parameterised queries only, no PII/token logging); migration dry-run clean.
- [ ] **Smoke**: single-binary boot with embedded store, `/readyz` green, a remember→(async)→recall round-trip, a forget→deletion-proof round-trip.

---

## Build-order summary (DAG)

```
Step 0  OQ-STORE spike  ── gate ADR-009 ──┐
Step 1  Scaffolding + X1,X3,X4,X5,X6      │  (Phase 0 foundations + shared types + providers)
Step 2  C1 Memory Store (+X7)             │  Phase 1
Step 3  C2 Work Queue · C3 Auth & Scope   │  Phase 2  (parallel)
Step 4  C4 Write Pipeline                 │  Phase 3  (C5 retired, ADR-014)
Step 5  C6 Retrieval · C7 Maintenance     │  Phase 4  (parallel)
Step 6  C8 HTTP API Edge (+X2,X8–X13)     │  Phase 5
Step 7  Integration & verification        ┘  whole-system gate
```

No cycles: each step depends only on lower-numbered steps. The async write path means C8 never calls
C4 synchronously — it enqueues on C2, so there is no C8→C4 edge.
