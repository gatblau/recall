# Phase 3 — Detailed Component Specifications (index)

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.7.0 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20 · **Amended:** 2026-06-22 (RFC 01, ADR-014; RFC 02, ADR-015), 2026-06-27 (RFC 01-MCP, ADR-016)

Phase 3 exceeds the single-file threshold, so each component is one self-contained file under
`docs/spec/components/`. Each follows the Phase 3 component template (Purpose, Approach, Shared
Context, Public Interface + Example, Internal Logic, Data Model, Error Table, Acceptance Criteria,
Performance/Security/Observability, Gaps) and carries its own `derivedFromHld:` pin (0.6.0 for the
specs amended under RFC 02 / ADR-015 — C4, C7, and C8; 0.5.0 for C6 amended under RFC 01 / ADR-014;
0.4.x otherwise). Every spec is
self-contained — a code-generating LLM implements it from its own file plus the shared types it
duplicates into its Shared Context.

The shared contract these specs consume — response envelopes, domain types, `ScopeContext` and the
read-filter rule, API payloads, work-queue types, the `MemoryStore`/`WorkQueue`
and provider traits, the typed `AppError`, and every env var — is defined once in
`phase-2-architecture.md` §2C/§2D and duplicated (not cross-linked) into each component below.

## Components (in build order — the Phase 2B DAG)

| # | Component | File | Phase | Depends on | Gaps |
|---|---|---|---|---|---|
| C1 | Memory Store | [components/memory-store.md](./components/memory-store.md) | 1 | — | None |
| C2 | Durable Work Queue | [components/work-queue.md](./components/work-queue.md) | 2 | C1 | None |
| C3 | Auth & Scope | [components/auth-scope.md](./components/auth-scope.md) | 2 | — | None |
| C4 | Write Pipeline | [components/write-pipeline.md](./components/write-pipeline.md) | 3 | C1, C2 | None |
| ~~C5~~ | ~~Freshness Checker~~ | [components/freshness-checker.md](./components/freshness-checker.md) | retired (ADR-014) | — | — |
| C6 | Retrieval Engine | [components/retrieval-engine.md](./components/retrieval-engine.md) | 4 | C1 | None |
| C7 | Maintenance Worker | [components/maintenance-worker.md](./components/maintenance-worker.md) | 4 | C1, C2 | None |
| C8 | HTTP API Edge | [components/http-api-edge.md](./components/http-api-edge.md) | 5 | C1, C2, C3, C6 | None |

The seven live component specs (C5 retired by ADR-014) have an empty *Gaps* section after the Phase 3
reconciliation pass (which promoted the deliberately-abbreviated `MemoryStore` stub to the full
canonical surface in §2C.6, added the cross-component config keys to §2D, added `pii_review` to `Fact`,
and gave the shared provider types — `ProviderError`, `PiiSpan` — one canonical home). No spec is
blocked from `codegen` on Gaps grounds; promotion to *Approved* is gated only by the Phase 6
self-audit.

External provider adapters (`EmbeddingClient`, `RerankClient`) are thin HTTP adapters defined by their
§2C.6 traits and carry no domain logic, so they have no standalone Phase 3 spec; the consuming
component specs depend on the trait signatures. The `PiiDetector` trait remains a DI seam, but its
default impl is **in-process and deterministic** (regex/pattern matching, no network call) per ADR-015.
Recall holds no LLM: there is no `LlmClient` adapter and no `InsightCandidate` type.

## HLD-impact-pass

The Phase 3.5 HLD-impact scan over these drafts surfaced findings that are routed back to the HLD as a
single combined review block — see the run output (glossary terms `stale-pending-refresh` /
`unverified-currency`, and the v1 deferral of LLM entity adjudication). Spec-level findings (exact
field types, config keys, trait signatures) stayed in the spec per the bubble-up rule and were
reconciled into Phase 2.

**RFC 01 / ADR-014 amendment (2026-06-22):** C5 Freshness Checker retired; the recall-side
source-change check, the `Currency`/`FreshnessChecker`/`BrokerClient`/`SourceState` types, and the
`ReReadSource` job are removed. C6 returns conditional source provenance (`include_provenance` →
`RankedFact.source`) so the agent runs the freshness loop. The HLD edits (ADR-014 superseding ADR-013;
glossary terms `stale-pending-refresh`/`unverified-currency` removed) landed in Review block 1; these
Phase 3 edits introduced no new HLD findings.

**RFC 02 / ADR-015 amendment (2026-06-22):** recall is **LLM-free**. C4 drops LLM-backed fact
extraction — a `remember` payload now carries a structured, agent-asserted `content` object that the
write pipeline wraps directly (the former agent-stated branch becomes the normal path), and entity
resolution's terminal tier is create-new by design (not a deferral). C7 drops server-side
consolidation, reducing the maintenance worker from five duties to four (supersession, decay/forget,
re-embed, verifiable hard delete). The `LlmClient` trait, the `HttpLlmClient` adapter, the
`InsightCandidate` type, the `Consolidate` `JobKind`, and the `RECALL_LLM_URL`/`RECALL_LLM_API_KEY`
config (plus the consolidation-only `RECALL_MAINT_CONSOLIDATE_MIN_EPISODES` /
`RECALL_INSIGHT_DECAY_FACTOR` keys) are removed; `RECALL_CONSOLIDATE_MAX_INTERVAL_SECS` is renamed
`RECALL_MAINT_MAX_INTERVAL_SECS`. PII detection is now an **in-process deterministic** detector
(regex/pattern matching, no network call, no endpoint or key). The `consolidated` `MemoryClass` and
`Fact.derived_from` are retained: the agent writes consolidated insights itself as agent-stated facts.
