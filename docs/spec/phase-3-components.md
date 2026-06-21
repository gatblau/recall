# Phase 3 — Detailed Component Specifications (index)

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.4.1 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20

Phase 3 exceeds the single-file threshold, so each component is one self-contained file under
`docs/spec/components/`. Each follows the Phase 3 component template (Purpose, Approach, Shared
Context, Public Interface + Example, Internal Logic, Data Model, Error Table, Acceptance Criteria,
Performance/Security/Observability, Gaps) and carries `derivedFromHld: 0.4.0`. Every spec is
self-contained — a code-generating LLM implements it from its own file plus the shared types it
duplicates into its Shared Context.

The shared contract these specs consume — response envelopes, domain types, `ScopeContext` and the
read-filter rule, API payloads, work-queue types, the `MemoryStore`/`WorkQueue`/`FreshnessChecker`
and provider traits, the typed `AppError`, and every env var — is defined once in
`phase-2-architecture.md` §2C/§2D and duplicated (not cross-linked) into each component below.

## Components (in build order — the Phase 2B DAG)

| # | Component | File | Phase | Depends on | Gaps |
|---|---|---|---|---|---|
| C1 | Memory Store | [components/memory-store.md](./components/memory-store.md) | 1 | — | None |
| C2 | Durable Work Queue | [components/work-queue.md](./components/work-queue.md) | 2 | C1 | None |
| C3 | Auth & Scope | [components/auth-scope.md](./components/auth-scope.md) | 2 | — | None |
| C4 | Write Pipeline | [components/write-pipeline.md](./components/write-pipeline.md) | 3 | C1, C2 | None |
| C5 | Freshness Checker | [components/freshness-checker.md](./components/freshness-checker.md) | 3 | C2 | None |
| C6 | Retrieval Engine | [components/retrieval-engine.md](./components/retrieval-engine.md) | 4 | C1, C5 | None |
| C7 | Maintenance Worker | [components/maintenance-worker.md](./components/maintenance-worker.md) | 4 | C1, C2 | None |
| C8 | HTTP API Edge | [components/http-api-edge.md](./components/http-api-edge.md) | 5 | C1, C2, C3, C6 | None |

All eight component specs have an empty *Gaps* section after the Phase 3 reconciliation pass (which
promoted the deliberately-abbreviated `MemoryStore` stub to the full canonical surface in §2C.6, added
the cross-component config keys to §2D, added `pii_review` to `Fact`, and gave the shared provider
types — `ProviderError`, `SourceState`, `PiiSpan` — and the `FreshnessChecker` trait one canonical
home). No spec is blocked from `codegen` on Gaps grounds; promotion to *Approved* is gated only by the
Phase 6 self-audit.

External provider adapters (`EmbeddingClient`, `RerankClient`, `LlmClient`, `BrokerClient`,
`PiiDetector`) are thin HTTP/model adapters defined by their §2C.6 traits and carry no domain logic,
so they have no standalone Phase 3 spec; the consuming component specs depend on the trait signatures.

## HLD-impact-pass

The Phase 3.5 HLD-impact scan over these drafts surfaced findings that are routed back to the HLD as a
single combined review block — see the run output (glossary terms `stale-pending-refresh` /
`unverified-currency`, and the v1 deferral of LLM entity adjudication). Spec-level findings (exact
field types, config keys, trait signatures) stayed in the spec per the bubble-up rule and were
reconciled into Phase 2.
