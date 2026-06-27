# Phase 6 — Self-Audit

> **Spec set:** `recall` (agentic memory service) · **Mode:** greenfield
> **derivedFromHld:** 0.7.0 · **Source HLD:** `docs/design/agentic-memory/` · **Authored:** 2026-06-20 · **Amended:** 2026-06-22 (RFC 01, ADR-014; RFC 02, ADR-015), 2026-06-27 (RFC 01-MCP, ADR-016)

This is the blocking gate that promotes the spec set from *Draft* to *Approved*. Each checklist item
is ticked against the generated specs (Phases 1–5). **Verdict: PASS** — every item is `[x]`.

**RFC 01 / ADR-014 re-audit (2026-06-22).** ADR-014 supersedes ADR-013: `recall` is a passive store,
freshness is agent-side. C5 Freshness Checker is **retired**; the recall-side source-change check, the
`Currency`/`FreshnessChecker`/`BrokerClient`/`SourceState` types, the `ReReadSource` job, and
`RECALL_BROKER_URL`/`RECALL_FRESHNESS_*` are removed; `RankedFact` drops `currency` and gains a
conditional `source` (`include_provenance`). The checklist below was re-run against the amended set
(seven live component specs + retired C5) and still passes; the affected items are annotated.

**RFC 02 / ADR-015 re-audit (2026-06-22).** `recall` is **LLM-free**: C4 drops LLM extraction (a
`remember` payload carries a structured agent-asserted `content` object the pipeline wraps directly);
C7 drops server-side consolidation (five duties → four). Removed: the `LlmClient` trait, `HttpLlmClient`,
`InsightCandidate`, the `Consolidate` job kind, and `RECALL_LLM_*` + the consolidation config (one
interval key renamed `RECALL_MAINT_MAX_INTERVAL_SECS`). PII detection is now in-process deterministic.
The checklist was re-run against the LLM-free set and still passes; affected items are annotated.

```
[x] Every entity has a complete data model with types and constraints.
[x] Every action has defined inputs, outputs, numbered steps, and errors.
[x] No banned phrases remain (see banned-phrases.md).
[x] Every component has ≥3 Gherkin acceptance criteria (happy, edge, error).
[x] Every component has an error table with ≥2 rows.
[x] Every cross-component interaction is documented on BOTH sides.
[x] Build order (Phase 2B) is a valid DAG — no circular dependencies.
[x] Every config value / env var listed with type, default, required flag, and owner component.
[x] Every spec is self-contained — implementable from its section alone.
[x] Assumptions register is complete; open questions are truly blocking only.
[x] Example I/O provided for every component with non-trivial logic.
[x] Shared types defined once in Phase 2C, referenced by name but duplicated into each "Shared Context".
[x] Security addressed for every entry point (HTTP handler, queue consumer, scheduler).
[x] Performance targets stated for every latency-sensitive component.
[x] All specifications use the active locale (en-GB).
[x] Destructive schema changes surface rollback considerations.
[x] Every Component, Cross-cutting, and Preamble spec carries a `derivedFromHld:` preamble field.
[x] No spec body contains an ADR-shaped paragraph; every system-level trade-off is in HLD 09-decisions.
[x] Every glossary term used in any spec is present in the HLD glossary.
[x] Every spec's `derivedFromHld:` matches the HLD's current `revision:` — no stale phases.
[x] In folder-form HLDs: every revision bump in the HLD has a corresponding `99-changelog.md` line.
```

## Evidence and notes per item

1. **Data models complete.** §2C.2 defines `Fact`/`Entity`/`Relationship`/`Source` with types and
   validation; C1 gives the SurrealDB DDL (`DEFINE FIELD`/`ASSERT`/`DEFINE INDEX`) for every table incl.
   `pii_review`, `embedding`, `embedding_model`. Component-owned tables (`quarantine`, `work_job`,
   `dead_letter`, `maintenance_state`, `idempotency_record`, `schema_migrations`, `audit_log`) each carry
   DDL in their owning spec.
2. **Actions specified.** Every component has numbered Internal Logic with the dependency called, errors
   possible, and what is logged per step.
3. **No banned phrases.** Scan for `etc.`, `and so on`, `as needed`, `as appropriate`, `should be
   robust`, `straightforward`, `obviously`, `properly`, `correctly` returns zero hits across `docs/spec/`.
   ("in-process", "process boundary", and the noun "handle" in `Source.origin_ref` are not banned-verb
   usages.)
4. **Gherkin ≥3.** Component counts: memory-store 6, work-queue 6, auth-scope 7, write-pipeline 10,
   retrieval-engine 7, maintenance-worker 6, http-api-edge 11 (C5 freshness-checker retired — ADR-014;
   maintenance-worker dropped its consolidation scenario — ADR-015).
   Each cross-cutting spec (X1–X13) has exactly 3 (happy/edge/error). All ≥3.
5. **Error table ≥2.** Components: memory-store 7, work-queue 5, auth-scope 5, write-pipeline 9,
   retrieval-engine 6, maintenance-worker 9, http-api-edge 17 (C5 retired). Cross-cutting specs
   that produce client/lifecycle errors carry ≥2 rows (X1, X2, X6, X7, X8, X9, X10, X12, X13). The
   pure-observability concerns **X3 Logging, X4 Metrics, X5 Tracing** and **X11 CORS (off by default)**
   record `N/A — <reason>` per the template's explicit "if a section genuinely does not apply" rule:
   they emit no client-facing error path. This is a documented, justified exception, not an omission.
6. **Both-side interactions.** C8↔C3 (auth), C8↔C6 (recall), C8↔C2 (enqueue), C8↔C1 (GET fact + audit);
   C6↔C1 (recall/store + `get_source` for conditional provenance); C4↔C2 (ExtractFact
   consume), C4↔C1 (persist/entity/source); C7↔C2 (ReEmbed/HardDelete consume), C7↔C1
   (scans/mutate/hard_delete); C2↔C1 (store-backed table + `list_tenants`). Each is stated on both the
   producing and consuming spec. (The C6↔C5 freshness-tag and C5↔C2 ReReadSource edges are removed — C5
   retired by ADR-014.)
7. **Valid DAG.** Phase 2B + Phase 5: C1 → {C2,C3} → C4 → {C6,C7} → C8, cross-cutting at Phase 0.
   Every edge points to a higher phase; the async write path means there is no synchronous C8→C4 edge.
   No cycles.
8. **Config complete.** §2D lists every env var with Type, Default, Required, Owner, Description (46
   keys after the ADR-014/ADR-015 removals — broker, freshness, and LLM/consolidation keys gone);
   conditional/mutual-exclusion rules stated; X6 specifies load order + startup validation.
9. **Self-contained.** Each component duplicates the shared types/env/read-filter it uses into its
   Shared Context (verified: C4's `Fact` copy carries `pii_review`, and it duplicates `Source` + the
   entity/source `MemoryStore` methods it calls). A code-gen LLM can implement each from its file alone.
10. **Assumptions / open questions.** Phase 1A holds 27 LLD assumptions (SA-*) plus the inherited HLD
    assumptions; Phase 1B confirms **no question blocks spec authoring** — OQ-STORE is non-blocking for
    authoring but load-bearing for the Rust/embedded build (surfaced as the Phase 5 Step-0 precondition).
11. **Examples.** Every component carries a concrete Example (real request/response or input/output).
12. **Shared types once.** Defined in §2C, duplicated (not cross-linked) into each consuming Shared
    Context; the canonical `MemoryStore` surface lives in §2C.6 and C1 implements it exactly.
13. **Security per entry point.** Every HTTP handler (X2 auth + per-route authz in C8), every queue
    consumer (C4/C7 run post-authorisation on a job-carried scope), and the scheduler (C7 maintenance
    `ScopeContext`, structural tenant isolation) has its security posture stated; X2/X12/X9 cover the
    edge.
14. **Performance targets.** C1 (ANN ≤ 50 ms), C6 (p95 ≤ 200 ms with SA-LAT-01 sub-budgets), C3
    (token validation ≤ 5 ms warm), C8 (edge overhead ≤ 5 ms), C2 (claim p95 ≤ 20 ms). Async components
    (C4/C7) state off-read-path budgets. (The C5 ≤ 25 ms freshness sub-budget is gone — C5 retired,
    ADR-014; the read path makes no freshness check.)
15. **Locale en-GB.** All narrative prose uses en-GB spelling/idiom.
16. **Rollback surfaced.** C1 (`0001` down path, `REMOVE NAMESPACE` erasure, schemaless→schemafull
    tightening, HNSW dimension change), X7 (migration down pairs, refused destructive down on populated
    namespaces), C8 (`idempotency_record` `REMOVE TABLE`), C7 (`maintenance_state` non-destructive drop).
17. **derivedFromHld present.** All six phase files, all eight component files (incl. the retired C5
    tombstone), and the 13 cross-cutting specs carry a `derivedFromHld:` field, all now `0.6.0` after the
    RFC-01 / ADR-014 and RFC-02 / ADR-015 amendments.
18. **No hidden ADRs.** Component *Approach* sections carry only component-scope "why-this-not-that"
    (permitted by the template); no system-level Decision·Context·Consequences·Alternatives paragraph
    sits in a spec — the v1 entity-resolution trade-off was promoted to HLD `10-risks.md` + the
    `02-architecture.md` responsibility line via the Phase 3.5 HLD-impact-pass.
19. **Glossary terms in HLD.** Under ADR-014 the read-path currency terms `stale-pending-refresh` and
    `unverified-currency` were **removed** from both the HLD glossary and the spec set (no longer recall
    concepts); the `Source provenance (response)` term the LLD now uses is spec-local (an implementation
    mechanism, correctly not an HLD domain term per the bubble-up rule). Under ADR-015 the `Consolidation`
    glossary term was **reframed agent-side** in both the HLD and Phase 1 (the agent consolidates; recall
    stores), and `Consolidated Insight` retained (still a memory class the agent writes). The remaining
    Phase-1 `[LLD]` terms (`Work job`, `Dead letter`, `Trust score`, `Scope partition key`) are
    implementation-mechanism names, correctly spec-local — not HLD domain terms.
20. **Pins current.** Every `derivedFromHld:` equals the HLD's current `revision: 0.6.0`; no stale
    phase (verified by scan — zero `0.4.x`/`0.5.x` pins remain).
21. **Changelog coverage.** The HLD bumps to 0.5.0 (RFC 01 / ADR-014: passive store, agent-side
    freshness) and 0.6.0 (RFC 02 / ADR-015: LLM-free, extraction + consolidation agent-side) each have a
    matching `99-changelog.md` line. README and all child banners read 0.7.0 (D-HLD-5 sibling consistency
    holds; D-HLD-6 satisfied).

## ADR-016 re-audit (RFC 01-MCP — MCP edge + shared Service Layer, 2026-06-27)

Scoped re-audit of the change that adds C9 (Service Layer) and C10 (MCP API Edge) and refactors C8 to
a thin adapter. Every checklist item re-verified against the changed scope:

```
[x] Every entity has a complete data model — C9/C10 own no tables (N/A stated); reused 2C types defined.
[x] Every action has inputs/outputs/numbered steps/errors — C9 (6 methods) and C10 (6 tools) each have Public Interface + numbered Internal Logic + Error Table.
[x] No banned phrases — re-scanned service-layer.md, mcp-api-edge.md, http-api-edge.md (the "handles" verbs reworded).
[x] ≥3 Gherkin per component — C9 has 4, C10 has 5.
[x] Error table ≥2 rows — C9 14 rows, C10 9 rows.
[x] Cross-component interaction on BOTH sides — C8 addendum + C10 spec both declare dependency on C9; C9 documents being consumed by both edges; C9↔C1/C2/C3/C6 ports duplicated in Shared Context.
[x] Build order is a valid DAG — C9 depends on C1/C2/C3/C6 (phase 5); C8, C10 depend only on C9 (phase 6). No cycle (phase-2 §2B DAG updated).
[x] Env vars typed/default/required/owner — RECALL_MCP_HTTP_ADDR, RECALL_MCP_PATH added (owner C10); idempotency + rate vars reassigned owner C8→C9.
[x] Each spec self-contained — C9/C10 duplicate ScopeContext, AppError, DTOs, config into Shared Context.
[x] Assumptions complete; OQs blocking-only — SA-SVC-01/SA-MCP-*/SA-BIN-01 added; OQ-LIB, OQ-NAME non-blocking.
[x] Example I/O — C9 and C10 each carry a concrete Example.
[x] Shared types — C9's CallContext/CallResult/RateSnapshot are C9-owned public-interface types, defined in C9 and duplicated into C10's Shared Context (consumed only by the two edges).
[x] Security for every entry point — C9 (service), C8 (HTTP), C10 (MCP) each have a Security subsection; identity is always a C3-derived ScopeContext, never request-supplied.
[x] Performance targets — C9 and C10 each carry the ≤5 ms in-process overhead budget (SA-LAT-01).
[x] en-GB throughout.
[x] Destructive schema — N/A (no DDL introduced).
[x] derivedFromHld present — C8/C9/C10 = 0.7.0; all phase + cross-cutting + component specs re-pinned to 0.7.0.
[x] No ADR-shaped paragraph in spec bodies — the Approach sections use the allowed chosen/rejected form; the full ADR-016 lives in HLD 09-decisions.md (promoted via the HLD-impact-pass).
[x] Every glossary term in the HLD glossary — MCP, Service Layer, MCP API edge added to HLD 00-overview.md; "Transport edge" is an [LLD] term (not required in HLD).
[x] derivedFromHld matches current revision — HLD now 0.7.0; all specs re-pinned to 0.7.0 (additive delta, orthogonal to C1–C7/X1–X13 content).
[x] Folder-form HLD: revision bump recorded — 99-changelog.md has the 0.7.0 line; all child banners + README read 0.7.0 (D-HLD-5 sibling consistency holds; D-HLD-6 satisfied).
```

New-component *Gaps*: `service-layer.md` — None; `mcp-api-edge.md` — None. No `[Gap]` outstanding.

## Verdict

**PASS — the spec set (including the ADR-016 additions) is *Approved*.** All Component specs have
empty *Gaps*; no Open Question is blocking (OQ-LIB / OQ-NAME are non-blocking, resolved at codegen).
`/breakdown` may consume `docs/spec/phase-5-playbook.md` — the ADR-016 work is Steps 6 (C9 → C8
refactor) and 6b (C10).

**One build-order precondition (not a spec blocker):** the OQ-STORE spike (Phase 5, Step 0) is
load-bearing for ADR-009 and must run **before** Rust scaffolding — a spike failure reopens ADR-003 and
ADR-009 and returns the work to `/design`.

**One build-order precondition (not a spec blocker):** the OQ-STORE spike (Phase 5, Step 0) is
load-bearing for ADR-009 and must run **before** Rust scaffolding — a spike failure reopens ADR-003 and
ADR-009 and returns the work to `/design`.
