# Recall is LLM-free: extraction and consolidation move to the agent

> Lane: **Standard** · Status: **Draft** · Source: chat change request (2026-06-22) · Builds on RFC 01 / ADR-014

## Restated request

`recall` should hold **no LLM dependency at all** — the natural completion of ADR-014's "recall is a passive store; the agent runs the agentic workflow." Today recall calls an LLM in two places (both asynchronous, off the read path): **fact extraction** on the write path and **episodic→semantic consolidation** in maintenance. Make recall LLM-free: the agent extracts and submits structured assertions, and consolidation's fate is decided (see the blocking open question).

Interpretation: "LLM-free" means removing the `LlmClient` (extraction + consolidation) and its config. The **embedding and reranker** providers are **retained** — they are model inferences intrinsic to hybrid retrieval, not reasoning/orchestration, and were already carved out under ADR-014 (`07-cross-cutting.md` *Outbound calls* stance).

## Motivation

After ADR-014, the LLM is the last piece of *reasoning* living inside recall. It is also the one piece most clearly aligned with "the agent's intelligence," not "the memory store's job":

- The **agent already has an LLM**. Extraction (turning prose into structured facts) is exactly the kind of work the agent does; recall already supports a server-side bypass for it (`agent_stated` content skips the LLM — `src/write_pipeline/mod.rs:481-489`).
- Removing the LLM makes recall a **pure memory store with no generative dependency**: simpler to run (no LLM provider, endpoint, or key required), cheaper, and with a smaller trust/security surface (no model in the ingest path interpreting untrusted source content).
- It finishes the ADR-014 story: recall stores, serves, and maintains memory; the agent does the reasoning (extract, synthesize, check freshness).

## Current behaviour

- **Write-path extraction (C4).** `WritePipeline::extract(content, agent_stated)` (`src/write_pipeline/mod.rs:477-493`): when `agent_stated` is true it wraps the supplied structured content as a single `ExtractedFact` (`confidence = 1.0`, `memory_class = Episodic`) and **does not** call the LLM; otherwise it calls `self.llm.extract(content)` (`src/write_pipeline/mod.rs:491`). The request flag is `RememberRequest.agent_stated` (`src/types/api.rs:65`).
- **Consolidation (C7).** The maintenance worker runs an idle-biased consolidation duty: `consolidate_tenant` → `self.llm.consolidate(&group)` (`src/maintenance/mod.rs:509`), validated and promoted to `memory_class = consolidated` facts carrying `derived_from`. Driven by `JobKind::Consolidate` (`src/maintenance/mod.rs:331`) and the idle/fallback scheduler; uses `InsightCandidate` (`src/types/ports.rs`).
- **LLM seam + config.** `LlmClient` trait (`src/types/ports.rs:363`); `HttpLlmClient` adapter (`src/providers/mod.rs`); `RECALL_LLM_URL` + `RECALL_LLM_API_KEY` are **required** at boot (`src/config.rs:243-244`).
- **Read path.** Already LLM-free (NFR-P1 / ADR-012); `src/retrieval/mod.rs` has no `LlmClient`. Embedding + reranker are used on the read path and at write time and are **not** in scope here.

## Desired behaviour

1. **No LLM call anywhere in recall**, and no LLM provider configuration required to boot.
2. **`POST /v1/memories` accepts only structured, agent-asserted content** — recall performs no server-side extraction. Content that would require LLM extraction is rejected with a clear typed error (or the API simply only accepts structured assertions). The remaining write-pipeline steps (filter, canonicalise, entity-resolve, score, PII scan, write-gate, embed+persist) are unchanged and require no LLM.
3. **Server-side consolidation is dropped** (OQ-1=a, RESOLVED). recall runs no LLM consolidation: the `Consolidate` job kind, the consolidate maintenance duty, `InsightCandidate`, and `llm.consolidate` are removed. The agent owns synthesis — it recalls episodes, distils an insight with its own LLM, and writes it back as an **agent-stated `consolidated` fact** (the `consolidated` class + `derived_from` are retained for exactly this). The maintenance worker keeps its non-LLM duties: supersession, graceful decay, re-embed (via the **embedding** provider), and verifiable hard delete.
4. **Intent realigned.** ADR-012 ("read path is LLM-free; write/maintenance may use the LLM") is amended/superseded by a new ADR stating recall is LLM-free end-to-end; the affected component specs match the code.

## Scope

**In scope**
- Remove LLM extraction from C4: drop the `self.llm.extract` branch (`src/write_pipeline/mod.rs:491`); require structured content; remove or repurpose the now-redundant `RememberRequest.agent_stated` flag (all content is agent-asserted).
- Remove the `LlmClient` trait (`src/types/ports.rs:363`), the `HttpLlmClient` adapter (`src/providers/mod.rs`), and `RECALL_LLM_URL` / `RECALL_LLM_API_KEY` (`src/config.rs`).
- **Drop server-side consolidation (OQ-1=a):** remove the consolidate duty (`consolidate_tenant` / `handle_consolidate`, `src/maintenance/mod.rs:427,509`), `JobKind::Consolidate` and its queue/maintenance match arms, `InsightCandidate`, `LlmClient::consolidate`, and the consolidation-only config (`RECALL_CONSOLIDATE_MAX_INTERVAL_SECS`, `RECALL_MAINT_CONSOLIDATE_MIN_EPISODES`, `RECALL_INSIGHT_DECAY_FACTOR`, and `RECALL_IDLE_QUIET_SECS` if the idle trigger is no longer needed once consolidation is gone). Retain the `consolidated` `MemoryClass` + `derived_from` (for agent-written insights) and the remaining maintenance duties + their scheduler.
- Amend the design: supersede/extend ADR-012 with a new ADR; update `components/write-pipeline.md`, `components/maintenance-worker.md`, `02-architecture.md`, `01-context.md` (LLM actor), `07-cross-cutting.md`, `phase-2` shared types/env, and the changelog.

- **PII detection becomes in-process and deterministic.** Today the PII detector is an outbound HTTP call to the LLM endpoint (`POST {RECALL_LLM_URL}/pii/scan`, `src/providers/mod.rs:367-405`); removing `RECALL_LLM_*` orphans it. Replace `HttpPiiDetector` with a **deterministic in-process Rust detector** (regex/pattern matching for structured identifiers — email, phone, card-via-Luhn, IP, national-id — with confidence by pattern strength) feeding the existing redact-≥0.9 / flag-lower logic (SA-PII-01). Keep the `PiiDetector` trait as a DI seam; remove the HTTP adapter and any `RECALL_PII_*` / LLM coupling. recall makes **no** outbound call for PII.

**Out of scope**
- **Embedding and reranker providers** — retained (model inferences for retrieval, not reasoning).
- Other maintenance duties — supersession, graceful decay, re-embed (uses the **embedding** provider, not the LLM), and verifiable hard delete — unchanged.
- The freshness/provenance behaviour shipped under ADR-014 — unchanged.
- The `consolidated` `MemoryClass` and `derived_from` field — retained regardless of OQ-1 so the agent can still write consolidated insights as agent-stated facts.

## Acceptance criteria

1. **Boots without an LLM.** recall starts and `/readyz` passes with `RECALL_LLM_URL` / `RECALL_LLM_API_KEY` unset; a config test confirms the keys are no longer read.
2. **No LLM seam remains.** `LlmClient` and `HttpLlmClient` are absent from the tree; `grep -rE "LlmClient|HttpLlmClient|RECALL_LLM" src/` returns zero.
3. **Structured write persists (BDD).** `POST /v1/memories` with a structured assertion body persists a fact end-to-end (write→drain→recall) with no LLM mock involved.
4. **Extraction path gone.** The write pipeline has no LLM extraction step; a unit/integration test confirms the `extract` step now only wraps structured content (no provider call), and the other seven steps still pass admit/quarantine/reject/PII scenarios.
5. **Raw-text rejection (if applicable).** If the contract requires structured content, a write whose `content` is not a structured assertion returns a clear typed validation error (named code).
6. **No server-side consolidation.** No `Consolidate` job is enqueued and the maintenance worker makes no LLM call; a BDD/integration test confirms an agent-written `consolidated` fact (with `derived_from`) round-trips through write→drain→recall, and the remaining maintenance duties (supersession, decay, re-embed, hard delete) still pass their scenarios.
7. **Intent realigned.** A new ADR supersedes/extends ADR-012; `sync-check` reports zero INTENT/CONTRACT/D-HLD drift for the write-pipeline and maintenance areas.
8. **No dead code.** `cargo build` + `cargo clippy --all-targets -- -D warnings` clean with the LLM removed; full test suite green.

## Constraints

- **Breaking API change.** `POST /v1/memories` no longer accepts free-text-for-extraction; clients must send structured assertions. recall is pre-1.0; the OpenAPI doc and any consumer notes must be updated in the same change.
- **No new reasoning dependency.** The change must not replace the LLM with another generative/reasoning call inside recall.
- **Memory-poisoning posture preserved.** The write gate, PII scan, and trust scoring (all non-LLM) remain the ingest defences; removing LLM extraction must not weaken them. Untrusted source content is still treated as data, never instructions.
- **Deployment ordering.** Spec amendment (ADR + component specs) lands before code (*fix the prompt first*). `RECALL_LLM_*` removal coordinated with any deployment manifests (sweep, as in RFC 01 FU-001).

## Proposed direction

recall becomes a pure, LLM-free memory store: it accepts structured agent-asserted facts, stores them with provenance, serves hybrid retrieval (embedding + reranker), and maintains memory with **non-LLM** hygiene (supersession, decay, re-embed, hard delete). The agent owns all reasoning — extraction and (recommended) synthesis. Implementation is largely *removal*: delete the `LlmClient` seam, the extraction branch, and the LLM config; require structured content; and resolve consolidation per OQ-1.

## Open questions

2. **(non-blocking) `agent_stated` flag fate.** With extraction removed, all content is agent-asserted. Default assumption: **remove the `agent_stated` flag** and require `content` to be a structured assertion object; the field becomes meaningless. (Alternative: keep it accepted-but-ignored for one version for client compatibility.)
3. **(non-blocking) Agent-specified memory class.** Today agent-stated content is forced to `Episodic` (`confidence 1.0`). The agent may want to assert `semantic`/`consolidated` directly. Default assumption: **let the write accept an optional `memory_class`** (validated against the existing enum), defaulting to `Episodic`. Refine in `/spec`.
4. **(non-blocking) In-process deterministic PII detector tradeoff.** The in-process detector reliably catches **structured** identifiers (email, phone, card, IP) but not free-text names/addresses an ML/NER model would. Accepted: recall now ingests structured agent-asserted facts (PII lands in known fields), and richer PII reasoning belongs with the agent; the `PiiDetector` trait stays a seam so a model-backed detector remains injectable. Recorded as an assumption, not a blocker.

### Resolved

- **OQ-1 — consolidation's fate. Resolved 2026-06-22: (a) drop server-side consolidation.** recall runs no LLM consolidation; the consolidate duty, `JobKind::Consolidate`, `InsightCandidate`, and `llm.consolidate` are removed. The agent owns synthesis and writes insights back as agent-stated `consolidated` facts; the `consolidated` class + `derived_from` are retained. Non-LLM maintenance duties (supersession, decay, re-embed, hard delete) stay.

No blocking open questions remain.

## Handoff to /spec

Intent-changing RFC: it amends ADR-012 and changes the `/v1/memories` contract. OQ-1 is resolved (drop consolidation), so the RFC is ready to hand off:

```
/spec docs/wip/rfc/02-recall-llm-free.md
```

After `/spec` exits clean, proceed with `/breakdown` against the updated write-pipeline and maintenance specs, then `/apply`, then `/sync-check`.
