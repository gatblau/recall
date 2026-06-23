# Plan — Recall is LLM-free (extraction + consolidation move to the agent)

> Source: `docs/wip/rfc/02-recall-llm-free.md` (Standard lane) · Spec: Phase-6 approved (HLD 0.6.0, ADR-015) · Plan written 2026-06-23

## Anchor (from input)

Implement ADR-015: `recall` holds **no LLM dependency**. Remove LLM fact-extraction (write path) and server-side episodic→semantic consolidation (maintenance); require structured agent-asserted writes; replace the HTTP PII detector (which calls the LLM endpoint) with an **in-process deterministic** detector. Embedding + reranker (retrieval inferences) are retained. Evidence: RFC 02 (OQ-1 resolved = drop consolidation; PII in-process), spec `phase-2-architecture.md` §2C.4/§2C.6/§2D, `components/{write-pipeline,maintenance-worker,http-api-edge}.md`, ADR-015 in `09-decisions.md`.

## Verified context

- **LLM seam:** `LlmClient` trait `src/types/ports.rs:363`; `InsightCandidate` `src/types/ports.rs:173`; `HttpLlmClient` `src/providers/mod.rs:196,272`; `ProviderError` doc `src/providers/mod.rs:65`, `src/types/ports.rs:188`.
- **Extraction (C4):** `WritePipeline.llm` field `src/write_pipeline/mod.rs:167`, `new()` param `:183`; dispatch `self.extract(&payload.content, payload.agent_stated)` `:335`; `extract` method `:476-493` (LLM call `:491`).
- **Consolidation (C7):** `llm` field `src/maintenance/mod.rs:58,184`; `consolidate` call `:509`; `handle_consolidate` `:427`; `consolidate_tenant` `:480`; `ConsolidationReport` `:148`; `CycleReport.consolidation` `:138`; `insight_confidence` `:745`; enqueue `JobKind::Consolidate` `:300`; dispatch `:331`; duty `:388`; cfg `consolidate_max_interval` `:75,98`, `min_episodes` `:102`.
- **JobKind::Consolidate:** enum `src/types/job.rs:34`; queue kind string `src/queue/mod.rs:608`.
- **PII:** `HttpPiiDetector` `src/providers/mod.rs:360,366,394`, constructed from `config.llm_url`/`llm_api_key` `:370-371`; `PiiDetector` trait + `PiiSpan` in `src/types/ports.rs`.
- **Config:** `llm_url`/`llm_api_key` `src/config.rs:119-120,243-244,423-424,492-493`; `consolidate_max_interval_secs` `:145,353`; `maint_consolidate_min_episodes` `:147,359`; `insight_decay_factor` `:148,363`.
- **API:** `RememberRequest` `src/types/api.rs:67`, `agent_stated` `:73`; remember handler passes `agent_stated` `src/api/v1.rs:509`.
- **Wiring:** `build_state` constructs `HttpLlmClient` + `HttpPiiDetector` and passes them into C4/C7 (`src/lib.rs`); the test harness mirrors this in `tests/support/mod.rs`.
- **Tests:** `tests/bdd.rs` (52 LLM/consolidate/agent_stated matches); features `write_pipeline.feature`, `maintenance.feature`, `system.feature`, `memory_store.feature`.

## Open assumptions

- **Integration tooling (resolved, not assumed):** the repo's existing harness is used — `cargo test` driving cucumber-rs over `tests/features/`, embedded in-memory SurrealDB, wiremock for the embedding + reranker providers, a local RS256 OIDC issuer + Dex testcontainer. After Phase 1 the PII wiremock stub is gone (PII is in-process); after Phase 3 the LLM wiremock stub is gone. No new test tooling introduced.
- **PII detector scope (accepted, RFC OQ-4):** the in-process detector reliably catches structured identifiers (email, phone, card-via-Luhn, IP); free-text names/addresses are out of scope (the agent handles richer PII). The `PiiDetector` trait stays a DI seam.
- **No blocking assumptions.** Removing previously-required env vars (`RECALL_LLM_*`) only loosens startup; it cannot break a running deployment.

## Scope

- **In scope:** remove LLM extraction (C4) + server-side consolidation (C7); remove the `LlmClient` trait, `HttpLlmClient`, `InsightCandidate`, the `Consolidate` job kind, and `RECALL_LLM_*` + consolidation config (rename one interval key); reshape `RememberRequest` (drop `agent_stated`, add optional `memory_class`); replace `HttpPiiDetector` with an in-process deterministic `PiiDetector` impl; update the BDD suite.
- **Out of scope:** embedding + reranker providers (retained); the C1 store, C2 queue, C3 auth, C6 retrieval behaviour (only the `JobKind` enum + PII wiring touch shared code); the `consolidated` `MemoryClass` + `Fact.derived_from` (retained for agent-written insights); the freshness/provenance change (already shipped under ADR-014, commit `4ea7b47`).

## Phases

### Phase 1 — In-process deterministic PII detector
- **Goal:** PII detection runs in-process with no network call, decoupling it from the LLM endpoint before that config is removed.
- **Changes:**
  - Add `LocalPiiDetector` (in-process) implementing `PiiDetector` — `src/providers/mod.rs` (or a new `src/providers/pii.rs`): regex/pattern scan over the content's string leaves (email, phone, card-via-Luhn, IP), emitting `PiiSpan { json_pointer, pii_type, confidence }` with confidence by pattern strength.
  - Remove `HttpPiiDetector` (`src/providers/mod.rs:360-...`) and its `/pii/scan` wire usage.
  - Wire `LocalPiiDetector` in `build_state` (`src/lib.rs`) and `tests/support/mod.rs` (drop the wiremock PII stub + `mount_pii_*`).
  - Rework the PII BDD scenarios (`write_pipeline.feature` + `tests/bdd.rs`) so redaction/flagging is asserted against the deterministic detector (a real email → high-confidence redact; an ambiguous pattern → `pii_review`).
- **Rationale:** first and standalone — it removes the only non-LLM consumer of `RECALL_LLM_*`, so Phase 3 can delete that config without breaking PII. Lowest-risk, self-contained.
- **Exit criteria:**
  - `cargo build` + `cargo clippy --all-targets -- -D warnings` clean.
  - `cargo test` green, incl. reworked PII scenarios; integrating services via the existing harness (embedded SurrealDB + wiremock embed/rerank + local issuer). No PII wiremock stub remains.
  - Behavioural check: a write whose content carries an email is persisted with the email redacted; a borderline pattern sets `pii_review` — both with no outbound PII call.
- **Risks/rollback:** detector tuning (false negatives on exotic formats) — RISK-001; revert the new module + wiring to back out.

### Phase 2 — Drop LLM extraction; structured-content intake
- **Goal:** the write path accepts only structured agent-asserted content; no LLM extraction.
- **Changes:**
  - `src/types/api.rs` `RememberRequest`: drop `agent_stated`; add `#[serde(default)] memory_class: Option<MemoryClass>`.
  - `src/api/v1.rs` remember handler: stop forwarding `agent_stated`; pass `memory_class` through to the job payload.
  - `src/write_pipeline/mod.rs`: remove the `llm` field + `new()` param + `LlmClient` import; replace `extract(content, agent_stated)` (`:476-493`) with `intake(content, memory_class)` that wraps the structured object as one `ExtractedFact` (`scan_entity_mentions`, `memory_class` or `Episodic`, `confidence 1.0`); reject non-object `content` as terminal `VAL_INVALID_BODY`; rename the trace span `write.extract` → `write.intake`.
  - `src/lib.rs` + `tests/support/mod.rs`: drop the `llm` arg from `WritePipeline::new` (keep `HttpLlmClient` constructed for C7 until Phase 3).
  - Tests: `write_pipeline.feature` + `tests/bdd.rs` — the agent-stated path becomes the normal path; remove the "LLM extractor returns…" and "extract times out" scenarios; add a non-object-content rejection.
- **Rationale:** removes one of the two `LlmClient` consumers; bounded to the write path. `LlmClient` still compiles (C7 uses it) so this ships independently.
- **Exit criteria:**
  - `cargo build` + `clippy -D warnings` clean.
  - `cargo test` green; a structured `POST`-shaped write job drains and is recallable; a non-object content job dead-letters with `VAL_INVALID_BODY`.
  - Behavioural check: `grep -n "agent_stated\|llm.extract" src/write_pipeline src/types/api.rs src/api/v1.rs` returns zero.
- **Risks/rollback:** clients still sending raw text now get a validation error (RISK-002, expected/breaking); revert the C4 + api edits.

### Phase 3 — Drop consolidation; remove LlmClient + RECALL_LLM_* + consolidation config
- **Goal:** remove the last LLM consumer and all LLM/consolidation machinery and config; recall becomes LLM-free.
- **Changes:**
  - `src/maintenance/mod.rs`: remove the `llm` field/param, `handle_consolidate`, `consolidate_tenant`, `insight_confidence`, `ConsolidationReport`, the `consolidation` field on `CycleReport`, the `Consolidate` enqueue (`:300`) + dispatch (`:331`) + cycle duty (`:388`); cycle order becomes `supersede → decay → reembed`; `tenant_is_due` uses the renamed interval.
  - `src/types/job.rs`: remove `JobKind::Consolidate`; `src/queue/mod.rs:608` drop its match arm.
  - `src/types/ports.rs`: remove the `LlmClient` trait + `InsightCandidate`; fix the `ProviderError` doc comment.
  - `src/providers/mod.rs`: remove `HttpLlmClient` + the consolidate/extract wire docs + `InsightCandidate` import.
  - `src/config.rs`: remove `llm_url`/`llm_api_key` (+ `RECALL_LLM_*` reads + minimal-env test row), `maint_consolidate_min_episodes`, `insight_decay_factor`; rename `consolidate_max_interval_secs`/`RECALL_CONSOLIDATE_MAX_INTERVAL_SECS` → `maint_max_interval_secs`/`RECALL_MAINT_MAX_INTERVAL_SECS`.
  - `src/lib.rs` + `tests/support/mod.rs`: drop the `HttpLlmClient` construction; drop the `llm` arg from `MaintenanceWorker::new`; remove the LLM wiremock stub + `RECALL_LLM_*` env rows.
  - Tests: `maintenance.feature` + `tests/bdd.rs` — remove the consolidation scenarios + harness; keep supersession/decay/re-embed/hard-delete.
- **Rationale:** with both consumers gone (Phase 2 = extract, this = consolidate), the `LlmClient` trait and config can be deleted in one warning-clean step (no dead-code window).
- **Exit criteria:**
  - `cargo build` + `clippy -D warnings` clean (no dead code, no orphan config).
  - `cargo test` green; the maintenance suite passes its four remaining duties; the worker enqueues/claims no `Consolidate`.
  - Config check: boots with `RECALL_LLM_URL`/`RECALL_LLM_API_KEY` unset and `/readyz` passes.
  - Behavioural check: `grep -rE "LlmClient|HttpLlmClient|RECALL_LLM|InsightCandidate|JobKind::Consolidate|consolidate_tenant" src/` returns zero.
- **Risks/rollback:** largest blast radius; revert is the whole-phase diff. RISK-003 (a deployment still sets `RECALL_LLM_*`) — boot is strictly more permissive after removal.

## Cross-cutting validation

After Phase 3: `cargo build` + `cargo clippy --all-targets -- -D warnings` clean; full `cargo test` (unit + `tests/bdd.rs` + `tests/store_remote.rs`) green with all integrating services up (embedded SurrealDB, wiremock embed/rerank, local OIDC issuer, Dex container); `grep -rE "LlmClient|RECALL_LLM|InsightCandidate|JobKind::Consolidate|HttpPiiDetector|/pii/scan" src/ tests/` returns zero live references; then `/sync-check` to confirm Design ↔ Spec ↔ Code alignment (HLD 0.6.0, ADR-015).

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Sweep deployment manifests / .env examples for RECALL_LLM_* and the renamed interval key
  why: RFC OQ — code no longer reads RECALL_LLM_*; RECALL_CONSOLIDATE_MAX_INTERVAL_SECS was renamed. RFC 01 found no in-repo manifests; re-confirm after this change.
  source: planning
  suggested-command: grep over deployment files; none expected in-repo
  status: open
  added: 2026-06-23
- id: FU-002
  title: Document the agent-side consolidation pattern for clients
  why: server-side consolidation is gone; agents now distil episodes and write consolidated insights themselves — worth a short client note so the capability is discoverable.
  source: planning
  suggested-command: /design or a docs note (out of recall's code scope)
  status: open
  added: 2026-06-23
- id: FU-003
  title: Drop the inert RECALL_LLM_URL/RECALL_LLM_API_KEY literals from tests/bdd.rs config-builder maps
  why: surfaced in apply-phase-3 — ~5 pairs remain in bdd.rs harness config maps; Config::load ignores unknown keys so they are inert, but they are stale. Left out of phase 3 to avoid many non-unique edits.
  source: apply-phase-3
  suggested-command: edit tests/bdd.rs (remove the RECALL_LLM_* rows in the wp/retr/maint/system/api config maps)
  status: done
  added: 2026-06-23
- id: FU-004
  title: De-LLM the write-pipeline test naming — rename the "LLM extractor returns one fact" given steps and drop the dead mount_extract stub
  why: surfaced folding FU-003 — the C4 scenarios still say "the LLM extractor returns one fact …" and mount a `/extract` stub the pipeline no longer calls (it wraps the structured content directly). Harmless (tests green) but misleading in an LLM-free codebase.
  source: apply-phase-3
  suggested-command: edit tests/features/write_pipeline.feature + tests/bdd.rs (rename given to "a structured fact … is submitted"; drop mount_extract)
  status: done
  added: 2026-06-23
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: In-process PII detector misses an unstructured PII format
  why: deterministic patterns catch structured identifiers but not free-text names/addresses; a write could persist unredacted free-text PII.
  source: planning
  likelihood: medium
  impact: medium
  mitigation: accepted per RFC OQ-4 — recall ingests structured facts (PII in known fields); the agent owns richer PII; PiiDetector stays a seam for a model-backed swap.
  status: accepted
  added: 2026-06-23
- id: RISK-002
  title: Clients sending raw free-text content break on the structured-only contract
  why: POST /v1/memories no longer extracts; a client sending prose for extraction now gets VAL_INVALID_BODY.
  source: planning
  likelihood: low
  impact: medium
  mitigation: recall is pre-1.0; OpenAPI + consumer notes updated in the change; the agent already does extraction.
  status: accepted
  added: 2026-06-23
- id: RISK-003
  title: A deployment still sets RECALL_LLM_* as required
  why: removing the reads makes a set-but-unused var harmless to recall, but external orchestration asserting it is consumed could lint.
  source: planning
  likelihood: low
  impact: low
  mitigation: FU-001 sweep; recall boot is strictly more permissive after removal.
  status: open
  added: 2026-06-23
- id: RISK-004
  title: Intermittent BDD flake — "Error path — body exceeds the size limit" (api_edge)
  why: RCA (2026-06-23) — the step `when_post_big` (tests/bdd.rs) streams a 2 MiB body and did `req.send().await.expect("send big POST")`. The edge enforces the 1 MiB `DefaultBodyLimit` (src/api/mod.rs:110) by responding 413 and closing the connection; on some TCP timings it closes before reqwest finishes streaming the body, so `send()` returns Err and `.expect` panicked, aborting the scenario (~1/10 runs). Server + spec (413 VAL_BODY_TOO_LARGE) are correct and aligned — a test-reliability defect, not code/spec drift.
  source: apply-phase-3
  likelihood: medium
  impact: low
  mitigation: FIXED 2026-06-23 — `when_post_big` now treats a send error (connection reset mid-upload) as the asserted 413, matching the Ok(413) path; no panic. Confirmed by a 20× repro loop.
  status: mitigated
  added: 2026-06-23
```
