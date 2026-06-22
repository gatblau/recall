# Plan — Recall is a passive memory store (agent-side freshness)

> Source: `docs/wip/rfc/01-agent-side-freshness.md` (Standard lane) · Spec: Phase-6 approved (HLD 0.5.0, ADR-014) · Plan written 2026-06-22

## Anchor (from input)

Implement ADR-014: `recall` performs **no outbound broker call, no source-change check, and no agentic orchestration**. Remove the read-path freshness subsystem (C5) and the `currency` verdict; return each sourced fact's `origin_ref` + `modification_marker` **on request** (`include_provenance`) so the agent runs the ask→check→update loop itself via the existing write/supersede endpoints. Evidence: RFC 01 (resolved OQ-1 remove `currency`, OQ-2 conditional provenance); spec `phase-2-architecture.md` §2C.4, `components/retrieval-engine.md`, `components/freshness-checker.md` (retired), ADR-014 in `09-decisions.md`.

## Verified context

- Read-path freshness tagging + `RankedFact { currency }`: `src/retrieval/mod.rs:118` (`freshness` field), `:127` (`new()` param), `:321-324` (RankedFact build), `:341-370` (`tag_freshness`), `:29,:32` (imports).
- Recall API types: `src/types/api.rs:46` (`RankedFact.currency`), `:52-55` (`Currency` enum), `RecallRequest` (no `include_provenance` yet), no `SourceProvenance`.
- Ports: `src/types/ports.rs:343-354` (`FreshnessChecker` trait), `:394-401` (`BrokerClient` trait), `:205` (`SourceState`), `:188` (`ProviderError` doc).
- Broker adapter: `src/providers/mod.rs:361-414` (`HttpBrokerClient`), `:9`/`:54`/`:70` (module doc + import).
- Wiring: `src/lib.rs:70-86` (`HttpBrokerClient` + `BrokerFreshnessChecker` + `RetrievalEngine::new(..., freshness, ...)`).
- Config: `src/config.rs:122` (`broker_url`), `:249` (`required("RECALL_BROKER_URL")`), `:438` (struct build), `:510` (test env); `freshness_budget_ms`/`freshness_per_call_ms` fields + `RECALL_FRESHNESS_*` reads (used at `src/lib.rs:74-75`).
- Job kind: `src/types/job.rs:34` (`ReReadSource`); match arms `src/queue/mod.rs:608`, `src/maintenance/mod.rs:954`; maintenance dispatch already `_ => Ok(())`.
- Freshness module: `src/freshness/mod.rs`, `src/freshness/tests.rs` (delete; remove `mod freshness`).
- Tests: `tests/features/freshness.feature` (whole C5 feature), currency/freshness scenarios in `tests/features/retrieval.feature` + `tests/features/system.feature`; `tests/bdd.rs` (114 currency/freshness/broker matches: C5 steps, freshness harness, currency assertions, broker mocks); `tests/support/mod.rs:32,:146,:197` (`RECALL_BROKER_URL` env), `:205-218` (broker+freshness wiring + `RetrievalEngine::new`), `:401-486` (broker stubs). `tests/features/api_edge.feature` does **not** reference currency/freshness/broker (unaffected). `src/api/openapi.rs` has no field-level `currency` (coarse route doc — no change needed).

## Open assumptions

- **Integration tooling (resolved, not assumed):** the repo's materialised harness is used as-is — `cargo test` driving cucumber-rs over `tests/features/`, with embedded in-memory SurrealDB (real engine, in-process), wiremock provider stubs, a local RS256 OIDC issuer, and a Dex testcontainer (`tests/support/dex.rs`). No new test-container or stub framework is introduced; no Practice-Pack choice is pending. Override only if you want a different harness.
- **No blocking assumptions.** Removing a previously-*required* env var (`RECALL_BROKER_URL`) makes startup strictly more permissive, so it cannot break a running deployment; leftover manifest entries are cosmetic (tracked as FU-001).

## Scope

- **In scope:** delete the C5 freshness subsystem + `BrokerClient`/`HttpBrokerClient`/`SourceState`/`FreshnessChecker`; remove `currency` from the recall response; add conditional `include_provenance`→`source`; remove `ReReadSource` job kind and `RECALL_BROKER_URL`/`RECALL_FRESHNESS_*` config; update the BDD suite; add e2e agent-control-loop coverage.
- **Out of scope:** the agent's own freshness/broker logic (lives in the Faraday client, not recall); the write/supersede path semantics (unchanged — the agent's UPDATE leg uses them as-is); maintenance consolidation/decay/re-embed/hard-delete; auth/tenancy.

## Phases

### Phase 1 — Additive: conditional provenance on the recall response
- **Goal:** the agent can opt into per-fact `origin_ref` + `modification_marker`, with no removals yet (fully backward-compatible).
- **Changes:**
  - `src/types/api.rs` — add `RecallRequest.include_provenance: bool` (`#[serde(default)]`); add `SourceProvenance { origin_ref, modification_marker }`; add `RankedFact.source: Option<SourceProvenance>` (`skip_serializing_if`). **Keep** `currency` for now.
  - `src/retrieval/mod.rs` — when `req.include_provenance`, attach `SourceProvenance` for each page fact with a `source_id`, reusing the `Source` already loaded in `tag_freshness` (or a `get_source` call); populate `RankedFact.source`. `currency` path unchanged.
  - `tests/features/retrieval.feature` + `tests/bdd.rs` — add a "provenance attached only when requested" scenario (on → `source.origin_ref`/`modification_marker` present; off → absent).
- **Rationale:** purely additive first reduces risk — the removal in Phase 2 then only deletes, never reshapes-and-deletes at once.
- **Exit criteria:**
  - `cargo build` and `cargo clippy --all-targets` clean.
  - `cargo test` green, including the new provenance scenario; integrating services via the existing harness (embedded SurrealDB in-process + wiremock providers + local issuer/Dex). Test file extended: `tests/features/retrieval.feature` + steps in `tests/bdd.rs`.
  - Behavioural check: a recall with `include_provenance:true` over a sourced fact returns `source.origin_ref`; with `false`/omitted, no `source` key.
- **Risks / rollback:** additive only; revert the three edits to back out.

### Phase 2 — Remove the freshness subsystem and the `currency` field
- **Goal:** delete C5 and every recall-side freshness/broker artefact; recall becomes a passive store.
- **Changes:**
  - Delete `src/freshness/mod.rs` + `src/freshness/tests.rs`; remove `mod freshness` (module tree in `src/lib.rs`).
  - `src/types/ports.rs` — remove `FreshnessChecker`, `BrokerClient`, `SourceState`; fix the `ProviderError` doc comment.
  - `src/providers/mod.rs` — remove `HttpBrokerClient`, its module doc (`:9`,`:54`), and the `BrokerClient` import.
  - `src/lib.rs` — drop the `broker`/`freshness` construction; call `RetrievalEngine::new(store_dyn, embedder, reranker, RetrievalConfig::from_config(&config))`.
  - `src/retrieval/mod.rs` — remove the `freshness` field, the `new()` param, the `FreshnessChecker` import, and `tag_freshness`; build `RankedFact { fact, score, source }` (drop `currency`).
  - `src/types/api.rs` — remove `RankedFact.currency` and the `Currency` enum.
  - `src/config.rs` — remove `broker_url` + `freshness_budget_ms` + `freshness_per_call_ms` (fields, `required`/env reads, struct build, the `RECALL_BROKER_URL` test-env row `:510`).
  - `src/types/job.rs` — remove `ReReadSource`; remove the match arms at `src/queue/mod.rs:608` and `src/maintenance/mod.rs:954`.
  - Tests: delete `tests/features/freshness.feature`; remove currency/freshness scenarios from `tests/features/retrieval.feature` and the "Recall freshness branch" scenario from `tests/features/system.feature`; in `tests/bdd.rs` remove the C5 step defs + freshness harness + `currency` assertions and update every `RetrievalEngine::new` call; in `tests/support/mod.rs` remove `RECALL_BROKER_URL` env entries (`:32,:146,:197`), the broker+freshness wiring (`:205-218`), and the broker stub helpers (`:401-486`).
- **Rationale:** one cohesive removal — the trait, impl, config, job kind, and module are mutually dependent, so deleting them together is the only warning-clean ("no dead code") step.
- **Exit criteria:**
  - `cargo build` and `cargo clippy --all-targets` clean (no dead-code warnings; no orphaned config).
  - `cargo test` green: full BDD suite (unit + `tests/bdd.rs` + `tests/store_remote.rs`) against the existing harness; `freshness.feature` gone; retrieval/system scenarios pass without `currency`.
  - Config check: process boots with `RECALL_BROKER_URL` **unset** and `/readyz` passes (extend a boot/readiness scenario in `tests/features/boot.feature` or the system harness).
  - Behavioural check: `grep -rE "currency|BrokerClient|FreshnessChecker|ReReadSource|RECALL_BROKER_URL" src/` returns zero.
- **Risks / rollback:** largest blast radius; revert is the whole-phase git diff. RISK-001 (a deployment manifest still sets the removed env) tracked below.

### Phase 3 — Model the agent control loop end-to-end (BDD)
- **Goal:** prove the agent-side ask→check→update loop through the real HTTP edge — the original "ultimate e2e check" request.
- **Changes (test-only):**
  - `tests/features/system.feature` — add: (a) **cold-start → learn → recall**: recall miss → agent-stated write with a cited source → drain → recall returns the fact; (b) **agent-side refresh loop**: recall with `include_provenance` returns `origin_ref`+`marker` → agent writes a fresh agent-stated note that supersedes the prior one → next recall returns the fresh note; (c) **provenance through the edge**: `include_provenance` on/off toggles the `source` object.
  - `tests/bdd.rs` — supporting steps: agent-stated sourced write through `POST /v1/memories`, assert `data.facts[].source.{origin_ref,modification_marker}` on the system recall response, assert supersession (`superseded_by` set / prior note absent from recall).
- **Rationale:** the spec's Phase-5 integration flows now name "the agent-side freshness loop"; this phase makes that runnable and closes the original BDD-coverage question. Test-only, so it ships last with zero production risk.
- **Exit criteria:**
  - `cargo test` green including the three new system scenarios; existing harness (edge served in-process on an ephemeral port + embedded SurrealDB + wiremock + local issuer).
  - Behavioural check: the refresh-loop scenario asserts the second recall returns the M2 note and the M1 note is superseded.
- **Risks / rollback:** test-only; revert the scenarios to back out.

## Cross-cutting validation

After Phase 3: `cargo build` + `cargo clippy --all-targets -- -D warnings` clean; full `cargo test` (unit + `tests/bdd.rs` + `tests/store_remote.rs`) green with all integrating services up (embedded SurrealDB, wiremock providers, local OIDC issuer, Dex container); `grep -rE "currency|BrokerClient|FreshnessChecker|SourceState|ReReadSource|RECALL_BROKER_URL|RECALL_FRESHNESS" src/ tests/` returns zero live references; then `/sync-check` to confirm Design ↔ Spec ↔ Code alignment (the *no exit without sync* gate).

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Remove RECALL_BROKER_URL / RECALL_FRESHNESS_* from deployment manifests and .env examples
  why: RFC OQ-4 — the code no longer reads them. Repo-wide grep over yaml/toml/env/sh/Dockerfile/md found NO references, so there is nothing to clean in-repo.
  source: planning
  suggested-command: (none — verified clean 2026-06-22)
  status: dropped
  added: 2026-06-22
- id: FU-002
  title: Confirm no central-broker deployment needs the old recall-side check behind a flag
  why: RFC OQ-5 — ADR-014 assumes broker is always per-agent local; a central-broker deployment would reopen the decision.
  source: planning
  suggested-command: /rfc (new) if a central-broker deployment is confirmed
  status: open
  added: 2026-06-22
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: A deployment still sets RECALL_BROKER_URL as required and fails config validation elsewhere
  why: If any external orchestration asserts the var is consumed, removing the read could surface a "set but unused" lint in that system; recall itself is unaffected (it boots fine without it).
  source: planning
  likelihood: low
  impact: low
  mitigation: FU-001 swept all in-repo manifests/env files — zero references found; recall boot is strictly more permissive after removal.
  status: mitigated
  added: 2026-06-22
- id: RISK-002
  title: External consumers reading `currency` from /v1/recall break on its removal
  why: Removing the field is a breaking response-contract change; a client branching on `currency` would see it disappear.
  source: planning
  likelihood: low
  impact: medium
  mitigation: recall is pre-1.0 single release; OpenAPI doc updated; the conditional `source` (Phase 1) ships before removal so a migrating client has the replacement available first.
  status: accepted
  added: 2026-06-22
```
