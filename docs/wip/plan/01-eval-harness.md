# Plan — `recall` evaluation harness (turn usefulness from claim into numbers)

> **Move:** CSPA 3 (Plan) · **Input:** `good-mem.md §13` (evaluation harness), `requirements.md §7`
> (acceptance posture), `competitive-positioning.md §5/§6` (the "no measurements of its own yet" gap)
> · **Authored:** 2026-06-26

## Anchor (from input)

`recall`'s usefulness is currently argued architecturally, not measured. Build a reproducible
evaluation harness that produces numbers across the four quality layers (`good-mem.md §1`) on
recognised memory benchmarks **and** an end-to-end agentic task with evolving facts, so the
usefulness claim rests on evidence. The north-star (`requirements.md §7`): on a representative
agentic-search workload with facts that change mid-session, task success stays high while
tokens/query and latency stay bounded and no poisoned or stale fact is returned as truth.

## Verified context

- **`recall` is a passive, LLM-free store (ADR-014, ADR-015).** It performs **no** fact extraction
  and **no** server-side consolidation; it accepts agent-asserted structured facts and serves ranked
  results. The read path is LLM-free but runs two model inferences (query-embed + rerank, ADR-012).
- **API surface the harness drives** (`docs/design/agentic-memory/08-interfaces.md`):
  `POST /v1/recall` (query + filters + `include_provenance`), `POST /v1/memories`
  (`Idempotency-Key`, structured `memory_class`), `POST /v1/memories/{id}/retire`,
  `DELETE /v1/memories/{id}` (returns deletion proof), `GET /v1/memories/{id}`
  (`If-Modified-Since`/`ETag`).
- **Performance targets to measure against:** NFR-P2 retrieval p95 ≤ 200 ms; NFR-P3 ANN ≤ 50 ms;
  NFR-P5 bounded response token budget. Phase-0 spike measured ANN p95 9.46 ms / stage-1 15.60 ms at
  25k facts/namespace (indexed) — a provisional, single-leg baseline (see backlog FU-007).
- **Governance primitives to exercise:** namespace-per-tenant isolation (ADR-011), write-gate
  quarantine/reject (FR-W7), `LocalPiiDetector` (in-process), verifiable hard delete with proof
  (FR-D3), abstention (FR-R8), supersession/knowledge-update (FR-C1).
- **Existing test harness** (`tests/bdd.rs`, cucumber-rs) uses **wiremock** stubs for embedding +
  reranker. That is correct for behavioural BDD but **wrong for accuracy measurement** — real
  retrieval quality needs real embedding + reranker models, not positional stubs.

## Open assumptions (decisions, per "assumptions over questions")

- **A1 — The harness is an external driver, not part of the `recall` crate.** It lives in a sibling
  `eval/` workspace member (or a separate repo) that speaks HTTP to a running `recall` instance. The
  service stays free of benchmark code. Override only if you want the harness inside `tests/`.
- **A2 — The "agent shim" is part of the harness and is held constant.** Because `recall` no longer
  extracts or consolidates, the harness supplies an **agent shim**: an LLM-backed component that
  turns benchmark conversation turns into structured facts, writes them to `POST /v1/memories`,
  distils insights into agent-stated `consolidated` facts, and runs the agent-side
  ask→check→refresh freshness loop using `include_provenance`. The shim's model and prompts are
  **pinned and version-stamped** in every result, so runs are comparable and `recall`'s contribution
  is isolatable.
- **A3 — One pinned model stack for accuracy runs.** A single embedding model, a single reranker,
  and a single shim-LLM are pinned (model id + version recorded in results). Default: the same
  providers `recall` is configured with in deployment. No wiremock on accuracy runs.
- **A4 — Datasets are the public releases.** LongMemEval and LoCoMo are fetched from their public
  releases; BEAM (1M–10M-token volumes) is treated as an optional scale leg, not a launch gate.
- **A5 — Numbers are reported with their confounders, not as bare leaderboard entries.** Every
  published figure carries: shim version, model stack, dataset version, and `recall` commit. Direct
  comparison to vendor-published Mem0/Zep numbers is annotated as **not apples-to-apples** (their
  numbers bundle their own extraction; ours isolates the store behind a fixed shim — A2).

## Scope

- **In scope:** the harness workspace; the agent shim; dataset adapters (LongMemEval, LoCoMo); a
  metrics collector across the four quality layers; the governance + failure-mode suite (leakage,
  deletion, poisoning, abstention, knowledge-update, freshness); one end-to-end evolving-facts
  agentic task; a results format + a short results note next to `competitive-positioning.md §4`.
- **Out of scope (deferred — see Follow-ups):** BEAM-scale stress and the OQ-VOLUMES real-corpus run
  (ties to FU-007); any change to `recall`'s retrieval algorithm in response to findings (that is a
  separate `/breakdown` once numbers exist); a CI gate on accuracy thresholds (add once a baseline is
  established); multimodal or procedural-memory evaluation (not modelled in v1).

## Phases

### Phase 0 — Harness scaffolding, agent shim, metrics spine
- **Goal:** a running harness can drive a live `recall`, write facts through the shim, query, and
  record metrics — proven on a 10-item smoke set before any real benchmark.
- **Changes:**
  - New workspace member `eval/` (binary): config (target base URL, OIDC token source, pinned model
    ids), an HTTP client over the six endpoints above, and a results writer (JSON Lines: one record
    per query with latency, tokens, scores, scope, outcome).
  - **Agent shim** module: `extract(turn) -> Vec<StructuredFact>` (LLM call), `write(fact)` →
    `POST /v1/memories` with `Idempotency-Key`, `consolidate(episodes) -> ConsolidatedFact`, and
    `freshness_check(fact)` running ask→check→refresh via `include_provenance` + a stub source-store
    the harness controls (so "the source changed" is injectable).
  - **Metrics collector** keyed to the four layers (`good-mem.md §1`): (1) task — success/abstention
    counts; (2) memory quality — answer correctness, contradiction rate, staleness rate; (3)
    efficiency — p50/p95/p99 latency, tokens/query, storage growth (bytes/namespace over the run);
    (4) governance — leakage count, deletion-proof verified, poison-survival count.
  - A `make eval-smoke` target that boots `recall` (embedded SurrealDB) with the pinned real model
    stack and runs the 10-item smoke set.
- **Rationale:** lock the plumbing and the shim before spending model budget on full datasets; the
  smoke set catches contract drift cheaply.
- **Exit criteria:** smoke run green; every metric populated and written to results; shim + model
  stack version-stamped in the output header.

### Phase 1 — LongMemEval + LoCoMo adapters → baseline accuracy
- **Goal:** first real numbers on the two recognised conversational benchmarks, as orientation
  ("smoke tests", `good-mem.md §13`), not a finish line.
- **Changes:**
  - **LongMemEval adapter** covering its five abilities — information extraction, multi-session
    reasoning, temporal reasoning, knowledge updates, abstention — each ability reported separately.
  - **LoCoMo adapter** covering single-hop, multi-hop, temporal, commonsense, and
    adversarial/unanswerable categories — each reported separately.
  - A per-dataset run report: accuracy per ability/category, plus the efficiency layer (latency
    distribution vs NFR-P2, tokens/query vs NFR-P5).
- **Rationale:** these datasets directly exercise the abilities `recall`'s design claims (temporal,
  knowledge-update, abstention); breaking results out per ability shows *where* it is and is not
  useful, not a single misleading aggregate.
- **Exit criteria:** both datasets run end-to-end; per-ability/per-category accuracy + latency
  recorded; results header carries all A5 confounders.

### Phase 2 — Governance + failure-mode suite
- **Goal:** measure the qualities that quietly fail and that the OSS memory libs mostly do not test.
- **Changes (each an explicit, scored test):**
  - **Cross-tenant leakage (must be 0):** seed two tenants with overlapping content; assert no
    query in tenant A ever returns a tenant-B fact (NFR-PR1, ADR-011). Any non-zero count is a hard
    fail.
  - **Verifiable deletion:** write → `DELETE /v1/memories/{id}` → assert the returned proof and that
    the fact is unrecoverable via recall, including from derived/embedded forms (FR-D3).
  - **Poisoning resistance:** inject instruction-like / false "facts" from an untrusted source;
    measure the write-gate quarantine/reject rate and whether a poisoned fact ever surfaces as truth
    (FR-W7, NFR-SEC4).
  - **Abstention:** pose questions with no supporting evidence; measure the rate of correct
    "insufficient evidence" vs fabrication (FR-R8).
  - **Knowledge update / supersession:** write a fact, then a contradicting successor; assert the
    current truth is answerable and the superseded fact is retained but not returned as current
    (FR-C1).
  - **Freshness loop (agent-side):** change a source in the harness's controlled source-store;
    assert the shim's `include_provenance` loop detects it and writes a superseding fact (ADR-014).
- **Rationale:** the north-star explicitly includes "no poisoned or stale fact returned as truth" and
  "deletion verifiably honoured" — these are usefulness-defining for an enterprise memory and must be
  measured directly, not inferred from accuracy.
- **Exit criteria:** all six tests run and scored; leakage count = 0; each failure mode has a number.

### Phase 3 — End-to-end evolving-facts agentic task (the real measure)
- **Goal:** the north-star metric — success on a representative agentic-search workflow whose facts
  change during the session.
- **Changes:**
  - One scripted multi-step task (interdependent steps; some facts deliberately change mid-run) run
    through the shim against `recall`, scored on: task success rate, tokens/query (bounded?), p95
    latency (≤ NFR-P2?), and zero stale/poisoned fact returned as truth.
  - An **ablation**: same task with the shim writing to a naive flat vector store instead of
    `recall`, to show what `recall`'s bi-temporal + governance machinery actually buys on a changing
    corpus (the usefulness delta, not just an absolute score).
- **Rationale:** `good-mem.md §13` warns that LoCoMo-perfect systems crater on embedded agentic
  tasks; the whole-task run is the measure that matches how `recall` is actually used. The ablation is
  what converts "useful in principle" into a quantified delta.
- **Exit criteria:** task runs reproducibly; the four north-star quantities recorded; the
  `recall`-vs-flat-store ablation delta reported.

### Phase 4 — Publish + wire the results back
- **Goal:** the usefulness claim now cites numbers.
- **Changes:** a short `docs/concept/eval-results.md` summarising the figures; update
  `competitive-positioning.md §5.3` ("no measurements of its own yet") and §6 to cite it; record the
  run recipe so it is repeatable.
- **Exit criteria:** results note published; positioning note updated from "aspirational" to "measured
  (date, commit)"; `/sync-check` clean.

## Follow-ups (not in this plan)

```yaml
- id: FU-E1
  title: BEAM-scale (1M–10M token) stress run, tied to OQ-VOLUMES
  why: BEAM and the real target corpus stress the latency tail (RISK-007) and the remote-store leg;
    run once OQ-VOLUMES is pinned and the remote SurrealDB leg is up. Relates to backlog FU-007.
  status: open
  added: 2026-06-26
- id: FU-E2
  title: CI accuracy-regression gate once a baseline exists
  why: after Phase 1 establishes per-ability baselines, add a gate that fails a PR on a >X% accuracy
    drop; deferred until a stable baseline and thresholds exist.
  status: open
  added: 2026-06-26
- id: FU-E3
  title: Second shim-LLM as an ablation on shim sensitivity
  why: A2 holds the shim constant; running a second pinned shim quantifies how much of the measured
    score is recall vs the extractor, strengthening A5's apples-to-apples caveat.
  status: open
  added: 2026-06-26
```

## Risks (not closed by this plan)

```yaml
- id: RISK-E1
  title: Agent-shim quality confounds recall's measured accuracy
  why: a weak extractor/consolidator makes recall look worse than it is; a strong one flatters it.
  likelihood: high
  impact: high
  mitigation: pin and version-stamp the shim (A2); report it with every figure (A5); FU-E3 ablation
    quantifies the sensitivity.
  status: accepted
  added: 2026-06-26
- id: RISK-E2
  title: Not apples-to-apples with vendor-published numbers
  why: Mem0/Zep figures bundle their own extraction; ours isolates the store behind a fixed shim, so
    a naive side-by-side misleads.
  likelihood: high
  impact: medium
  mitigation: annotate every comparison (A5); prefer the Phase-3 ablation delta over cross-vendor
    leaderboard comparison.
  status: accepted
  added: 2026-06-26
- id: RISK-E3
  title: Real model-provider cost to run full datasets
  why: accuracy runs need real embedding + reranker + shim-LLM calls; full LongMemEval/LoCoMo passes
    cost model spend per run.
  likelihood: medium
  impact: low
  mitigation: gate full runs behind the Phase-0 smoke set; cache embeddings by content hash; run
    full passes on demand, not per-commit (until FU-E2).
  status: accepted
  added: 2026-06-26
```
