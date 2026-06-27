# 10 — Risks, Alternatives, Dependencies, Rollout

> **Mode:** draft · **Revision:** 0.6.0 · **Last updated:** 2026-06-22

## Risks

- **Store immaturity — load-bearing for the language choice.** SurrealDB (or any "does-everything"
  engine) may under-deliver on one capability at the target scale. Because **Rust was chosen primarily
  to embed SurrealDB in-process** (ADR-009), a spike failure is not just a store swap — it **reopens
  the language decision** and the single-binary deployment story. *Mitigation:* run the OQ-STORE spike
  **before Rust scaffolding**, not merely before codegen; keep store access abstracted (NFR-MA1) with a
  documented fallback (graph DB + vector index); accept that the fallback is a rearchitecture.
- **Embedded↔remote store abstraction.** The claim that scaling from embedded in-process to a remote
  SurrealDB / TiKV cluster needs "no rewrite" (ADR-009/011) spans different transaction, latency, and
  error semantics. *Mitigation:* treat the engine abstraction (NFR-MA1) as a first-class, tested seam;
  exercise the remote path in the spike, not only the embedded one.
- **Benchmark-vs-reality gap.** Memory that scores well on QA benchmarks can crater inside whole
  agentic tasks. *Mitigation:* evaluate end-to-end on representative agentic-search workflows with
  evolving facts, not just LoCoMo/LongMemEval (`good-mem.md` §13).
- **Memory poisoning.** Booby-trapped source content tries to write false facts or inject
  instructions. *Mitigation:* write-gate trust validation (ADR-008), trust-aware retrieval,
  untrusted-data separation, provenance on every fact.
- **Self-reinforcing error.** A wrong consolidated insight becomes permanent "knowledge".
  *Mitigation:* validate insights against sources before promotion, decaying confidence on
  inferences, never let an insight outrank its source facts (ADR-006, sequences §Consolidate).
- **Confident staleness.** A high-relevance fact returned after its source changed. *Mitigation:*
  freshness loop on the read path, recency weighting, conditional requests.
- **Trust-threshold miscalibration.** The write gate rejects good facts or admits poison.
  *Mitigation:* tune against labelled data; quarantine (not hard-reject) the uncertain; monitor
  rejected-write metrics.
- **Eventual-consistency surprise.** A just-written fact is not immediately retrievable (async write
  path). *Mitigation:* document the contract; idempotent acks; fast-track indexing where needed.
- **Entity-resolution precision in v1.** v1 resolves entities by deterministic rules then ML
  similarity, and *creates a new entity* rather than risk a wrong merge when neither tier is confident;
  LLM-based adjudication is deferred. *Mitigation:* a maintenance-time `merge_entities` pass folds
  duplicates; revisit LLM adjudication once duplication rates are measured (surfaced by `/spec` Phase
  3.5 from the Write Pipeline LLD).
- **Cross-scope leakage.** A bug surfaces one user's fact to another. *Mitigation:* scope enforced
  server-side on the authenticated claim; isolation tested as a first-class case (NFR-PR1).
- **Slow / flaky integration tests.** Containerised dependencies make suites heavy. *Mitigation:*
  reuse/singleton container sessions, in-memory SurrealDB for the inner loop, parallel scope-isolated
  scenarios, Dex-in-a-container for hermetic OIDC (ADR-010).

## Alternatives considered (system-level)

- **Pre-indexed static memory** (bulk-index all documents on a schedule). Rejected — goes stale,
  mixes versions, creates a central privacy surface, and pays the full indexing cost up front. The
  learn-as-you-ask model with a freshness loop is the chosen approach (`agentic-mem.md` §7).
- **Embed memory in model weights (parametric).** Rejected — sacrifices auditability, deletion, and
  cheap updates, all of which the governance and freshness requirements demand.
- **No dedicated service; memory as a library inside each agent.** Rejected — no shared,
  consistently-governed, per-user-isolated store; every agent would reinvent write/forget/audit.

## Dependencies

- **Broker** — authenticates as the user, injects the OIDC token, reads source systems on the
  user's behalf, runs the system allowlist and broker-side audit.
- **OIDC Identity Provider** — issues tokens and exposes discovery/JWKS.
- **Memory store** — the persistence engine (SurrealDB candidate).
- **Embedding / reranker / LLM providers** — for the async write/maintenance path.
- **Observability stack** — logs, metrics, traces sink.
- **Active Practice Pack** — supplies the build/test/lint toolchain.

## Rollout and Rollback

- **Rollout.** Ship as a single self-contained binary / container with the store embedded in-process;
  start single-node; register `recall` as an allowlisted endpoint in the broker. Begin with heuristic control and conservative write-gate
  thresholds. Follow the staged build order from `good-mem.md` §14 — crawl (working, honest memory) →
  walk (stays true over time) → run (gets smarter) — each stage independently shippable and gated by
  the evaluation harness.
- **Rollback.** The service is stateless apart from the store; a bad release rolls back by redeploying
  the prior image. Because writes are non-destructive by default (supersession, not overwrite) and
  the store is the durable system of record, a rollback does not lose memory. Store schema migrations
  are dry-run before apply and shipped with a down path; applying a migration against a shared store
  is an explicit user action, not part of an automated deploy.
