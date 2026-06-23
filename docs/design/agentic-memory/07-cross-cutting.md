# 07 — Cross-cutting Concerns & NFRs

> **Mode:** draft · **Revision:** 0.6.0 · **Last updated:** 2026-06-22

## Cross-cutting Concerns

| Concern | Stance |
|---|---|
| Authentication | **Applies** — OIDC bearer JWT, validated against the issuer's JWKS (signature, issuer, audience, expiry). `recall` validates, never mints; holds no end-user credentials. See ADR-001. |
| Authorisation | **Applies** — per-operation scopes (read / write / forget) from token claims, plus hierarchical data isolation: a hard Tenant boundary (namespace) and logical Team / User visibility via record permissions driven by membership claims. Enforced server-side on the authenticated identity; never trusts a scope value from the request body. (ADR-011.) |
| Logging | **Applies** — structured logs with correlation IDs and context propagation; never logs credentials, tokens, or raw PII. For debugging/operations, **not** the compliance record. |
| Audit (governance) | **Applies** — a **dedicated, append-only audit trail**, distinct from operational logs: one record per call (subject, operation, scope, outcome, token `jti` — never the token). Held **per-tenant** (within the tenant namespace, ADR-011), so it is isolated and erased with the tenant. It is the authority for accountability (NFR-OB1). |
| Metrics | **Applies** — the four quality layers from `good-mem.md` §13: task usage, memory quality (contradiction/staleness rate), efficiency (latency, tokens/query, storage growth), governance (rejected writes, deletions). |
| Tracing | **Applies** — request context propagated across the API edge, retrieval path, and async workers. |
| Rate limiting | **Applies** — agent-aware tiers with standard `RateLimit-*` / `Retry-After` headers (agents emit far more calls than humans). |
| Pagination | **Applies** — recall returns bounded, capped, paginated results; default result cap to stay token-efficient. |
| Input validation | **Applies** — validated at every API boundary; parameterised queries only against the store. |
| Error handling / envelope | **Applies** — one consistent success envelope and one error envelope (machine-readable `code` + human `message`); meaningful HTTP status codes; typed errors only. |
| Configuration | **Applies** — issuer, audience, store endpoint, model providers, ports, secrets all from environment/config; nothing hardcoded. |
| Health checks | **Applies** — liveness and readiness endpoints; readiness reflects store and IdP-discovery reachability. |
| Migrations | **Applies** — store schema migrations (e.g. schemaless→schemafull tightening as the model stabilises); dry-run before apply; applies to shared databases are user actions. |
| Graceful shutdown | **Applies** — drain in-flight requests, finish or re-enqueue in-flight async jobs, no work lost on restart. |
| CORS | **Does not apply (default)** — callers are server-side (the broker), not browsers; configurable if a first-party browser client is ever added. |
| Multi-tenancy | **Applies** — bridge model (ADR-011): namespace-per-tenant hard boundary + logical Team / User visibility within; bi-temporal facts and the vector index isolated per tenant; no cross-tenant leakage. Database-per-team is the escape hatch for teams needing physical isolation. |
| Outbound calls / orchestration | **Constrained** — `recall` makes **no** outbound call to the broker or source systems and runs **no** agentic-workflow orchestration; freshness checking and source re-reads are the agent's responsibility (ADR-014). The only outbound calls are model-provider inferences: embedding + reranker on the read path, the extraction/consolidation LLM off it (ADR-012). |

## Non-functional Requirements

- **Performance targets.**
  - Read path makes **no LLM calls** (NFR-P1) — but it is **not model-free**: it performs exactly two
    read-path **model inferences**, query embedding and cross-encoder rerank, neither of which is an LLM
    call (ADR-012). Each carries an explicit latency sub-budget within NFR-P2.
  - Retrieval **p95 ≤ 200 ms** for a typical interactive query, excluding caller network time (NFR-P2),
    inclusive of query-embed and rerank (no read-path freshness check — ADR-014).
  - Vector similarity search (ANN only, excluding query-embed) **≤ 50 ms** at target corpus size
    (NFR-P3, SHOULD).
  - Typical recall response within a **bounded token budget** (single-digit thousands, not tens of
    thousands) (NFR-P5).
  - Extraction, consolidation, and re-embedding run **off the read path** (NFR-P4). Source re-reads are
    not performed by `recall` at all — they are the agent's responsibility (ADR-014).
- **Scale.** Continuously growing fact store served without breaching NFR-P2 at the target volume
  (volume TBC — OQ-VOLUMES); **storage growth bounded** by decay/forgetting (NFR-S2); architecture
  scales single-node → distributed without a rewrite (NFR-S3).
- **Availability.** A failure in the async write/maintenance path **must not** take down the read path
  (NFR-AV1); all external calls have timeouts and bounded retry-with-backoff (NFR-AV2); writes are
  retry-safe via idempotency keys (NFR-AV3). Target SLO TBC with deployment context.
- **Security posture.** OIDC authn on every request; per-operation authz with namespace-per-tenant
  hard isolation plus logical team/user visibility (ADR-011); no credentials/ tokens stored or logged; TLS in transit, encryption at rest; **memory-poisoning defence** via the
  write gate and trust-aware retrieval; externally-sourced content treated as untrusted data, never
  instructions; input validated at every boundary; parameterised store queries only.
