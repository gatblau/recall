# 05 — Tech Stack

> **Mode:** draft · **Revision:** 0.7.0 · **Last updated:** 2026-06-27

Committed defaults below are the starting position; the materially contested ones are recorded as ADRs
(see [09 — Decisions](./09-decisions.md)) and open questions (see [10 — Risks](./10-risks.md) and the
Open Questions table). Several rows are assumption-backed because the concept notes did not commit a
specific product — those are flagged.

| Layer | Choice | Rationale (one line) |
|---|---|---|
| Language | **Rust** *(committed — ADR-009)* | SurrealDB is Rust-native and can only be embedded in-process from Rust; mature async/HTTP/OIDC crates (`tokio`, `axum`, `openidconnect`) cover the rest of the service. |
| Framework / runtime | `tokio` async runtime + `axum` HTTP *(assumed; `actix-web` an equivalent option)* | Small task-shaped API on a proven async stack; the exact web framework is non-architectural. |
| Datastore | **Embedded SurrealDB** (SurrealKV or RocksDB backend), engine-abstracted *(ADR-003, ADR-009)* | In-process graph + vector (HNSW) + keyword (BM25) over rich bi-temporal edges, no client/server hop; the same engine abstraction can target a remote SurrealDB / TiKV cluster for scale-out. Capability validated by the spike (OQ-STORE). |
| Message broker / queue | Durable work queue *(assumed; store-backed or NATS — OQ-QUEUE)* | Decouples async write/maintenance from the request and makes writes retry-safe; concrete product deferred. A store-backed queue keeps the single-binary story intact. |
| Deployment substrate | Single self-contained binary / container; single-node first, distributed later *(NFR-MA3, NFR-S3)* | Store linked in-process gives a genuine single-binary single-node deployment; scale out to a SurrealDB server / TiKV cluster without a rewrite. |
| Observability (logs / metrics / traces) | Structured logs + metrics + traces (OpenTelemetry-style) *(assumed)* | Correlation IDs, context propagation, and the four-layer quality metrics from `good-mem.md` §13. |
| Signing / crypto | OIDC/JWT validation via issuer JWKS; TLS in transit; encryption at rest | `recall` validates tokens (never mints them); standard transport and at-rest protection. |
| Build / test toolchain | Cargo + the active Practice Pack; BDD (e.g. `cucumber`) + testcontainers, Dex for OIDC tests *(ADR-010)* | Outside-in integration/BDD suite through the public API with containerised dependencies; in-memory SurrealDB for the fast inner loop; reused container sessions for speed; Practice Pack supplies exact commands. |
| Embedding model | External provider; **fact-content embedding async at write time, query embedding on the read path** *(OQ-MODELS, ADR-012)* | Fact vectors are computed off the read path and cached; the per-query embedding is a read-path model inference inside the latency budget. A local/in-process embedder is the latency mitigation if a remote hop threatens NFR-P2/P3. |
| Reranker | Cross-encoder, **on the read path** over the bounded stage-1 candidate set *(OQ-MODELS, ADR-012)* | Stage 2 of two-stage retrieval (`good-mem.md` §7.2); a discriminative model inference (not an LLM, so NFR-P1 holds) that still consumes read-path latency. Local vs hosted is therefore a latency decision, not async cost tuning. |
| ~~Extraction / consolidation LLM~~ | **None — recall is LLM-free (ADR-015)** | Extraction is performed by the agent; server-side consolidation is dropped. `recall` holds no LLM provider or key. |
| MCP transport | A maintained Rust **MCP server library** over streamable-HTTP *(assumed; exact crate chosen at codegen — OQ-LIB, ADR-016)* | The `recall-mcp` binary exposes the service operations as MCP tools; the library handles the MCP/JSON-RPC framing while the shared Service Layer does the work. Non-architectural — swapping the library does not change the service. |

**Notes.**

- No row is wire-level; product commitments that change the contract become ADRs, not silent edits
  (per the HLD↔LLD rule).
- The store is a committed hybrid-store *capability* (graph + vector + keyword + rich bi-temporal
  edges) realised by **embedded SurrealDB** (ADR-003/ADR-009). The spike in `agentic-mem.md` §9.5 now
  *validates* that choice against the latency/scale targets rather than selecting the engine, with a
  documented fallback if it falls short (OQ-STORE).
