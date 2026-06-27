# `recall` — Requirements (Functional & Non-Functional)

*Status: requirements, reconciled to the design on 2026-06-26. Originally derived from the concept
notes [`agentic-mem.md`](./agentic-mem.md) (concepts, graph model, SurrealDB candidate, agent-facing
API) and [`good-mem.md`](./good-mem.md) (how to make the memory excellent), then reconciled to the
HLD (rev 0.6.0) after **ADR-014** (freshness is agent-side) and **ADR-015** (`recall` is LLM-free)
reversed two concept-stage positions. Same tone — factual, pragmatic, no marketing. Where a
requirement was superseded by a decision, it now traces forward to that ADR.*

This document states **what `recall` must do** and **the qualities it must have**, not how to build
it. Each requirement has a stable ID, a priority, and (where useful) a trace back to the concept
note that motivates it. Priorities use RFC 2119 keywords: **MUST** (required), **SHOULD** (strongly
recommended, deviation needs justification), **MAY** (optional).

---

## 0. Scope and assumptions

**In scope.** `recall` is a standalone memory service for AI agents. It stores facts learned from
interactions and sources, lets an agent write/recall/forget those facts over a network API, keeps
them current and consistent over time, and does so under per-user access control.

**Out of scope** (owned elsewhere or explicitly deferred):
- The sandbox, credential broker, system allowlist, and audit infrastructure that *call* `recall` —
  these belong to the surrounding agent platform (`agentic-mem.md` §7.2). `recall` is a well-behaved allowlisted endpoint, not
  the broker.
- The agent/LLM that decides *what to ask* `recall`. `recall` serves memory; it is not the agent.
- Learned (RL-based) memory control — a later frontier (`good-mem.md` §10), not a launch requirement.

**Binding assumptions** (documented decisions, per the project's "assumptions over questions" rule;
each may be revisited but is treated as settled unless changed):

- **A1 — Identity is supplied, not minted.** Callers reach `recall` through the broker, which
  authenticates as the end user and injects a bearer token. `recall` validates that token and never
  handles raw user credentials (`agentic-mem.md` §7.2, §11.2 P3).
- **A2 — OIDC is the authentication standard.** Tokens are OIDC-issued JWTs validated against a
  configured issuer. (Affirmed below; rationale in §4. The specific IdP and exact claim set the
  broker emits remain an open question — §8.)
- **A3 — Multi-tenant by user.** Every fact belongs to an owning scope (a user, and/or an
  organisation/tenant). Cross-scope leakage is a defect, not a tuning parameter (`good-mem.md` §2).
- **A4 — Hybrid store with a temporal model.** The store provides graph, vector, and keyword
  retrieval and can hold bi-temporal facts (`agentic-mem.md` §4–§5). **SurrealDB is committed** as the
  single multi-model store (ADR-003, ADR-009).
- **A5 — Read path is synchronous and fast; write/maintenance is asynchronous** (`good-mem.md` §3).

---

## 1. Functional requirements

### 1.1 Writing memory

- **FR-W1 (MUST)** — `recall` MUST accept a request to remember a fact, given the fact content, its
  source reference, and the owning scope (derived from the authenticated identity, not the request
  body).
- **FR-W2 (MUST)** — `recall` MUST store facts as **structured assertions** (entities and
  relationships), not opaque text blobs (`agentic-mem.md` §5, `good-mem.md` §4.2).
- **FR-W3 (MUST)** — Every stored fact MUST carry, as first-class fields: **source/provenance**,
  **ingestion time**, **confidence score**, and **importance/salience score** (`good-mem.md` §4.5).
- **FR-W4 (MUST)** — Writes MUST be **idempotent** when an `Idempotency-Key` is supplied: a replay
  returns the original result and creates no duplicate (`agentic-mem.md` §11.2 P5).
- **FR-W5 (MUST)** — `recall` MUST accept **agent-asserted structured facts** and performs **no
  server-side LLM extraction**; turning unstructured content into structured assertions is the agent's
  responsibility (ADR-015, supersedes the original single-pass-extraction requirement). Conflict
  resolution is still deferred to maintenance (`good-mem.md` §4.2).
- **FR-W6 (SHOULD)** — `recall` SHOULD record agent-stated facts (confirmations, conclusions), not
  only user-stated ones (`good-mem.md` §4.3).
- **FR-W7 (MUST)** — `recall` MUST apply a **write gate**: content from an untrusted source is
  trust-scored and may be quarantined or rejected rather than admitted to memory (`good-mem.md` §4.6,
  §9.2). Rejected/quarantined writes MUST return a distinguishable result, not a silent success.

### 1.2 Retrieving / searching memory

- **FR-R1 (MUST)** — `recall` MUST accept a natural-language query plus optional structured filters
  (time window, entity, source) and return a ranked set of facts scoped to the caller.
- **FR-R2 (MUST)** — Retrieval MUST combine **multiple signals** — semantic (vector), keyword
  (BM25/full-text), and graph/entity — fused into a single ranking (`good-mem.md` §7.1).
- **FR-R3 (SHOULD)** — Retrieval SHOULD use a **two-stage funnel**: broad multi-signal recall, then
  cross-encoder reranking of the top candidates (`good-mem.md` §7.2).
- **FR-R4 (MUST)** — Every returned fact MUST include its **provenance** (source, edit/ingestion
  date) and **confidence**, so callers can attribute and weigh it (`agentic-mem.md` §11.2 P7).
- **FR-R5 (MUST)** — Retrieval ranking MUST apply **recency weighting** so stale, untouched facts
  rank lower (`good-mem.md` §7.4, §8.1).
- **FR-R6 (SHOULD)** — `recall` SHOULD apply **retrieval gating**: when no candidate is sufficiently
  relevant, return an empty/low-confidence result rather than padding with weak matches
  (`good-mem.md` §7.5).
- **FR-R7 (MUST)** — Responses MUST be **bounded and token-efficient**: a default result cap,
  pagination, and a way to request facts with or without full provenance (`agentic-mem.md` §11.2 P4).
- **FR-R8 (MUST)** — `recall` MUST support **abstention**: it must be able to indicate "insufficient
  evidence" rather than fabricate a fact (`good-mem.md` §13).

### 1.3 Freshness

- **FR-F1 (MUST)** — `recall` MUST make **no** source-change check and **no** outbound call; it runs no
  freshness loop (ADR-014, supersedes the recall-side loop of the original requirement). Instead, on
  request (`include_provenance`) it MUST return each sourced fact's `origin_ref` +
  `modification_marker` so the **agent** — which holds the local broker and document access — runs
  ask → check → refresh itself, writing a fresh superseding fact when a source changed
  (`agentic-mem.md` §7.1, `good-mem.md` §7.4).
- **FR-F2 (SHOULD)** — `recall` SHOULD support **conditional requests** (`ETag` / `If-Modified-Since`)
  on a fact fetch (`GET /v1/memories/{id}`) so a caller can cheaply tell whether a *stored fact*
  changed. The *source-document* currency check is the agent's, against its own broker — never
  `recall`'s (ADR-014; `agentic-mem.md` §11.2 P6).

### 1.4 Consolidation and maintenance (asynchronous)

- **FR-C1 (MUST)** — `recall` MUST detect and resolve contradictions using **supersession**, not
  destructive overwrite: a superseded fact's validity is ended and the successor recorded; history
  remains queryable (`agentic-mem.md` §5.2, `good-mem.md` §5).
- **FR-C2 (MUST NOT)** — `recall` MUST run **no** server-side consolidation and make no LLM call
  (ADR-015, supersedes the original background-consolidation requirement). The **agent** distils
  recurring episodes into an insight with its own model and writes it back as an agent-stated
  `consolidated` fact; `recall` stores and serves it, retaining the `consolidated` class +
  `derived_from` for exactly this (`good-mem.md` §6.1–§6.2).
- **FR-C3 (MUST)** — Validating a consolidated/inferred fact against its source facts before it is
  written is the **agent's** responsibility (consolidation is agent-side — ADR-015). `recall` MUST
  still enforce that a consolidated fact carries lower or decaying confidence and **never outranks the
  verified facts it derives from** (ADR-006; `good-mem.md` §6.3).
- **FR-C4 (SHOULD)** — `recall` SHOULD perform **entity resolution and deduplication** as distinct
  steps (normalise → deduplicate → resolve identity), not a single fuzzy match (`good-mem.md` §4.4).

### 1.5 Forgetting and deletion

- **FR-D1 (MUST)** — `recall` MUST apply **graceful decay** (gradual, reinforcement-resettable), not
  only hard TTL, so used facts persist and unused ones fade (`good-mem.md` §8.1).
- **FR-D2 (MUST)** — Forgetting MUST honour a **salience floor**: a high-importance fact is never
  pruned on time-and-disuse alone (`good-mem.md` §8.2).
- **FR-D3 (MUST)** — `recall` MUST support **explicit, verifiable deletion** of specified facts on
  request, including from derived summaries and embeddings, with proof the data is gone
  (`good-mem.md` §8.3).
- **FR-D4 (SHOULD)** — Retire/forget operations SHOULD be non-destructive by default (end validity)
  and require explicit intent for hard deletion (`agentic-mem.md` §11.3).

### 1.6 API and discoverability

- **FR-A1 (MUST)** — `recall` MUST expose a **network API over HTTP** with a small set of task-shaped
  endpoints (recall / remember / retire / get-fact / delete). There is no standalone freshness-check
  endpoint — provenance rides on the recall response and a fact fetch supports conditional requests
  (ADR-014; `agentic-mem.md` §11.2 P1, §11.3).
- **FR-A2 (MUST)** — The API MUST be **self-describing**: a machine-readable contract (OpenAPI/JSON
  Schema) and a capabilities endpoint, so an agent can call it without external documentation
  (`agentic-mem.md` §11.2 P2).
- **FR-A3 (MUST)** — The API MUST use a **stable, versioned contract** with consistent success and
  error envelopes (machine-readable `code` + human `message`) and meaningful HTTP status codes
  (`agentic-mem.md` §11.2 P9).
- **FR-A4 (MUST)** — Returned facts MUST be **structured data, clearly separated from any
  instruction**, and MUST NOT embed executable or instruction-like content (`agentic-mem.md` §11.2
  P8, `good-mem.md` §9.2).

### 1.7 Multi-tenancy and access

- **FR-T1 (MUST)** — Every fact MUST belong to an owning scope, and retrieval MUST return only facts
  the authenticated caller is authorised to see (`good-mem.md` §2, §9.3).
- **FR-T2 (MUST)** — `recall` MUST enforce access control **server-side**, keyed on the authenticated
  identity, not on any scope value the caller supplies in the request (`agentic-mem.md` §11.2 P3).

---

## 2. Non-functional requirements

### 2.1 Performance and latency

- **NFR-P1 (MUST)** — The synchronous **read path MUST make no LLM calls** (`good-mem.md` §3).
- **NFR-P2 (MUST)** — Retrieval p95 latency MUST be **≤ 200 ms** for an interactive query of typical
  size, excluding network time to the caller (`good-mem.md` §3, §11).
- **NFR-P3 (SHOULD)** — Vector similarity search SHOULD complete in **≤ 50 ms** at the target corpus
  size (`good-mem.md` §11).
- **NFR-P4 (MUST)** — `recall`'s asynchronous duties — re-embedding, supersession, graceful decay, and
  verifiable hard delete — MUST run **off the read path** (`good-mem.md` §3, NFR-P1). Extraction and
  consolidation are not `recall`'s duties at all; they run at the agent (ADR-015).
- **NFR-P5 (SHOULD)** — A typical recall response SHOULD stay within a **bounded token budget** (target
  on the order of single-digit thousands of tokens, not tens of thousands) (`good-mem.md` §11).

### 2.2 Scalability and capacity

- **NFR-S1 (MUST)** — `recall` MUST hold and serve a continuously growing fact store without retrieval
  latency degrading past NFR-P2 at the stated target volume (to be set during design).
- **NFR-S2 (MUST)** — Storage growth MUST be **bounded over time** by the forgetting/decay subsystem
  (FR-D1), not left to grow unbounded (`good-mem.md` §1 layer 3, §8).
- **NFR-S3 (SHOULD)** — The architecture SHOULD allow scaling from a **single embedded/single-node
  deployment to a distributed one** without a rewrite (`agentic-mem.md` §9.3).

### 2.3 Availability and reliability

- **NFR-AV1 (MUST)** — A failure in the asynchronous write/maintenance path MUST NOT take down the
  synchronous read path.
- **NFR-AV2 (MUST)** — All external calls `recall` makes — the store and the read-path model providers
  (embedding + reranker) — MUST have **timeouts**, and transient failures MUST use **bounded retry
  with backoff** (project rule C9). `recall` makes no source-freshness call (ADR-014).
- **NFR-AV3 (MUST)** — Writes MUST be **retry-safe** (FR-W4) so a client/broker retry never corrupts
  state.

### 2.4 Security — authentication, authorisation, integrity

- **NFR-SEC1 (MUST)** — Every API request MUST be **authenticated**; unauthenticated requests are
  rejected. (Authentication mechanism: §4, OIDC.)
- **NFR-SEC2 (MUST)** — `recall` MUST enforce **authorisation** per operation (read / write / forget)
  based on the authenticated identity and its scopes/claims (FR-T2).
- **NFR-SEC3 (MUST)** — `recall` MUST NOT store or log raw credentials or bearer tokens; only
  non-sensitive identifiers (e.g. subject, token id) may be logged (project rule C10, `good-mem.md`
  §9.3).
- **NFR-SEC4 (MUST)** — `recall` MUST defend against **memory poisoning** via the write gate (FR-W7)
  and trust-aware retrieval, and MUST treat externally-sourced content as untrusted data, never as
  instructions (`good-mem.md` §9, `agentic-mem.md` §11.2 P8).
- **NFR-SEC5 (MUST)** — All inter-service traffic MUST use **TLS**; no secrets, issuer URLs, or ports
  hardcoded — all from configuration/environment (project rules C4, C10).
- **NFR-SEC6 (SHOULD)** — Input MUST be validated at every API boundary; queries against the store
  MUST be parameterised (project rule C10).

### 2.5 Privacy and compliance

- **NFR-PR1 (MUST)** — Facts MUST be **scoped per user/tenant** with no cross-scope leakage at rest or
  in retrieval (FR-T1, `good-mem.md` §9.3).
- **NFR-PR2 (MUST)** — `recall` MUST support **auditable deletion** to satisfy data-deletion
  obligations (FR-D3).
- **NFR-PR3 (SHOULD)** — `recall` SHOULD redact or flag PII on the write path where feasible
  (`good-mem.md` §9.3).
- **NFR-PR4 (SHOULD)** — Data SHOULD be **encrypted at rest** (`good-mem.md` §9.3).

### 2.6 Observability, audit, and governance

- **NFR-OB1 (MUST)** — `recall` MUST **audit every call** — who (subject), what operation, when, on
  what scope, and the outcome — to support attribution and accountability (`agentic-mem.md` §11.2 P10,
  `good-mem.md` §9.3).
- **NFR-OB2 (MUST)** — `recall` MUST emit **structured logs with correlation IDs** and propagate
  request context (project rule C4).
- **NFR-OB3 (SHOULD)** — `recall` SHOULD expose metrics for the four quality layers — task usage,
  memory quality (contradiction/staleness rate), efficiency (latency, tokens, storage growth), and
  governance (rejected writes, deletions) (`good-mem.md` §1, §13).
- **NFR-OB4 (SHOULD)** — `recall` SHOULD return **agent-aware rate-limit headers** (`RateLimit-*`,
  `Retry-After`) (`agentic-mem.md` §11.2 P10).

### 2.7 Cost

- **NFR-CO1 (MUST)** — `recall` has **no LLM dependency**: it makes no LLM call on any path and
  requires no LLM provider to run (ADR-015, supersedes the "minimise write-time LLM usage"
  requirement). The only model inferences are embedding + reranker on the read path (`good-mem.md`
  §4.2, §11).
- **NFR-CO2 (SHOULD)** — `recall` SHOULD cache results for repeated/semantically-similar queries to
  cut redundant cost and latency (`good-mem.md` §11).

### 2.8 Maintainability, portability, and quality

- **NFR-MA1 (SHOULD)** — Storage backend choice SHOULD be **abstracted** so the engine (or a specific
  index) can change without rewriting application logic (`agentic-mem.md` §9.4).
- **NFR-MA2 (MUST)** — `recall` MUST ship with tests covering each error path and core behaviour, to
  the project's stated coverage bar (project rule C3).
- **NFR-MA3 (SHOULD)** — `recall` SHOULD be deployable as a **single binary/container** for the
  embedded/single-node case, and configurable for distributed deployment (NFR-S3).

---

## 3. Data requirements

- **DR1 (MUST)** — A fact record MUST capture: identity, content (structured), owning scope, source
  reference, ingestion time, validity interval (valid-from / valid-to), confidence, salience, and
  supersession links (`agentic-mem.md` §5.2, `good-mem.md` §4.5, §5).
- **DR2 (MUST)** — Relationships (edges) MUST be able to **carry their own properties** (timestamps,
  confidence, metadata) — i.e. rich edges (`agentic-mem.md` §9.2).
- **DR3 (MUST)** — Historical (superseded) facts MUST be **retained and queryable**, not deleted, on
  supersession (FR-C1).

---

## 4. Authentication & authorisation — OIDC (the answer to "should it be OIDC?")

**Yes — OIDC is the right choice, and it is adopted (assumption A2).** The reasoning:

- `recall` sits behind the broker, which already authenticates as the end user and makes the
  call on their behalf (`agentic-mem.md` §7.2). The clean way to convey "this call is user X" across
  a service boundary is a **signed bearer token**, and OIDC (OAuth 2.0 + identity layer) is the
  industry-standard, IdP-agnostic way to issue and validate one.
- It keeps `recall` **credential-free** (assumption A1): `recall` validates a token, it never sees or
  stores a password or API key. This matches the §11.2 P3 principle that identity rides on a header
  the broker sets and the agent's script never constructs.
- It is standards-based, so `recall` can work with **any compliant identity provider** rather than a
  bespoke scheme.

Concrete requirements:

- **AUTH1 (MUST)** — `recall` MUST authenticate requests via an **OIDC-issued JWT bearer token** in
  the `Authorization: Bearer` header.
- **AUTH2 (MUST)** — `recall` MUST validate the token's **signature (via the issuer's JWKS), issuer,
  audience, and expiry**, and reject any token failing validation.
- **AUTH3 (MUST)** — `recall` MUST support **OIDC discovery** (`.well-known/openid-configuration`) and
  obtain signing keys from the issuer's JWKS endpoint; issuer and audience MUST come from
  configuration, never hardcoded (NFR-SEC5).
- **AUTH4 (MUST)** — `recall` MUST derive the **caller identity from a token claim** (e.g. `sub`) and
  map it to the owning memory scope; it MUST NOT trust a scope/user value supplied in the request
  body or URL (FR-T2).
- **AUTH5 (SHOULD)** — `recall` SHOULD enforce **per-operation authorisation** from token scopes/claims
  (e.g. a `recall:read` vs `recall:write` vs `recall:forget` distinction) and SHOULD honour
  **least-privilege, short-lived tokens** (`agentic-mem.md` §11.2 P3).
- **AUTH6 (MAY)** — `recall` MAY additionally accept **service-to-service tokens** (OIDC client-
  credentials) for the broker's own administrative calls, kept distinct from user-scoped access.
- **AUTH7 (MUST)** — The audit log (NFR-OB1) MUST record the authenticated subject and token id
  (`jti`) — never the token itself (NFR-SEC3).

What remains open (not blocking adoption): the **specific identity provider** the deployment uses and
the **exact claim set** the broker emits (subject format, scope/role claims, audience value).
`recall` should be built IdP-agnostic against the OIDC standard; the concrete values are configuration
and are tracked in §8.

---

## 5. Out of scope (restated for clarity)

- The broker, sandbox, system allowlist, and broker-side audit (`agentic-mem.md` §7.2).
- The calling agent / LLM reasoning.
- Learned (RL) memory control at launch (`good-mem.md` §10) — heuristic control is the requirement.
- Multimodal memory (image/audio) — text/structured facts only at launch.

---

## 6. Traceability

| Area | Functional | Non-functional | Concept source |
|---|---|---|---|
| Hybrid retrieval | FR-R1–R3 | NFR-P1–P3 | `agentic-mem.md` §4, `good-mem.md` §7 |
| Temporal model / supersession | FR-C1, DR1–DR3 | — | `agentic-mem.md` §5.2, `good-mem.md` §5 |
| Provenance & freshness | FR-R4, FR-F1–F2, FR-W3 | NFR-OB1 | `agentic-mem.md` §7.1, §11.2 P6–P7 |
| Forgetting | FR-D1–D4 | NFR-S2 | `good-mem.md` §8 |
| API for agents | FR-A1–A4 | NFR-P5, NFR-OB4 | `agentic-mem.md` §11 |
| Auth (OIDC) | FR-T2 | NFR-SEC1–SEC5, AUTH1–AUTH7 | `agentic-mem.md` §7.2, §11.2 P3 |
| Safety / governance | FR-W7 | NFR-SEC4, NFR-PR1–PR4 | `good-mem.md` §9 |

---

## 7. Acceptance posture

`recall` is "built right" when, against a representative agentic-search workload with facts that
change during a session: task success stays high; no stale or poisoned fact is returned as truth;
retrieval stays within NFR-P2; tokens/query and storage growth stay bounded; every call is
authenticated, authorised, and audited; and a deletion request is verifiably honoured
(`good-mem.md` §13).

---

## 8. Open questions

**Still open:**

- **OQ1 — Identity provider & claims.** Which OIDC IdP, and the exact claim set / audience the
  broker emits (§4). `recall` is IdP-agnostic; values are configuration. (Tracked as FU-011.)
- **OQ3 — Target volumes & SLAs.** Concrete corpus size and tenant counts behind NFR-S1/NFR-P2.
  (Tracked as FU-007.)

**Resolved during design (kept for the audit trail):**

- **OQ2 — Store commitment.** RESOLVED — SurrealDB, a single multi-model store (ADR-003, ADR-009).
- **OQ4 — Tenancy granularity.** RESOLVED — bridge model: namespace-per-tenant hard boundary +
  logical team/user scoping (ADR-011).
- **OQ5 — Freshness loop placement.** RESOLVED — entirely at the agent; `recall` runs none of it
  (ADR-014).
- **OQ6 — Consolidation cadence.** RESOLVED — not applicable; `recall` runs no consolidation, so
  there is no cadence to choose (ADR-015).
