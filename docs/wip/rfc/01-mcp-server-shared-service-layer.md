# MCP server as a second manifestation of one shared service layer

> Lane: **Standard** · Status: **Ready for `/spec`** · Source: chat change request (2026-06-27)
>
> **Decision (2026-06-27): Option A.** The MCP edge runs as a **networked service over MCP
> streamable-HTTP, carrying the same OIDC bearer the REST edge already validates.** This resolves the
> two former blocking questions (OQ-1 identity ingress, OQ-2 transport): the shared service-layer auth
> seam is C3 reused verbatim — identity is a verified `ScopeContext` derived from the bearer, never a
> local/ambient trust model. Rationale: `recall` is a central, multi-tenant, OIDC-authenticated store
> (the per-agent-local component is the broker, not `recall`), so a networked-and-authenticated edge
> fits its nature, reuses all governance unchanged, and makes the two edges genuinely one service
> differing only in wire format. A stdio/local-process edge (Option B) was rejected — it would require
> inventing a new identity model and weaken the multi-tenant security posture; a local stdio shim, if
> ever needed, is the broker's responsibility, not `recall`'s.

## Restated request

Add an **MCP (Model Context Protocol) server** as a **separate binary**, built on a **transport-agnostic
service layer** shared with the existing REST API, so the HTTP/REST deployment and the MCP deployment
are two *manifestations* of the same `recall` service rather than two implementations. The service
behaviour (auth, scoping, operations, idempotency, audit, governance) lives once in the shared layer;
each binary is a thin transport adapter over it.

Interpretation choice: this RFC treats the change as **structural** — extract the orchestration that
currently lives inside the HTTP edge into a reusable service layer, then add MCP as a second edge over
it. It is read as *"one service core, two edges, two binaries"*, **not** *"add MCP handlers alongside
the HTTP handlers in the same binary"* and **not** *"duplicate the edge logic for MCP"*. If a
single-binary-multiple-listeners shape is actually wanted, that is a different change and should be
re-scoped.

## Motivation

Agents in this ecosystem increasingly consume tools over MCP rather than bespoke HTTP. Exposing
`recall` over MCP lets an agent discover and call memory operations through its native tool interface,
with no custom client. The constraint that matters is **not duplicating the service**: the auth,
scope-derivation, rate-limiting, idempotency, audit, and error-classification logic that make `recall`
correct and governable must not be re-implemented per transport — divergence there is a security and
correctness hazard (e.g. an MCP path that forgets to write an audit row, or derives scope differently).
The request's core ask — *"different manifestations of the same service"* — is precisely a demand that
this logic be shared, with the transport as the only thing that differs.

## Current behaviour

`recall` ships a **single binary** today.

- One `[[bin]]` target named `recall` (`Cargo.toml:12`), entry `src/main.rs`, which loads config (X6),
  initialises observability, calls `build_state` (`src/lib.rs`), builds the axum router
  (`src/api/mod.rs:92` `build_router`), and serves.
- The seven external operations are **axum HTTP handlers** (`src/api/mod.rs:97-103`): capabilities
  (`GET /v1`), recall (`POST /v1/recall`), remember (`POST /v1/memories`, async 202), get
  (`GET /v1/memories/:id`), retire (`POST /v1/memories/:id/retire`), delete (`DELETE
  /v1/memories/:id`), and `GET /openapi.json`.
- The per-operation orchestration is **HTTP-coupled**, executed inside each handler via `EdgePipeline`
  (`src/api/v1.rs:45-167`). `EdgePipeline::begin` runs: step 3 authenticate (bearer token →
  `Authenticator::validate` → `ScopeContext`, `src/auth/mod.rs:204`), authorise the op-class
  (`Authenticator::authorise`, `src/auth/mod.rs:252`), step 4 rate-limit per `(subject, op-class)`;
  `finish_success`/`finish_error` call the component, write the audit row (`write_audit`,
  `src/api/v1.rs:153`), and map the result to an HTTP envelope.
- That pipeline **mixes two kinds of concern**: transport-agnostic (auth→`ScopeContext`, authorise,
  rate-limit, call component, audit-write, `AppError`→X1 code via `crate::error::map_error`,
  `src/api/v1.rs:141`) and HTTP-specific (bearer extraction from `HeaderMap` at
  `src/api/v1.rs:181`, JSON (de)serialisation, `Response`/`StatusCode`, `RateLimit-*`/correlation
  headers).
- The request/response DTOs are already **transport-neutral** serde types (`RecallRequest`,
  `RememberRequest`, `RecallResponse`, `RankedFact` — `src/types/api.rs:11,67,40,45`), so the data
  contract is reusable as-is.
- `AppState` (`src/api/mod.rs:57`) already holds every component handle (store, queue, engine, auth,
  rate, metrics, config) and is `Clone`/`Arc`-backed — it is the de-facto service container, but it is
  owned by the `api` module and the orchestration around it lives in the axum handlers.
- **No MCP surface exists** anywhere in `src/`, `docs/`, or `Cargo.toml` (verified: zero matches).
- The self-describing surface today is `GET /v1` (capabilities) plus `GET /openapi.json`
  (`src/api/v1.rs:324,740`).

## Desired behaviour

A future reader can test these outcomes:

1. **Two binaries, one service core.** Building the project produces (at least) two binary targets: the
   existing REST `recall` and a new MCP server. Both construct their component state from the same
   `build_state` path and drive the same shared service layer; neither contains a private copy of the
   auth / scope / rate-limit / idempotency / audit / error-classification logic.
2. **Operation parity.** Every externally-meaningful operation is reachable on both edges with
   identical service semantics: recall, remember (async-accepted), get, retire, delete, and capability
   discovery. The same `ScopeContext` derivation, the same op-class authorisation, the same per-subject
   rate limiting, the same idempotency behaviour, and the same audit record are produced regardless of
   which transport invoked the operation.
3. **Identity is enforced on both edges, the same way.** The MCP edge carries the same OIDC bearer as
   REST (Option A); a call without a valid, verifiable bearer is rejected with the same error class as
   the equivalent unauthenticated HTTP call; scope is never taken from caller-supplied fields (the
   `ScopeContext`-from-verified-identity rule holds on both edges, via C3 reused verbatim).
4. **Discovery parity.** The MCP edge advertises the operation set with machine-readable input schemas
   through MCP's native tool-discovery, the MCP analogue of `GET /v1` + `openapi.json`.
5. **Error parity.** A given service error (X1 code) renders as the transport-appropriate
   representation on each edge — an HTTP status + error envelope on REST, an MCP error/tool-failure on
   MCP — driven by the same `AppError`→code mapping, not a per-edge re-classification.
6. **The REST contract is unchanged.** The existing HTTP behaviour (paths, status codes, envelopes,
   headers) is byte-for-byte preserved; the REST binary is a pure refactor with no observable contract
   change.
7. **Governance invariants preserved.** No new outbound calls and no LLM are introduced (ADR-014 /
   ADR-015 hold — the MCP edge is a transport, not new service behaviour); per-tenant isolation,
   audit, PII, and rate limiting behave identically across edges.

## Scope

- **In scope:**
  - Extracting a transport-agnostic **service layer** that owns per-operation orchestration (auth →
    authorise → rate-limit → component call → audit → error-classification) returning transport-neutral
    results.
  - Refactoring the HTTP edge so its handlers become thin adapters over that layer (no behaviour
    change).
  - A new **MCP server binary** that adapts the same service layer to the MCP transport, including tool
    discovery and the MCP-side identity ingress and error rendering.
  - Wiring both binaries to the shared `build_state`, configuration (X6), and observability seams
    (X3/X5).
- **Out of scope:**
  - Changing any REST request/response contract or adding new memory operations (transport addition
    only).
  - New service capabilities, new model inferences, or any outbound call (ADR-014/015 unchanged).
  - The store, queue, retrieval, write-pipeline, and maintenance component **internals** — they are
    consumed unchanged; only the edge/orchestration layer moves.
  - Schema or migrations (no DDL — this change touches no `*.sql`).
  - Packaging/distribution of the second binary (container images, release artefacts) — a follow-up.

## Acceptance criteria

1. `cargo build` produces two binary targets — the existing `recall` (REST) and a new MCP server —
   and a test (or `cargo run`) confirms each starts from the same `build_state` construction path.
2. A shared-service-layer test exercises each operation (recall, remember, get, retire, delete) through
   the service layer **with no HTTP/axum types in scope**, asserting the auth → authorise → rate-limit
   → component → audit chain runs for each.
3. **Identity parity:** an MCP invocation lacking valid identity and an HTTP request lacking a valid
   bearer both yield the same X1 error code (the unauthenticated class); a parity test asserts this.
4. **Audit parity:** an operation invoked via the MCP edge writes an `audit_log` row whose fields
   (subject, operation, scope, outcome, `token_jti`, `correlation_id`) match those written by the
   equivalent HTTP operation; asserted by a test reading the row back.
5. **Error parity:** a table-driven test maps representative `AppError` values to the rendered result on
   each edge and asserts both derive from the same X1 code (HTTP status on one side, MCP error on the
   other).
6. **Discovery:** the MCP edge's tool-list response enumerates the operation set with input schemas; a
   test asserts the tool names/inputs correspond to the REST operations and their DTOs.
7. **REST regression:** the full existing BDD suite (`cargo test --test bdd`) passes **unmodified**,
   proving the HTTP contract is unchanged by the refactor.
8. **Idempotency parity:** a replayed write over the MCP edge with the same idempotency key returns the
   original result and creates no duplicate (mirror of FR-W4), asserted by a test.
9. **Governance regression:** `grep` over the new MCP binary and the service layer confirms no outbound
   broker/source call and no LLM client is introduced (ADR-014/015 preserved); a cross-tenant
   isolation test passes when the operation is driven via the service layer.
10. **No duplicated orchestration:** the auth, scope-derivation, rate-limit, idempotency, and
    audit-write logic each have a single definition consumed by both edges (verified by inspection —
    no second copy under the MCP edge).

## Constraints

- **REST backward compatibility is absolute.** No path, status code, envelope, header, or error code on
  the existing HTTP surface may change (`docs/spec/components/http-api-edge.md`,
  `docs/design/agentic-memory/08-interfaces.md`).
- **Single source of truth for orchestration.** Auth (C3), scope derivation, rate limiting,
  idempotency, and audit (SA-AUDIT-01) must each exist once in the shared layer; a per-edge copy is a
  defect, not an option — that duplication is the exact thing the request forbids.
- **Cross-cutting policies hold on both edges:** the X1 error registry, X3 structured logging with
  correlation IDs, X5 tracing, OIDC identity (C3), per-tenant isolation (ADR-011), and rate limiting
  apply uniformly.
- **ADR-014 / ADR-015 preserved:** the MCP edge adds a transport only — no outbound calls, no LLM.
- **Identity is OIDC-bearer on both edges (Option A).** The MCP edge runs over streamable-HTTP and
  carries the same `Authorization: Bearer <OIDC JWT>` the broker injects for REST; C3
  (`Authenticator::validate`) is reused unchanged. No local/ambient/stdio identity model is
  introduced. This keeps AUTH1–AUTH7 and per-tenant isolation (ADR-011) identical across edges.
- **Interacts with ADR-009 (single embedded binary).** This introduces a *second* binary; both should
  retain the embedded-and-remote store options ADR-009/FU-009 provide. The "single binary" framing of
  ADR-009 is about deployment simplicity of the store, not a prohibition on a second edge binary — but
  the design should state this explicitly so the two ADRs do not appear to conflict.
- **Configuration unity (X6).** Both binaries read the same configuration model; transport-specific
  settings (e.g. the MCP listener/transport) are additive, never a forked config.

## Proposed direction (non-prescriptive)

The natural shape is to lift the transport-agnostic half of `EdgePipeline` into a service layer keyed on
a verified `ScopeContext` and transport-neutral request/response DTOs (which already exist), returning a
typed success or an `AppError`; the HTTP edge then becomes a thin adapter (header/JSON/status mapping)
and the MCP edge a second thin adapter (tool dispatch / MCP error mapping) over the same layer, each in
its own binary built from the shared `build_state`. With Option A settled, identity reaches the service
layer the same way on both edges — a bearer extracted at the transport boundary and handed to C3, which
returns the `ScopeContext`; the MCP edge runs over streamable-HTTP so the bearer travels in the
`Authorization` header exactly as for REST. The exact module/crate boundary and the MCP library remain
design decisions for `/spec` and `/breakdown`.

## Open questions

1. **(RESOLVED — Option A) How does the MCP edge authenticate and derive a `ScopeContext`?** The MCP
   edge carries the same OIDC bearer as REST; C3 (`Authenticator::validate`, `src/auth/mod.rs:204`) is
   reused verbatim. Rejected: stdio/handshake token (b) and local-broker ambient trust (c) — both would
   require a new identity model and weaken the multi-tenant posture.
2. **(RESOLVED — Option A) Which MCP transport(s)?** Streamable-HTTP, networked — the same shape as the
   REST service. stdio is not built; if a true local-tool use-case appears later, a local shim
   forwarding to the networked service is the broker's responsibility, not `recall`'s.
3. **(non-blocking) Which operations are exposed as MCP tools, and is destructive `delete` among
   them?** Default assumption: mirror all five operations plus discovery. Hard `delete` (verifiable
   erasure) may warrant exclusion or a stronger confirmation on a local tool surface; decide during
   `/spec`.
4. **(non-blocking) MCP implementation: a Rust MCP SDK vs a hand-rolled JSON-RPC layer.** Affects
   dependencies and the second binary's build. Proposed as a constraint to settle in `/spec`, not an
   architecture-blocker.
5. **(non-blocking) How is `remember`'s async-accepted (202) semantics represented as an MCP tool
   result?** Proposed default: the tool returns the same idempotent acknowledgement the HTTP 202 body
   carries. Confirm in `/spec`.
6. **(non-blocking) Naming and packaging of the second binary** (target name, container image). Cosmetic
   for `/breakdown`; defer.

## Blocking decisions outstanding (layman language)

None — both former blocking questions are resolved by the Option A decision recorded at the top:

- **How does the new MCP server know who is calling?** Resolved — it runs as a web-style service and
  reuses the existing signed-login check exactly, so there is no new way to prove identity to design.
- **Local pipe or network?** Resolved — over the network, like the REST API; it stays a shared,
  governed service rather than a private per-user gadget.

## Handoff

This is an **intent-changing** RFC (a new external interface and a structural service-layer extraction),
so the component spec(s) must be updated before code is patched, per *fix the prompt first*. The
blocking questions are resolved (Option A), so it is ready to proceed.

Next move: `/spec docs/wip/rfc/01-mcp-server-shared-service-layer.md` — introduce a service-layer
Component spec and an MCP-edge Component spec, and reconcile `docs/spec/components/http-api-edge.md` to
the "thin adapter over the shared layer" shape. After `/spec` exits clean (Phase 6 audit passed), run
`/breakdown` against the updated specs, then `/apply`, then `/sync-check`.
