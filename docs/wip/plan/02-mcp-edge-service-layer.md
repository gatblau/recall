# Plan — MCP edge over a shared service layer (ADR-016)

> **Move:** CSPA 3 (Plan increments) · **Input:** `docs/spec/phase-5-playbook.md` Steps 6/6b (Approved, Phase 6 PASS) · Specs: `components/service-layer.md` (C9), `components/mcp-api-edge.md` (C10), `components/http-api-edge.md` (C8, reconciled) · **Authored:** 2026-06-27

## Anchor (from input)

Implement ADR-016 against the existing `recall` codebase: **extract the transport-agnostic orchestration** currently fused into the C8 axum handlers (`EdgePipeline`) into a **Service Layer (C9, `src/service`)**, refactor **C8 to a thin HTTP adapter** over it (REST contract byte-for-byte unchanged), and add a **second binary `recall-mcp` (C10, `src/mcp`)** exposing the same operations as MCP tools over streamable-HTTP, carrying the same OIDC bearer (Option A). Evidence: Phase-5 Steps 6 (C9 → C8 refactor) and 6b (C10), the three component specs, ADR-016 in `09-decisions.md`.

## Verified context

- **Orchestration to extract:** `EdgePipeline` `src/api/v1.rs:45` (`begin` `:57` = auth→authorise→rate; `finish_success` `:117`; `finish_error` `:139`; `write_audit` `:153`); handlers call it — `recall` `:369`, `remember` `:422`, `get_memory` `:538`, `retire_memory` `:627`, `delete_memory` `:690`, `capabilities` `:324`, `openapi_doc` `:740`.
- **State + wiring:** `AppState` `src/api/mod.rs:57` (holds config, metrics, `Arc<Store>`, `Arc<StoreWorkQueue>`, `Arc<RetrievalEngine>`, `Arc<Authenticator>`, `RateLimiter`); `build_router` `src/api/mod.rs:92`; `build_state` `src/lib.rs:47`; `serve`/`serve_on_listener` `src/lib.rs:102,:112`; REST entry `src/main.rs`.
- **Cargo:** lib `recall` (`src/lib.rs`), bin `recall` (`Cargo.toml:12`, `src/main.rs`), test bin `bdd` (`Cargo.toml:41`). `axum = "0.7"`, `tokio` full.
- **Test harness (materialised convention — ADR-010):** cucumber-rs over `tests/features/*.feature` driven by `tests/bdd.rs`; the C8 edge is served **in-process** via `build_router(AppState)` on an ephemeral port and exercised through a `reqwest` client at `base_url` (`tests/bdd.rs:108-112`); providers are **wiremock** stubs; identity is a **local RS256 issuer** (`tests/support/issuer.rs`) plus a **Dex testcontainer** (`tests/support/dex.rs`, `tests/bdd.rs:1252+`); embedded **in-memory SurrealDB** is the store. `api_edge.feature` + `system.feature` cover the edge.

## Open assumptions

- **OQ-LIB (non-blocking):** the exact Rust MCP **server** crate is chosen at Phase 2. Requirement: it serves **MCP streamable-HTTP** (not just stdio), registers tools with JSON-Schema inputs, and composes with `tokio`/`axum` (so it can be served on an ephemeral port in-process for tests). The implementer pins a maintained crate then; if none meets the bar, fall back to a hand-rolled JSON-RPC-over-HTTP handler honouring the MCP `initialize`/`tools/list`/`tools/call` shapes. **Not blocking** — it does not change the service or the C9 boundary.
- **OQ-NAME (non-blocking):** second binary named `recall-mcp` (assumed; cosmetic).
- **Tool-input JSON Schemas** are generated from the 2C request types (`schemars` or equivalent derive); the exact derive crate is a Phase-2 implementation choice. Non-blocking.
- No blocking assumptions — every code target above is verified in the repo.

## Scope

- **In scope:** new `src/service` (C9) owning auth→authorise→rate→idempotency→component→audit→error-classification; refactor of `src/api` (C8) to a thin adapter over C9 with an unchanged external contract; new `src/mcp` + `[[bin]] recall-mcp` (C10) + the MCP library dep + `RECALL_MCP_HTTP_ADDR`/`RECALL_MCP_PATH` config; MCP integration + REST↔MCP parity tests.
- **Out of scope:** the component internals (C1 store, C2 queue, C3 auth, C6 retrieval) — consumed unchanged; the REST request/response contract (no field, path, status, or code changes); schema/migrations (no DDL); the eval harness (plan 01); packaging/release of the second binary (FU below).

## Phases

### Phase 1 — Extract the Service Layer (C9) and make C8 a thin adapter (behaviour-preserving)
- **Goal:** the per-operation orchestration lives once in `src/service`, and the existing REST behaviour is provably unchanged.
- **Changes:**
  - New `src/service/mod.rs` — a `Service` struct holding the component handles (the orchestration half of today's `AppState`), with `CallContext`/`CallResult`/`RateSnapshot` types and the six methods (`capabilities`, `recall`, `remember`, `get_fact`, `retire`, `delete`) per `components/service-layer.md`. Move the body of `EdgePipeline::{begin,finish_success,finish_error,write_audit}` (`src/api/v1.rs:45-167`) and `decrement_rate` into the `Service` `run` helper; the validate→authorise→rate→idempotency→component→audit sequence is preserved exactly.
  - `src/api/mod.rs` — `AppState` carries an `Arc<Service>` (or is reduced to it plus the HTTP-only bits: the rate snapshot is now returned by C9). `build_state` (`src/lib.rs:47`) constructs the `Service` and the `AppState` around it; `build_router`/`serve` signatures unchanged.
  - `src/api/v1.rs` — each handler becomes thin: assign correlation id, enforce body-size, extract the bearer, build `CallContext`, call the matching `Service` method, render `CallResult`/`AppError` to the existing `Success`/`ErrorEnvelope` + `RateLimit-*`/`ETag` headers + status. Conditional-GET (ETag/`If-Modified-Since`) stays in the handler.
  - No change to `tests/features/*.feature` or `tests/bdd.rs` (that immutability is the gate).
- **Rationale:** C10 cannot exist without C9; doing the extraction first, gated by the unchanged BDD suite, de-risks the second edge — any behaviour change shows up as a REST regression before MCP is added.
- **Exit criteria:**
  - Build/typecheck: `cargo build` and `cargo clippy --all-targets -- -D warnings` clean.
  - **Integration (the regression gate):** `cargo test --test bdd` passes **with `tests/features/` and `tests/bdd.rs` unmodified** — the full edge suite (`api_edge.feature`, `system.feature`, `auth_scope.feature`, …) green against the in-process `build_router(AppState)` server, **wiremock** providers, the **local RS256 issuer**, the **Dex testcontainer** (`dexidp/dex` pinned tag from `tests/support/dex.rs`), and in-memory SurrealDB. Unmodified-suite-passing is the proof of behaviour preservation.
  - Add C9 unit tests `src/service/` (`#[cfg(test)]`) asserting the chain runs per operation **with no `axum` types in scope** (table-driven over the six ops; auth/rate/idempotency/audit observed via the in-memory store) — supportive, not the gate.
  - Behavioural check: a remember→(drained)→recall round-trip and an unauth `POST /v1/recall`→`401 AUTH_MISSING_TOKEN`-with-no-audit scenario (already in the suite) pass unchanged.
- **Risks / rollback:** risk = a subtle ordering/behaviour change in the move (e.g. audit-on-error vs audit-on-success, rate-header attachment). Mitigation = the unmodified BDD suite is the tripwire. Rollback = revert the Phase-1 commit; `src/service` is additive and `src/api` returns to owning the pipeline.

### Phase 2 — Add the MCP edge binary (C10) over C9
- **Goal:** `recall-mcp` serves the six operations as MCP tools over streamable-HTTP, driving the same `Service`.
- **Changes:**
  - `Cargo.toml` — add `[[bin]] name = "recall-mcp", path = "src/mcp/main.rs"`; add the chosen MCP server crate (OQ-LIB) and the JSON-Schema derive crate.
  - `src/config.rs` — add `RECALL_MCP_HTTP_ADDR` (default `0.0.0.0:8081`) and `RECALL_MCP_PATH` (default `/mcp`) to `Config` (both `no`-required); shared `Config`, read by both binaries (SA-BIN-01).
  - New `src/mcp/` — `main.rs` (load `Config`, init obs, `build_state`, construct `Service`, serve MCP on the listener) and `tools.rs` (register `recall`/`remember`/`get`/`retire`/`delete`/`capabilities` + `tools/list`; per `tools/call`: body-size guard, bearer from `Authorization`, mint correlation id, deserialise args into the 2C type, build `CallContext`, call `Service`, map `CallResult`→tool result / `AppError`→MCP error with the registry `code`). Input schemas generated from the 2C request types.
- **Rationale:** depends only on C9 (Phase 1); additive (a new binary + new config keys + a new module) — it cannot regress the REST edge.
- **Exit criteria:**
  - Build: `cargo build` produces **both** `recall` and `recall-mcp`; `cargo clippy --all-targets -- -D warnings` clean.
  - **Integration:** a new MCP suite — either `tests/features/mcp_edge.feature` driven from `tests/bdd.rs` or a dedicated `tests/mcp.rs` integration binary — serves `recall-mcp` **in-process on an ephemeral port** (mirroring the C8 harness) over the same in-memory SurrealDB + **wiremock** providers + **local RS256 issuer**, and a streamable-HTTP MCP client asserts: `tools/list` returns the six tools with input schemas; a valid-bearer `recall` tool call returns ranked facts; a `remember` tool call enqueues and acks; a no-bearer call returns an MCP error `AUTH_MISSING_TOKEN`. Command: `cargo test --test bdd -- mcp` (or `cargo test --test mcp`).
  - Existing `cargo test --test bdd` (REST suite) still green (no regression).
  - Behavioural check: `recall-mcp` boots with the embedded store, `tools/list` answers, and a remember→recall over MCP round-trips.
- **Risks / rollback:** risk = the chosen MCP crate cannot serve streamable-HTTP in-process for tests (RISK-002). Mitigation = the hand-rolled JSON-RPC-over-HTTP fallback in Open assumptions. Rollback = drop the `[[bin]]` + `src/mcp` + the two config keys; C9 and C8 are untouched.

### Phase 3 — REST↔MCP parity + governance test suite
- **Goal:** prove the two edges are the same service — identical identity, error codes, audit, idempotency, and tenant isolation — not merely two surfaces.
- **Changes:**
  - Extend the MCP suite (Phase 2) with parity scenarios driven through **both** edges against the same store: (a) **error-code parity** — the same bad input yields the same registry `code` on REST (status) and MCP (error); (b) **audit parity** — an MCP op writes an `audit_log` row with the same fields as the REST op; (c) **idempotency parity** — a replayed MCP `remember`/`retire`/`delete` returns the original result, no new side effect; (d) **identity parity** — no/invalid bearer rejected identically, no audit written; (e) **cross-tenant isolation** over MCP (NFR-PR1) as a first-class case.
  - No production-code change expected; any parity failure routes back to Phase 1/2 as a fix.
- **Rationale:** the parity guarantees are the whole point of ADR-016 (one core, two manifestations); they are asserted last, once both edges exist, against the shared store.
- **Exit criteria:**
  - **Integration:** `cargo test --test bdd` (full REST + MCP + parity suites) green with all integrating services up (in-memory SurrealDB, wiremock providers, local RS256 issuer, Dex testcontainer).
  - Behavioural check: a table-driven parity test enumerates representative `AppError` inputs and asserts REST-status-code and MCP-error-code derive from the same registry `code`.
  - `grep -rE "EdgePipeline|finish_success|finish_error" src/api/ src/mcp/` shows the orchestration is **not** duplicated in either edge (single definition in `src/service`).
- **Risks / rollback:** risk = a latent divergence surfaces (e.g. MCP write missing the idempotency key requirement). Mitigation = the parity suite is exactly the detector. Rollback = parity tests are test-only; a failure is fixed in Phase 1/2 code, not rolled back.

## Cross-cutting validation

Final black-box gate (the gate `/sync-check` relies on): `cargo build` (both binaries) + `cargo clippy --all-targets -- -D warnings` clean; full `cargo test --test bdd` (REST + MCP + parity) + `cargo test` (lib units incl. C9) green with embedded in-memory SurrealDB, wiremock embedding/reranker, the local RS256 issuer, and the Dex testcontainer all up; coverage ≥ 70% (C3 rule); `secscan` clean; `cargo audit` clean (per `.cargo/audit.toml`). Then `/sync-check` to confirm Design ↔ Spec ↔ Code alignment (HLD 0.7.0, ADR-016; C8/C9/C10 specs).

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Package and release the recall-mcp binary (container image, release artefact)
  why: ADR-016 ships a second binary; building it is in scope, distributing it (image, compose/helm wiring, release notes) is a deployment concern out of this plan.
  source: planning
  suggested-command: /breakdown FU-001 from docs/wip/plan/02-mcp-edge-service-layer.md
  status: open
  added: 2026-06-27
- id: FU-002
  title: Pin the production MCP client/transport configuration (OQ-LIB outcome) in deployment docs
  why: once the MCP crate is chosen at Phase 2, the streamable-HTTP endpoint, auth header, and client config want a short operator note alongside the REST one.
  source: planning
  suggested-command: a docs note after Phase 2 settles OQ-LIB
  status: open
  added: 2026-06-27
- id: FU-003
  title: Sweep deployment manifests / .env examples for the new RECALL_MCP_* keys
  why: two additive config keys; prior sweeps found no in-repo manifests, re-confirm for any out-of-repo deployment.
  source: planning
  suggested-command: grep over deployment files; none expected in-repo
  status: open
  added: 2026-06-27
- id: FU-004
  title: Reconcile the remember non-object-content rejection (spec says reject; code does not)
  why: surfaced in apply-phase-1. The C9 spec (components/service-layer.md Internal Logic step 5) says reject a non-object `remember.content` with VAL_INVALID_BODY, but the original C8 handler never enforced this, so the extraction omitted it to stay strictly behaviour-preserving. Either add the check (+ a test) in a later phase or reconcile the spec to match the as-built behaviour (a non-object content currently enqueues). A latent CONTRACT-DRIFT that /sync-check will flag.
  source: apply-phase-1
  suggested-command: /sync-check then /apply or /sync-spec for components/service-layer.md
  status: open
  added: 2026-06-27
- id: FU-005
  title: Confirm the body-bytes signature of Service::{recall,remember} fits the MCP edge cleanly
  why: surfaced in apply-phase-1. To preserve the original auth-before-parse ordering, Service::recall/remember take the raw body `&[u8]` and deserialise internally (rather than the spec's typed `req` param). The MCP edge (Phase 2) gets tool args as JSON; it must serialise them to bytes to call these methods. Works (auth-before-parse preserved) but verify it reads cleanly when C10 lands; reconcile the C9 spec signature note if needed.
  source: apply-phase-1
  suggested-command: revisit during /apply phase 2
  status: open
  added: 2026-06-27
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: The C8 extraction subtly changes REST behaviour
  why: moving auth/rate/idempotency/audit/error-ordering out of the handlers could shift an edge case (e.g. audit-on-error best-effort vs blocking, rate-header attachment on a 429) that the contract depends on.
  source: planning
  likelihood: medium
  impact: high
  mitigation: the existing BDD suite must pass UNMODIFIED (Phase 1 gate) — it is the behaviour-preservation tripwire; revert the Phase-1 commit if it breaks.
  status: open
  added: 2026-06-27
- id: RISK-002
  title: No maintained Rust MCP crate serves streamable-HTTP in-process for tests
  why: the MCP server-side Rust ecosystem is young; a stdio-only or non-embeddable crate would block the in-process integration harness pattern the repo relies on.
  source: planning
  likelihood: medium
  impact: medium
  mitigation: fall back to a hand-rolled JSON-RPC-over-HTTP handler honouring initialize/tools-list/tools-call (Open assumptions, OQ-LIB); the C9 boundary is unaffected either way.
  status: open
  added: 2026-06-27
- id: RISK-003
  title: MCP error rendering loses the registry code or leaks internal detail
  why: MCP error objects differ from HTTP envelopes; a naive mapping could drop the SCREAMING_SNAKE_CASE code or include a development-mode detail string in production.
  source: planning
  likelihood: low
  impact: medium
  mitigation: Phase 3 error-code-parity test asserts the same registry code on both edges; RECALL_ENV gates detail off in production (carried from X1).
  status: open
  added: 2026-06-27
```
