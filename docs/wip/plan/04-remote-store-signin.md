# Plan — Remote SurrealDB signin for secured stores (FU-019)

> **Move:** CSPA 3 (Plan increments) · **Input:** FU-019 of `docs/wip/backlog.md` (the entry migrated there when the greenfield plan was retired) · **Authored:** 2026-06-28
> **Driver:** plan 03 phase 2 shipped a compose stack against an *unauthenticated* SurrealDB precisely because of this gap; FU-019 closes it so a *secured* shared store is usable.

## Anchor (from input)

FU-019: *"Wire remote SurrealDB credentials (signin) for secured endpoints."* `Store::connect` opens a remote `ws(s)://`/`http(s)://` endpoint via `engine::any::connect` but never calls `signin(Root{…})`, and `Config` has no remote-credential fields — so a **secured** SurrealDB server rejects recall's DDL/DML (a no-auth server works today). Add `RECALL_STORE_REMOTE_USER` / `RECALL_STORE_REMOTE_PASS` config and a `signin` after `connect` when both are set; the per-operation `use_ns`/`use_db` already selects the tenant namespace, so a root signin at connect is sufficient.

## Verified context

- **The gap:** `Store::connect` (`src/store/mod.rs:48`) builds the endpoint and calls `surrealdb::engine::any::connect(endpoint)` (`:62`) — **no `signin`**. `new_in_memory` (`:73`) is unaffected.
- **Namespace selection already exists:** every store method calls `ensure_and_use(tenant)` → `use_ns(tenant).use_db(DB_NAME)` (`src/store/mod.rs:98-99,107`), so a single root signin at connect grants the session; per-op `use_ns` does the rest. No per-op auth change needed.
- **Config has the pieces:** `Secret(String)` with `.expose()` (never logged) at `src/config.rs:70-95`; `store_remote_url: Option<String>` (`:105`); secrets loaded via `Secret::new(src.required(...))` (`:238`); optional values via `src.get(...)` (`:253`).
- **Authenticated-server test pattern exists:** `tests/store_remote.rs` starts `surrealdb/surrealdb:v3.1.5` with `start --user root --pass root … memory` and uses `surrealdb::opt::auth::Root` + `db.signin(Root{…})` — the exact server + auth shape this plan exercises through `Store::connect`.
- **Spec surface to keep aligned:** the §2D env table (`docs/spec/phase-2-architecture.md:544`) and the C1 spec (`docs/spec/components/memory-store.md:206` env table, `:401-404` connect step) describe `RECALL_STORE_REMOTE_URL`; the two new keys belong alongside.

## Open assumptions

- **Root scope is the right auth level (non-blocking).** recall selects the namespace/database per operation (`ensure_and_use`), so it needs a credential with rights across the tenant namespaces — `Root` matches the test server and the single-tenant-server deployment model. A narrower namespace/database-scoped signin is a later refinement if a deployment restricts recall to one namespace (recorded as a follow-up). Override if a deployment mandates NS/DB-scoped creds.
- **Both-or-neither credential semantics (non-blocking, decided here):** signin fires only when **both** user and pass are set; if only one is set, startup fails fast with a clear config error (a half-set credential is a misconfiguration, not a silent no-auth connect).
- **No blocking assumptions** — every target is verified in the repo.

## Scope

- **In scope:** the two `Config` fields + their load; the `signin(Root)` in `Store::connect` when set; the spec reconciliation (§2D + C1 env tables and the C1 connect step); an integration test against a secured SurrealDB; and the compose/runbook update to support a secured store.
- **Out of scope:** namespace/database-scoped (non-root) signin (follow-up); credential rotation/refresh (SurrealDB sessions don't expire mid-connection for recall's use); securing the *queue* backend (it shares the store handle, so it inherits the store's auth); TLS to the store (`wss://`/`https://` already works via `engine::any` — no code change, a deployment config).

## Phases

### Phase 1 — Config credentials + signin in Store::connect (+ spec reconciliation)
- **Goal:** recall authenticates to a secured remote SurrealDB; the embedded default and no-auth remote are unchanged.
- **Changes:**
  - `src/config.rs` — add `pub store_remote_user: Option<String>` and `pub store_remote_pass: Option<Secret>` to `Config`; in `load()`, read `RECALL_STORE_REMOTE_USER` (`src.get`) and `RECALL_STORE_REMOTE_PASS` (`src.get` → `Secret::new`); validate the both-or-neither rule (exactly one set → `ConfigError`). Add the two fields to the struct build and the test-config builders.
  - `src/store/mod.rs` `connect` — after `engine::any::connect(endpoint)` succeeds, when the endpoint is remote **and** both creds are set, call `db.signin(Root { username: user, password: pass.expose() }).await` (import `surrealdb::opt::auth::Root`), mapping a signin failure to `StoreError::Unavailable`. Embedded (`surrealkv://`) and unset-cred remote paths skip signin. Never log the password.
  - `docs/spec/phase-2-architecture.md` §2D + `docs/spec/components/memory-store.md` — add the two env vars (`Type: string`/`secret`, `Default: (unset)`, `Required: no`, `Owner: C1`) and a one-line note in the C1 connect step that a signin fires when both are set (keeps spec↔code aligned so `/sync-check` stays clean).
- **Rationale:** the credential plumbing + signin is the FU-019 core; doing the spec edit in the same phase keeps the contract aligned (fix the prompt alongside the code for a small additive change).
- **Exit criteria:**
  - Build/lint: `cargo build` + `cargo clippy --all-targets -- -D warnings` clean.
  - **Integration (testcontainers-rs):** extend `tests/store_remote.rs` (the existing secured-server harness) with: (a) a **positive** test — start `surrealdb/surrealdb:v3.1.5` with `--user root --pass root … memory`, set `RECALL_STORE_REMOTE_URL` + `RECALL_STORE_REMOTE_USER=root` + `RECALL_STORE_REMOTE_PASS=root`, call `Store::connect`, and assert a `put_fact` → `get_fact` round-trip succeeds (proving the signin happened); (b) a **negative** test — `Store::connect` against the same secured server with **no** creds fails a write with an auth error (proving the server really is secured and signin is required). Command: `cargo test --test store_remote`. Real service: `surrealdb/surrealdb:v3.1.5` via testcontainers-rs (the repo's existing pin).
  - Behavioural check: the positive round-trip persists and reads back a fact through a credentialed connection; the negative path errors without panicking.
- **Risks / rollback:** risk = a wrong `Root` scope or a SurrealDB API shape change breaks connect for *all* remote deployments. Mitigation = signin is gated on both-creds-set, so the unset path (embedded + no-auth remote) is byte-for-byte unchanged; the positive/negative tests cover the credentialed path. Rollback = revert the `connect` hunk + the two config fields; spec edit reverts with it.

### Phase 2 — Wire the secured store into deployment (compose + runbook)
- **Goal:** the compose stack can run against a secured SurrealDB, and the docs drop the "unauthenticated only" caveat.
- **Changes:**
  - `docker-compose.yml` — switch the `surrealdb` service to an **authenticated** start (`start --user root --pass root --bind 0.0.0.0:8000 memory`) and add `RECALL_STORE_REMOTE_USER`/`RECALL_STORE_REMOTE_PASS` to both edge services (sourced from compose env / `.env`, not literals). Keep a commented note that production supplies real creds + `wss://`.
  - `docs/runbooks/recall-containers.md` — replace the FU-019 caveat with the now-supported secured-store instructions (the two env vars, the authenticated `surreal start`); note FU-019 is closed.
- **Rationale:** completes the story plan 03 deferred — depends only on Phase 1's code, and makes the local stack production-shaped.
- **Exit criteria:**
  - Build/validate: `docker compose config` exits 0.
  - **Integration smoke:** `docker compose up -d`, then `curl -fsS localhost:8080/healthz` → `status: live` and a `tools/list` POST to `localhost:8081/mcp` → six tools, with the **authenticated** SurrealDB (proving recall signed in); then `docker compose down -v`. Real services: the authenticated `surrealdb/surrealdb:v3.1.5` + Dex + both `recall:ci` edges.
  - Behavioural check: both edges boot and log `store.connect backend=remote` against the secured server (a no-signin build would fail the round-trip here).
- **Risks / rollback:** risk = the compose creds drift from what recall sends, failing boot. Mitigation = the same `root`/`root` pair in the surreal start and the edge env; the smoke catches a mismatch. Rollback = revert the compose/runbook to the unauthenticated Phase-03 form.

## Cross-cutting validation

Final gate: `cargo build` + `cargo clippy --all-targets -- -D warnings` clean; `cargo test --test store_remote` green (secured round-trip + negative) with `surrealdb/surrealdb:v3.1.5` via testcontainers; the full suite (`cargo test --test bdd`, `--test mcp`) still green (the credential path is additive, embedded default unchanged); `docker compose config` valid and a secured-store `up`/probe/`down` smoke passes. Then `/sync-check` to confirm the two new env vars are reconciled across §2D, the C1 spec, and the code.

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Namespace/database-scoped (non-root) signin for least-privilege deployments
  why: this plan signs in as Root (matches the single-server model); a deployment that restricts recall to one namespace would want a NS/DB-scoped credential instead.
  source: planning
  suggested-command: /breakdown FU-001 from docs/wip/plan/04-remote-store-signin.md
  status: open
  added: 2026-06-28
- id: FU-002
  title: Document wss:// (TLS) to the remote store in the runbook
  why: engine::any already supports wss://; a production secured store should use TLS, worth an explicit runbook line (no code change).
  source: planning
  suggested-command: edit docs/runbooks/recall-containers.md
  status: open
  added: 2026-06-28
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: A half-set credential pair silently degrades to a no-auth connect
  why: if only USER or only PASS is set and the code skipped signin, recall would connect unauthenticated to a server the operator believed secured.
  source: planning
  likelihood: low
  impact: high
  mitigation: Phase 1 validates both-or-neither at config load — exactly one set is a fail-fast ConfigError, never a silent no-auth connect.
  status: mitigated
  added: 2026-06-28
- id: RISK-002
  title: The store password leaks into a log or error
  why: a credential mishandled in a tracing field or an error string would expose it.
  source: planning
  likelihood: low
  impact: high
  mitigation: the password is a `Secret` (Debug/Display redacted); only `.expose()` at the signin call reveals it; secscan + a grep for the password in logs is part of the phase gate.
  status: mitigated
  added: 2026-06-28
```
