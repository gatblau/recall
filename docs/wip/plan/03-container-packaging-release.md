# Plan — Container packaging & GHCR release (FU-001 from plan 02)

> **Move:** CSPA 3 (Plan increments) · **Input:** FU-001 of `docs/wip/plan/02-mcp-edge-service-layer.md` (ADR-016 ships a second binary; package + release it) · **Authored:** 2026-06-28
> **Decisions (from /breakdown):** one image carrying both binaries (deploy command selects `recall` | `recall-mcp`) · publish to **GHCR `ghcr.io/gatblau/recall`** · smoke-test via **testcontainers-rs**.

## Anchor (from input)

FU-001: *"Package and release the recall-mcp binary (container image, release artefact)."* The repo currently has **no packaging at all** — no Dockerfile, `.dockerignore`, compose/helm, CI, or Makefile — and `recall-mcp` builds from the same crate as `recall`. This plan establishes a single multi-stage container image carrying **both** binaries (the deployment runs `recall` or `recall-mcp` as the container command), local compose wiring, and a GHCR release workflow.

## Verified context

- **Two bin targets, one crate:** `recall` (`Cargo.toml:12`, `src/main.rs`) and `recall-mcp` (`Cargo.toml:16`, `src/mcp/main.rs`); `edition = "2021"` (`Cargo.toml:4`); no MSRV pin (built with cargo 1.96).
- **Boot dependency:** `build_state` constructs the C3 `Authenticator`, which performs OIDC discovery + the first JWKS fetch and **fails fast if the IdP is unreachable** (`src/lib.rs:42,82-84`). Both binaries therefore need a reachable OIDC issuer to boot. The embedding/reranker provider clients are **lazy** (HTTP only on use), so dummy provider URLs are sufficient for a boot/discovery smoke probe.
- **Unauthenticated probes available once booted:** `recall` serves `GET /healthz` with no downstream call (`src/api/health.rs:16`); `recall-mcp` answers `initialize` / `tools/list` as pure MCP discovery with no Service call (no bearer, no provider).
- **Smoke OIDC source:** the BDD harness pins a real `dexidp/dex` image — `DEX_IMAGE = "dexidp/dex"` + `DEX_TAG` (`tests/support/dex.rs:21,85`); the container smoke reuses the same image/tag via testcontainers-rs (already a dev-dependency, `Cargo.toml` `testcontainers = "0.23"`).
- **No packaging files exist** (verified: no `Dockerfile*`, `.dockerignore`, `.github/workflows/`, `docker-compose*.yml`, `Makefile`) — every file below is **new**.

## Open assumptions

- **Builder + runtime base images (non-blocking):** builder `rust:1-bookworm` (or the pinned toolchain), runtime `debian:bookworm-slim` (glibc present for the rustls/aws-lc-rs build; distroless is an alternative). Exact tags pinned at Phase 1. Override if a distroless/alpine target is preferred.
- **Store path in-container (non-blocking):** the embedded SurrealKV store writes to `RECALL_STORE_PATH`; the image sets a default under a writable, non-root-owned dir (e.g. `/var/lib/recall`). A real deployment mounts a volume there.
- **GHCR auth (non-blocking):** the release workflow authenticates to GHCR with the workflow's `GITHUB_TOKEN` (native, no extra secret) and `packages: write` permission.
- **Provider + OIDC config at deploy time (non-blocking):** real embedding/reranker endpoints and the production OIDC issuer are deployment config (the compose file ships Dex for local OIDC and documents the provider URLs as required env).
- **No blocking assumptions** — every code target above is verified; the image build is the only unverified artefact and Phase 1 builds it.

## Scope

- **In scope:** a multi-stage `Dockerfile` producing one image with both binaries; `.dockerignore`; a testcontainers-rs container smoke test; a `docker-compose.yml` running both edges locally (+ Dex); a short ops note; a GHCR release workflow (build → smoke → publish on tag).
- **Out of scope:** Helm charts / Kubernetes manifests (a later follow-up if needed); production secret management; the model-provider deployment; multi-arch (arm64) builds unless trivially free via buildx (recorded as a follow-up); changing any application source code (packaging only).

## Phases

### Phase 1 — Multi-stage Dockerfile + container smoke test
- **Goal:** one image builds and runs **either** binary, proven by a testcontainers-rs smoke against a real Dex.
- **Changes:**
  - `Dockerfile` (new) — multi-stage: a `rust:1-bookworm` builder running `cargo build --release` (produces `target/release/recall` and `target/release/recall-mcp`), then a `debian:bookworm-slim` runtime that copies both binaries to `/usr/local/bin/`, creates a non-root user, sets `RECALL_STORE_PATH=/var/lib/recall` (owned by that user), exposes 8080 (REST) + 8081 (MCP), and defaults `CMD ["recall"]` (overridden to `recall-mcp` for the MCP edge). Use a cargo build-cache layer (copy `Cargo.toml`/`Cargo.lock` first) for incremental builds.
  - `.dockerignore` (new) — exclude `target/`, `.git/`, `docs/`, `tests/`, the build cache, and editor cruft so the build context is lean.
  - `tests/container.rs` (new) — a testcontainers-rs integration test that: (1) brings up `dexidp/dex` (reusing `DEX_IMAGE`/`DEX_TAG` from `tests/support/dex.rs`) on a shared docker network; (2) runs the pre-built image (`recall:ci`) twice on that network — once `command: recall-mcp` with env `RECALL_OIDC_ISSUER`→the Dex container, dummy `RECALL_EMBED_URL`/`RECALL_RERANK_URL`, `RECALL_OIDC_AUDIENCE`, and a writable store path; once `command: recall`; (3) probes `POST /mcp` `tools/list` on the MCP container → a JSON-RPC result with six tools, and `GET /healthz` on the REST container → 200 `status: live`.
- **Rationale:** the image is the foundational artefact every later phase consumes; gating it on a real-Dex boot proves the fail-fast OIDC path works inside the container, not just `cargo build`.
- **Exit criteria:**
  - Build: `docker build -t recall:ci .` exits 0 and the image contains both `/usr/local/bin/recall` and `/usr/local/bin/recall-mcp` (`docker run --rm recall:ci ls /usr/local/bin` shows both).
  - **Integration (testcontainers-rs):** `cargo test --test container` is green. Integrating services brought up real-via-test-containers: **`dexidp/dex`** (image+tag from `tests/support/dex.rs`) for OIDC, and the **built `recall:ci` image** run as each binary. The test asserts `recall-mcp` answers `tools/list` (six tools) and `recall` answers `GET /healthz` 200.
  - Behavioural check: `docker run --rm -e RECALL_OIDC_ISSUER=<unreachable> recall:ci recall-mcp` exits non-zero with a clear OIDC-unreachable error (proving fail-fast boot is intact in the image).
- **Risks / rollback:** risk = the `surrealdb`/`aws-lc-rs`/`rustls` native build needs system libs absent from `debian-slim` (e.g. CA certs, `libssl`); mitigation = install `ca-certificates` (and any libc deps) in the runtime stage; the build error is loud at `docker build`. Rollback = delete `Dockerfile`/`.dockerignore`/`tests/container.rs`; no source touched.

### Phase 2 — Local deployment wiring (docker compose)
- **Goal:** a single `docker compose up` brings both edges live against a local Dex.
- **Changes:**
  - `docker-compose.yml` (new) — services: `dex` (OIDC, reusing the BDD Dex config shape), `recall` (image `recall:ci` or the GHCR ref, `command: recall`, port 8080, env → dex + provider URLs + store volume), `recall-mcp` (same image, `command: recall-mcp`, port 8081, same env). A named volume for `RECALL_STORE_PATH`. Provider URLs documented as required env (real endpoints supplied by the operator; the compose file ships only Dex).
  - `docs/runbooks/recall-containers.md` (new) — a short ops note: how to build the image, run the compose stack, the two ports/commands, and the required env (OIDC issuer/audience, embed/rerank URLs).
- **Rationale:** compose is the smallest real "two manifestations from one image" deployment; it depends only on the Phase-1 image and makes the both-edges story runnable by hand.
- **Exit criteria:**
  - Build/validate: `docker compose config` exits 0 (the compose file parses and resolves).
  - **Integration:** `cargo test --test container` extended with a compose-or-network scenario, OR a scripted `docker compose up -d` followed by the same two probes (`tools/list` on :8081, `/healthz` on :8080) both succeeding, then `docker compose down`. Real services: the composed Dex + both `recall:ci` containers.
  - Behavioural check: with the stack up, `curl -s localhost:8080/healthz` returns `status: live` and a `tools/list` POST to `localhost:8081/mcp` returns the six tools.
- **Risks / rollback:** risk = inter-container DNS/issuer-URL mismatch (the issuer URL in tokens must match what `recall` validates) — the same class of issue the BDD Dex harness already solved; mitigation = set `RECALL_OIDC_ISSUER` to the in-network Dex service URL. Rollback = delete the compose file + runbook.

### Phase 3 — GHCR release workflow
- **Goal:** a tagged release builds, smoke-tests, and publishes the image to GHCR.
- **Changes:**
  - `.github/workflows/release.yml` (new) — on `push` of a tag `v*`: checkout, set up Docker buildx, `docker build -t ghcr.io/gatblau/recall:<tag> -t ghcr.io/gatblau/recall:<sha> .`, run the Phase-1 container smoke (`cargo test --test container` against the freshly built image), `docker/login-action` to GHCR with `GITHUB_TOKEN`, then push both tags. `permissions: { contents: read, packages: write }`. Optionally generate release notes from the commit range (a `releasegen`-style step) and attach them to the GitHub release.
  - `.github/workflows/ci.yml` (new, optional but recommended) — on PR/push: `cargo build`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (incl. the BDD + MCP suites where Docker is available on the runner), so the release path is never the first place tests run.
- **Rationale:** publishing is the actual FU-001 deliverable; it comes last because it consumes the verified image (Phase 1) and the runnable stack (Phase 2). Gating publish on the container smoke means a broken image never reaches the registry.
- **Exit criteria:**
  - Lint: `actionlint .github/workflows/*.yml` exits 0 (or, if `actionlint` is unavailable, the YAML parses via `python3 -c 'import yaml,sys; yaml.safe_load(open(...))'` and the steps are reviewed against the GHCR publish recipe).
  - **Integration (the locally-runnable core of the workflow):** the build+smoke the workflow runs is exactly `docker build` + `cargo test --test container` from Phase 1 — green locally proves the workflow's gate. The push step is verified on a real tag (CI cannot publish from this environment); the plan records this as the one step proven only in CI.
  - Behavioural check: pushing a throwaway tag on a fork (or a `workflow_dispatch` dry-run with the push step gated behind an input) produces `ghcr.io/gatblau/recall:<tag>` — performed by the operator at release time.
- **Risks / rollback:** risk = GHCR auth/permission misconfig (missing `packages: write`) fails the push; mitigation = the explicit `permissions` block + `GITHUB_TOKEN`; the failure is loud and publishes nothing. Rollback = delete the workflow file(s); no image or source affected.

## Cross-cutting validation

Final gate (what `/sync-check` and a release reviewer rely on): `docker build -t recall:ci .` clean; `cargo test --test container` green (both binaries boot in-image against real Dex and answer their unauthenticated probes); `docker compose config` valid and a manual `docker compose up` brings both edges live; `actionlint` (or YAML parse) clean on the workflows. Application source is unchanged, so `cargo build` / `cargo clippy` / `cargo test --test bdd` / `--test mcp` remain green from plan 02.

## Follow-ups (not in this plan)

```yaml
- id: FU-001
  title: Multi-arch (arm64 + amd64) image builds via buildx
  why: the release workflow builds the host arch only; arm64 (Apple Silicon / Graviton) wants a buildx matrix. Deferred unless a target needs it.
  source: planning
  suggested-command: /breakdown FU-001 from docs/wip/plan/03-container-packaging-release.md
  status: open
  added: 2026-06-28
- id: FU-002
  title: Helm chart / Kubernetes manifests for the two edges
  why: compose covers local/single-node; a k8s deployment (two Deployments over one image, command-selected) is a separate packaging concern.
  source: planning
  suggested-command: /breakdown FU-002 from docs/wip/plan/03-container-packaging-release.md
  status: open
  added: 2026-06-28
- id: FU-003
  title: Image supply-chain hardening (SBOM, provenance/attestation, vuln scan)
  why: once images publish to GHCR, add an SBOM (syft), build provenance, and a Trivy/grype scan gate to the release workflow.
  source: planning
  suggested-command: /breakdown FU-003 from docs/wip/plan/03-container-packaging-release.md
  status: open
  added: 2026-06-28
```

## Risks (not closed by this plan)

```yaml
- id: RISK-001
  title: Native crypto/store build (aws-lc-rs / surrealdb) fails or bloats the runtime image
  why: the build pulls native deps; a missing system lib breaks `docker build`, and a fat runtime base inflates the image.
  source: planning
  likelihood: medium
  impact: low
  mitigation: install only ca-certificates (+ any libc deps) in the slim runtime; the build error is loud and caught at Phase 1 `docker build`.
  status: open
  added: 2026-06-28
- id: RISK-002
  title: Container smoke is flaky on the Dex/issuer-URL handshake inside a docker network
  why: the OIDC issuer URL embedded in tokens must exactly match what recall validates across container DNS — a known sharp edge (the BDD Dex harness already navigates it).
  source: planning
  likelihood: medium
  impact: medium
  mitigation: set RECALL_OIDC_ISSUER to the in-network Dex service URL and reuse the BDD harness's Dex config shape; assert readiness before probing.
  status: open
  added: 2026-06-28
- id: RISK-003
  title: GHCR publish misconfiguration ships nothing or the wrong tag
  why: a missing `packages: write` permission or a wrong tag expression fails the push or mistags the image.
  source: planning
  likelihood: low
  impact: medium
  mitigation: explicit `permissions` block + GITHUB_TOKEN; gate push behind the green container smoke; verify on a throwaway tag first.
  status: open
  added: 2026-06-28
```
