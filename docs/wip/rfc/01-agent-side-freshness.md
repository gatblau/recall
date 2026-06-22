# Recall is a passive memory store: move freshness orchestration to the agent

> Lane: **Standard** · Status: **Draft** · Source: chat change request (2026-06-22)

## Restated request

Recall must be conceived to *empower* an agentic workflow that runs **at the agent**, not to *perform* one. Concretely: **recall makes no outbound API calls and runs no agentic orchestration.** The read-path freshness loop currently built into recall (the `recall → broker` conditional source-change check, the `currency` verdict it computes, and the `ReReadSource` job it enqueues) must be removed, and the recall contract reshaped so the agent — which is the only party with a local broker and document access — can run the *ask → check → update* loop itself.

Interpretation choice: this RFC treats the change as **architectural and contract-level** (reverse ADR-013; recall becomes a passive store), not as a config toggle that keeps the broker code behind a flag. The directive "no outbound api calls should happen at recall" is read as *remove*, not *make optional*. If a centrally-reachable broker deployment must still be supported, that reopens OQ-5 (below).

## Motivation

The freshness design rests on an environment assumption that does not hold in the real deployment.

- ADR-013 (`docs/design/agentic-memory/09-decisions.md:186-204`) committed the freshness check to **recall-side**: on the read path recall calls the broker "as the user", and on a detected change enqueues an asynchronous re-read. It explicitly **rejected** the agent-driven/broker-side alternative as the default because it "pushes orchestration back onto the agent the API is meant to keep simple" — keeping it only as a *deployment variant* (`09-decisions.md:201-203`).
- In the actual deployment the broker is a **per-agent local component** (e.g. Copilot in VS Code on a developer's machine). A central recall service **cannot** open a connection to it: no routable address, behind NAT, and only alive while that user's IDE runs.

The consequences in the current code:

1. **Recall's freshness call can never succeed in production.** `HttpBrokerClient` issues `GET {config.broker_url}/sources/{origin_ref}/freshness` (`src/providers/mod.rs:388-413`). Against a laptop-local broker this fails transport, and C5 absorbs every failure into `UnverifiedCurrency` (`src/freshness/mod.rs:9-12,149-158`). The `Current` / `StalePendingRefresh` outcomes are unreachable.
2. **The agent cannot run the check either**, because recall withholds what it would need: the `/v1/recall` response carries only an opaque `source_id` and **never returns** the `origin_ref` (document handle) or `modification_marker` (version the note was based on) — those flow inbound on write only (`src/api/v1.rs:511-516`).
3. **Dead orchestration.** On a (hypothetical) change, C5 enqueues a `ReReadSource` job (`src/freshness/mod.rs:101-118`) that no worker consumes — the maintenance dispatcher handles only `ReEmbedFact`, `Consolidate`, `HardDelete`; everything else is a no-op completion (`src/maintenance/mod.rs:330-341`).

So freshness is broken both ways and recall carries outbound machinery it cannot use. The fix is to align recall with how it is actually deployed: a passive store that hands the agent the provenance it needs and lets the agent's local broker do the checking and refreshing.

## Current behaviour

- **Recall response shape.** `/v1/recall` returns `RecallResponse { facts: Vec<RankedFact> }`; `RankedFact { fact: Fact, score, currency }` (`src/types/api.rs:36-47`). `Fact.source_id` is an opaque `"source:<uuid>"`; `Source.origin_ref` and `Source.modification_marker` exist (`src/types/domain.rs:77-87`) but are **not** included in any read response. There is no `GET /v1/sources/:id` route (`src/api/mod.rs:97-103`).
- **`currency` verdict.** `Currency { Current, StalePendingRefresh, UnverifiedCurrency }` (`src/types/api.rs:50-56`), assigned per fact by the retrieval engine step 9 (`src/retrieval/mod.rs:301-343`), which delegates to the C5 `FreshnessChecker` seam (`src/retrieval/mod.rs:118,127`).
- **Outbound call.** C5 `BrokerFreshnessChecker` fans out one conditional check per distinct source via `BrokerClient::check_source` (`src/types/ports.rs:394-401`, `src/freshness/mod.rs:198-215`), implemented as an HTTP GET to `config.broker_url` (`src/providers/mod.rs:393`). Identity is conveyed by **trusted headers** `X-Recall-Tenant` / `X-Recall-User` (`src/providers/mod.rs:401-402`), not the user's token.
- **Async re-read.** Changed sources enqueue a `ReReadSource` job (`JobKind::ReReadSource`, `src/types/job.rs:34`; enqueue at `src/freshness/mod.rs:103`) with no consumer.
- **Config.** `RECALL_BROKER_URL` is **required** at boot (`src/config.rs:249`).
- **Write / supersede already support agent-side update.** Writes accept `source { origin_ref, modification_marker }` (`src/types/api.rs:68-72`, `src/api/v1.rs:511-516`); supersession is available via `/v1/memories/:id/retire` (`src/api/mod.rs:101`) and the store's supersede path; `Fact.supersedes` / `superseded_by` record the chain (`src/types/domain.rs:38-41`). Agent-stated writes bypass extraction (`RememberRequest.agent_stated`, `src/types/api.rs:65`).

## Desired behaviour

A future reader can test these outcomes:

1. **Zero egress from recall.** No code path in request handling or background workers makes an outbound call to a broker or any source system. Recall boots and serves `/v1/recall` with no broker configured.
2. **Provenance returned for checking, on request.** The recall call carries a **request parameter** by which the agent opts in to provenance. When set, each recalled fact that has a source includes the document handle (`origin_ref`) and the stored version token (`modification_marker`), so the agent's local broker can perform the `If-Modified-Since`-style check itself; when unset, the response stays lean. The agent chooses per call (RESOLVED — OQ-2).
3. **No freshness verdict at all.** The `currency` field is **removed**. Recall cannot determine staleness, so it asserts nothing about it — it returns only the document reference and last-updated token (per item 2) and leaves the decision entirely to the agent (RESOLVED — OQ-1).
4. **No read-path job orchestration for freshness.** Recalling a fact never enqueues a `ReReadSource` (or any) job.
5. **The agent-side loop closes through existing endpoints.** Given a stored note based on version M1, after the agent re-reads and writes a fresh note (agent-stated, marker M2) that supersedes the old one, a subsequent recall returns the fresh note (marker M2) and the old note is superseded — using only `/v1/recall`, `/v1/memories`, and the supersede path.
6. **Intent realigned.** ADR-013 is superseded by a new ADR stating freshness is agent-side and recall makes no outbound calls; the retrieval-engine and freshness component specs match the code.

## Scope

**In scope**
- Remove recall-side freshness orchestration: the C5 `BrokerFreshnessChecker` module (`src/freshness/`), the `BrokerClient` port (`src/types/ports.rs:394-401`) and `HttpBrokerClient` adapter (`src/providers/mod.rs:361-414`), the `RECALL_BROKER_URL` config key (`src/config.rs:122,249,438`), and the read-path `ReReadSource` enqueue (`src/freshness/mod.rs`).
- Reshape the `/v1/recall` (and `GET /v1/memories/:id`) read response to expose per-fact source provenance (`origin_ref` + `modification_marker`); remove or redefine `currency` (OQ-1) and the C5 delegation in the retrieval engine step 9 (`src/retrieval/mod.rs:301-343`).
- Amend the design: supersede ADR-013; update affected specs (`docs/spec/components/retrieval-engine.md`, the freshness-checker component spec, `08-interfaces.md`, `03-sequences.md`, `02-architecture.md`, `07-cross-cutting.md`) and the HLD changelog.
- Add/adjust BDD coverage to model the agent control loop client-side (ASK → agent CHECK → UPDATE via supersede → ASK) in `tests/features/system.feature`.

**Out of scope**
- The agent's own freshness logic and local-broker implementation — those live in the agent / Faraday client, not in recall.
- The write/extraction pipeline behaviour beyond removing broker coupling — extraction, scoring, PII, the gate, and supersession semantics are unchanged.
- Maintenance worker (C7) consolidation / decay / re-embed / hard-delete — unchanged except removing the dead `ReReadSource` branch handling if present.
- Auth, tenancy, visibility, and the read filter — unchanged (provenance is only ever returned for facts the caller is already permitted to recall).
- Removing `Source.trust_signal` or its use in the write gate — trust is stored at write time, not fetched from a broker (`src/write_pipeline/mod.rs:827-830`), so it is unaffected.

## Acceptance criteria

1. **Boots without a broker.** Recall starts and `/readyz` passes with `RECALL_BROKER_URL` unset; a config test confirms the key is no longer read (`src/config.rs`).
2. **No outbound client remains.** `BrokerClient`, `HttpBrokerClient`, and `BrokerFreshnessChecker` are absent from the tree; a grep-based or compile-time check confirms the read path constructs no HTTP client targeting a broker.
3. **Provenance present on recall, when requested.** A system BDD scenario asserts that, with the provenance request parameter set, a recalled fact carrying a source returns non-empty `origin_ref` and the stored `modification_marker` in the `/v1/recall` response body; and that, with the parameter unset, those fields are absent (lean response).
4. **No currency field.** The `/v1/recall` response no longer contains any `currency` / freshness verdict; the `Currency` type and the retrieval engine's `FreshnessChecker` reference are removed.
5. **No read-path job enqueue.** A test confirms that invoking `/v1/recall` enqueues zero work-queue jobs (in particular zero `ReReadSource`).
6. **Cold-start path (BDD).** For an unseen topic, `/v1/recall` returns no facts; after an agent-stated write and its job drain, `/v1/recall` returns the fact. (Extends the existing eventual-consistency scenario.)
7. **Agent-side update loop (BDD).** A note stored with marker `M1` is returned by recall with `M1`; after a superseding agent-stated write with marker `M2`, recall returns the fresh note with `M2` and the prior note is superseded (`superseded_by` set). Uses only `/v1/recall`, `/v1/memories`, and the supersede path.
8. **Intent realigned.** A new ADR supersedes ADR-013; `sync-check` reports zero `INTENT-DRIFT` and zero `CONTRACT-DRIFT` for the retrieval-engine and freshness areas after the spec update.
9. **No dead code / orphan config.** `cargo build` and the full test suite pass with C5 and the broker provider removed; no unused `JobKind` arm produces a warning (OQ-3 decides whether the variant is retained for wire-compat).
10. **OpenAPI updated.** The generated OpenAPI document (`/openapi.json`, `src/api/openapi.rs`) reflects the new recall response shape (provenance fields in, broker-derived currency out).

## Constraints

- **Breaking API change.** Removing/redefining `currency` and adding provenance fields changes the `/v1/recall` contract. Recall is pre-1.0 (single initial release), so a hard change is acceptable, but the OpenAPI doc and any consumer notes must be updated in the same change; if any external consumer already reads `currency`, a deprecation note is required.
- **No new outbound dependency.** The change must not introduce any other network egress to replace the broker call — the architectural invariant is *recall performs no outbound calls during request handling or background work*.
- **No freshness blindness without provenance.** `currency` may only be removed if the provenance fields (AC-3) ship in the same change, so clients are never left unable to assess staleness.
- **Access control preserved.** Provenance is returned only for facts already admitted by the read filter; no `origin_ref` / `modification_marker` may be exposed for records outside the caller's scope (`src/auth` read-filter behaviour unchanged).
- **Deployment ordering.** The spec amendment (ADR + component specs) lands before code per the *fix the prompt first* rule; `RECALL_BROKER_URL` removal must be coordinated with any deployment manifests that currently set it.

## Proposed direction

Recall collapses to a passive bi-temporal memory store: it stores notes with provenance markers, returns them *with* provenance on recall, and accepts superseding writes. Every `ask → check → update` move moves to the agent and its local broker. Implementation-wise this is mostly *removal* (the C5 module, the broker port/adapter, the config key, the read-path enqueue) plus a small *additive* contract change (expose `origin_ref` + `modification_marker` on the read responses) and the disposition of `currency`. The write/supersede path already supports the agent's UPDATE leg, so no new write capability is required.

## Open questions

3. **(non-blocking)** Retain `JobKind::ReReadSource` (`src/types/job.rs:34`) for wire/forward-compat, or remove the variant? Default assumption: **remove**, since nothing enqueues or consumes it after this change.
4. **(non-blocking)** Does any current deployment manifest set `RECALL_BROKER_URL` such that its removal breaks a running environment? Needs an ops check before the config key is deleted.
5. **(non-blocking)** Is there *any* deployment where recall can reach a central broker that should keep the old behaviour behind a flag? Default assumption per the directive: **no — fully remove** (recall does no outbound calls). Recorded as an assumption; flip to blocking only if such a deployment is confirmed.

### Resolved

- **OQ-1 — `currency` field disposition. Resolved 2026-06-22:** **remove it entirely.** Recall has no way to determine staleness; it returns only the document reference (`origin_ref`) and last-updated token (`modification_marker`) and leaves the decision to the agent. The `Currency` type is deleted from the contract.
- **OQ-2 — how to expose provenance. Resolved 2026-06-22:** **conditional inline provenance via a request parameter.** The recall call carries a flag by which the agent opts in; when set, each sourced result includes `origin_ref` + `modification_marker`; when unset, the response stays lean. The agent chooses per call depending on whether it intends to run a freshness check.

No blocking open questions remain.

## Handoff to /spec

This is an **intent-changing** RFC: it reverses a committed architectural decision (ADR-013) and changes the `/v1/recall` contract. The affected design and component specs must be updated **before** any code is patched (*fix the prompt first*).

Both blocking open questions are resolved (see *Resolved* above), so the RFC is ready to hand off:

```
/spec docs/wip/rfc/01-agent-side-freshness.md
```

After `/spec` exits clean (Phase 6 audit passed), proceed with `/breakdown` against the updated retrieval-engine and freshness specs, then `/apply`, then `/sync-check`.
