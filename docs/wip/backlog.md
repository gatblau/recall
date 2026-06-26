# Recall — deferred backlog (follow-ups & open risks)

> Consolidated 2026-06-24 from the now-removed WIP plans/RFCs (greenfield build, ADR-014 agent-side freshness, ADR-015 LLM-free). Those plans and RFCs were fully implemented and shipped (greenfield build; `4ea7b47` ADR-014; `5f81a11` ADR-015); the decisions of record live in `docs/design/agentic-memory/09-decisions.md` (ADR-012…015) and the spec set (`docs/spec/phase-1..6`). **Only the still-open items are carried here.** Closed/done/dropped items and the full plan history remain in git.
>
> Some greenfield follow-ups predate ADR-014 (freshness removed) and ADR-015 (recall LLM-free); where a later ADR has overtaken an item, it is marked **OBSOLETE** with the reason — keep the entry only as a record of the original intent.

## Open follow-ups

```yaml
# --- from the greenfield build plan ---
- id: FU-001
  title: Add NATS work-queue backend behind the WorkQueue trait
  why: store-backed queue ships first to keep the single-binary story (SA-QUEUE-01); NATS is the scale-out option (OQ-QUEUE), non-architectural.
  status: open
  added: 2026-06-20
- id: FU-002
  title: Add local/in-process embedder option for the read path
  why: ADR-012 names a local embedder as the NFR-P2/P3 latency mitigation if the remote query-embed hop is too slow; not needed for v1 correctness. Latency escape hatch for RISK-004.
  status: open
  added: 2026-06-20
- id: FU-003
  title: Implement and A/B-gate query reformulation in the Retrieval Engine
  why: off by default (good-mem §7.3 reports it can underperform dense retrieval); RECALL_REFORMULATION_ENABLED is wired but the reformulation logic is deferred (no-op pass-through today).
  status: open
  added: 2026-06-20
- id: FU-004
  title: LLM entity adjudication tier in the Write Pipeline
  why: v1 uses rules→ML→create-new for entity resolution; the original plan deferred an LLM adjudication tier until duplication rates are measured (blocked-by FU-005).
  status: OBSOLETE
  obsoleted-by: ADR-015 — recall is LLM-free; any LLM adjudication is now the agent's responsibility, not a recall write-pipeline tier. The rules→ML→create-new path stays; richer adjudication is agent-side.
  added: 2026-06-20
- id: FU-005
  title: Instrument entity-duplication rate metric
  why: measures whether richer entity adjudication is worth it; the maintenance merge_entities pass folds duplicates in the meantime. Still useful as an observability metric even with FU-004 obsoleted.
  status: open
  added: 2026-06-20
- id: FU-006
  title: Team→database physical-isolation escape hatch
  why: ADR-011 residual — promote a team to its own database when it needs physical isolation; v1 uses record-level team scoping only.
  status: open
  added: 2026-06-20
- id: FU-007
  title: Complete OQ-STORE remote leg + confirm at real OQ-VOLUMES scale
  why: Phase 0 passed the embedded leg provisionally (ANN p95 9.46ms, stage-1 15.60ms at 25k/namespace, indexed); the remote SurrealDB/TiKV leg could not run here (no docker daemon) and the real target corpus is OQ-VOLUMES-TBC. Run the remote leg on real infra and re-confirm once OQ-VOLUMES is pinned. The `ent`/scope index is load-bearing for the graph leg (49ms→2ms). Relates to FU-019.
  status: open
  added: 2026-06-20
- id: FU-011
  title: Pin the production OIDC tenant/jti/groups claims (OQ-IDP) + Dex test-coverage note
  why: real Dex (password connector) emits only iss/sub/aud/exp/iat/email — not custom `tenant`, `jti`, or `groups`. The PRODUCTION IdP must be configured to emit `tenant` (RECALL_OIDC_TENANT_CLAIM), `groups` (RECALL_OIDC_TEAMS_CLAIM) and a `jti` (SA-AUDIT-01). Resolve OQ-IDP with the chosen provider and add a deployment OIDC-config check.
  status: open
  added: 2026-06-21
- id: FU-017
  title: Run the cargo-audit dependency-CVE gate (tool absent in the build env)
  why: the Phase-10 security gate names `cargo audit` alongside secscan, but `cargo-audit` was not installed in the build env. The four mandatory gates (build, lint, integration, secscan) passed; coverage 77.73% ≥ 70%. Install and run before shipping.
  suggested-command: cargo install cargo-audit && cargo audit
  status: open
  added: 2026-06-21
- id: FU-019
  title: Wire remote SurrealDB credentials (signin) for secured endpoints
  why: FU-009 unified the connection on `engine::any`, but `Store::connect` does not call `signin(Root{...})` and `Config` has no remote-credential fields — a *secured* remote server cannot authenticate (a no-auth endpoint works today). Add `RECALL_STORE_REMOTE_USER`/`RECALL_STORE_REMOTE_PASS` (Secret) config + a `signin` after `connect` when set, and a `use_ns`/`use_db` root-scope check. Deployment-blocking only for secured remote stores; embedded default unaffected.
  status: open
  added: 2026-06-21

# --- from the ADR-014 (agent-side freshness) plan ---
- id: FU-014-OQ5
  title: Confirm no central-broker deployment needs the old recall-side freshness check
  why: RFC 01 OQ-5 — ADR-014 assumes the broker is always per-agent local; a centrally-reachable broker deployment would reopen the decision (recall makes no outbound calls today).
  suggested-command: /rfc (new) if a central-broker deployment is confirmed
  status: open
  added: 2026-06-22

# --- from the ADR-015 (LLM-free) plan ---
- id: FU-015-MANIFEST
  title: Sweep deployment manifests / .env examples for RECALL_LLM_* and RECALL_BROKER_URL / RECALL_FRESHNESS_*
  why: code no longer reads RECALL_LLM_*, RECALL_BROKER_URL, RECALL_FRESHNESS_*, or the renamed consolidation interval key. Prior in-repo sweeps found zero references; re-confirm in any out-of-repo deployment/orchestration before release (set-but-unused vars are harmless to recall but may lint elsewhere).
  status: open
  added: 2026-06-23
- id: FU-015-DOCS
  title: Document the agent-side consolidation pattern for clients
  why: server-side consolidation is gone (ADR-015); agents now distil episodes and write back agent-stated `consolidated` facts (the `consolidated` class + `derived_from` are retained for this). Worth a short client note so the capability is discoverable.
  status: open
  added: 2026-06-23
```

## Open risks

```yaml
# --- from the greenfield build plan ---
- id: RISK-001
  title: Embedded SurrealDB misses the latency/scale targets at real scale
  why: ADR-009 (Rust + embedded) rests on the store embedding in-process; a failure reopens ADR-003/ADR-009 (rearchitecture). Phase 0 passed provisionally; real-scale confirmation rides on FU-007.
  likelihood: medium
  impact: high
  status: open
- id: RISK-002
  title: Cross-tenant data leakage
  why: a read-filter or namespace-selection bug could surface one tenant's facts to another; the graph/vector neighbour paths are the subtle cases.
  likelihood: low
  impact: high
  mitigation: namespace-per-tenant is structural (ADR-011); cross-tenant isolation is a first-class BDD scenario (NFR-PR1).
  status: open
- id: RISK-003
  title: Write-gate trust-threshold miscalibration
  why: thresholds too strict reject good facts; too loose admit poison (ADR-008). Defaults are unvalidated against labelled data.
  likelihood: medium
  impact: medium
  mitigation: quarantine (not hard-reject) the uncertain; monitor writes_rejected_total/writes_quarantined_total; tune post-launch.
  status: open
- id: RISK-004
  title: External provider latency or contract drift threatens NFR-P2
  why: query-embed + rerank are on the read path (ADR-012); a slow or contract-divergent provider blows the 200 ms budget. (The broker/freshness leg of the original risk is void under ADR-014 — recall makes no broker call.)
  likelihood: medium
  impact: medium
  mitigation: per-inference sub-budgets with degradation (SA-LAT-01); wiremock contract tests in CI; FU-002 local embedder as the latency escape hatch.
  status: open
- id: RISK-005
  title: Self-reinforcing consolidation error
  why: a wrong consolidated insight could become permanent knowledge and outrank its sources.
  status: OBSOLETE
  obsoleted-by: ADR-015 — recall runs no server-side consolidation. Insight synthesis is now agent-side; this risk, if it persists, belongs to the agent that writes consolidated facts. The "insight never outranks its sources" ranking rule (ADR-006) still holds in recall.
- id: RISK-006
  title: Eventual-consistency surprise on write→recall
  why: the async write path means a just-remembered fact is not immediately recallable; callers may expect read-after-write.
  likelihood: medium
  impact: low
  mitigation: document the contract; idempotent acks; round-trip scenario asserts eventual (not immediate) visibility.
  status: open
- id: RISK-007
  title: HNSW ANN p99 latency tail under embedded SurrealDB
  why: the OQ-STORE spike showed ANN p95 well under 50ms but occasional p99 outliers of 78–135ms at 25k/namespace; if the tail worsens at real scale it threatens an NFR-P2 p99 SLO.
  likelihood: medium
  impact: low
  mitigation: tune HNSW EF/M params and warm the index; assert p95 in retrieval integration tests; revisit under FU-007 at real scale.
  status: open
```
