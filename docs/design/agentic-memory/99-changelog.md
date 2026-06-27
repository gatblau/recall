# Agentic Memory (`recall`) HLD — Changelog

Append-only. One line per edit, newest at the bottom. Format: `<date> — <origin>: <summary> → revision: <new-revision>`.

2026-06-20 — initial draft (designgen, mode=draft): bootstrapped HLD for agentic-memory from docs/concept/{agentic-mem,good-mem,requirements}.md → revision: 0.1.0
2026-06-20 — direct edit: committed Rust + embedded SurrealDB (ADR-009, ADR-003, tech stack, context, architecture); resolved OQ-LANG-EMBED, reframed OQ-STORE → revision: 0.2.0
2026-06-20 — direct edit: added ADR-010 outside-in integration/BDD test strategy (testcontainers, Dex for OIDC, session optimisation) → revision: 0.2.0
2026-06-20 — direct edit: added ADR-011 bridge-model tenancy (namespace-per-tenant + logical team/user scoping); resolved OQ-TENANCY; updated Scope domain model, authorisation, multi-tenancy, regulatory → revision: 0.3.0
2026-06-20 — review-driven edit (concept/HLD validation, B1): corrected read-path model posture — query embedding + cross-encoder rerank are on-path non-LLM inferences with latency sub-budgets (new ADR-012); fixed 01-context, 02-architecture, 03-sequences, 05-tech-stack, 07-cross-cutting; reformulation made A/B-gated → revision: 0.4.0
2026-06-20 — review-driven edit (B2): committed freshness placement (new ADR-013 — recall-side conditional check on the read path + async re-read); resolved OQ-FRESH-PLACEMENT; added outbound recall→broker interface (08-interfaces); updated recall sequence → revision: 0.4.0
2026-06-20 — review-driven edit (S1): flagged OQ-STORE as load-bearing for ADR-009 (Rust reopens if the store spike fails); updated ADR-003/009 consequences, Open Questions preamble, and 10-risks → revision: 0.4.0
2026-06-20 — review-driven edit (S2–S4 + nits): made procedural memory and semantic caching explicit deferrals (00-overview, 04-domain); named the append-only per-tenant audit trail's storage (06-regulatory, 07-cross-cutting); replaced banned "etc." and vague "as needed"/"where feasible" qualifiers → revision: 0.4.0
2026-06-20 — promoted from phase-3/phase-4 (HLD-impact-pass): added glossary terms stale-pending-refresh, unverified-currency (D-HLD-1, 00-overview) → revision: 0.4.1
2026-06-20 — promoted from phase-3 (HLD-impact-pass): clarified v1 entity resolution (rules→ML→create-new; LLM adjudication deferred) in 02-architecture and added the matching risk in 10-risks → revision: 0.4.1
2026-06-22 — RFC 01 (HLD-impact-pass): ADR-014 supersedes ADR-013 — recall is a passive store, freshness/orchestration is agent-side; removed recall→broker outbound (08), the Freshness Checker from the read path (02/03), currency + stale/unverified glossary terms (00), the recall→broker context edge (01), and the freshness NFR clauses (07) → revision: 0.5.0
2026-06-22 — RFC 02 (HLD-impact-pass): ADR-015 — recall is LLM-free; extraction moves to the agent and server-side consolidation is dropped; removed the LLM provider actor (01), the extraction/consolidation edges (02), the LLM consolidation sequence (03), the LLM tech-stack row (05), and reframed the consolidation glossary + goal (00); amended ADR-012's async clause → revision: 0.6.0

2026-06-27 — RFC 01 (HLD-impact-pass): ADR-016 — one service core, two transport edges (REST + new MCP over streamable-HTTP reusing OIDC); added Service Layer + MCP API Edge components (02), the MCP interface (01/08), MCP/Service-Layer/MCP-edge glossary terms (00), and an MCP transport-library row (05) → revision: 0.7.0
