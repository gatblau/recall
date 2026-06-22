# 09 — Architectural Decisions, Assumptions & Open Questions

> **Mode:** draft · **Revision:** 0.5.0 · **Last updated:** 2026-06-22

## Architectural Decisions

### ADR-001: OIDC bearer-token authentication with broker-injected identity
- **Decision.** `recall` authenticates every request via an OIDC-issued JWT bearer token, validated
  against the configured issuer's JWKS; identity is taken from a token claim and mapped to a Scope.
- **Context.** `recall` sits behind the Faraday broker, which authenticates as the end user. Identity
  must cross a service boundary without `recall` ever holding raw credentials, and the agent's
  sandboxed script must not be able to construct or widen its own identity.
- **Consequences.** Standards-based and IdP-agnostic; `recall` stays credential-free; per-user
  isolation keys off a verifiable claim. Requires reachable OIDC discovery/JWKS; the exact claim set
  is provider-dependent (OQ-IDP).
- **Alternatives considered.** Static API keys (rejected — no per-user identity, theft risk); mutual
  TLS only (rejected — authenticates the broker, not the end user); bespoke signed headers (rejected
  — reinvents OIDC, harder to integrate).

### ADR-002: Bi-temporal knowledge graph; supersession instead of overwrite
- **Decision.** Facts are stored with two timelines (validity interval + ingestion time); a changed
  fact ends the old fact's validity and records the successor — never a destructive overwrite.
- **Context.** Facts evolve; the named production failure mode is "a high-relevance fact becomes
  confidently wrong when circumstances change". History must remain queryable.
- **Consequences.** Free history, clean contradiction handling, and the temporal/knowledge-update
  quality the concept notes target. Costs more storage and a supersession step in maintenance.
- **Alternatives considered.** Overwrite-in-place (rejected — loses history, silent staleness);
  keep-both-flat (rejected — accumulates contradictions retrieval cannot adjudicate).

### ADR-003: Hybrid multi-model store — embedded SurrealDB
- **Decision.** Use one store providing graph, vector, and keyword retrieval over rich bi-temporal
  edges; the chosen engine is **embedded SurrealDB** (committed via ADR-009). The benchmarking spike
  now *validates* that SurrealDB meets the latency/scale targets rather than selecting between
  engines, with a documented fallback if it does not (OQ-STORE).
- **Context.** Real memory queries need semantic + exact-term + relational signals together, and
  edges must carry validity/confidence/source. Assembling three separate systems adds integration and
  consistency burden.
- **Consequences.** One engine simplifies operations and makes the bi-temporal model natural; couples
  capabilities into one blast radius and depends on a younger engine — hence the validation spike
  (OQ-STORE). **If the spike fails**, the fallback (dedicated graph DB + separate vector index) is a
  **rearchitecture, not a config swap**, and it **reopens ADR-009** — the Rust choice is justified
  primarily by in-process SurrealDB embedding. OQ-STORE is therefore load-bearing for the language
  commitment and should run before any Rust scaffolding.
- **Alternatives considered.** Dedicated graph DB + separate vector index + search engine (rejected
  for v1 — integration cost, cross-store consistency); vector-store-only (rejected — no relationships,
  poor contradiction handling).

### ADR-004: Separate fast LLM-free read path from async write/maintenance path
- **Decision.** Retrieval is synchronous and makes no LLM calls; extraction, consolidation, and
  re-embedding run asynchronously behind a durable queue.
- **Context.** Interactive latency budgets (~200 ms) cannot absorb LLM/embedding calls; writes must
  be retry-safe and must not block reads.
- **Consequences.** Predictable read latency and resilient writes; introduces eventual consistency
  (a just-written fact may not be immediately retrievable) and a queue to operate.
- **Alternatives considered.** Synchronous write-and-index (rejected — LLM latency on the hot path,
  fragile under retries).

### ADR-005: Two-stage retrieval — multi-signal recall then cross-encoder rerank
- **Decision.** Stage 1 fuses semantic + keyword + graph recall; stage 2 reranks the top candidates
  with a cross-encoder; then gating and recency weighting.
- **Context.** Multi-signal fusion beats any single signal; cross-encoders give fine-grained
  relevance but are too slow to run over the whole store.
- **Consequences.** Strong relevance within budget; adds a reranking dependency and tuning surface.
- **Alternatives considered.** Single-stage vector search (rejected — misses exact terms and
  relationships, weaker ranking).

### ADR-006: Graceful decay with a salience floor for forgetting
- **Decision.** Forgetting uses gradual, reinforcement-resettable decay plus TTL on long-tail
  entries, protected by a salience floor; confidence-linked decay fades uncertain facts faster.
- **Context.** Unbounded memory drowns retrieval and bloats storage; crude hard-TTL deletes important
  but rarely-touched facts.
- **Consequences.** Bounded storage and lower retrieval noise while high-importance facts survive
  disuse; requires tuning the decay curve and the floor against real usage.
- **Alternatives considered.** Hard TTL only (rejected — over-forgets); never forget (rejected —
  unbounded growth, contradiction build-up).

### ADR-007: Heuristic memory control at launch
- **Decision.** Memory operations (store / retrieve / update / consolidate / discard) are governed by
  heuristic rules at launch; prompted or learned (RL) control is deferred.
- **Context.** Heuristics are predictable, debuggable, cheap, and auditable — the governance layer
  needs auditability. Learned control is powerful but opaque.
- **Consequences.** Fast to build and reason about; may leave quality on the table that learned
  control could capture later. Instrument to find those gaps.
- **Alternatives considered.** RL-based control from day one (rejected — complexity and opacity
  outweigh launch value); fully LLM-decided operations (rejected — cost and unpredictability).

### ADR-008: Write-gate trust validation and untrusted-data separation
- **Decision.** Every write passes a trust-scoring gate that may quarantine or reject untrusted/
  instruction-like content; externally-sourced content is data to remember, never instructions.
- **Context.** Memory-poisoning attacks via ordinary content have very high reported success rates;
  an agentic-search system that learns from documents is directly exposed.
- **Consequences.** Materially harder to poison; adds a calibration burden (too strict rejects good
  facts, too loose lets poison through) and a quarantine path to operate.
- **Alternatives considered.** Trust-all writes (rejected — unsafe); read-time filtering only
  (rejected — poison already resident; defence in depth needs the write gate too).

### ADR-009: Rust implementation with embedded SurrealDB
- **Decision.** Build `recall` in Rust and embed SurrealDB **in-process** (SurrealKV or RocksDB
  backend) rather than running it as a separate server; retain the engine abstraction so a remote
  SurrealDB / TiKV deployment is reachable later without a code change.
- **Context.** SurrealDB is Rust-native and **only Rust can embed it in-process** — the Go (and
  other) SDKs are clients that require a separate database process. NFR-MA3 wants a single-binary
  single-node deployment, and the read-path latency budget (NFR-P2/P3) benefits from removing the
  client/server hop.
- **Consequences.** A genuine single self-contained binary, lowest read-path latency, and full native
  access to graph + vector + keyword over rich bi-temporal edges. Also a **testing dividend** — the
  same engine runs in-memory for fast tests (ADR-010). Trades the team's Go familiarity for Rust;
  SurrealKV (pure Rust) is the younger backend and RocksDB carries a C++ build dependency. Auth and
  API rely on Rust crates (`tokio`, `axum`, `openidconnect` / `jsonwebtoken`). **Dependency on
  ADR-003:** Rust is chosen *because* SurrealDB is Rust-native and only Rust can embed it in-process;
  if OQ-STORE fails and the store fallback is taken, the embedded-single-binary rationale collapses (a
  network-hop store would not require Rust) and this decision reopens. **Run OQ-STORE before committing
  Rust scaffolding**, not merely before codegen.
- **Alternatives considered.** Go + SurrealDB-as-a-server (rejected — cannot embed; two processes to
  operate; an extra hop on the hot path). Rust + remote-only SurrealDB (rejected for v1 — forgoes the
  single-binary benefit, though available later for scale-out). Other languages (rejected — weaker fit
  with the Rust-native store).

### ADR-010: Outside-in, integration-first BDD test strategy with containerised dependencies
- **Decision.** Test **outside-in**: drive behaviour through the public HTTP API using **BDD**
  scenarios (Given/When/Then) as the primary suite, backed by **real dependencies in test containers**
  — SurrealDB in server mode, the work queue, and **Dex as the OIDC provider** for auth scenarios,
  with stand-ins for the embedding/LLM providers. **Optimise container sessions** for fast cycles:
  reuse/singleton containers across a scenario suite, run the fastest inner loop against SurrealDB's
  **in-memory** backend, and execute scenarios in parallel with per-scenario scope isolation. Pure
  unit tests are reserved for algorithmic cores (decay maths, ranking fusion, entity-resolution rules).
- **Context.** `recall`'s correctness is mostly *emergent from how components interact* — auth→scope→
  retrieval, async write→store→retrieval, supersession over time. Mock-heavy unit tests miss that and
  drift from real dependency behaviour; the team wants confidence the assembled system behaves, with
  fast feedback.
- **Consequences.** High-fidelity tests that exercise real wiring and catch integration drift; BDD
  scenarios double as living documentation and map directly to the spec's Gherkin (C3). Containers are
  heavier than mocks, so the session optimisations above are required to keep cycles short — and the
  embedded-Rust decision (ADR-009) pays off here, since the same store runs in-memory for speed and
  containerised for fidelity. Dex gives a real OIDC token issuer in tests without depending on a
  hosted IdP (the production IdP stays configuration — OQ-IDP).
- **Alternatives considered.** Mock-everything unit pyramid (rejected — misses the cross-component
  behaviour that is most of `recall`'s risk; mocks drift from real dependencies). End-to-end-only
  against a deployed stack (rejected — slow, flaky, hard to seed state). A hosted IdP for auth tests
  (rejected — network dependency and flakiness; Dex-in-a-container is hermetic).

### ADR-011: Bridge-model tenancy — namespace-per-tenant + logical team/user scoping
- **Decision.** Isolate tenants (organisations) with a **SurrealDB namespace each** — a hard boundary
  with no cross-tenant query, traversal, or vector neighbour — and segment **teams and users
  logically within** a tenant via record-level `PERMISSIONS` keyed on the authenticated membership
  claims, with a Fact-level **visibility** (user-private | team-shared | tenant-shared). Where a team
  needs physical isolation, give it its own **database within the tenant namespace**.
- **Context.** Tenants must never commingle (compliance, plus the graph "walk into the wrong
  neighbour" traversal risk and vector-ANN leakage on a shared index), but intra-tenant sharing and
  team-level consolidation are features, not leaks. SurrealDB's namespace → database → record
  hierarchy supplies exactly these two seams, and its vector index is per-table, so a structural
  tenant boundary also isolates ANN candidates.
- **Consequences.** Hard cross-tenant isolation with clean per-tenant backup / export / erasure
  (drop the namespace), plus efficient sharing and consolidation inside a tenant on one index. Two
  mechanisms to reason about (structural + logical); correct record-permission rules and
  membership-claim handling become security-critical and a first-class test target (ADR-010). Scales
  out by moving a large tenant's namespace to its own instance / cluster without a data-model change
  (ADR-009 abstraction).
- **Alternatives considered.** Per-user physical graphs (rejected — kills sharing / consolidation,
  explodes object count). Pure pool / tenant-column-only (rejected — traversal and vector-ANN
  leakage, weaker compliance and deletion story). Silo-per-team everywhere (rejected as the default —
  blocks cross-team sharing and multiplies objects; retained only as the per-team escape hatch).

### ADR-012: The read path is LLM-free but performs two model inferences (query embed + rerank)
- **Decision.** NFR-P1 ("no LLM calls on the read path") is scoped to **LLM** calls. The synchronous
  read path **does** perform exactly two non-LLM **model inferences**: (1) embedding the incoming query
  (semantic recall needs a query vector, which a novel query cannot have cached), and (2) a
  cross-encoder **rerank** of the bounded stage-1 candidate set. Each gets an explicit latency
  sub-budget inside NFR-P2; only extraction and consolidation (true LLM calls) run asynchronously off
  the path.
- **Context.** Earlier draft prose described the embedding provider as "off the read path /
  asynchronous" and the reranker as "off the read-path candidate set", which contradicted both the
  architecture (stage-2 rerank is synchronous — `good-mem.md` §7.2) and the need to embed each query at
  read time. Left uncorrected, `/spec` would derive a read-path latency contract with no line item for
  these two inferences.
- **Consequences.** The recall latency budget must allocate sub-budgets for query-embed and rerank; the
  embedding/reranker **hosting choice (local vs remote) becomes a read-path latency decision**, not the
  async "cost tuning" OQ-MODELS implied — a local/in-process embedder is the mitigation if a remote hop
  threatens NFR-P2/P3. Query **reformulation stays optional and A/B-gated** (`good-mem.md` §7.3 warns it
  can hurt), not a fixed stage.
- **Alternatives considered.** Keep the read path fully model-free (rejected — impossible for semantic
  recall, which requires a query vector, and it would forgo the cross-encoder precision `good-mem.md`
  §7.2 shows dominates single-stage retrieval). Move rerank async (rejected — reranking after the
  response is returned defeats its purpose).

### ADR-013: Freshness check is recall-side and conditional on the read path; re-read is asynchronous
> **Superseded by ADR-014 (2026-06-22).** The recall-side check assumed a centrally-reachable broker; the broker is in fact a per-agent local component, so freshness moves to the agent.
- **Decision.** The freshness loop runs **recall-side**. On the read path `recall` performs only a
  **cheap conditional source-change check** (modification marker / `If-Modified-Since`) by calling the
  broker **as the user**; if the source changed, the actual **re-read and re-extraction are enqueued
  and run asynchronously**, and the answer flags the fact *stale-pending-refresh*. If the broker or
  source is unreachable, the stored fact is returned flagged *unverified-currency*.
- **Context.** Resolves OQ-FRESH-PLACEMENT. A naive "re-read on the read path" would put a source fetch
  (and its latency) inside the ≤200 ms budget; doing nothing would return confidently stale facts (the
  central named failure mode). The conditional-check / async-reread split keeps currency verification
  cheap on the path while still catching change. It requires a re-entrant `recall`→broker call, whose
  shape is named in [08 — Interfaces](./08-interfaces.md) (Outbound interfaces).
- **Consequences.** The recall endpoint's latency contract includes one conditional broker round-trip;
  the broker must expose a conditional source-read (`If-Modified-Since`) callable as the user; reads are
  **eventually-fresh** (a changed source is reflected after the async refresh, not within the same
  call). Pairs with ADR-004 — the refresh rides the existing async queue.
- **Alternatives considered.** Re-read synchronously on the read path (rejected — source-fetch latency
  blows NFR-P2). Broker-side freshness (rejected as default — pushes orchestration back onto the agent
  the API is meant to keep simple; still available as a deployment variant). No freshness check
  (rejected — returns confidently stale facts).

### ADR-014: Recall is a passive memory store — freshness and orchestration are agent-side (supersedes ADR-013)
- **Decision.** `recall` performs **no read-path source-change check, computes no currency verdict, makes
  no outbound broker call, and enqueues no re-read job.** On recall it returns each fact's stored
  provenance (`origin_ref` + `modification_marker`) **conditionally**, gated by a request parameter, so
  the agent — which is co-located with its broker and holds document access — runs the
  ask → check → update loop itself. The UPDATE leg uses the existing write/supersede endpoints.
  **Supersedes ADR-013.**
- **Context.** ADR-013 placed the freshness check inside `recall`, assuming a centrally-reachable broker.
  In the real deployment the broker is a **per-agent local component** (e.g. Copilot in VS Code on a
  developer's machine); a central `recall` cannot reach it (no routable address, behind NAT, alive only
  while the IDE runs). The recall-side check is therefore unreachable and would degrade every fact to
  *unverified-currency*. ADR-013 itself named the agent-driven approach as the deployment variant; this
  deployment makes it the only viable one.
- **Consequences.** `recall` has no outbound dependency for freshness. Removed: the C5 Freshness Checker,
  the `BrokerClient` port + `HttpBrokerClient` adapter, the `RECALL_BROKER_URL` config, the `ReReadSource`
  job kind, and the `currency` field on the recall response. Added: conditional source provenance on the
  recall (and get-fact) responses. Reads are no longer "eventually-fresh via recall" — freshness is the
  agent's responsibility. The embedding and reranker read-path inferences are retained (model inferences,
  not orchestration).
- **Alternatives considered.** Keep ADR-013 recall-side check (rejected — unreachable broker). Keep it
  behind a deployment flag (rejected per the directive — `recall` makes no broker calls). Have the broker
  call `recall` to push freshness (rejected — inverts the topology and re-adds orchestration to `recall`).

## Assumptions

| ID | Area | Assumption | Rationale |
|---|---|---|---|
| ASSUMP-IDENTITY | Auth | Identity is supplied by the broker; `recall` validates the token and never handles raw user credentials. | Matches the Faraday broker model (`agentic-mem.md` §7.2). |
| ASSUMP-OIDC | Auth | OIDC JWT is the authentication standard. | Standards-based, IdP-agnostic way to convey user identity across a boundary (ADR-001). |
| ASSUMP-TENANCY | Tenancy | Memory is segmented Tenant → Team → User via the bridge model (namespace-per-tenant + logical scoping). | Committed via ADR-011; supports hard cross-tenant isolation and intra-tenant sharing. |
| ASSUMP-STORE-CAP | Storage | The store provides graph + vector + keyword retrieval over rich bi-temporal edges; embedded SurrealDB is the committed engine. | Required capability set (`agentic-mem.md` §4–§5, §9); committed via ADR-003/ADR-009; spike validates (OQ-STORE). |
| ASSUMP-ASYNC | Architecture | Read path is synchronous and LLM-free; write/maintenance is asynchronous. | Latency and retry-safety (ADR-004). |
| ~~ASSUMP-LANG~~ | Tech | _Superseded — language is now a committed decision, not an assumption._ | Promoted to **ADR-009** (Rust + embedded SurrealDB). |
| ASSUMP-QUEUE | Tech | A durable work queue backs the async path; concrete product deferred. | Decoupling and retry-safety; product choice is non-architectural (OQ-QUEUE). |
| ASSUMP-MODELS | Tech | Embedding, reranker, and extraction/consolidation LLM are external, configurable providers. **Query embedding and rerank run on the read path; fact-content embedding and the extraction/consolidation LLM run off it** (ADR-012). | Keeps the read path **LLM-free** (not model-free); providers are configuration (OQ-MODELS). |

## Open Questions

Each has a defensible default that lets `/spec` proceed. **One exception:** OQ-STORE is non-blocking
for *spec authoring* but **load-bearing for the Rust / embedded commitment** (ADR-009) — run its spike
before Rust scaffolding, since a failure reopens the language choice. The rest are confirm-early-if-you-can.

| ID | Question | Plain-English | Options | Impact | Blocking? |
|---|---|---|---|---|---|
| OQ-STORE | Does embedded SurrealDB meet the latency/scale targets, or is a fallback needed? | Does the chosen all-in-one database actually keep up, or do we need a backup plan? | Validate and keep (expected) · fallback to graph DB + vector index | Engine is committed (ADR-003/009); the spike validates rather than selects. **Load-bearing for ADR-009** — a spike failure reopens both ADR-003 and ADR-009 (the language choice). | No for `/spec` authoring; **yes for the Rust / embedded commitment** — run before scaffolding |
| OQ-VOLUMES | What are the target corpus size, tenant counts, and SLOs? | How much memory, how many users, and how fast must it stay? | TBC with deployment context | Pins the scale/latency NFRs; defaults hold until set. | No |
| OQ-IDP | Which OIDC provider, and the exact claims/audience the broker emits in production? | Which login system issues the tokens in production, and what's inside them? | Any compliant OIDC provider (configuration); **Dex** used for testing (ADR-010) | `recall` is built IdP-agnostic; production values are config, tests use Dex. | No |
| OQ-CONSOLIDATE-CADENCE | When does consolidation run — on write, on a timer, or on idle? | How often do we distil raw memories into durable knowledge? | Idle-biased (default) · timer · on-write | Affects freshness of semantic facts vs background cost. | No |
| OQ-QUEUE | Which durable-queue product backs the async path? | What technology carries background work? | Store-backed · NATS · other | Operational, not architectural; deferrable. | No |
| OQ-MODELS | Which embedding / reranker / LLM providers, and how hosted? | Which AI models do the work, and where do they run? | External providers (configuration); local/in-process embedder as a latency option | **Read-path latency** (query-embed + rerank) *and* async cost (extraction/consolidation); local vs hosted for the embedder/reranker is a read-path latency decision (ADR-012). | No |

**Resolved**

- **OQ-LANG-EMBED** — *Is single-binary in-process store embedding a hard requirement?* **Resolved
  2026-06-20:** yes — committed to **Rust + embedded SurrealDB** (ADR-009), giving a genuine
  single-binary single-node deployment (NFR-MA3).
- **OQ-TENANCY** — *Per-user only, or team / organisation sharing?* **Resolved 2026-06-20:** adopt the
  **bridge model** (ADR-011) — namespace-per-tenant hard boundary with logical Team / User visibility
  within. Residual sub-question for `/spec` / the store spike: the threshold at which a team is
  promoted to its own database (the physical-isolation escape hatch).
- **OQ-FRESH-PLACEMENT** — *How much of ask→check→refresh→answer runs inside `recall` vs the broker?*
  **Resolved 2026-06-20:** **recall-side**, via the split committed in **ADR-013** — a cheap
  conditional source-change check on the read path (through the broker, as the user) plus an
  **asynchronous** source re-read/re-extract off it. The outbound `recall`→broker shape is named in
  [08 — Interfaces](./08-interfaces.md).
