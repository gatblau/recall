# Competitive Positioning — where `recall` sits, and what it adds

*Status: concept / design-research note. Companion to [`agentic-mem.md`](./agentic-mem.md) (concepts,
the named-system survey in its §8, the SurrealDB candidacy) and [`good-mem.md`](./good-mem.md) (the
reference blueprint §12 and the evaluation harness §13). This note answers one question: **how does
`recall` compare to the other agentic-memory initiatives, and where does it genuinely add value?**
Same tone as the companions — factual, non-promotional. Where `recall` leads it says so; where it
merely matches the field, or has not yet proven a claim, it says that too.*

> Read `agentic-mem.md §8` (the factual landscape) and `09-decisions.md` (ADR-012, ADR-014,
> ADR-015) first. This note assumes that vocabulary and refers to those decisions by number.

---

## 1. The shape of `recall` after ADR-014 and ADR-015

Two recent decisions define `recall`'s competitive identity more than any retrieval detail:

- **ADR-014 — `recall` is a passive store; freshness and orchestration are agent-side.** `recall`
  makes no outbound calls. The ask → check → refresh freshness loop runs at the agent, which is the
  only party with a local broker and document access. `recall` returns each fact's `origin_ref` and
  `modification_marker` on request so the agent can do the currency check itself.
- **ADR-015 — `recall` is LLM-free.** No fact extraction on the write path and no server-side
  episodic→semantic consolidation. The agent extracts structured facts and distils insights with its
  own model and writes them back; `recall` stores and serves them. The embedding and reranker model
  inferences on the read path are retained (they are retrieval, not reasoning).

The consequence: `recall` is a deliberately **dumb, fast, governed memory substrate**, not an
autonomous memory agent. This is an inversion of the mainstream design, in which the memory system
owns the reasoning. It is the single most important thing to understand before any feature-level
comparison.

> Note on the concept docs: the reference blueprint in `good-mem.md §12` (points 2, 3, 5) still
> describes *server-side* extraction, consolidation and freshness. That blueprint predates ADR-014
> and ADR-015; the shipped architecture has deliberately narrowed away from it. This note describes
> the system as decided, not as originally blueprinted.

---

## 2. The landscape (recap of `agentic-mem.md §8`)

A non-promotional recap; benchmark numbers move quickly and are not restated here.

- **Letta (formerly MemGPT)** — memory organised on an OS hierarchy (core / recall / archival); the
  **agent edits its own memory** via tools and decides what to promote or evict.
- **Zep / Graphiti** — a **temporal (bi-temporal) knowledge graph**; strong at evolving facts and
  contradiction handling; hybrid retrieval (embeddings + BM25 + graph) with no LLM on the read path.
  Reported as a leader on long-memory benchmark accuracy.
- **Mem0** — **vector-centric** semantic recall with priority scoring and forgetting/eviction; large
  ecosystem; positioned as turnkey application-level memory.
- **GraphRAG (Microsoft)** — entity graph plus community clustering and **pre-computed summaries**;
  strong for broad thematic questions, weak when data changes often (summaries need recomputation).

The recurring lesson from the survey: **there is no single winner** — the right design follows the
workload (how often facts change, how relational the questions are, how much the agent manages its
own memory, how strict freshness and permissions are).

---

## 3. Feature-level comparison

How `recall` maps onto each comparator. "Match" means `recall` does roughly what they do; "Differ"
is where it diverges.

| System | Their model | `recall`'s position |
|---|---|---|
| **Letta / MemGPT** | Memory logic lives *inside* the agent loop; the agent self-edits | **Differ.** `recall` externalises memory as a multi-tenant service with hard isolation and audit; it does not manage itself. ADR-014/015 hand reasoning to the agent — but the *store* stays a service, not an in-agent scratchpad. |
| **Zep / Graphiti** | Bi-temporal KG, hybrid retrieval, LLM-free reads | **Match + extend.** `recall` follows this design closely (ADR-002 bi-temporal supersession; ADR-012 LLM-free read path). It extends with source-freshness provenance (ADR-014) and multi-tenant governance, and uses one multi-model engine rather than a graph DB plus add-ons. |
| **Mem0** | Server-side extraction + eviction; turnkey, vector-centric | **Differ.** `recall` pushes extraction and consolidation *out* to the agent (ADR-015). It is a lower-level building block, not drop-in memory; it trades turnkey convenience for a smaller trust surface and no LLM dependency. |
| **GraphRAG** | Entity graph + pre-computed community summaries | **Differ.** `recall` avoids pre-compute staleness; bi-temporal live facts with supersession are the answer to changing data, not periodic summary recomputation. |

---

## 4. Where `recall` genuinely differentiates

Four things `recall` does that the off-the-shelf systems mostly do not:

1. **The passive-store inversion (ADR-014/015) — the core idea.** Mem0, Zep and Letta all put
   reasoning *inside* the memory system. `recall` bets the opposite: keep the store dumb, fast,
   secure and verifiable; let the agent (which already has the LLM and the local broker) do
   extraction, consolidation and freshness. In the actual deployment the broker is a
   per-agent local component, so a centrally-hosted memory service *cannot* reach it — the
   server-side freshness check (old ADR-013) was literally unreachable there. The inversion is not a
   preference; it follows from the deployment reality (RFC 01 motivation).
2. **Source-freshness provenance loop.** On request, every sourced fact carries `origin_ref` (the
   document handle) and `modification_marker` (the version the note was based on), so the agent's
   broker can run an `If-Modified-Since`-style check and re-read only what changed. Most memory
   systems do not model source-document currency at all. This is the standout capability for agents
   grounded on a *changing* corpus.
3. **Enterprise governance as a first-class concern.** Per-user OIDC identity (`recall` holds no raw
   credentials), namespace-per-tenant hard isolation (ADR-011), a write-gate that quarantines or
   rejects poison/instruction-like content, PII redaction, an audit log, and verifiable deletion.
   This is stronger than the single-tenant OSS memory libraries.
4. **Operational profile.** A single Rust binary that can run embedded (ADR-009), with an LLM-free
   interactive read path. Cheaper and lower-latency to run than anything that keeps an LLM in the
   request loop, and with no external model provider required to serve reads.

---

## 5. Where the value is weaker — the honest trade-offs

1. **The retrieval core is competent, not novel.** Bi-temporal KG + hybrid recall + cross-encoder
   rerank is essentially the Zep / Graphiti design, which already leads the benchmarks. `recall`
   replicates the state of the art here; it does not advance it.
2. **It gives up the turnkey value proposition.** By exporting extraction and consolidation to the
   agent (ADR-015), `recall` is *more work to adopt* than Mem0 or Zep. Its value materialises only
   when the integrator already owns a capable agent and broker. Outside that ecosystem, the
   off-the-shelf systems deliver more for less.
3. **No measurements of its own yet.** The concept docs cite others' LoCoMo / LongMemEval / BEAM
   numbers, but `recall` has not been run against them. "Competitive accuracy" is therefore
   aspirational — the evaluation harness in `good-mem.md §13` is a plan, not a result.
4. **Coupling and maturity risk.** The differentiators (freshness loop, passive store) pay off
   specifically inside this per-agent-local broker architecture, so the addressable niche is narrow. And the chosen
   engine carries the risks the team already recorded: SurrealDB / SurrealKV maturity and the
   "one engine, one blast radius" concentration (`agentic-mem.md §9.4`, RISK-001).

---

## 6. Verdict — does it add value?

**Yes, in a specific niche, and not on the axis the headline competitors compete on.**

`recall` adds real value as a **governed, source-grounded, operationally-light memory substrate** for
an agent platform that already does its own reasoning. It is not trying to win the "best drop-in
agent memory" contest against Mem0 or Zep, and should not be judged there. Its bet is architectural —
a dumb-but-trustworthy store paired with a smart agent — and after ADR-014/015 that bet is
differentiated and internally consistent.

The exposure is equally clear: the niche is narrow, the differentiators are ecosystem-coupled, and
the retrieval quality is so far asserted rather than proven. The single highest-value move to
de-risk the positioning is to **convert claim into evidence** — stand up the `good-mem.md §13`
evaluation harness (LongMemEval for knowledge-update and abstention; LoCoMo for temporal and
adversarial; a representative end-to-end agentic-search task with facts that change mid-session) and
publish the numbers next to the qualitative claims in §4.

---

## 7. Choose-`recall`-when (and when not to)

A blunt decision aid.

**Choose `recall` when:**
- You are building on a per-agent-local broker stack and need source
  freshness checked by the party that owns the documents.
- Multi-tenant isolation, per-user OIDC scoping, audit and verifiable deletion are hard requirements.
- You want a low-latency, LLM-free read path and a single-binary deployment with no model provider on
  the serving path.
- Your agent is already capable of extracting structured facts and distilling insights itself.

**Prefer an off-the-shelf system (Mem0 / Zep / Letta) when:**
- You want turnkey memory with server-side extraction and consolidation out of the box.
- You have no broker and no need for source-document freshness.
- Single-tenant is fine and governance is light.
- You want published benchmark accuracy today rather than a harness still to be run.

---

## 8. References

- Internal: [`agentic-mem.md`](./agentic-mem.md) §8 (landscape), §9 (SurrealDB candidacy);
  [`good-mem.md`](./good-mem.md) §12 (blueprint), §13 (evaluation harness);
  [`requirements.md`](./requirements.md);
  `docs/design/agentic-memory/09-decisions.md` (ADR-002, ADR-009, ADR-011, ADR-012, ADR-014, ADR-015);
  `docs/design/agentic-memory/10-risks.md` (RISK-001).
- External (carried from the companion notes, not re-verified here):
  - [AI Agent Memory 2026 — Comparing Mem0, Zep, Graphiti, Letta, LangMem (Medium)](https://medium.com/@wasowski.jarek/i-compared-5-ai-agent-memory-systems-across-6-dimensions-none-wins-6a658335ed0a)
  - [Agent Memory at Scale 2026: Letta, Zep, Mem0, LangMem Compared (AgentMarketCap)](https://agentmarketcap.ai/blog/2026/04/10/agent-memory-vendor-landscape-2026-letta-zep-mem0-langmem)
  - [Graphiti: Knowledge graph memory for an agentic world (Neo4j)](https://neo4j.com/blog/developer/graphiti-knowledge-graph-memory/)
  - [State of AI Agent Memory 2026: Benchmarks, Architectures & Production Gaps (Mem0)](https://mem0.ai/blog/state-of-ai-agent-memory-2026)
  - [AI Memory Benchmarks 2026: LoCoMo, LongMemEval & BEAM (Mem0)](https://mem0.ai/blog/ai-memory-benchmarks-in-2026)
