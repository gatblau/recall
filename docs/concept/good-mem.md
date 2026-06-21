# Building Excellent Agentic Memory

*Status: concept / design-research note. Companion to [`agentic-mem.md`](./agentic-mem.md), which
covers the concepts, the graph-theory angle, the candidate store (SurrealDB), and the Faraday-facing
REST API. This document goes one level deeper and answers a narrower question: **given all of that,
how do we make `recall`'s memory not merely functional but genuinely excellent?** Same tone as the
companion — factual, pragmatic, no marketing. Where the field has a clear answer it is stated; where
it is unsolved, that is said plainly.*

> Read `agentic-mem.md` first. This note assumes its vocabulary (episodic/semantic/procedural,
> the bi-temporal model, the write/read/maintain operations, the lazy "learn-as-you-ask" Faraday
> model) and refers back to its sections by number.

---

## 1. What "good" actually means

"Good memory" is not one property; it is a stack of them, and they trade off against each other. The
single most useful framing from the current literature is a **four-layer metric stack** — you cannot
claim excellent memory unless you can measure all four:

| Layer | The question it answers | Example metrics |
|---|---|---|
| **1. Task effectiveness** | Did remembering actually make the agent succeed? | Task success rate, plan completion, answer correctness |
| **2. Memory quality** | Are the stored facts right, current, and consistent? | Precision/recall of recall, contradiction rate, staleness rate, hallucination/omission per fact |
| **3. Efficiency** | What did it cost to get there? | Retrieval latency, tokens consumed per query, storage growth over time |
| **4. Governance** | Is it safe, private, and accountable? | Privacy/PII leakage, deletion compliance, poisoning resistance, auditability |

The trap is optimising layer 2 (or worse, a single retrieval metric like precision@k) and declaring
victory. Classical retrieval metrics *"ignore whether agents actually use retrieved information
correctly or whether retrieval justified its latency cost."* Excellence means **layer 1 goes up
without layers 3 and 4 going wrong.**

### The sobering reality check
Two findings should calibrate ambition:

- **Memory architecture is high-leverage.** *"The gap between 'has memory' and 'does not have memory'
  is often larger than the gap between different LLM backbones."* Investing in memory design rivals
  investing in a bigger model. This is the good news.
- **Benchmarks flatter you.** Systems that score near-perfect on the popular conversational benchmark
  (LoCoMo) *"plummet to 40–60%"* when memory is embedded in a complete agentic task with
  interdependencies (MemoryArena). A number on a leaderboard is not the same as memory that holds up
  inside real work. This is the warning.

So "excellent" is defined against **layer-1 success inside realistic, multi-step, evolving-fact
workloads** — not against a single QA leaderboard.

---

## 2. The quality bar `recall` specifically has to clear

`recall` is not a generic chatbot memory. It serves the agentic-search process described in
`agentic-mem.md` §7 (Faraday's learn-as-you-ask model). That imposes four extra, non-negotiable
quality requirements on top of the generic ones:

1. **Freshness is correctness.** In agentic search, a confidently-returned stale fact is a *wrong
   answer*, not a stale cache entry. The "high-relevance fact becomes confidently wrong when
   circumstances change" failure (e.g. someone changed jobs) is the central named production gap. A
   good `recall` treats currency as a first-class quality dimension, not an afterthought (§5, §8
   below).
2. **Provenance is mandatory.** Every fact must carry where it came from and when, because Faraday
   answers cite sources and edit dates, and because provenance is also the backbone of trust and
   poisoning-defence (§9 below).
3. **Permissions are inherited, not invented.** Memory quality includes *not leaking* — a fact one
   user is allowed to see must never surface for another. This is layer-4 governance, and it follows
   the per-user model of `agentic-mem.md` §7.2 / §11.
4. **It must learn cheaply over time.** The lazy model means quality should *improve with use* while
   cost stays bounded — the day-1 / day-30 / day-100 evolution in `agentic-mem.md` §7.1.

Keep these four in view: every technique below is judged not just on generic accuracy but on whether
it serves these.

---

## 3. The shape of an excellent memory pipeline

Before the detail, the whole picture. An excellent system is a loop with a **fast synchronous read
path** and a **slower asynchronous write/maintain path**, deliberately separated so the expensive
work never sits in front of the user:

```
                    ┌──────────────────────── ASYNC (off the hot path) ────────────────────────┐
   interaction ──▶  │  WRITE: filter → extract → canonicalise → dedup/resolve → score → store   │
                    │  MAINTAIN: consolidate (episodic→semantic) · detect contradictions ·       │
                    │            supersede · decay/forget · re-embed                              │
                    └──────────────────────────────────────────────────────────────────────────┘
                                                   │  (reads from / writes to)
                                                   ▼
                                         ┌───────────────────┐
   query ──────────────────────────────▶│   hybrid store     │
                    SYNC (fast):         │ graph + vector +   │──▶  rank → freshness-check → answer
                    retrieve → rerank    │ keyword, bi-temporal│      (+ provenance, + confidence)
                    → gate → freshness   └───────────────────┘
```

The two golden rules that make this excellent rather than merely working:

- **Keep LLM calls off the read path.** Retrieval should hit a target like sub-50ms for similarity
  search and stay inside a ~200ms total budget for interactive use. LLM reasoning belongs on the
  write/consolidation side, which can be async and is allowed to be slow.
- **Do the hard thinking when nobody is waiting.** Consolidation, contradiction resolution, and
  re-embedding happen in the background — ideally during idle periods, mirroring how human memory
  consolidates "offline."

The next sections take each stage and describe what *excellent* looks like, versus merely adequate.

---

## 4. The write path — getting facts in cleanly

Garbage in, garbage forever. The write path is where memory quality is won or lost, because a bad
fact written confidently will be retrieved confidently for months. Excellent write paths do six
things, in order.

### 4.1 Filter before you store
Do not store everything. Most interaction content is noise. Filter to the salient — the facts, the
decisions, the preferences, the outcomes — and drop the rest. Storing everything bloats the store,
slows retrieval, and raises cost with no quality gain.

### 4.2 Extract structured facts, not text blobs
Turn raw content into structured assertions ("Team Alpha owns the `orders` table", with fields) —
ideally graph nodes/edges per `agentic-mem.md` §5. Structured facts deduplicate, supersede, and
retrieve far better than paragraphs. A practical optimisation that the strongest production systems
use: **single-pass extraction** — one LLM call that extracts the fact and adds it, deferring
conflict resolution to later — which *cuts write-time LLM calls by 60–70%* versus extract-then-
reconcile-synchronously.

### 4.3 Count the agent's own statements as facts
A subtle but real coverage gap: systems often store what the *user* said but ignore what the *agent*
confirmed, recommended, or concluded. Excellent memory treats **agent-generated facts as
first-class** — "we agreed to deploy on Friday" is as much a memory as anything the user typed.

### 4.4 Canonicalise and resolve identity — and don't conflate the three steps
This is where most knowledge-graph memories quietly rot. Three *distinct* operations are routinely
collapsed into one fuzzy check, and that conflation is precisely what corrupts the graph:

| Step | What it does |
|---|---|
| **Normalisation** | Makes records comparable (casing, formats, units, aliases) |
| **Deduplication** | Removes records that are literally the same |
| **Entity resolution** | Decides which *differently-worded* records describe the **same real thing** ("Sarah", "S. Jones", "the lead engineer") |

Entity resolution uses a blend of string/name similarity (e.g. Levenshtein), co-occurrence with
other entities, and temporal proximity of mentions. A pragmatic, cost-aware ladder is **rules → ML →
LLM**: cheap deterministic rules first, learned matching for the ambiguous middle, an LLM only for
the genuinely hard cases. Getting this right is the difference between a graph that sharpens with use
and one that fills with near-duplicate ghosts of the same entity.

### 4.5 Score importance and confidence at write time
Two scores travel with every fact for its whole life:

- **Importance / salience** — how much this matters. Feeds the salience floor that protects it from
  forgetting (§8).
- **Confidence** — how sure we are. Facts born of weak retrieval, a noisy summary, or an inference
  start *uncertain* and should decay faster and be trusted less than verified facts. Never give a
  guess the lifespan of a fact.

### 4.6 A write gate, not an open door
Because memory can be *poisoned* (§9), the write path is also a security boundary. An excellent
system validates writes — trust-scoring the source, refusing or quarantining low-trust or
instruction-like content — rather than accepting anything an interaction proposes. Write-gate
validation is one of the two industry-wide blind spots (§9); building it in from the start is a
differentiator, not a nicety.

**Write-path summary:** filter → extract (single-pass) → canonicalise → resolve identity (rules→ML→
LLM) → score importance+confidence → gate → store. All of it async, none of it in front of the user.

---

## 5. The store — represent facts so they can evolve

The representation is covered in `agentic-mem.md` §4–§5 (hybrid graph + vector + keyword, with the
**bi-temporal model**). One point deserves elevating to a quality principle because it is the named
gap that separates good systems from the rest:

> **Treat changing facts as *evolution*, not *replacement*.**

The common failure is that systems "treat state changes as replacements rather than evolution" — they
overwrite "Sarah is lead" with "Tom is lead" and lose the history, the transition, and the ability to
answer "who *was* lead in March?" The bi-temporal model (validity interval + ingestion time, end the
old edge instead of deleting it) is the concrete fix, and it is what makes the big benchmark gains in
*temporal reasoning (+~30 points)* and *knowledge-update* questions possible. If you implement one
"advanced" thing well, make it this.

---

## 6. Consolidation — turning experience into knowledge

Consolidation is the engine of "gets smarter over time." It is also the most-skipped stage and the
one with the most subtle failure modes.

### 6.1 The principle: episodic → semantic (and why)
Borrowed from Complementary Learning Systems theory: a fast system captures single experiences
(episodic), and over time repeated episodes consolidate into slow-changing general knowledge
(semantic). For `recall` this means a background job that notices recurring patterns across many
episodes and distils them into durable facts — the move described in `agentic-mem.md` §3.

### 6.2 The techniques worth using
- **Reflection** (the Generative Agents mechanism): periodically synthesise recent episodes —
  weighted by recency, relevance, and salience — into higher-level insights, and store those.
- **Surprise-/error-driven consolidation** (e.g. NEMORI's approach): prioritise consolidating what
  was *unexpected*. Information that diverged from what the system predicted is the highest-value
  thing to remember; the routine and anticipated is cheap to reconstruct. This is a strong filter
  for "what deserves memory."
- **Offline consolidation during idle periods.** Run the heavy synthesis when the system is not
  serving queries, mirroring biological sleep-time consolidation. This keeps it off the hot path and
  is where the field thinks principled consolidation is heading.

### 6.3 The two failure modes to design against
Consolidation that is naïve makes memory *worse*, confidently:

- **Summarisation drift.** Repeated compression silently discards low-frequency details, producing
  "pathological failures on edge cases requiring rare instructions." Guard against it by keeping the
  source facts (don't only keep the summary), and by protecting rare-but-important items with a
  salience floor.
- **Self-reinforcing error.** A wrong reflection becomes permanent "knowledge" that is never
  reconsidered. Guard against it with **uncertainty decay** (confidence in an inference erodes unless
  re-confirmed), **external validation** (check a reflection against source facts before promoting
  it), **expiration policies** on inferred conclusions, and adversarial probing. A consolidated
  insight should never outrank the verified facts it was derived from.

Consolidation excellence is therefore not "summarise aggressively" — it is "distil the surprising,
verify before promoting, keep the sources, and let inferences expire."

---

## 7. Retrieval — getting the right memory back, fast

This is the read path. It must be fast (§3) and it must be *right*, and the two pull against each
other. The current consensus design is a **two-stage funnel**.

### 7.1 Stage 1 — broad multi-signal recall
Cast a wide, cheap net combining several scoring passes and fusing them:

- **Semantic** (dense vector) — "means something like this."
- **Keyword** (BM25) — exact terms, names, codes the embedding blurs.
- **Entity / graph** — facts attached to the entities in the query, reachable by traversal.

Multi-signal fusion *"outperforms any individual signal"* — this is the empirically-backed minimum
viable baseline, not a luxury. Add metadata filters (time window, user, source) so a query like
"what did this user say about billing in the last 30 days" works.

### 7.2 Stage 2 — precise reranking
Take the top ~20–50 candidates from stage 1 and rerank them with a **cross-encoder**, which sees
query and candidate together and judges relevance far more finely than vector distance. Cross-
encoders are too slow to run over the whole store, which is exactly why the funnel exists: cheap
recall first, expensive precision second. *"Hybrid retrieval followed by cross-encoder reranking
dominates all single-stage methods."*

### 7.3 The query is not the user's words
A crucial, under-appreciated point: *"user input often makes poor search terms."* The raw question is
frequently a bad query. Techniques to bridge the gap:

- **Query reformulation** — rewrite the question into good retrieval terms.
- **HyDE** (hypothetical document embeddings) — generate a hypothetical answer and search with *that*.
  Useful, but with an honest caveat from the benchmarks: HyDE and query rewriting *do not always
  help* and can underperform plain dense retrieval in some settings. Treat them as tools to A/B, not
  defaults to adopt blindly.

### 7.4 Recency weighting and freshness
Multiply relevance by a recency factor so stale, untouched memories fade in ranking (the Ebbinghaus
idea applied to retrieval, §8). And for `recall` specifically, fold the **freshness-verification
loop** (`agentic-mem.md` §7.1: check whether the source changed, refresh if so) into the read so a
confidently-stale fact is caught before it is returned.

### 7.5 Retrieval gating — did it actually help?
Excellent systems do not blindly inject whatever stage 2 returns. They **gate**: if the best
candidate is weak, retrieve nothing rather than pad the context with marginally-relevant noise (which
costs tokens and can mislead). The metric that matters is not "did we retrieve something similar" but
"did retrieval improve the outcome and justify its latency."

### 7.6 The frontier worth knowing about
Semantic retrieval answers *"what looks like this?"* but not *"what caused this?"* **Causally-grounded
retrieval** (traversing cause→effect edges in the graph) is an open research direction and a natural
fit for a graph-backed memory — worth designing the graph so it *could* support causal edges later,
even if v1 doesn't use them.

---

## 8. Forgetting — a feature, engineered deliberately

Without forgetting, memory grows without bound, retrieval drowns in noise, and old contradictions
linger. But crude forgetting throws away the wrong things. Excellent forgetting is graceful and
protected.

### 8.1 Gradual decay, not hard deletion
Model decay on the **Ebbinghaus forgetting curve**: memories fade *exponentially* but *gradually*,
fastest right after creation and slower thereafter, and **reinforcement resets the clock** — a memory
that is recalled, cited, or re-verified strengthens. This is far better than a hard TTL that deletes
abruptly: frequently-used facts persist naturally, genuinely-unused ones fade. New memories get a
short protection window before decay starts.

### 8.2 The composite production policy
The field's recommended default, assembled:

- **TTL** on long-tail entries to bound storage.
- **LRU-style / exponential decay** on retrieval scores to bound interference.
- **Confidence-linked decay** — low-confidence memories fade faster (§4.5).
- **Active supersession on every write** so contradictions never accumulate (§5).
- **A salience floor** — the critical guard rail — so a high-importance fact (production region,
  security boundary) is *never* pruned on time-and-disuse alone.

### 8.3 Selective forgetting and the right to be deleted
Governance forces a stronger form: the ability to *deliberately and verifiably* forget specific
information — a user exercising deletion rights, a fact that must be expunged. This connects to
**machine unlearning** and is a layer-4 requirement, not just a housekeeping one. Crucially,
**post-deletion verification** (proving the thing is actually gone, including from derived summaries
and embeddings) is one of the two industry-wide blind spots. Building auditable deletion in early is
a real quality edge.

---

## 9. Trust, safety, and governance — the layer everyone underbuilds

Memory that influences decisions is an attack surface and a liability. This is layer 4, and the
research is blunt that *no published memory architecture covers all the governance primitives* — so
doing it well is genuinely differentiating, not table stakes.

### 9.1 Memory poisoning is real and effective
Adversaries can inject malicious "facts" through ordinary query interactions that corrupt long-term
memory and steer future behaviour. Reported injection success rates are *over 95%* under favourable
conditions. For an agentic-search system that *learns from documents*, a booby-trapped document is
exactly the vector — which is why Faraday marks returned data untrusted (`agentic-mem.md` §7.2).

### 9.2 The defences worth building
- **Write-gate validation** (§4.6) — composite trust scoring across orthogonal signals before
  anything enters memory; quarantine the doubtful.
- **Trust-aware retrieval / sanitisation** — temporal decay plus pattern-based filtering at read
  time, so even if something poisoned slips in, it is down-weighted and screened. The catch, stated
  honestly: trust thresholds need careful calibration — too strict rejects good facts, too loose lets
  poison through.
- **Provenance everywhere** — every fact tagged with source and authorised actor, so the system can
  treat only facts from trusted origins as actionable and everything else as untrusted context. This
  is the same provenance that serves freshness and citations (§2) — one mechanism, three payoffs.
- **Separate untrusted data from instructions** — content read from documents is *data to remember*,
  never *instructions to act on*. This is the API-side discipline of `agentic-mem.md` §11.2 (P8).

### 9.3 Privacy and compliance as memory features
Encryption, per-user scoping (no cross-user leakage — §2 requirement 3), PII redaction on the write
path, auditable deletion (§8.3), and an audit log of what was learned, when, and from where. These
are not bolt-ons; in a memory that accumulates real organisational facts, they *are* part of what
"good" means.

---

## 10. Who decides — heuristic vs. prompted vs. learned control

A cross-cutting design choice: *what* policy decides when to store, retrieve, update, summarise, and
discard? Three levels, increasing in capability and complexity:

| Control policy | How it works | When it fits |
|---|---|---|
| **Heuristic rules** | Fixed thresholds and rules (store if salience > X, decay at rate Y) | Start here. Predictable, debuggable, cheap. |
| **Prompted self-control** | The LLM decides memory operations via tools (MemGPT/Letta style) | When rules are too rigid for the variety of content. |
| **Learned control (RL)** | The five memory operations are trainable tools optimised by reinforcement learning (AgeMem, MemRL) | The frontier: discovers non-obvious tactics like proactive summarisation before context fills; highest capability, highest complexity and opacity. |

The pragmatic recommendation for `recall`: **start heuristic, instrument everything, and only climb
to prompted or learned control where the data shows heuristics leaving quality on the table.** Learned
control is powerful but sacrifices the auditability that the governance layer (§9) needs, so adopt it
with eyes open.

---

## 11. Cost and latency — excellence is also cheap and fast

Quality that is too slow or too expensive does not ship. Concrete budgets and levers from current
practice:

- **Latency budgets.** Interactive chat breaks past ~200ms; an enterprise copilot can stretch to
  ~400ms inside a few-second window; async/batch work has no tight ceiling. Similarity search itself
  should be **sub-50ms** even over millions of vectors, or it eats the whole budget before reranking
  runs.
- **Async-by-default writes** (§3, §4.2). The single biggest latency win: keep extraction and
  consolidation off the response path; single-pass extraction cuts write-time LLM calls 60–70%.
- **Token efficiency is the cost story.** The strongest reported systems answer using **~6,900 tokens
  per query versus ~26,000 for full-context** baselines — memory is partly a *cost-reduction*
  technique, not only a capability one. Return only what is needed (the API discipline of
  `agentic-mem.md` §11.2 P4).
- **Semantic caching.** Cache answers keyed on query embeddings; for repetitive workloads this has
  reported *up to ~73% cost reduction*. Pairs naturally with the lazy "frequently-asked topics get
  cached" evolution of `agentic-mem.md` §7.1.
- **Async prefetching.** For predictable next steps, speculatively fetch likely memories during the
  model's "think time" so the latency is hidden entirely.
- **Embedding cost is real but small** (~$0.13 per million tokens for a large embedding model, with
  per-call latency from a few ms to hundreds), so budget it — but it is rarely the dominant cost
  versus generation.

---

## 12. A reference blueprint for `recall`

Pulling all of the above into one concrete (still proposal-level) design. None of these names exist
in the repo yet — `recall` is greenfield — so treat them as a design target, not a description.

1. **Store:** one hybrid multi-model store (SurrealDB is the §9 candidate in `agentic-mem.md`) holding
   a **bi-temporal knowledge graph** with vector and BM25 indexes; rich edges carry validity
   interval, ingestion time, confidence, salience, and source.
2. **Write path (async):** filter → single-pass structured extraction (including agent-stated facts)
   → normalise → resolve identity (rules→ML→LLM) → score importance + confidence → **write-gate**
   trust check → store. Contradiction resolution deferred to maintenance.
3. **Maintenance (idle/background):** surprise-weighted consolidation (episodic→semantic) with
   external validation before promotion and uncertainty-decay on inferences; bi-temporal supersession
   of contradictions; Ebbinghaus decay with a salience floor; auditable selective forgetting;
   re-embedding.
4. **Read path (sync, LLM-free, <~200ms):** query reformulation → multi-signal stage-1 recall
   (semantic + BM25 + graph) with metadata filters → cross-encoder rerank of top-k → retrieval gate →
   recency/freshness check → return ranked facts **with provenance and confidence**.
5. **Freshness:** server-side ask→check→refresh→answer loop (`agentic-mem.md` §7.1), conditional
   requests for cheap currency checks.
6. **Governance throughout:** provenance on every fact, per-user scoping and permission inheritance,
   trust-aware retrieval, PII redaction, audit log, verifiable deletion.
7. **Control:** heuristic to begin; instrument for a later move to prompted/learned control where it
   pays.
8. **Cost/latency:** async writes, semantic cache, async prefetch, tight token budgets on responses.

---

## 13. How to know it is actually excellent — the evaluation harness

You cannot improve what you do not measure, and §1 warned that the easy metrics lie. A credible
`recall` evaluation harness should:

- **Measure all four layers** (§1), not just retrieval accuracy. Track task success, contradiction
  rate, staleness rate, latency, tokens/query, storage growth, and governance metrics (leakage,
  deletion compliance, poisoning resistance) over time.
- **Use the recognised benchmarks for orientation, not as the finish line.** *LongMemEval* (five
  abilities: information extraction, multi-session reasoning, temporal reasoning, knowledge updates,
  abstention) and *LoCoMo* (single-hop, multi-hop, temporal, commonsense, adversarial/unanswerable)
  are the standard conversational tests; *BEAM* pushes to 1M–10M-token volumes. Treat them as smoke
  tests.
- **Evaluate inside whole tasks.** Because LoCoMo-perfect systems crater on *MemoryArena*-style
  embedded agentic tasks, the real measure for `recall` is end-to-end success on representative
  agentic-search workflows with interdependent steps and *evolving* facts — not isolated QA.
- **Test the failure modes explicitly.** Abstention (does it decline when evidence is insufficient
  rather than hallucinate?), knowledge updates (does it follow a fact that changed?), and poisoning
  resistance (does an injected false fact survive?) are where memory quietly fails — measure them
  directly.

A blunt north-star metric: **on a realistic agentic-search task with facts that change over the
session, does success rate stay high while tokens/query and latency stay bounded and no poisoned or
stale fact is returned as truth?**

---

## 14. A pragmatic build order

Excellence is reached incrementally; trying to build all of the above at once is how projects stall.
A sensible crawl-walk-run, each stage shippable:

1. **Crawl — a memory that works and is honest.** Hybrid store; structured single-pass extraction;
   two-stage retrieval (multi-signal + rerank); provenance on every fact; per-user scoping;
   heuristic control; async writes. Measure layers 1–3. This already beats most "stuff text in a
   vector DB" systems.
2. **Walk — a memory that stays true over time.** Add the bi-temporal model and supersession;
   Ebbinghaus decay with a salience floor; freshness-verification loop; write-gate trust check;
   audit log and verifiable deletion. Now temporal/knowledge-update quality and governance are real.
3. **Run — a memory that gets smarter.** Add surprise-weighted offline consolidation with external
   validation and uncertainty decay; retrieval gating; semantic cache and prefetch; and — only where
   instrumentation shows heuristics leaving quality on the table — prompted or learned control.

At each step, the evaluation harness (§13) tells you whether the added complexity actually moved
layer-1 success, or just added moving parts. That feedback loop — not any single technique — is what
ultimately produces excellent memory.

---

## Sources

Accessed June 2026. (See `agentic-mem.md` for the agentic-memory, graph, SurrealDB, and API sources
this note builds on.)

- [Memory for Autonomous LLM Agents: Mechanisms, Evaluation, and Emerging Frontiers (arXiv 2603.07670)](https://arxiv.org/html/2603.07670v1)
- [State of AI Agent Memory 2026: Benchmarks, Architectures & Production Gaps (Mem0)](https://mem0.ai/blog/state-of-ai-agent-memory-2026)
- [AI Memory Benchmarks 2026: LoCoMo, LongMemEval & BEAM (Mem0)](https://mem0.ai/blog/ai-memory-benchmarks-in-2026)
- [Evaluating Very Long-Term Conversational Memory of LLM Agents — LoCoMo (Snap Research)](https://snap-research.github.io/locomo/)
- [What Deserves Memory: Adaptive Memory Distillation for LLM Agents (arXiv 2508.03341)](https://arxiv.org/abs/2508.03341)
- [Position: Episodic Memory is the Missing Piece for Long-Term LLM Agents (arXiv 2502.06975)](https://arxiv.org/pdf/2502.06975)
- [Episodic Memory for AI Agents: How It Works (Atlan)](https://atlan.com/know/episodic-memory-ai-agents/)
- [Searching for Best Practices in Retrieval-Augmented Generation (arXiv 2407.01219)](https://arxiv.org/pdf/2407.01219)
- [Optimizing RAG with Hybrid Search & Reranking (Superlinked VectorHub)](https://superlinked.com/vectorhub/articles/optimizing-rag-with-hybrid-search-reranking)
- [Hybrid Search for RAG: Vector + Keyword + Reranking (buildmvpfast)](https://www.buildmvpfast.com/blog/hybrid-search-rag-vector-keyword-reranking-2026)
- [Entity Resolution at Scale: Deduplication Strategies for Knowledge Graph Construction (Medium)](https://medium.com/@shereshevsky/entity-resolution-at-scale-deduplication-strategies-for-knowledge-graph-construction-7499a60a97c3)
- [Knowledge Graphs: Normalization, Deduplication, and Entity Resolution (Medium)](https://medium.com/@QuarkAndCode/knowledge-graphs-normalization-deduplication-and-entity-resolution-a8ba384d539c)
- [iText2KG: Incremental Knowledge Graphs Construction Using LLMs (arXiv 2409.03284)](https://arxiv.org/pdf/2409.03284)
- [Memory eviction and forgetting in AI agents (Mem0)](https://mem0.ai/blog/memory-eviction-and-forgetting-in-ai-agents)
- [FadeMem: Biologically-Inspired Forgetting for Efficient Agent Memory (arXiv 2601.18642)](https://arxiv.org/pdf/2601.18642)
- [Memory Poisoning Attack and Defense on Memory Based LLM-Agents (arXiv 2601.05504)](https://arxiv.org/abs/2601.05504)
- [A Survey on the Security of Long-Term Memory in LLM Agents (arXiv 2604.16548)](https://arxiv.org/html/2604.16548v1)
- [The Landscape of Agentic Reinforcement Learning for LLMs: A Survey (arXiv 2509.02547)](https://arxiv.org/pdf/2509.02547)
- [Memory Retrieval Latency Budgets (Supermemory)](https://blog.supermemory.ai/latency-budgets-memory-retrieval/)
- [The 2026 Token Optimization Playbook (Mem0)](https://mem0.ai/blog/the-2026-token-optimization-playbook-cut-ai-agent-memory-costs-3%E2%80%934x)
- [GPT Semantic Cache (arXiv 2411.05276)](https://arxiv.org/html/2411.05276v3)
