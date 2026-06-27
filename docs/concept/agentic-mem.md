# Agentic Memory — Concepts for Design

*Status: concept note. Purpose: give everyone working on `recall` a shared, plain-English
understanding of agentic memory so we can design one. This is background and analysis, not a
specification. Where it makes a recommendation, it says so; where the field disagrees, it says
that too.*

---

## 1. What this document is for

`recall` is going to need a memory. Before we design it, we should agree on what "memory" actually
means for an agent, what other people have built, and what trade-offs we are signing up for. This
note pulls together the current thinking on agentic memory, the graph-theory angle, the practical
mechanics of reading/writing/searching memory, and the specific agent environment `recall` is
designed to operate inside. It is written to be read by an engineer, a
product person, or a reviewer — no prior background assumed.

That assumed environment — a sandboxed agent, a trusted credential-holding broker, and a
"learn-as-you-ask" model of enterprise search — is described in section 7, because its model is
directly relevant to how `recall` might behave.

> **Companion documents:** this note covers the *concepts and building blocks*.
> [`good-mem.md`](./good-mem.md) goes a level deeper into *how to make the memory excellent* — the
> write/consolidate/retrieve/forget techniques, the trust-and-governance layer, the cost/latency
> budgets, the evaluation harness, and a staged build order. [`requirements.md`](./requirements.md)
> distils both notes into numbered functional and non-functional requirements (including the OIDC
> authentication model) that trace back to the sections here. Read this document first, then
> `good-mem.md`, then `requirements.md`.

> **Architecture update (ADR-014, ADR-015).** This note predates two decisions that reshaped
> `recall`. Freshness checking and orchestration moved to the **agent** (ADR-014 — `recall` makes no
> outbound call and runs no freshness loop), and fact extraction + episodic→semantic consolidation
> moved to the **agent** (ADR-015 — `recall` is LLM-free). The *technique analysis* below stands;
> only the **locus** changed. Wherever a section proposes that `recall` extract facts, consolidate
> insights, or run the freshness loop **server-side**, read it as the conceptual pipeline: in the
> shipped architecture the agent performs those steps with its own model and writes the results back
> as structured, agent-asserted facts, and `recall` stores, ranks, and serves them. Embedding +
> reranker (read-path model inferences) remain in `recall` (ADR-012).

---

## 2. The problem memory solves

A language model on its own is stateless. Every time you call it, it starts from nothing and only
knows what is in the prompt you hand it. That works for a single question. It falls apart the
moment you want an agent that:

- remembers what happened earlier in a long task,
- remembers things across sessions (yesterday's decision, last week's user preference),
- learns from doing the same kind of work repeatedly,
- and does all of that without re-reading every source document every time.

"Agentic memory" is the system that sits beside the model and gives it those abilities. It is the
difference between an assistant that forgets you between every message and one that builds up a
working picture of your world. The plain framing: **memory turns a reactive tool into something
that accumulates and reuses knowledge.**

The context window is *not* memory. It is a small, expensive, temporary workspace. Memory is the
larger, cheaper, persistent store that we selectively load *into* that workspace when relevant.

---

## 3. The kinds of memory

The field has largely settled on a borrowed-from-psychology vocabulary. It is useful because the
categories map cleanly onto different storage and retrieval needs.

### Short-term (working) memory
The immediate context of the current task or conversation — the last few messages, the current
plan, intermediate results. Lives in or near the context window. Small, fast, volatile. When the
task ends or the window fills, it is summarised or discarded.

### Long-term memory
Persistent across sessions. Conventionally split three ways:

| Type | Plain meaning | Example for `recall` |
|---|---|---|
| **Episodic** | Specific things that happened, in sequence | "On 12 June the user asked to audit the SQL schema and we found three missing indexes." |
| **Semantic** | General facts and relationships, stripped of when they happened | "The `orders` table is owned by Team Alpha. Sarah is the lead engineer." |
| **Procedural** | How to do things — repeatable patterns and workflows | "To cut a release: group commits by type, recommend a semver bump, draft notes." |

The single most important transformation in a memory system is **episodic → semantic**: noticing
patterns across many specific events and distilling them into reusable facts. An agent that only
ever stores raw episodes drowns in detail; one that consolidates episodes into durable facts gets
smarter over time. This is the same move a person makes when, after the tenth time, "this specific
meeting" becomes "we always do standups at 9am."

### A practical layering (the "OS" mental model)
Several systems describe memory as a hierarchy modelled on a computer's memory hierarchy. It is a
helpful design frame:

- **Core / in-context memory** — small, always visible to the model, like RAM. Holds the few facts
  that must be present for the current work (who the user is, the active goal).
- **Recall / recent memory** — recent history, outside the context window but cheaply searchable,
  like an L2/L3 cache.
- **Archival memory** — the unbounded store the agent queries on demand, like disk.

The agent moves information up and down this hierarchy: promoting a frequently-used archival fact
into core memory, evicting a stale core fact back down. Designing those movement rules is most of
the work.

---

## 4. How memory is represented

There are three common shapes. They are not mutually exclusive — the strongest systems combine
them.

### 4.1 Vector store (semantic similarity)
Text is turned into embeddings (numeric vectors) and stored. At query time you embed the question
and find the nearest stored vectors. Good at "find me things that *mean* something like this."
Weak at precise facts, at relationships between things, and at handling contradictions — a flat
vector store will happily keep two opposite facts and surface whichever is more textually similar.

### 4.2 Knowledge graph (entities and relationships)
Information is stored as **nodes** (entities — people, teams, projects, tables) and **edges**
(relationships — "owns", "is lead of", "supersedes"). This is the part the user called out as
"graph theory," and it deserves its own section (below).

### 4.3 Hybrid
Most production systems use both: a graph for structure and precise relationships, vectors for
fuzzy semantic recall, and often keyword search (BM25) on top for exact-term matching. Retrieval
then blends all three signals. Empirically this hybrid is what real workloads need — a query like
*"what did this user say about billing in the last 30 days?"* requires semantic matching **and** a
date filter **and** an entity (this user) at once.

---

## 5. The graph-theory angle

Why model memory as a graph at all? Because a lot of what an agent needs to know is *relational*,
and relationships are exactly what a flat list of text chunks throws away.

### 5.1 What the graph buys you

- **Multi-hop reasoning.** "Who owns the system that the failing job writes to?" is two hops:
  job → table → owning team. A graph traversal answers it directly. Pure similarity search cannot
  — there is no single chunk of text that contains the answer, only a path through several.
- **Structure survives.** Entities and edges are first-class. You can ask "everything connected to
  Project Apollo" and get a precise sub-graph rather than a fuzzy top-k of vaguely related text.
- **Contradictions become visible.** When two facts disagree, they are two edges between the same
  nodes, and you can reason about which wins (see temporal model below) instead of silently keeping
  both.

### 5.2 The temporal (bi-temporal) model
Facts change. "Sarah is the lead engineer" is true until it isn't. A naive store either overwrites
the old fact (losing history) or keeps both (creating a contradiction). The bi-temporal model — the
approach the Graphiti project documents — tracks **two timelines** on every edge:

- **when the fact was true in the world** (validity interval — valid-from, valid-to), and
- **when the system learned it** (ingestion time).

When a new fact contradicts an old one, the system does **not** delete the old edge. It marks the
old edge's validity as ended ("invalidates" it) and adds the new one. You keep a queryable history
("who *was* the lead in March?") while always being able to ask for the current truth. This is the
cleanest known answer to the "facts evolve" problem, and it is worth adopting.

### 5.3 Communities and summaries
A purely entity-level graph can get large and noisy. Some approaches (Microsoft's GraphRAG is the
named example) cluster related entities into **communities** and pre-compute summaries of each
cluster, which helps answer broad "what is going on with X" questions. The trade-off, openly
acknowledged in the literature, is that this pre-computation is **expensive to keep fresh** — when
the underlying data changes, large parts may need recomputing, and multi-step summarisation makes
retrieval slow (tens of seconds in some reports). For a memory that changes constantly, eager
community summarisation is a poor fit; incremental graph update is better. This tension —
**pre-compute for speed vs. update-on-demand for freshness** — is the central design axis and
recurs throughout this document.

---

## 6. The three core operations

Designing a memory system is mostly designing three things well: how you **write**, how you
**read/search**, and how you **maintain** (consolidate and forget). Get these right and the rest
follows.

### 6.1 Writing — deciding what to remember

The naive approach (store everything) fails: the store bloats, retrieval gets noisy, and cost
climbs. A good write path is selective and structured.

- **Extract, don't dump.** Rather than storing whole conversations, extract the salient facts and
  store those — ideally as graph nodes/edges or structured records, not free text. "Apollo launches
  12 October; lead engineer Sarah; owned by Team Alpha" is a far more useful memory than the
  paragraph it came from.
- **Score importance at write time.** Use priority/salience tagging so trivia and signal aren't
  stored with equal weight. This filtering is what keeps an agent focused.
- **Track confidence.** Some memories are born uncertain — from a noisy summary, a weak retrieval
  match, an inferred intention. Low-confidence memories should be marked as such and should decay
  faster than high-confidence facts. Don't give a guess the same lifespan as a verified fact.
- **Supersede on write.** When a new fact contradicts an existing one, resolve it *at write time* —
  end the old fact's validity, add the new one — so contradictions never accumulate. (This is the
  temporal model from §5.2 applied as a write policy.)

A reasonable default write policy, combining the above: **extract structured facts → tag importance
and confidence → check for and supersede contradictions → link to source for provenance.**

### 6.2 Reading and searching — getting the right memory back

- **Hybrid retrieval.** Combine semantic (vector) similarity, keyword/exact match, and graph
  traversal. Add metadata filters (time ranges, entity, source). No single method covers real
  queries.
- **Recency-weighted ranking.** A common and effective scoring function multiplies semantic
  similarity by an exponential time-decay factor, so a memory that hasn't been touched recently
  loses salience gradually. This mirrors the Ebbinghaus "forgetting curve": traces fade unless
  reinforced. Reinforcement = being recalled, cited, or re-verified.
- **Multi-round / recursive retrieval.** For hard questions, retrieve once to get cues, then follow
  associations (graph edges, or a second retrieval seeded by the first result). Increases depth at
  the cost of latency.
- **Keep retrieval cheap.** A notable property of the Graphiti approach is that retrieval makes
  **no LLM calls** — it leans on embeddings + BM25 + graph indexes and hits ~300ms p95. LLM calls
  belong on the *write/consolidation* path (which can be slower and asynchronous), not the hot read
  path. This is a good principle to copy.

### 6.3 Maintaining — consolidation and forgetting

This is the part most prototypes skip and most production systems regret skipping.

**Consolidation** periodically:
- extracts the salient points from clusters of episodic memories,
- merges duplicate or near-duplicate entries into a single richer fact,
- promotes recurring episodic patterns into semantic facts (the episodic → semantic move),
- and re-phrases for reuse so a fact learned in one context transfers to another.

This is typically a background job, often LLM-assisted, run off the hot path.

**Forgetting** is a feature, not a failure. Without it, memory grows without bound, old
contradictions linger, and retrieval drowns in noise. Common, composable mechanisms:

- **TTL** (time-to-live) on long-tail entries to bound storage.
- **Decay** — LRU-style or exponential decay on retrieval scores, so unused memories fade.
- **Confidence-linked decay** — lower-confidence memories expire faster.
- **Active supersession** — contradictions resolved on every write (above), so they never pile up.

The crucial guard rail: a **salience floor**. Combine any time-based forgetting with a minimum-
importance protection so that a high-value fact ("the production database is in `eu-west-1`") is
never pruned just because nobody asked about it for a while. TTL/LRU alone over-forgets; a salience
floor fixes that.

There is also a governance angle the research flags: memory that evolves can be *manipulated* (a
poisoned source writes a false "fact") or can *drift* into an unsafe state. If memory feeds
decisions, write paths need validation and provenance, and ideally an audit trail of what was
learned, when, and from where.

---

## 7. The assumed agent environment — "learn as you ask"

`recall` is designed to operate inside a specific kind of agent environment — a sandboxed agent, a
trusted credential-holding broker, and a "learn-as-you-ask" model of enterprise search. That
environment is worth understanding in its own right, because it shapes what `recall` must and must
not do.

### 7.1 The core idea — lazy, question-driven learning
This model reframes enterprise search. The conventional approach pre-indexes every company document
into a central database on a schedule. The learn-as-you-ask model instead **builds memory from the
questions people actually ask** — it learns lazily.

The mechanism:

- **Lazy learning.** A document is only read and summarised when a question makes it relevant.
  Nothing is indexed up front.
- **A growing shared memory of facts.** When someone asks about "Project Apollo," an agent reads
  the source, writes a concise note ("Apollo launches 12 October, lead engineer Sarah, owned by
  Team Alpha"), and stores it. Future questions hit the note instead of re-reading the document.
- **Facts stored as links, not blobs.** Information is kept as relationships between entities — i.e.
  a graph — rather than text chunks.
- **A freshness-verification loop on every query.** Four steps: *ask → check whether the source
  document changed (via modification dates) → update the note if it changed → answer.* This
  deliberately trades a little speed for correctness: instead of trusting a possibly-stale index,
  it verifies currency at query time.
- **Conflict resolution by source ranking.** When documents disagree, sources are ranked by
  recency and authority.
- **Transparency.** Answers carry citations and edit dates.

The expected evolution over time: on day one the system feels slow (it is reading sources for the
first time); by day thirty common topics are cached in memory; by day one hundred the memory
reflects how the organisation actually works, and stays current automatically. The cost model is
inverted from traditional search — **no large up-front indexing investment, cost is paid
incrementally as questions arrive.**

### 7.2 The safety architecture
The environment achieves the above without leaking data or getting compromised by malicious
content. The relevant pieces:

- **Sealed sandbox.** The short scripts the agent writes (typically Python) run in an isolated
  environment with no direct internet, disk, or program-launch access. A booby-trapped document
  therefore cannot reach out or exfiltrate.
- **Credential-holding broker.** A trusted broker outside the sandbox holds company credentials and
  makes authenticated calls on the user's behalf. The agent's code never touches credentials, so it
  cannot steal them even if tricked.
- **Per-user permissions.** The broker calls each source system *as the requesting user*. There is
  no central master-permission store; the source systems' own access controls enforce boundaries,
  so memory cannot leak facts across authorisation levels.
- **Approved-system allowlist.** The agent can only reach administrator-approved endpoints.
- **Untrusted-data handling.** Results returned from sources are marked untrusted and are *not* fed
  straight back into the agent as instructions to act on — a defence against prompt injection from
  document content.
- **Audit logging.** Every call is recorded for attribution and accountability.

This environment deliberately scopes itself to the *sandbox + broker + per-user calls + allowlist +
audit* infrastructure. It leaves the **memory implementation, the permission-filtering logic, and
summary storage out of its own scope** — which is exactly the space `recall` occupies.

### 7.3 Why this matters for `recall`
This model says something opinionated and useful: **a memory does not have to be pre-built. It
can grow from use, verify its own freshness, store facts as a graph, and respect existing
permissions rather than inventing new ones.** Whether `recall` adopts the full lazy model or a
hybrid (some pre-indexing of known-hot sources, lazy for the long tail) is a design decision, but
the freshness-verification loop and the source-ranked conflict resolution are strong patterns to
carry forward.

---

## 8. What others have built (factual landscape)

A brief, non-promotional survey of named systems, to inform — not pre-empt — our own design. These
are reference points; details below come from the cited sources and benchmark numbers move quickly.

- **Letta (formerly MemGPT)** — Organises memory on the OS hierarchy described in §3: core
  (in-context), recall (recent, searchable), archival (unbounded, on-demand). The agent edits its
  own memory using tools and decides what to promote or evict. Best fit when you want the agent
  itself to manage memory.
- **Zep / Graphiti** — A temporal knowledge graph (the bi-temporal model in §5.2). Strong at
  evolving facts and contradiction handling; retrieval blends embeddings, BM25, and graph traversal
  with no LLM call on the read path. Reported as leading on long-memory benchmark accuracy.
- **Mem0** — Vector-centric semantic recall with priority scoring and forgetting/eviction policies.
  Large ecosystem and adoption; positioned for application-level memory.
- **GraphRAG (Microsoft)** — Entity graph plus community clustering and pre-computed summaries.
  Strong for broad thematic questions; weak when data changes often because summaries need
  recomputation (the freshness/pre-compute tension from §5.3).

The recurring lesson across all of them: **there is no single winner.** The right design follows
the workload — how often facts change, how relational the questions are, how much the agent should
manage its own memory, and how strict the freshness and permission requirements are.

---

## 9. SurrealDB as a candidate store for `recall`

The user asked us to look at [SurrealDB](https://github.com/surrealdb/surrealdb) specifically. It
is worth a section because the shape of what it offers lines up unusually well with the design
positions in §9 — but it is a candidate to evaluate, not a foregone conclusion. The vendor's own
materials are heavily marketed (it now brands itself "the context layer for AI agents"); the
summary below is stripped to the technical facts and the honest trade-offs.

### 9.1 What it is
SurrealDB is a multi-model database written in Rust. The selling point relevant to us is that a
**single record can simultaneously be a document, a graph node, a vector-indexed embedding, and a
full-text-search item**, and one query language (SurrealQL) can traverse all of those in a single
round trip. In other words it tries to be the "hybrid representation" of §4.3 out of the box,
rather than something we assemble from a graph database plus a vector store plus a search engine.

### 9.2 The capabilities that map onto our design

| Our design position (§4–§6) | What SurrealDB provides |
|---|---|
| Hybrid representation: graph + vectors + keyword (§4.3) | Document, graph, relational, vector, and full-text in one engine; one query can combine cosine similarity, graph traversal, BM25 full-text, and structured filters. |
| Knowledge graph with rich edges (§5) | Tables are entity types; typed relations are edges, and **edges are full documents** — they can carry properties, timestamps, confidence scores, and metadata. |
| Bi-temporal / confidence on facts (§5.2, §6.1) | Because edges carry arbitrary fields, validity intervals, ingestion time, and confidence can be stored directly on the relationship. (The temporal *logic* — invalidating superseded edges — is still ours to implement; the store just holds the fields.) |
| LLM-free, fast read path (§6.2) | Vector search uses HNSW (cosine / euclidean / manhattan); full-text uses BM25 with configurable tokenizers; graph traversal needs no JOINs. None of this calls an LLM. |
| "Discover then materialise" links (§5.1) | A documented pattern: use vector similarity to find a connection, then write it as a graph edge for cheap future traversal. This is a concrete way to grow the graph from semantic recall. |
| Provenance and freshness loop (§6.1, §7.1) | Live queries push changes to subscribers over WebSocket; records can carry source links and edit timestamps, supporting the freshness-verification pattern. |

### 9.3 Deployment and storage — the pragmatic facts
- **Runs embedded or as a server.** It can run **in-process as a library** (relevant if `recall`
  wants a single-binary, no-external-service deployment), as a single node, as a distributed
  cluster, or compiled to WebAssembly for edge/browser. The same SDK abstracts over these.
- **Storage backends are pluggable.** In-memory (ephemeral), **RocksDB** (local persistent, the
  current default for single-node), **SurrealKV** (its own Rust-native engine), and **TiKV** (the
  distributed backend for clustering, scaling to 100+ TB).
- **ACID transactions** across multiple rows and tables; schemafull *or* schemaless per table, so
  we can start loose and tighten as the memory model stabilises.
- **Broad SDK coverage** — Rust, Python, JavaScript/Node, Go, .NET, PHP, Java, Deno.

### 9.4 The honest trade-offs
- **Maturity.** SurrealDB is younger than the established single-purpose stores (a dedicated graph
  DB, or a dedicated vector index). "Does everything" engines carry the risk that each individual
  capability is less battle-tested than a specialist. This needs validating against our actual
  query patterns and data volumes, not taken on faith.
- **Moving parts in the storage layer.** SurrealKV (the native engine) is **explicitly still
  evolving** — its file format, APIs, and feature set may change before it stabilises, and RocksDB
  remains the default. Choosing a backend is a real decision with its own consequences (RocksDB
  pulls in a non-Rust dependency at build time; TiKV is operationally heavier).
- **One engine, one blast radius.** Consolidating document + graph + vector + search into a single
  system simplifies operations but couples them: a limitation or bug in one model affects the whole
  store, and you cannot swap out just the vector index for a better one. The specialist-vs-unified
  choice is the trade-off to weigh.
- **The temporal/forgetting logic is still ours.** SurrealDB gives us a place to *store* validity
  intervals, confidence, and decay scores on rich edges; it does not give us the bi-temporal
  invalidation, consolidation, or salience-floor *policies* of §6. Those are application logic
  regardless of store.

### 9.5 Where it leaves us
If `recall` wants the hybrid graph+vector+text model of §4.3 **without** standing up and
integrating three separate systems, SurrealDB is a genuinely strong candidate to prototype against
— rich edges in particular make the bi-temporal/confidence model (§5.2, §6.1) natural to express,
and embedded mode keeps deployment simple early on. The responsible next step is a **spike**: model
a slice of the intended memory graph, load representative data, and benchmark the three query shapes
we care about (semantic recall, multi-hop traversal, freshness check) against our latency targets,
while keeping a fallback design in mind in case maturity or a single capability falls short.

---

## 10. Implications for designing `recall`

Pulling the above into concrete design positions. These are starting recommendations to debate, not
decisions. The companion note [`good-mem.md`](./good-mem.md) expands each of these into the specific
techniques, failure modes, and metrics needed to make the memory not just work but excel — including
a reference blueprint (`good-mem.md` §12) and a staged build order (`good-mem.md` §14).

1. **Use a hybrid representation.** A knowledge graph for entities, relationships, and the temporal
   model; vectors for semantic recall; keyword search for exact terms. Don't pick one.
2. **Adopt the bi-temporal model.** Never destructively overwrite a fact. End its validity and add
   the successor. This gives free history and clean contradiction handling.
3. **Keep the read path LLM-free and fast.** The expensive, LLM-assisted work (extraction,
   consolidation, summarisation) runs off the read path — and, in `recall`'s architecture, at the
   **agent**, not in `recall` (ADR-015). Target low-latency retrieval.
4. **Make writing selective and provenanced.** Extract structured facts, tag importance and
   confidence, link every memory to its source, and supersede contradictions at write time.
5. **Treat forgetting as a first-class subsystem.** Decay + TTL + confidence-linked expiry, with a
   salience floor so important facts survive disuse. Consolidation — the episodic → semantic
   distillation — runs at the **agent**, which writes the insight back as a `consolidated` fact
   (ADR-015); `recall` stores and serves it.
6. **Consider the lazy / learn-as-you-ask model.** A freshness-verification loop (check source
   modification, refresh if stale, then answer) keeps memory current without a heavy indexing
   pipeline. A pragmatic middle path: pre-index a small set of known-hot sources, learn the long
   tail lazily.
7. **Respect existing permissions; don't build a parallel one.** Following the assumed environment: access source
   systems as the requesting user, allowlist reachable systems, mark externally-returned data as
   untrusted, and keep an audit trail. This matters more, not less, if memory feeds decisions.
8. **Plan for memory safety/governance.** Provenance, validation on the write path, and an audit of
   what was learned and from where — because evolving memory can be poisoned or can drift.

---

## 11. Making `recall`'s REST API trivial to call from an agent

*Note: `recall` has no API yet — the repository is greenfield at the time of writing. Everything
below is design guidance for the API we are about to build, not a description of something that
exists. Endpoint names are illustrative proposals, not a fixed contract.*

The goal is narrow and concrete: **a sandboxed agent should be able to use `recall`'s memory in a few
lines of code, without reading documentation, without holding credentials, and without any single
call being able to do harm.** That goal is shaped by how the assumed agent environment actually works
(§7), so we design to its grain rather than against it.

### 11.1 What "called from a sandboxed agent" actually means
Recall the environment model from §7.2. The agent writes a short script that runs in a **sealed
sandbox** with no direct internet or disk. It does not hold credentials. A **credential-holding
broker** makes the authenticated call on the user's behalf, but only to **administrator-allowlisted
endpoints**, and returned data is treated as **untrusted**. Every design choice below follows from
those four facts:

1. The script is short and disposable → the API must be usable in a few lines with no SDK.
2. The broker injects identity, the script never sees it → auth must ride on a header the broker
   sets, never on anything the script constructs.
3. The endpoint must be allowlisted → the API must present a small, stable, predictable surface an
   administrator can approve once.
4. Responses are untrusted → the API must return clean, structured, provenance-tagged data, not
   prose that could carry injected instructions.

### 11.2 Design principles

**P1 — Few, task-shaped endpoints, not fine-grained CRUD.** An agent wants to *remember*, *recall*,
and *forget* — verbs that match intent. Three or four high-level endpoints mean the agent's script
is a few lines and one round trip, instead of orchestrating a dozen low-level calls. This also
keeps the allowlist (and the audit trail) small and legible.

**P2 — Self-describing, so the agent needs no external docs.** Ship a machine-readable contract
(OpenAPI/JSON Schema) and a single capabilities endpoint (`GET /` returning the available
operations and their shapes). Because the agent *writes code* against the API on the fly,
a runtime-discoverable schema is the difference between "it just works" and "it needs a human to
read the manual first."

**P3 — Identity on a header the broker sets; never in the URL or body.** The broker authenticates
as the requesting user and injects a bearer token (`Authorization: Bearer …`). The API enforces
per-user permissions **server-side** keyed on that identity — the same "no central master
permission store, the source enforces access" stance the environment takes (§7.2). The agent's script
constructs no identity and no scope, so a prompt-injected script cannot widen its own access.
Tokens should be **least-privilege and short-lived** (a token handed out early in a long agent run
should not still grant broad write access later).

**P4 — Token-efficient responses.** Every field returned is passed back into a model's context and
costs tokens and latency. Return only what the agent needs: ranked top-k results with scores, tight
JSON, bounded page sizes, and a way to ask for "just the facts" versus "facts plus full
provenance." No over-fetching by default.

**P5 — Idempotent writes.** Agents retry aggressively, and a sandboxed script that times out will
re-run. Every write (`remember`, `forget`) must accept an `Idempotency-Key` header; the server
stores the key with a TTL and returns the original result on replay, so a retry never creates a
duplicate memory or double-applies a supersession. (This pairs naturally with the supersede-on-write
policy of §6.1.)

**P6 — Return the provenance the agent needs to check freshness itself.** The environment's value
pattern is *ask → check source freshness → update if stale → answer* (§7.1). `recall` does **not**
run that loop — it makes no outbound call (ADR-014). Instead, on request (`include_provenance`) the
recall response carries each sourced fact's `origin_ref` + `modification_marker`, so the agent's
local broker runs the check and writes a superseding fact if the source changed. A fact fetch also
supports conditional requests (`ETag` / `If-Modified-Since`) for the cheap "has the *stored fact*
changed?" case.

**P7 — Provenance on every returned fact.** Each recalled item carries its source link, the edit/
ingestion date, a confidence score, and its validity interval (the §5.2 / §6.1 fields). This serves
two masters at once: the transparency requirement (answers cite sources and dates) and the
agent's need to judge how much to trust each memory.

**P8 — Treat the response as untrusted data, and make that easy for the caller.** Return structured
fields, not free-form narrative that could smuggle instructions; tag each item with its provenance
so the broker/agent can apply the "untrusted, not directly actionable" rule (§7.2). The API should
never embed executable or instruction-like content in a memory it hands back. This is the API side
of prompt-injection defence: untrusted input stays clearly labelled and structurally separated.

**P9 — A stable, predictable, versioned contract.** One consistent success envelope, one consistent
error envelope (machine-readable `code` + human `message`), meaningful HTTP status codes, and
explicit versioning (`/v1/…`). An agent — and the administrator allowlisting the endpoint — should
be able to rely on the shape not shifting under them.

**P10 — Agent-aware operational limits.** Agents can emit far more calls than humans. Provide
agent-tier rate limits with standard headers (`RateLimit-*`, `Retry-After`) the agent can read and
back off against, and **audit every call** server-side — which aligns with the environment's own audit
logging (§7.2) and the memory-governance need from §6.3.

### 11.3 An illustrative endpoint sketch (proposed, not fixed)

| Operation | Proposed shape | Notes |
|---|---|---|
| Discover | `GET /v1` | Returns capabilities + link to OpenAPI; lets the agent write code with no manual. |
| Recall | `POST /v1/recall` `{ "query": "...", "filters": {...}, "k": 5 }` | Hybrid retrieval (§4.3); returns ranked facts with provenance (P7), plus `origin_ref` + `modification_marker` when `include_provenance` is set so the agent checks freshness itself (P6, ADR-014). LLM-free read path (§6.2). |
| Remember | `POST /v1/memories` `{ "fact": {...}, "source": "...", "confidence": 0.8 }` + `Idempotency-Key` | Store the **agent-asserted** structured fact (no server-side extraction — ADR-015); supersede contradictions server-side (§6.1). |
| Forget / supersede | `POST /v1/memories/{id}/retire` + `Idempotency-Key` | Ends validity rather than hard-deleting (§5.2); never destructive by default. |
| Freshness check | `GET /v1/memories/{id}` with `If-Modified-Since` | Cheap currency check (P6) without a full re-read. |

The whole point of the table is that an agent's script to use memory is roughly *"POST my
question to `/v1/recall`, read back the ranked facts and their sources"* — one call, no credentials,
no SDK, a response it can trust because every item is provenance-tagged.

### 11.4 What this does not solve
The API surface makes `recall` *callable*; it does not make the memory *good* — the write,
retrieval, consolidation, and forgetting policies of §6 still have to be right behind it. Nor does a
clean API remove the need for the broker/sandbox/allowlist controls of §7.2; those belong to the
surrounding agent platform, and `recall` simply has to be a well-behaved allowlisted endpoint that
authenticates per user and audits every call.

---

## 12. Open questions to resolve in design

- **Lazy vs. eager vs. hybrid indexing** — how much does `recall` pre-build versus learn on demand?
- **Who manages memory** — the agent (Letta-style self-editing) or a fixed system policy?
- **Graph store choice** — a unified multi-model engine (e.g. SurrealDB, §9) vs. dedicated graph
  database + separate vector index vs. graph-on-relational; weigh single-system simplicity against
  specialist maturity, and run the §9.5 spike before committing.
- **Consolidation cadence** — RESOLVED (ADR-015): not applicable to `recall`; consolidation is
  agent-side, so its cadence is the agent's concern, not `recall`'s.
- **Forgetting parameters** — decay curve shape, TTL values, salience-floor threshold; these need
  tuning against real usage, not guessed.
- **Multi-tenant / per-user isolation** — how memory is partitioned and how permissions are
  enforced at retrieval.
- **Evaluation** — what does "good memory" mean for `recall`, and which benchmark or harness proves
  it (long-memory recall, contradiction handling, freshness)?
- **API auth and broker integration** (§11) — what token format and scope model does the broker
  inject, how short-lived are tokens, and how does `recall` map a caller identity to its
  per-user memory partition and permissions?
- **Server-side vs. client-side freshness loop** (§11.2 P6) — RESOLVED (ADR-014): the loop runs
  entirely at the agent; `recall` makes no outbound call and only returns provenance on request.

---

## Sources

Concepts and claims above are drawn from the following, accessed June 2026:

- [Memory Types in Agentic AI: A Breakdown (Medium)](https://medium.com/@gokcerbelgusen/memory-types-in-agentic-ai-a-breakdown-523c980921ec)
- [Beyond Short-term Memory: The 3 Types of Long-term Memory AI Agents Need (MachineLearningMastery)](https://machinelearningmastery.com/beyond-short-term-memory-the-3-types-of-long-term-memory-ai-agents-need/)
- [Architecture and Orchestration of Memory Systems in AI Agents (Analytics Vidhya)](https://www.analyticsvidhya.com/blog/2026/04/memory-systems-in-ai-agents/)
- [Memory in the Age of AI Agents: A Survey — paper list (GitHub)](https://github.com/Shichun-Liu/Agent-Memory-Paper-List)
- [What Is Agent Memory? (MongoDB)](https://www.mongodb.com/resources/basics/artificial-intelligence/agent-memory)
- [Graphiti: Knowledge graph memory for an agentic world (Neo4j)](https://neo4j.com/blog/developer/graphiti-knowledge-graph-memory/)
- [Graphs Meet AI Agents: Taxonomy, Progress, and Future Opportunities (arXiv 2506.18019)](https://arxiv.org/abs/2506.18019)
- [Graph-based Agent Memory: Taxonomy, Techniques, and Applications (arXiv 2602.05665)](https://arxiv.org/pdf/2602.05665)
- [Mem0 — Memory eviction and forgetting in AI agents](https://mem0.ai/blog/memory-eviction-and-forgetting-in-ai-agents)
- [Mem0 — Memory in Agents: What, Why and How](https://mem0.ai/blog/memory-in-agents-what-why-and-how)
- [Governing Evolving Memory in LLM Agents: the SSGM Framework (arXiv 2603.11768)](https://arxiv.org/pdf/2603.11768)
- [AI Agent Memory 2026 — Comparing Mem0, Zep, Graphiti, Letta, LangMem (Medium)](https://medium.com/@wasowski.jarek/i-compared-5-ai-agent-memory-systems-across-6-dimensions-none-wins-6a658335ed0a)
- [Agent Memory at Scale 2026: Letta, Zep, Mem0, LangMem Compared (AgentMarketCap)](https://agentmarketcap.ai/blog/2026/04/10/agent-memory-vendor-landscape-2026-letta-zep-mem0-langmem)
- [Best AI Agent Memory Frameworks in 2026 (Atlan)](https://atlan.com/know/best-ai-agent-memory-frameworks-2026/)
- [SurrealDB — repository and README](https://github.com/surrealdb/surrealdb)
- [SurrealDB — Knowledge Graphs use case](https://surrealdb.com/use-cases/knowledge-graphs)
- [SurrealDB — "Graph RAG does not need a graph database" (blog)](https://surrealdb.com/blog/graph-rag-does-not-need-a-graph-database-it-needs-a-database-that-does-everything)
- [SurrealDB — Deployment & storage layer considerations](https://surrealdb.com/learn/fundamentals/performance/deployment-storage)
- [SurrealKV — deep dive on the SurrealDB 2.0 storage engine (Medium)](https://ori-cohen.medium.com/surrealkv-diving-deep-with-the-new-storage-engine-in-surrealdb-2-0-5c8d276aaaf6)
- [REST API for AI Models Explained: 2026 Guide (MLflow)](https://mlflow.org/articles/rest-api-for-ai-models-explained-2026-guide/)
- [Make Your Agent's API Calls Idempotent Before You Need To (DEV)](https://dev.to/mukundakatta/make-your-agents-api-calls-idempotent-before-you-need-to-2994)
- [API security best practices for the age of AI agents (WorkOS)](https://workos.com/blog/api-security-best-practices-for-ai-agents)
- [Design Patterns for Securing LLM Agents against Prompt Injections (arXiv 2506.08837)](https://arxiv.org/pdf/2506.08837)
- [LLM Prompt Injection Prevention (OWASP Cheat Sheet)](https://cheatsheetseries.owasp.org/cheatsheets/LLM_Prompt_Injection_Prevention_Cheat_Sheet.html)
