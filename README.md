<img src="recall.png" height="100" style="float: right">

# recall

A memory service for AI agents.

An AI agent on its own forgets everything between calls. It can't remember what
happened earlier in a task, can't carry knowledge across sessions, and has to
re-read every source document each time it needs something. `recall` is the
piece that sits next to the agent and gives it a memory: it stores facts the
agent learns, hands them back quickly when asked, keeps them current, and
forgets the ones that stop mattering.

It's a standalone service with an HTTP API. An agent talks to it over the
network — write a fact, recall facts, forget a fact, check whether a fact is
still current. That's the whole surface.

Because the memory lives in a shared service rather than inside any one agent,
**many agents can read and write the same store**. A fact one agent learns is
immediately recallable by the others working in the same scope, so a fleet of
agents collaborates through one common, growing memory instead of each
rediscovering the same things in isolation. This is the main reason to run
`recall` as a service at all.

## What it does

**Agentic search, not RAG.** There's no up-front pass that indexes every
document, and no separate index to keep re-synced as sources change. Agents
build the memory by searching it as they work: a source is read and turned into
a fact the first time a question needs it, and every later question hits the
stored fact instead of re-reading. The store grows from use and keeps itself
current in the background — a self-healing memory rather than an index that has
to be rebuilt.

**Shared memory for collaborating agents.** Memory is a service, not state
trapped inside a single agent, so several agents can both draw on and contribute
to the same store. What one agent learns, the others in the same scope can
recall — within the tenant/team/user visibility boundaries described below.

**Two paths, kept separate.** Reading memory is fast, so the read path does no
slow work — no LLM calls. Writing and tidying memory is allowed to be slow, so
all the expensive work (pulling facts out of text, resolving contradictions,
consolidating, forgetting) happens asynchronously in the background, off the
path the user is waiting on.

**Facts, not text blobs.** Memory is stored as structured assertions in a graph
("Team Alpha owns the orders table"), not as paragraphs dumped into a vector
database. Each fact carries where it came from, when it was learned, how
confident the system is in it, and how important it is.

**Facts change without losing history.** When something that was true stops
being true, the old fact isn't overwritten — its validity is ended and the new
fact recorded alongside it. You can still ask "who was the lead in March?"

**Freshness is treated as correctness.** A confidently-returned stale fact is a
wrong answer, not a cache miss. When a fact's source might have changed, the
service checks before answering and flags what's out of date.

**Memory fades, but on purpose.** Unused facts gradually decay so the store
doesn't grow without bound, but important facts are protected from being pruned
just because they haven't been touched, and recalling a fact resets its clock.

**Scoped, authenticated, audited.** Every fact belongs to a scope — tenant,
team, and user — and carries a visibility: private to its user, shared with a
team, or shared across the tenant. Sharing is what lets agents collaborate, but
it is deliberate and bounded: facts cross between agents only within a tenant,
and the tenant is a hard isolation wall that memory never crosses. The service
authenticates every request via OIDC and only returns facts the caller is
allowed to see; one tenant's memory never leaks to another. Every call is
logged. `recall` validates tokens against a configured identity provider; it
never handles raw credentials.

**It doesn't trust what it reads.** Content from documents is data to remember,
never instructions to act on. A write gate trust-scores incoming content and can
quarantine or reject it, so a booby-trapped document can't poison memory.

## How it's built

- **Rust**, because the datastore is Rust-native and runs in-process.
- **Embedded SurrealDB** for storage — a graph, vector, and keyword store in
  one, running inside the binary. The storage choice is abstracted, so it can
  move to a remote cluster without a rewrite.
- **axum / tokio** for the async HTTP service.
- **OIDC** for authentication, validated against a configured identity provider.
- Ships as a single self-contained binary for the simple case, and scales out to
  a distributed setup.

The decisions behind these choices are recorded as ADRs in
`docs/design/agentic-memory/09-decisions.md`.

## Scope

In scope: storing facts, recalling them, keeping them current, forgetting them,
and doing all of that under per-user access control.

Out of scope: the agent or service that decides what to ask `recall`,
image/audio memory, and machine-learned memory control. It uses simple
heuristics and handles text and structured facts only.
