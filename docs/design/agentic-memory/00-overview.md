# 00 — Overview

> **Mode:** draft · **Revision:** 0.6.0 · **Last updated:** 2026-06-22

## Summary

`recall` is a memory service for AI agents. It accepts facts learned from interactions and source
documents, stores them as a bi-temporal knowledge graph with vector and keyword indexes, and serves
them back over an authenticated HTTP API on a fast, LLM-free read path. The expensive work —
extraction, consolidation, contradiction resolution, decay — runs asynchronously off the read path.
`recall` sits behind the Faraday agentic-search broker and inherits per-user identity via OIDC; it
holds no raw credentials of its own. This design covers system shape, the memory model, the API
surface, the authentication model, and cross-cutting governance. Wire-level detail (field types,
error codes, JSON schemas) is deferred to `/spec`.

## Motivation

A language model is stateless: each call starts from nothing. An agent that forgets everything
between invocations cannot remember earlier task context, cannot carry knowledge across sessions, and
must re-read every source document on every question. The conventional fix — pre-indexing all
documents into a static store — goes stale, mixes versions, and creates a privacy surface. `recall`
instead provides a memory that learns from use (the Faraday "learn-as-you-ask" model), surfaces each
fact's source and version so the agent can verify freshness, stores facts as relationships rather than
text blobs, resolves contradictions over time, and respects the access rights of the systems it learns
from. Without it, agents built on this
stack remain amnesiac and the organisation pays the full cost of re-reading sources on every query.

The full reasoning, the survey of existing systems, and the quality techniques are recorded in
`docs/concept/agentic-mem.md` and `docs/concept/good-mem.md`; the numbered requirements are in
`docs/concept/requirements.md`. This HLD is the architectural synthesis of those three notes.

## Goals

- An agent can write, recall, and forget memory through a small, self-describing HTTP API in a few
  lines of code, without holding credentials and without reading external documentation.
- Recall is fast (interactive-latency, no LLM call on the read path) and returns ranked facts, each
  carrying its source provenance and a confidence score.
- Facts that change over time are handled as evolution, not destructive overwrite: history is
  preserved and the current truth is always answerable.
- Memory improves with use — the **agent** distils episodic experience into reusable semantic insights
  and writes them back; `recall` stores and serves them and keeps storage growth bounded by graceful
  forgetting.
- Every request is authenticated (OIDC), authorised per user, and audited; no cross-user leakage.
- Externally-sourced content cannot poison memory or be treated as instructions.

## Non-goals

- **The broker, sandbox, system allowlist, and Faraday-side audit** — owned by Faraday, not `recall`.
- **The calling agent / LLM reasoning** — `recall` serves memory; it does not decide what to ask.
- **Learned (reinforcement-learning) memory control at launch** — heuristic control ships first;
  learned control is a later frontier (see `good-mem.md` §10).
- **Multimodal memory (image / audio)** — text and structured facts only at launch.
- **Procedural memory (how-to workflows)** — deferred. v1 stores **episodic** and **semantic** facts
  (plus consolidated insights derived from them); the procedural type from `agentic-mem.md` §3 is not
  modelled at launch. Revisit once the episodic→semantic loop is proven.
- **Semantic answer caching and async prefetch** — deferred to the post-launch "Run" stage
  (`good-mem.md` §14). NFR-CO2 is a SHOULD, not a launch requirement; the v1 read path computes each
  result fresh.
- **Cross-tenant sharing** — memory is never shared between tenants (organisations); the tenant
  namespace is a hard boundary. Sharing *within* a tenant — user → team → tenant visibility — is
  supported (the bridge model, ADR-011).

## Glossary

| Term | Definition | Example |
|---|---|---|
| Fact | A structured assertion stored in memory, with provenance, validity, confidence and salience. | "Team Alpha owns the `orders` table" (valid from 2026-01-04, confidence 0.9). |
| Entity | A node in the memory graph — a person, team, system, or thing a fact refers to. | "Team Alpha", "Sarah", "`orders` table". |
| Relationship (edge) | A typed connection between entities, itself carrying properties (timestamps, confidence, source). | "owns", "is lead of", "supersedes". |
| Episodic memory | A record of a specific thing that happened, in sequence. | "On 2026-06-12 the user asked to audit the schema." |
| Semantic memory | General, time-independent knowledge distilled from many episodes. | "Schema audits usually surface missing indexes." |
| Consolidation | The **agent** distilling recurring episodes into a semantic fact and writing it back as a `consolidated` memory; `recall` stores it but performs no consolidation itself (ADR-015). | The agent promoting ten "standup at 9am" episodes into one durable fact and writing it to `recall`. |
| Bi-temporal model | Storing two timelines per fact: when it was true in the world, and when the system learned it. | A "lead engineer" edge with validity 2026-01→2026-03 plus ingestion date. |
| Supersession | Ending a superseded fact's validity and recording the successor, instead of deleting. | "Sarah is lead" ended; "Tom is lead" added; both retained. |
| Salience | A fact's importance score, used to protect it from forgetting. | A production-region fact has high salience. |
| Confidence | How sure `recall` is that a fact is true; lower confidence decays faster. | An inferred fact starts at 0.5; a verified one at 0.95. |
| Salience floor | A minimum-importance threshold below which time-based forgetting may not prune. | High-salience facts survive disuse. |
| Provenance | The source and ingestion date attached to every fact, supporting citation and trust. | "from document X, ingested 2026-06-12". |
| Freshness loop | Ask → `recall` returns the note plus its source + version → the **agent** checks whether the source changed → re-reads and writes a fresh superseding note if stale. Runs at the agent, not in `recall` (ADR-014). | The agent re-reading a changed document and writing an updated note. |
| Write gate | The trust check on the write path that may quarantine or reject untrusted content. | Rejecting an instruction-like "fact" from a booby-trapped document. |
| Scope | The ownership/access boundary of a fact, modelled as Tenant → Team → User. | User U's facts within Tenant T. |
| Tenant | An organisation — the hard isolation boundary; one SurrealDB namespace per tenant, no commingling. | "Acme Corp". |
| Team | A group within a tenant that may share memory. | Acme's Platform team. |
| Visibility | Whether a fact is private to its user, shared to a team, or shared across the tenant. | A team-shared runbook fact. |
| Faraday broker | The trusted component that authenticates as the user and calls `recall` on their behalf; co-located with the agent, it also reads source documents and checks their freshness (never `recall`'s job — ADR-014). | Injects the OIDC bearer token; re-reads a changed document for the agent. |
