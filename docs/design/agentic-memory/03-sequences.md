# 03 — Principal Sequences

> **Mode:** draft · **Revision:** 0.5.0 · **Last updated:** 2026-06-22

Logical order-of-operations for the four principal flows. Payload detail is deferred to `/spec`.

## Sequence: Recall (golden path)

```mermaid
sequenceDiagram
    participant Broker as Faraday broker (as user)
    participant API as HTTP API + Auth
    participant Ret as Retrieval Engine
    participant Embed as Embedding provider
    participant Store as Memory Store
    participant Rerank as Reranker (cross-encoder)

    Broker->>API: POST /v1/recall (query, filters, include_provenance) + OIDC bearer
    API->>API: validate token, map identity→scope, authorise read
    API->>Ret: scoped query
    opt reformulation A/B-gated (off by default — good-mem §7.3)
        Ret->>Ret: reformulate query
    end
    Ret->>Embed: embed query (read-path model inference)
    Embed-->>Ret: query vector
    Ret->>Store: multi-signal recall (semantic + keyword + graph), scoped
    Store-->>Ret: candidate facts (top-k)
    Ret->>Rerank: cross-encoder rerank top-k (read-path model inference, not an LLM)
    Rerank-->>Ret: reordered candidates
    Ret->>Ret: recency weighting + retrieval gating
    opt provenance requested (include_provenance)
        Ret->>Ret: attach each sourced fact's origin_ref + modification_marker
    end
    Ret-->>API: ranked facts + provenance + confidence (or abstain)
    API-->>Broker: bounded, token-efficient response
    Note over Broker: the agent (with its local broker) checks source freshness<br/>and, if stale, writes a fresh superseding note — outside recall (ADR-014)
```

- **Trigger:** the broker forwards a memory query on behalf of the user.
- **Result:** a ranked, scoped set of facts, each with source and confidence — or an explicit
  "insufficient evidence" abstention. **No LLM call on this path**; two read-path **model inferences**
  (query embed, cross-encoder rerank) run here and carry their own latency sub-budgets within NFR-P2
  (ADR-012). `recall` performs no source-change check; it returns each sourced fact's provenance on
  request so the **agent** verifies freshness and writes a fresh superseding note if stale (ADR-014).
- **Error posture:** invalid/expired token → reject (401-class); no sufficiently relevant candidate →
  abstain rather than pad; store timeout → fail fast with a typed error; embedding/reranker provider
  timeout → degrade within the ADR-012 budget (fail fast, or skip rerank and return stage-1 order).

## Sequence: Remember (write)

```mermaid
sequenceDiagram
    participant Broker as Faraday broker (as user)
    participant API as HTTP API + Auth
    participant Q as Durable work queue
    participant WP as Write Pipeline
    participant Store as Memory Store

    Broker->>API: POST /v1/memories (content, source) + OIDC bearer + Idempotency-Key
    API->>API: validate token, map scope, authorise write
    API->>Q: enqueue write job (idempotency-keyed)
    API-->>Broker: accepted (idempotent ack)
    Q->>WP: dequeue
    WP->>WP: filter → extract structured fact → normalise → entity-resolve → score importance+confidence
    WP->>WP: write gate (trust score)
    alt trusted
        WP->>Store: persist fact (provenance, validity, scores)
    else untrusted / instruction-like
        WP->>Store: quarantine or reject (distinguishable outcome)
    end
```

- **Trigger:** the broker submits content to remember.
- **Result:** a clean, scoped, provenance-tagged fact in the store (or a quarantined/rejected record);
  contradiction resolution is deferred to maintenance.
- **Error posture:** replay with same Idempotency-Key → original result, no duplicate; extraction/
  embedding provider failure → bounded retry with backoff, then dead-letter for later reprocessing;
  write never blocks a read.

## Sequence: Consolidate & maintain (asynchronous, idle-biased)

```mermaid
sequenceDiagram
    participant Sched as Scheduler / idle trigger
    participant MW as Maintenance Worker
    participant Store as Memory Store
    participant LLM as LLM provider

    Sched->>MW: run maintenance cycle
    MW->>Store: read recent episodes + contradiction candidates (scoped)
    MW->>LLM: surprise-weighted consolidation (episodic→semantic)
    LLM-->>MW: candidate insights
    MW->>MW: validate insights against source facts, assign decaying confidence
    MW->>Store: promote validated insights, supersede contradictions (end validity, keep history)
    MW->>Store: apply graceful decay with salience floor, then re-embed changed facts
```

- **Trigger:** schedule or idle period.
- **Result:** episodes distilled into semantic facts, contradictions superseded (history retained),
  stale low-salience facts decayed, embeddings refreshed.
- **Error posture:** a failed consolidation cycle leaves prior memory intact (no destructive step);
  an insight that fails validation is not promoted; inferences carry expiring confidence so a wrong
  one self-heals.

## Sequence: Forget / verifiable deletion

```mermaid
sequenceDiagram
    participant Broker as Faraday broker (as user)
    participant API as HTTP API + Auth
    participant MW as Maintenance Worker
    participant Store as Memory Store

    Broker->>API: POST /v1/memories/{id}/retire (or delete) + OIDC bearer + Idempotency-Key
    API->>API: validate token, authorise, confirm scope owns the fact
    alt retire (default)
        API->>Store: end validity (non-destructive, history retained)
    else hard delete (explicit intent)
        API->>MW: schedule verifiable deletion
        MW->>Store: remove fact + derived summaries + embeddings
        MW-->>API: deletion proof
    end
    API-->>Broker: outcome (retired / deleted with proof)
```

- **Trigger:** the broker requests a fact be forgotten, or a user exercises deletion rights.
- **Result:** the fact is retired (validity ended) by default, or hard-deleted — including from
  derived summaries and embeddings — with proof on explicit intent.
- **Error posture:** scope mismatch → reject (the caller does not own the fact); partial deletion →
  the operation is not reported complete until proof is obtained.
