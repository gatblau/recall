# 08 — External Interfaces

> **Mode:** draft · **Revision:** 0.6.0 · **Last updated:** 2026-06-22

Names and shapes only. Exact JSON schemas, field types, error codes, and status-code tables belong in
`/spec`. All endpoints are **new**. All require an OIDC bearer token except the unauthenticated
operational endpoints noted.

## HTTP API (versioned under `/v1`)

| Operation | Endpoint (shape) | Purpose |
|---|---|---|
| Discover capabilities | `GET /v1` | Returns the available operations and a link to the OpenAPI document, so an agent can use the API without external docs. |
| Recall | `POST /v1/recall` | Hybrid retrieval; body carries a natural-language query, optional filters, a result cap, and a provenance opt-in. Returns ranked facts with confidence, or an abstention; when provenance is requested, each sourced fact also carries its `origin_ref` + `modification_marker` so the agent can check source freshness itself (ADR-014). |
| Remember | `POST /v1/memories` | Submit content to remember; honours an `Idempotency-Key` header. Returns an idempotent acknowledgement (write is async). |
| Get / freshness-check a fact | `GET /v1/memories/{id}` | Fetch a fact; supports `If-Modified-Since` / `ETag` for cheap currency checks. |
| Retire (forget, non-destructive) | `POST /v1/memories/{id}/retire` | Ends a fact's validity; honours `Idempotency-Key`. |
| Delete (verifiable) | `DELETE /v1/memories/{id}` | Hard deletion including derived summaries and embeddings; explicit intent; returns deletion proof. |
| OpenAPI contract | `GET /openapi.json` | Machine-readable contract for runtime code generation. |

## Operational endpoints (unauthenticated, no fact data)

| Operation | Endpoint (shape) | Purpose |
|---|---|---|
| Liveness | `GET /healthz` | Process is up. |
| Readiness | `GET /readyz` | Store and OIDC discovery reachable. |
| Metrics | `GET /metrics` | Observability scrape endpoint (no fact content). |

## Outbound interfaces

`recall` makes **no outbound calls to the broker or source systems** (ADR-014). Freshness checking and
source re-reads are the agent's responsibility, performed by the agent and its co-located broker; `recall`
stores the version marker the agent supplied at write time and returns it on request (see the Recall row
above and [01 — Context](./01-context.md)). The only outbound calls `recall` makes are to its configured
model providers (embedding, reranker on the read path; LLM off it) — model inferences, not orchestration.

## Conventions (shape-level, detail in `/spec`)

- **Auth header:** `Authorization: Bearer <OIDC JWT>`, injected by the broker.
- **Idempotency:** `Idempotency-Key` header on all writes (remember / retire / delete).
- **Rate limiting:** `RateLimit-*` and `Retry-After` response headers.
- **Envelopes:** one consistent success envelope and one error envelope (`code` + `message`) across
  all endpoints — exact fields specified in `/spec`.
- **Versioning:** path-versioned (`/v1`); breaking changes take a new version.
