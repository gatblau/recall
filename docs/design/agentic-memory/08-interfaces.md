# 08 — External Interfaces

> **Mode:** draft · **Revision:** 0.4.1 · **Last updated:** 2026-06-20

Names and shapes only. Exact JSON schemas, field types, error codes, and status-code tables belong in
`/spec`. All endpoints are **new**. All require an OIDC bearer token except the unauthenticated
operational endpoints noted.

## HTTP API (versioned under `/v1`)

| Operation | Endpoint (shape) | Purpose |
|---|---|---|
| Discover capabilities | `GET /v1` | Returns the available operations and a link to the OpenAPI document, so an agent can use the API without external docs. |
| Recall | `POST /v1/recall` | Hybrid retrieval; body carries a natural-language query, optional filters, and a result cap. Returns ranked facts with provenance and confidence, or an abstention. |
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

## Outbound interfaces (recall → broker)

`recall` is also a **client** of the Faraday broker, for one purpose: the read-path freshness check
(ADR-013). Names and shapes only; the broker owns the concrete contract.

| Operation | Shape (recall → broker) | Purpose |
|---|---|---|
| Conditional source-change check | broker source-read endpoint with `If-Modified-Since` / modification-marker, called **as the authenticated user** | Cheap "did the source change since we last read it?" check on the read path; a *not-modified* answer carries no body. Any actual re-read is enqueued and runs asynchronously, never on the read path. |

This is the only call `recall` makes outbound to the broker; it never reads source systems directly,
so source access rights stay with the source systems (see [01 — Context](./01-context.md),
[04 — Domain](./04-domain.md)). If the broker is unreachable, the stored fact is returned flagged
*unverified-currency*.

## Conventions (shape-level, detail in `/spec`)

- **Auth header:** `Authorization: Bearer <OIDC JWT>`, injected by the broker.
- **Idempotency:** `Idempotency-Key` header on all writes (remember / retire / delete).
- **Rate limiting:** `RateLimit-*` and `Retry-After` response headers.
- **Envelopes:** one consistent success envelope and one error envelope (`code` + `message`) across
  all endpoints — exact fields specified in `/spec`.
- **Versioning:** path-versioned (`/v1`); breaking changes take a new version.
