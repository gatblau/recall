### SPEC: MCP API Edge
**File:** `src/mcp` (binary `recall-mcp`) | **Package:** `recall::mcp` | **Phase:** 6 | **Dependencies:** C9 (Service Layer)

> **Mode:** greenfield
> **derivedFromHld:** 0.7.0

#### Purpose

The MCP API Edge is the second externally-reachable surface of `recall` (ADR-016) — a thin transport adapter that exposes the same service operations as **MCP (Model Context Protocol) tools over streamable-HTTP**, shipped as a separate binary `recall-mcp`. It terminates the MCP streamable-HTTP transport, advertises the operation set through native MCP tool discovery (`tools/list`), dispatches each `tools/call` to the matching C9 Service Layer method, and renders the typed result or `AppError` as an MCP tool result or MCP error. It carries the same broker-injected `Authorization: Bearer <OIDC JWT>` as the REST edge and passes it straight to C9, so identity, scope, rate limiting, idempotency, and audit are exactly those of the REST edge — there is no second copy of that logic (SA-SVC-01). It holds no domain logic and no orchestration: every operation is a call into C9.

#### Approach

A small binary `recall-mcp` builds the same component state via the shared `build_state` and constructs a `Service` (C9), then serves an MCP server over streamable-HTTP using a maintained Rust MCP library (the exact crate is OQ-LIB, chosen at codegen; the library performs the MCP/JSON-RPC wire framing only). Each MCP tool is a thin closure: extract the bearer from the request's `Authorization` header, mint a correlation id, deserialise the tool arguments into the corresponding 2C request type, call the C9 method, and map the `CallResult`/`AppError` to a tool result/error. The chosen design is **one MCP tool per service operation** mirroring the REST routes, with tool input schemas **generated from the same 2C DTOs that back the OpenAPI document** — rejected alternative (a) hand-authored per-tool schemas (drifts from REST); rejected alternative (b) a single generic `call(operation, args)` tool (loses MCP's per-tool discovery and typed inputs). The edge owns no state and no tables; rate snapshots returned by C9 are surfaced as tool-result metadata where the MCP client can use them, but MCP defines no rate-limit headers, so a `RateLimited` error is rendered as an MCP error carrying the `RATE_LIMITED` code.

#### Shared Context

Duplicated from Phase 2C/2D and the C9 spec (implement from this section alone).

##### Service Layer interface consumed (C9)

```rust
pub struct CallContext<'a> { pub bearer: &'a str, pub correlation_id: &'a str, pub idempotency_key: Option<&'a str> }
pub struct CallResult<T> { pub data: T, pub rate: RateSnapshot, pub replayed: bool }
// Service methods: capabilities, recall, remember, get_fact, retire, delete — see the C9 spec.
```

`AppError` (2C.7) and its canonical registry `code`s are exactly as in the C9 / C8 specs. Request/response payloads (`RecallRequest`, `RememberRequest`, `RecallResponse`, `Fact`, `WriteAck`, `RetireAck`, `DeletionProof`, `Capabilities`) are the 2C.4 types, unchanged.

##### Configuration & environment variables (2D, edge-owned + consumed)

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_MCP_HTTP_ADDR` | socket addr | `0.0.0.0:8081` | no | Bind address for the MCP streamable-HTTP listener. |
| `RECALL_MCP_PATH` | path | `/mcp` | no | HTTP path the MCP endpoint is served at. |
| `RECALL_MAX_BODY_BYTES` | u32 | `1048576` | no | Max request body in bytes (1 MiB), applied at this HTTP transport as at the REST edge. |
| `RECALL_OIDC_*`, `RECALL_STORE_*`, `RECALL_EMBED_*`, `RECALL_RERANK_*`, `RECALL_RATE_*`, `RECALL_IDEMPOTENCY_TTL_SECS`, `RECALL_ENV` | — | — | — | Read identically to the REST binary via the same `Config` (SA-BIN-01); both binaries share one configuration model. |

#### Public Interface

The MCP surface is the standard MCP methods over streamable-HTTP at `RECALL_MCP_PATH`. Every call requires `Authorization: Bearer <OIDC JWT>` (the MCP transport is HTTP; the bearer rides the HTTP request).

**`tools/list`** — returns the tool catalogue: `recall`, `remember`, `get`, `retire`, `delete`, `capabilities`. Each tool entry carries a name, a one-line description, and a JSON-Schema `inputSchema` generated from the corresponding 2C request type. This is the MCP analogue of `GET /v1` + `/openapi.json`.

**`tools/call` — `recall`** — input: `RecallRequest` shape (`query`, optional `filters`, `result_cap`, `cursor`, `include_provenance`). Calls C9 `recall`. Result: the `RecallResponse` (ranked facts, each with `source` only when `include_provenance` was set) plus `abstained`/`next_cursor` surfaced in the tool-result structured content.

**`tools/call` — `remember`** — input: `RememberRequest` shape (`content` object, optional `source`, `memory_class`) **plus a required `idempotency_key` string** (1–255 chars — the MCP analogue of the REST `Idempotency-Key` header). Calls C9 `remember`. Result: `WriteAck` (`job_id`, `status` = `accepted` | `already-accepted`).

**`tools/call` — `get`** — input: `{ id: string }`. Calls C9 `get_fact`. Result: the `Fact`. (Conditional-GET/ETag is a REST-only optimisation and is not offered over MCP.)

**`tools/call` — `retire`** — input: `{ id: string, idempotency_key: string }`. Calls C9 `retire`. Result: `RetireAck`.

**`tools/call` — `delete`** — input: `{ id: string, idempotency_key: string }`. Calls C9 `delete`. Result: `DeletionProof` (verifiable hard delete; unchanged semantics, SA-DELETE-01).

**`tools/call` — `capabilities`** — input: none. Calls C9 `capabilities`. Result: `Capabilities`.

The MCP server also implements the protocol's `initialize` handshake and serves only over `RECALL_MCP_HTTP_ADDR`; no stdio transport is offered (ADR-016).

##### Example

`tools/call { "name": "recall", "arguments": {"query":"who owns the orders table","result_cap":5} }` with header `Authorization: Bearer eyJ…` → tool result with structured content `{"facts":[{"fact":{…},"score":0.87}],"abstained":false}`. A `tools/call` to `remember` without a valid bearer → an MCP error with `code` `AUTH_MISSING_TOKEN` / `AUTH_INVALID_TOKEN` (the same registry code the REST edge returns at 401).

#### Internal Logic

1. **Bootstrap.** `main` loads `Config` (X6), initialises observability (X3/X5), calls the shared `build_state(config)` to assemble the component stack, constructs `Service` (C9), registers the six tools, and serves the MCP server on `RECALL_MCP_HTTP_ADDR` at `RECALL_MCP_PATH`. Only `main` may panic on unrecoverable bootstrap (X1).
2. **Per `tools/call`:** (a) enforce the body-size limit (`RECALL_MAX_BODY_BYTES`) → an oversize body is an MCP error carrying `VAL_BODY_TOO_LARGE`; (b) extract the bearer from the HTTP `Authorization` header (`""` if absent); (c) mint a correlation id (inbound `X-Correlation-Id` if a valid UUID, else a fresh UUIDv4); (d) deserialise the tool `arguments` into the 2C request type — a shape mismatch is an MCP error carrying `VAL_INVALID_BODY`; for writes, read the required `idempotency_key` argument; (e) build `CallContext{ bearer, correlation_id, idempotency_key }` and call the matching C9 method.
3. **Map the result.** `Ok(CallResult{ data, rate, replayed })` → an MCP tool result whose structured content is `data` serialised as JSON; the correlation id and `rate` (limit/remaining/reset) are attached as tool-result metadata. `Err(AppError)` → an MCP error whose payload carries the registry `code` (the same string the REST edge maps to a status) and the registry human `message` (production: fixed text; development: underlying detail appended), plus the correlation id. The classification is C9's `AppError`→`code` mapping reused verbatim — the edge invents no codes.
4. **`tools/list`** returns the six tool descriptors with `inputSchema` generated from the 2C request types (the same type definitions the OpenAPI document is built from), so the MCP and REST contracts cannot drift.
5. **No audit/auth/rate logic here.** Authentication, authorisation, rate limiting, idempotency, and the audit write all happen inside the C9 call (steps 1–6 of the C9 Internal Logic). The edge adds none of them.

#### Data Model

N/A — the MCP API Edge owns no tables and introduces no DDL. Idempotency records and the audit trail are written by C9 via C1 ports.

#### Error Table

The edge renders every C9 `AppError` as an MCP error carrying the **same registry `code`** the REST edge maps to the HTTP status shown (parity is the contract, SA-MCP-MAP-01). Plus two transport-local conditions:

| Condition | Equivalent HTTP status | Code | MCP rendering |
|-----------|------|------|---------------|
| Tool arguments fail to deserialise into the request type | 400 | VAL_INVALID_BODY | MCP error, `code` VAL_INVALID_BODY |
| Request body exceeds `RECALL_MAX_BODY_BYTES` | 413 | VAL_BODY_TOO_LARGE | MCP error, `code` VAL_BODY_TOO_LARGE |
| Missing / invalid bearer | 401 | AUTH_MISSING_TOKEN / AUTH_INVALID_TOKEN | MCP error with the code (from C9) |
| Insufficient scope | 403 | AUTH_INSUFFICIENT_SCOPE | MCP error with the code (from C9) |
| Not found / out of scope | 404 | NOT_FOUND | MCP error with the code (from C9) |
| Missing `idempotency_key` argument on a write tool | 400 | VAL_MISSING_IDEMPOTENCY_KEY | MCP error with the code (from C9) |
| Rate-limit exhausted | 429 | RATE_LIMITED | MCP error with the code (no MCP rate headers exist) |
| Store/queue/provider failures | 502/503/504 | PROVIDER_* / STORE_* / QUEUE_* | MCP error with the code (from C9) |
| Unmapped internal failure | 500 | INTERNAL | MCP error, `code` INTERNAL |

Minimum-two satisfied; every code traces to the central registry via C9.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: MCP API Edge

  Scenario: Happy path — recall tool returns ranked facts via the Service Layer
    Given the recall-mcp server is running and a valid read-scoped bearer in the Authorization header
    And the store holds facts matching "who owns the orders table" in the caller's scope
    When the client sends tools/call name "recall" arguments {"query":"who owns the orders table","result_cap":5}
    Then the tool result structured content contains the ranked facts
    And exactly one audit record with operation "recall" outcome "success" was written by the Service Layer

  Scenario: Edge case — tools/list advertises the six tools with input schemas
    When the client sends tools/list
    Then the result lists tools recall, remember, get, retire, delete, capabilities
    And each tool carries an inputSchema generated from the same types that back the OpenAPI document

  Scenario: Edge case — idempotent remember replay over MCP returns the original ack
    Given remember was called with idempotency_key "k-001" one minute ago
    When the client calls the remember tool again with the same idempotency_key
    Then no new WorkJob is enqueued and the result status is "already-accepted"

  Scenario: Error path — missing bearer yields an MCP error with the same code as REST
    Given no Authorization header
    When the client sends tools/call name "recall"
    Then the response is an MCP error whose code is AUTH_MISSING_TOKEN
    And no audit record is written

  Scenario: Error path — error-code parity with the REST edge
    Given a fact id that does not exist in the caller's scope
    When the client calls the get tool for that id
    Then the MCP error code is NOT_FOUND, identical to the REST edge's 404 code for the same input
```

#### Performance, Security, Observability

- **Performance targets:** MCP-edge in-process overhead (bearer extraction, correlation id, argument deserialisation, result serialisation — excluding the C9 call) **≤ 5 ms p95**, matching the REST edge; the recall operation's end-to-end ≤ 200 ms p95 (NFR-P2) is met by C6 via C9, unchanged.
- **Security:** every tool call requires a valid OIDC bearer, validated by C3 inside C9; identity is never taken from tool arguments; the same per-tenant isolation, rate limiting, and audit apply because they are C9's. The token, tool arguments, and fact content are never logged or audited. `RECALL_ENV=production` suppresses internal error detail in the MCP error `message`. The MCP transport is networked streamable-HTTP only (no stdio / ambient identity), preserving AUTH1–AUTH7 (ADR-016).
- **Observability:** metrics `recall_mcp_tool_calls_total{tool,outcome}`, `recall_mcp_edge_overhead_seconds{tool}` (histogram), reusing C9's `recall_service_calls_total` for the orchestration view. Log fields: `correlation_id`, `subject`, `tenant`, `jti`, `tool`, `outcome`, `code`, `latency_ms` (subject/tenant/jti come back from C9 on the call). Trace spans: `mcp.tools_call` (root, attributes `recall.tool`, `recall.correlation_id`) wrapping the C9 `service.call` span.

#### Gaps

None.
