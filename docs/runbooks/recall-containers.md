# Runbook — recall containers (local docker-compose)

Operational note for running both recall edges locally from the single container image. Covers
building the image, bringing the stack up, the ports/commands, the required configuration, and the
one honest caveat (placeholder providers).

## What this stack runs

One image (`recall:ci`) carries two binaries; the compose file runs each as its own service, plus a
shared store and a local OIDC issuer:

| Service     | Image                       | Command        | Host port | Role                          |
|-------------|-----------------------------|----------------|-----------|-------------------------------|
| `recall`    | `recall:ci`                 | `recall`       | `8080`    | REST API edge                 |
| `recall-mcp`| `recall:ci`                 | `recall-mcp`   | `8081`    | MCP edge (JSON-RPC at `/mcp`)  |
| `surrealdb` | `surrealdb/surrealdb:v3.1.5`| `start --user … --pass … memory` | `8000`  | Shared store (AUTHENTICATED; recall signs in, FU-019) |
| `dex`       | `dexidp/dex:v2.41.1`        | `dex serve …`  | `35357`   | Local OIDC issuer (token mint) |

Both edges share ONE store (`surrealdb`) so they are genuinely two manifestations of one service: a
`remember` written via REST is visible via MCP and vice versa. They cannot share an embedded
SurrealKV file, so the stack uses a shared **remote** SurrealDB reached over `ws://` — both edges set
`RECALL_STORE_REMOTE_URL=ws://surrealdb:8000` and `Store::connect` resolves the `ws://` scheme to the
remote engine (`src/store/mod.rs`).

## Build the image

The compose file references the locally-built tag `recall:ci`, produced by the Phase-1 Dockerfile:

```sh
docker build -t recall:ci .
```

A published release uses `ghcr.io/gatblau/recall:<tag>` instead (Phase 3); swap the `image:` lines in
`docker-compose.yml` to deploy a released image.

## Bring the stack up

```sh
docker compose up -d        # start all four services in the background
docker compose ps           # watch the edges settle (they restart-loop until deps are ready)
docker compose logs -f recall recall-mcp   # follow edge boot
```

### Boot ordering — the edges restart until their dependencies are up

`build_state` constructs the OIDC Authenticator (discovery + JWKS fetch) and opens the store, and
**exits non-zero with no internal retry** if either the OIDC issuer or the store is unreachable.
`depends_on` only orders *start*, it does not wait for *readiness*, so each edge carries
`restart: on-failure`: it dies fast if Dex/SurrealDB are not yet listening and compose restarts it
until boot succeeds. Expect a few restart cycles on a cold `up`; this is normal, not a fault.

## Verify

```sh
# REST liveness — expect {"data":{"status":"live"},...}
curl -fsS localhost:8080/healthz

# MCP discovery — expect a JSON-RPC result listing six tools
curl -fsS -X POST localhost:8081/mcp \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

The six MCP tools are `recall`, `remember`, `get`, `retire`, `delete`, `capabilities`.

## Tear down

```sh
docker compose down -v       # stop and remove containers + the (anonymous) volumes
```

## Required configuration (per edge)

Set in `docker-compose.yml` under each edge's `environment:`. The authoritative key list is the §2D
table in the configuration spec; the keys that matter for this stack:

| Variable                   | Stack value (local)         | Purpose                                              |
|----------------------------|-----------------------------|------------------------------------------------------|
| `RECALL_OIDC_ISSUER`       | `http://dex:5556/dex`       | OIDC issuer; the in-network Dex name so `iss` validates |
| `RECALL_OIDC_AUDIENCE`     | `recall-test`               | Expected `aud` (the Dex static client id)            |
| `RECALL_STORE_REMOTE_URL`  | `ws://surrealdb:8000`       | Shared remote store — identical on BOTH edges        |
| `RECALL_EMBED_URL`         | placeholder                 | Embedding provider endpoint (operator-supplied)      |
| `RECALL_EMBED_API_KEY`     | placeholder                 | Embedding provider key (operator-supplied)           |
| `RECALL_RERANK_URL`        | placeholder                 | Reranker provider endpoint (operator-supplied)       |
| `RECALL_RERANK_API_KEY`    | placeholder                 | Reranker provider key (operator-supplied)            |
| `RECALL_EMBED_DIM`         | `1024`                      | Embedding vector dimension (must match the provider) |

The image defaults `RECALL_HTTP_ADDR=0.0.0.0:8080` and `RECALL_MCP_HTTP_ADDR=0.0.0.0:8081`, so the
listen addresses need no overriding.

## Honest caveats

### The shared store is AUTHENTICATED (FU-019 closed)

`surrealdb` starts with root credentials (`--user`/`--pass`, default `root`/`root` for local,
overridable via a `.env` file). recall's `Store::connect` signs in to the secured store when
`RECALL_STORE_REMOTE_USER`/`RECALL_STORE_REMOTE_PASS` are set — both edges carry the same pair as the
`surrealdb` service, so they cannot drift. Setting exactly one of user/pass fails startup
(both-or-neither). **For production:** supply strong credentials (never the `root`/`root` default) and
use `wss://` (TLS) to the store; a namespace/database-scoped (non-root) credential is a further
hardening step (tracked as a follow-up). The earlier unauthenticated-store caveat is resolved.

### Provider placeholders mean recall/remember is partial

`RECALL_EMBED_URL`/`RECALL_RERANK_URL` (and their API keys) are placeholders. The provider clients are
lazy (HTTP only on use), so the edges **boot and serve discovery/health** fine, but `recall` and
`remember` will not fully work until real embedding/reranker endpoints are supplied. Replace the
placeholder URLs and keys with real providers (and set `RECALL_EMBED_DIM` to that model's output
dimension) before exercising the memory paths.

### The `memory` store is ephemeral

`surrealdb` runs the in-memory backend, so the store is wiped on every `down`. For a store that
survives restarts, change the SurrealDB command to a `surrealkv://` path and mount a named volume for
it.

## Local Dex / token minting

For an authenticated call against the REST edge, mint an `id_token` from the host-exposed Dex
(`localhost:35357`) via the password grant (client `recall-test` / `recall-test-secret`, user
`tester@example.com` / `password123`). The Dex config (`deploy/dex.yaml`) mirrors the BDD harness in
`tests/support/dex.rs`; its client secret and bcrypt password hash are test fixtures, not real
credentials.
