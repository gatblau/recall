# syntax=docker/dockerfile:1
#
# Multi-stage image carrying BOTH recall binaries (Phase 1, plan 03).
#
# Builder stage compiles the crate in release mode, producing `target/release/recall` and
# `target/release/recall-mcp`. Runtime stage is a slim Debian image carrying only the two
# binaries, the CA bundle (TLS for OIDC discovery / JWKS fetch), a non-root user, and a
# writable store directory. The default command is `recall`; deployments override it to
# `recall-mcp` for the MCP edge.

# ---- Builder ----------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /usr/src/recall

# Dependency-cache layer: copy the manifests first and build a throwaway crate so the
# (slow) dependency compile is cached independently of the source. surrealdb + aws-lc-rs
# pull in C/asm toolchains already present in the rust:1-bookworm base.
#
# The manifest declares a `[[test]] name = "bdd"` target whose file (`tests/bdd.rs`) is
# excluded from the build context (see .dockerignore), so cargo would refuse to parse the
# manifest without it. We create an empty placeholder so the manifest is valid; building
# only the two `--bin` targets never compiles the test target.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/mcp tests \
    && echo 'fn main() {}' > src/main.rs \
    && echo 'fn main() {}' > src/mcp/main.rs \
    && echo '' > src/lib.rs \
    && echo '' > tests/bdd.rs \
    && cargo build --release --locked --bin recall --bin recall-mcp 2>/dev/null; rm -rf src

# Real source. Touch the entry points so cargo recompiles them over the cached deps; keep
# the placeholder `tests/bdd.rs` in place so the manifest still parses.
COPY src ./src
COPY migrations ./migrations
RUN touch src/main.rs src/mcp/main.rs src/lib.rs \
    && cargo build --release --locked --bin recall --bin recall-mcp

# ---- Runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# TLS trust roots for OIDC discovery and the JWKS fetch performed at boot.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user owning the writable store directory.
RUN groupadd --system --gid 10001 recall \
    && useradd --system --uid 10001 --gid recall --home-dir /var/lib/recall --shell /usr/sbin/nologin recall \
    && mkdir -p /var/lib/recall \
    && chown -R recall:recall /var/lib/recall

COPY --from=builder /usr/src/recall/target/release/recall /usr/local/bin/recall
COPY --from=builder /usr/src/recall/target/release/recall-mcp /usr/local/bin/recall-mcp

ENV RECALL_STORE_PATH=/var/lib/recall \
    RECALL_HTTP_ADDR=0.0.0.0:8080 \
    RECALL_MCP_HTTP_ADDR=0.0.0.0:8081

EXPOSE 8080 8081

USER recall

# Default to the REST binary; deployments override the command to `recall-mcp`.
CMD ["recall"]
