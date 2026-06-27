# Agentic Memory (`recall`) — High-Level Design

> **Mode:** draft
> **Generated from:** inline context — `docs/concept/agentic-mem.md`, `docs/concept/good-mem.md`, `docs/concept/requirements.md`
> **Revision:** 0.6.0
> **Last updated:** 2026-06-22

## Orientation

This HLD describes `recall`, a standalone memory service for AI agents. It stores facts learned from
interactions and source documents, lets an agent write / recall / forget those facts over an
authenticated HTTP API, keeps them current and consistent over time, and does so under per-user
access control. It is designed to sit behind the agentic-search broker — an agent's sandboxed
script calls `recall` through the broker, which injects an OIDC-issued identity. The design is
forward-looking (greenfield); `recall` does not yet exist in code. The next move is `/spec` against
this folder to produce the Low-Level Design.

## Contents

- [00 — Overview](./00-overview.md) — Summary, Motivation, Goals, Non-goals, Glossary.
- [01 — Context](./01-context.md) — System context diagram, external actors, neighbouring systems.
- [02 — Architecture](./02-architecture.md) — System architecture diagram, component responsibilities.
- [03 — Sequences](./03-sequences.md) — Principal sequence diagrams (recall, remember, consolidate, forget).
- [04 — Domain](./04-domain.md) — Domain Model, Data Lifecycle.
- [05 — Tech stack](./05-tech-stack.md) — Tech choices and rationale.
- [06 — Regulatory](./06-regulatory.md) — Compliance obligations and how the design addresses them.
- [07 — Cross-cutting](./07-cross-cutting.md) — Cross-cutting Concerns table, NFRs.
- [08 — Interfaces](./08-interfaces.md) — External interfaces (names and shapes only).
- [09 — Decisions](./09-decisions.md) — Architectural Decision Records (ADRs).
- [10 — Risks](./10-risks.md) — Risks, Alternatives, Dependencies, Rollout/Rollback.
- [99 — Changelog](./99-changelog.md) — HLD revision history.
