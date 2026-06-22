### SPEC: Freshness Checker — RETIRED (superseded by ADR-014)

> **Status:** Retired 2026-06-22 (RFC 01, ADR-014) · **derivedFromHld:** 0.5.0

This component is **removed**. ADR-013 placed a recall-side source-change check here; ADR-014 reverses
that — the broker is a per-agent local component a central `recall` cannot reach, so freshness is the
agent's responsibility. `recall` performs no source-change check, makes no outbound broker call, and
enqueues no `ReReadSource` job.

What replaces it: the Retrieval Engine (C6) returns each sourced fact's `origin_ref` +
`modification_marker` on request (`include_provenance`), and the agent (with its co-located broker)
runs the ask → check → update loop, writing a fresh superseding note via `POST /v1/memories` when a
source has changed. No code under `src/freshness/` is part of the build; the `BrokerClient` port,
`HttpBrokerClient` adapter, `RECALL_BROKER_URL`, and the `ReReadSource` job kind are removed.
