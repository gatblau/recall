# 06 — Regulatory and Compliance Context

> **Mode:** draft · **Revision:** 0.5.0 · **Last updated:** 2026-06-22

`recall` accumulates real facts about people and organisations, so data-protection obligations apply
even though no specific regulated sector is committed. The jurisdiction is not yet fixed (see
OQ-VOLUMES / deployment context); the design provides the *capabilities* such regimes require so a
specific jurisdiction can be satisfied by configuration and policy rather than redesign.

| Obligation | Source | How the design addresses it |
|---|---|---|
| Right to erasure / deletion | GDPR-style data-protection law (jurisdiction TBC) | Verifiable hard deletion of a Fact including derived summaries and embeddings, with proof (FR-D3, Forget sequence). Per-tenant erasure is clean — dropping the tenant namespace removes all its data (ADR-011). |
| Data minimisation | GDPR-style data-protection law | Write path filters to salient facts only; stores structured assertions, not raw transcripts (Write Pipeline). |
| Purpose limitation / access control | GDPR-style data-protection law | Namespace-per-tenant hard isolation plus logical Team / User visibility (ADR-011); access only as the authenticated user; source access rights enforced by source systems, not duplicated. |
| Personal-data protection | GDPR-style data-protection law | On the write path, PII is **redacted when the detector flags it with high confidence and flagged-for-review otherwise** (exact thresholds in `/spec`); encryption at rest; no credentials or tokens logged. |
| Accountability / auditability | GDPR-style data-protection law; general security governance | Every call audited (subject, operation, scope, outcome, token `jti`) to a **dedicated append-only audit trail, held per-tenant and stored distinctly from operational logs** (see [07 — Cross-cutting → Audit](./07-cross-cutting.md)); the trail is itself covered by per-tenant erasure. |
| Integrity of stored records | General security governance | Bi-temporal supersession preserves history; write gate resists memory poisoning; provenance on every fact. |

**Sector-specific regimes (PCI-DSS, IFRS17, HIPAA, and comparable sector frameworks):** N/A at
launch — no payment, financial-reporting, or health-record processing is in scope. If a deployment ingests regulated data, the
obligation is added here and routed through `/spec` via the HLD-impact-pass before that deployment.
