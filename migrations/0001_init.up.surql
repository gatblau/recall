-- migration 0001_init.up.surql — the full recall schema, applied within each tenant namespace/database.
-- The Migrator selects the namespace (USE NS <tenant>; USE DB recall) before running these statements,
-- so this file contains only the table/field/index DDL. All DEFINE statements use IF NOT EXISTS so the
-- migration is idempotent (re-running it is a no-op). One namespace per tenant (ADR-011), so every
-- table below is per-tenant and isolation is structural.
--
-- This is the squashed greenfield schema: the C1 graph/vector/keyword store + audit, the C2 work queue,
-- the C4 quarantine table, the C7 maintenance bookkeeping, and the C8 idempotency record — previously
-- split across 0001–0005, consolidated into one initial migration before first release.
--
-- SurrealDB 3.x adaptations baked in:
--   * BM25 uses `FULLTEXT ANALYZER … BM25` over a derived `content_text` projection (the 2.x
--     `SEARCH ANALYZER` form is rejected).
--   * The `<dim>` placeholder in the HNSW DIMENSION is substituted by the Migrator from RECALL_EMBED_DIM.
--   * Schemafull tables that carry arbitrary nested JSON (`work_job.payload`, `dead_letter.payload`,
--     `dead_letter.scope`, `quarantine.content`, `audit_log.scope`, `idempotency_record.response_body`)
--     use `TYPE object FLEXIBLE` (3.x requires FLEXIBLE after TYPE and rejects un-enumerated nested keys
--     on a bare `TYPE object`).
--   * Domain ids are stored as "table:key" strings throughout (see src/store/convert.rs), so link-shaped
--     fields are `string` / `array<string>` / `option<string>`, not native record links.

-- ====================================================================================================
-- C1 — Memory Store: fact / entity / relationship / source, plus the append-only audit trail.
-- ====================================================================================================

-- fact: schemaless body, indexed for the three retrieval signals.
DEFINE TABLE IF NOT EXISTS fact SCHEMALESS;
DEFINE FIELD IF NOT EXISTS owner       ON fact TYPE object;
DEFINE FIELD IF NOT EXISTS valid_from  ON fact TYPE datetime;
DEFINE FIELD IF NOT EXISTS valid_to    ON fact TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS ingested_at ON fact TYPE datetime;
DEFINE FIELD IF NOT EXISTS confidence  ON fact TYPE float ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS salience    ON fact TYPE float ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS stability   ON fact TYPE float ASSERT $value >= 0.0;
DEFINE FIELD IF NOT EXISTS visibility  ON fact TYPE string
    ASSERT $value IN ['user-private','team-shared','tenant-shared'];
DEFINE FIELD IF NOT EXISTS memory_class ON fact TYPE string
    ASSERT $value IN ['episodic','semantic','consolidated'];
DEFINE FIELD IF NOT EXISTS pii_review      ON fact TYPE bool DEFAULT false;
DEFINE FIELD IF NOT EXISTS embedding       ON fact TYPE option<array<float>>;
DEFINE FIELD IF NOT EXISTS embedding_model ON fact TYPE option<string>;
-- BM25 keyword analyzer + full-text index over the serialised content (3.x syntax).
DEFINE ANALYZER IF NOT EXISTS recall_text TOKENIZERS class FILTERS lowercase, ascii;
DEFINE INDEX IF NOT EXISTS fact_bm25 ON fact FIELDS content_text FULLTEXT ANALYZER recall_text BM25;
-- HNSW vector index; DIMENSION must equal RECALL_EMBED_DIM (SA-EMBED-01).
DEFINE INDEX IF NOT EXISTS fact_hnsw ON fact FIELDS embedding HNSW DIMENSION <dim> DIST COSINE;
-- secondary indexes for scope filtering, the graph leg, and bi-temporal scans.
DEFINE INDEX IF NOT EXISTS fact_owner_user ON fact FIELDS owner.user;
DEFINE INDEX IF NOT EXISTS fact_owner_team ON fact FIELDS owner.team;
DEFINE INDEX IF NOT EXISTS fact_valid      ON fact FIELDS valid_to;
DEFINE INDEX IF NOT EXISTS fact_ent        ON fact FIELDS entities;

DEFINE TABLE IF NOT EXISTS entity SCHEMALESS;
DEFINE FIELD IF NOT EXISTS canonical_name ON entity TYPE string
    ASSERT string::len($value) > 0 AND string::len($value) <= 512;
DEFINE FIELD IF NOT EXISTS aliases        ON entity TYPE array<string>;
DEFINE FIELD IF NOT EXISTS owner          ON entity TYPE object;
DEFINE INDEX IF NOT EXISTS entity_name    ON entity FIELDS canonical_name;
DEFINE INDEX IF NOT EXISTS entity_owner   ON entity FIELDS owner.user, owner.team;

DEFINE TABLE IF NOT EXISTS relationship SCHEMALESS;
DEFINE FIELD IF NOT EXISTS kind       ON relationship TYPE string
    ASSERT string::len($value) > 0 AND string::len($value) <= 128;
DEFINE FIELD IF NOT EXISTS from_ent   ON relationship TYPE string;
DEFINE FIELD IF NOT EXISTS to_ent     ON relationship TYPE string;
DEFINE FIELD IF NOT EXISTS valid_from ON relationship TYPE datetime;
DEFINE FIELD IF NOT EXISTS valid_to   ON relationship TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS confidence ON relationship TYPE float ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS owner      ON relationship TYPE object;
DEFINE INDEX IF NOT EXISTS rel_from   ON relationship FIELDS from_ent;
DEFINE INDEX IF NOT EXISTS rel_to     ON relationship FIELDS to_ent;

DEFINE TABLE IF NOT EXISTS source SCHEMALESS;
DEFINE FIELD IF NOT EXISTS origin_ref           ON source TYPE string;
DEFINE FIELD IF NOT EXISTS modification_marker  ON source TYPE option<string>;
DEFINE FIELD IF NOT EXISTS trust_signal         ON source TYPE float ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS owner                ON source TYPE object;

-- audit_log: schemafull + append-only from the first migration (SA-AUDIT-01).
DEFINE TABLE IF NOT EXISTS audit_log SCHEMAFULL
    PERMISSIONS FOR create FULL FOR select FULL FOR update NONE FOR delete NONE;
DEFINE FIELD IF NOT EXISTS subject        ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS operation      ON audit_log TYPE string;
-- `scope` is `TYPE object FLEXIBLE`: the audit scope carries the full ScopeRef sub-keys (tenant/team/user).
DEFINE FIELD IF NOT EXISTS scope          ON audit_log TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS outcome        ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS token_jti      ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS correlation_id ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS at             ON audit_log TYPE datetime;
DEFINE INDEX IF NOT EXISTS audit_at ON audit_log FIELDS at;

-- ====================================================================================================
-- C2 — Durable Work Queue: work_job + dead_letter.
-- ====================================================================================================

DEFINE TABLE IF NOT EXISTS work_job SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS kind            ON work_job TYPE string
    ASSERT $value IN ['extract_fact','re_embed_fact','re_read_source','consolidate','hard_delete'];
DEFINE FIELD IF NOT EXISTS payload         ON work_job TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS scope           ON work_job TYPE object;
DEFINE FIELD IF NOT EXISTS scope.tenant    ON work_job TYPE string;
DEFINE FIELD IF NOT EXISTS scope.team      ON work_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS scope.user      ON work_job TYPE string;
DEFINE FIELD IF NOT EXISTS idempotency_key ON work_job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS attempts        ON work_job TYPE int    DEFAULT 0 ASSERT $value >= 0;
DEFINE FIELD IF NOT EXISTS status          ON work_job TYPE string
    ASSERT $value IN ['pending','leased','done','dead_letter'];
DEFINE FIELD IF NOT EXISTS not_before      ON work_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS created_at      ON work_job TYPE datetime;
DEFINE FIELD IF NOT EXISTS leased_until    ON work_job TYPE option<datetime>;
-- Claim-path index: the claim WHERE filters on status, kind, not_before.
DEFINE INDEX IF NOT EXISTS work_job_claim_idx ON work_job FIELDS status, kind, not_before;
-- Dedup index: (scope, idempotency_key) unique when the key is present (a NONE key never collides).
DEFINE INDEX IF NOT EXISTS work_job_idem_idx ON work_job FIELDS scope, idempotency_key UNIQUE;

DEFINE TABLE IF NOT EXISTS dead_letter SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS kind             ON dead_letter TYPE string;
DEFINE FIELD IF NOT EXISTS payload          ON dead_letter TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS scope            ON dead_letter TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS idempotency_key  ON dead_letter TYPE option<string>;
DEFINE FIELD IF NOT EXISTS attempts         ON dead_letter TYPE int;
DEFINE FIELD IF NOT EXISTS original_job_id  ON dead_letter TYPE string;
DEFINE FIELD IF NOT EXISTS created_at       ON dead_letter TYPE datetime;
DEFINE FIELD IF NOT EXISTS dead_lettered_at ON dead_letter TYPE datetime;
DEFINE INDEX IF NOT EXISTS dead_letter_scope_idx ON dead_letter FIELDS scope.tenant, dead_lettered_at;

-- ====================================================================================================
-- C4 — Write Pipeline: quarantine (gate-uncertain content awaiting review).
-- ====================================================================================================

DEFINE TABLE IF NOT EXISTS quarantine SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS content            ON quarantine TYPE object FLEXIBLE; -- redacted content (PII removed)
DEFINE FIELD IF NOT EXISTS entities           ON quarantine TYPE array<string>;   -- resolved entity ids (>=1)
DEFINE FIELD IF NOT EXISTS source_id          ON quarantine TYPE option<string>;  -- provenance, null for agent-stated
DEFINE FIELD IF NOT EXISTS memory_class       ON quarantine TYPE string
    ASSERT $value IN ['episodic','semantic','consolidated'];
DEFINE FIELD IF NOT EXISTS owner              ON quarantine TYPE object;          -- { tenant, team, user }
DEFINE FIELD IF NOT EXISTS owner.tenant       ON quarantine TYPE string;
DEFINE FIELD IF NOT EXISTS owner.team         ON quarantine TYPE option<string>;
DEFINE FIELD IF NOT EXISTS owner.user         ON quarantine TYPE string;
DEFINE FIELD IF NOT EXISTS trust_score        ON quarantine TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;                                        -- the gate's trust in [0,1]
DEFINE FIELD IF NOT EXISTS confidence         ON quarantine TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS salience           ON quarantine TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD IF NOT EXISTS is_instruction_like ON quarantine TYPE bool;           -- imperative-pattern detector result
DEFINE FIELD IF NOT EXISTS pii_review         ON quarantine TYPE bool DEFAULT false;
DEFINE FIELD IF NOT EXISTS idempotency_key    ON quarantine TYPE option<string>;  -- carried from the originating job
DEFINE FIELD IF NOT EXISTS reason             ON quarantine TYPE string;          -- audit: why quarantined
DEFINE FIELD IF NOT EXISTS quarantined_at     ON quarantine TYPE datetime;        -- server-set, RFC3339 ms
DEFINE INDEX IF NOT EXISTS quarantine_owner_idx ON quarantine FIELDS owner.user, quarantined_at;
DEFINE INDEX IF NOT EXISTS quarantine_idem_idx  ON quarantine FIELDS idempotency_key UNIQUE;  -- replay-safe

-- ====================================================================================================
-- C7 — Maintenance Worker: maintenance_state (per-tenant cycle bookkeeping; maintenance-only).
-- ====================================================================================================

DEFINE TABLE IF NOT EXISTS maintenance_state SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant              ON maintenance_state TYPE string
    ASSERT $value != "";                                                          -- the owning tenant id
DEFINE FIELD IF NOT EXISTS last_cycle_at       ON maintenance_state TYPE option<datetime>; -- last successful run_cycle
DEFINE FIELD IF NOT EXISTS embed_model_version ON maintenance_state TYPE option<string>;   -- active embed model seen
DEFINE INDEX IF NOT EXISTS maintenance_state_tenant ON maintenance_state FIELDS tenant UNIQUE;

-- ====================================================================================================
-- C8 — HTTP API Edge: idempotency_record (one stored write outcome per (tenant,user,route,key)).
-- ====================================================================================================

DEFINE TABLE IF NOT EXISTS idempotency_record SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS tenant          ON idempotency_record TYPE string;
DEFINE FIELD IF NOT EXISTS user            ON idempotency_record TYPE string;
DEFINE FIELD IF NOT EXISTS route           ON idempotency_record TYPE string;   -- e.g. "POST /v1/memories"
DEFINE FIELD IF NOT EXISTS idempotency_key ON idempotency_record TYPE string
    ASSERT string::len($value) >= 1 AND string::len($value) <= 255;
DEFINE FIELD IF NOT EXISTS response_body   ON idempotency_record TYPE object FLEXIBLE; -- original Success payload, replayed verbatim
DEFINE FIELD IF NOT EXISTS status_code     ON idempotency_record TYPE int;
DEFINE FIELD IF NOT EXISTS created_at      ON idempotency_record TYPE datetime   VALUE time::now();
DEFINE FIELD IF NOT EXISTS expires_at      ON idempotency_record TYPE datetime;  -- created_at + RECALL_IDEMPOTENCY_TTL_SECS
DEFINE INDEX IF NOT EXISTS idx_idem_key    ON idempotency_record FIELDS tenant, user, route, idempotency_key UNIQUE;
DEFINE INDEX IF NOT EXISTS idx_idem_expiry ON idempotency_record FIELDS expires_at;
