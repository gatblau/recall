-- migration 0001_init.down.surql — destructive reversal of the full schema, run within each tenant
-- namespace/database. The Migrator selects the namespace (USE NS <tenant>; USE DB recall) before
-- running these statements. Statements run in reverse dependency order to the up migration.
--
-- DESTRUCTIVE (sql-safety): REMOVE TABLE / REMOVE INDEX discards every fact, entity, relationship,
-- source, audit record, queued/dead-lettered job, quarantined item, maintenance bookkeeping row, and
-- idempotency outcome in the namespace, and drops the vector/keyword indexes. Intended ONLY to unwind a
-- failed initial migration on an empty store; the Migrator refuses this down path against a populated
-- namespace without an explicit override (X7).

-- C8 — idempotency_record
REMOVE INDEX IF EXISTS idx_idem_expiry ON idempotency_record;
REMOVE INDEX IF EXISTS idx_idem_key    ON idempotency_record;
REMOVE TABLE IF EXISTS idempotency_record;

-- C7 — maintenance_state
REMOVE INDEX IF EXISTS maintenance_state_tenant ON maintenance_state;
REMOVE TABLE IF EXISTS maintenance_state;

-- C4 — quarantine
REMOVE INDEX IF EXISTS quarantine_idem_idx  ON quarantine;
REMOVE INDEX IF EXISTS quarantine_owner_idx ON quarantine;
REMOVE TABLE IF EXISTS quarantine;

-- C2 — work queue
REMOVE INDEX IF EXISTS dead_letter_scope_idx ON dead_letter;
REMOVE INDEX IF EXISTS work_job_idem_idx ON work_job;
REMOVE INDEX IF EXISTS work_job_claim_idx ON work_job;
REMOVE TABLE IF EXISTS dead_letter;
REMOVE TABLE IF EXISTS work_job;

-- C1 — store + audit
REMOVE INDEX IF EXISTS fact_hnsw ON fact;
REMOVE INDEX IF EXISTS fact_bm25 ON fact;
REMOVE INDEX IF EXISTS fact_owner_user ON fact;
REMOVE INDEX IF EXISTS fact_owner_team ON fact;
REMOVE INDEX IF EXISTS fact_valid ON fact;
REMOVE INDEX IF EXISTS fact_ent ON fact;
REMOVE INDEX IF EXISTS entity_name ON entity;
REMOVE INDEX IF EXISTS entity_owner ON entity;
REMOVE INDEX IF EXISTS rel_from ON relationship;
REMOVE INDEX IF EXISTS rel_to ON relationship;
REMOVE INDEX IF EXISTS audit_at ON audit_log;
REMOVE ANALYZER IF EXISTS recall_text;
REMOVE TABLE IF EXISTS audit_log;
REMOVE TABLE IF EXISTS source;
REMOVE TABLE IF EXISTS relationship;
REMOVE TABLE IF EXISTS entity;
REMOVE TABLE IF EXISTS fact;
