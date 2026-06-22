//! X7 — Database migrations. A `Migrator` applies numbered, ordered `NNNN_<slug>.{up,down}.surql`
//! pairs idempotently per tenant namespace, recording applied versions in a `schema_migrations`
//! table. Tables start schemaless and tighten later (SA-MIG-01). Applies to a shared store are an
//! explicit user action; `dry_run` is the review artefact (sql-safety layer rule).

use serde_json::Value as Json;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

use crate::types::ports::StoreError;

/// One embedded migration: a zero-padded version, a slug, and its up/down SurrealQL text. The text is
/// compiled into the binary via `include_str!` so the running service is self-contained.
struct Migration {
    version: u32,
    slug: &'static str,
    up: &'static str,
    down: &'static str,
}

/// The ordered, monotonic migration set. New migrations append here with the next version number.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    slug: "init",
    // The full greenfield schema (C1 store + audit, C2 queue, C4 quarantine, C7 maintenance_state,
    // C8 idempotency_record) squashed into one initial migration before first release.
    up: include_str!("../../migrations/0001_init.up.surql"),
    down: include_str!("../../migrations/0001_init.down.surql"),
}];

/// The database name used inside every tenant namespace (ADR-011: one namespace per tenant, one
/// database `recall` inside each).
pub const DB_NAME: &str = "recall";

/// Applies and reverses the numbered migration set against a per-tenant SurrealDB namespace.
pub struct Migrator<'a> {
    db: &'a Surreal<Any>,
    /// The embedding dimension substituted into the HNSW index DDL (`RECALL_EMBED_DIM`, SA-EMBED-01).
    embed_dim: u32,
}

impl<'a> Migrator<'a> {
    /// Build a migrator bound to a live connection and the configured embedding dimension.
    pub fn new(db: &'a Surreal<Any>, embed_dim: u32) -> Self {
        Self { db, embed_dim }
    }

    /// Select the tenant namespace and `recall` database, defining both idempotently first.
    async fn select_namespace(&self, tenant: &str) -> Result<(), StoreError> {
        // DEFINE NAMESPACE/DATABASE are parameterised by binding the identifiers as query vars is not
        // supported for DDL identifiers; tenant ids are validated by C3 before reaching the store and
        // are restricted to namespace-safe characters, so they are interpolated into the DDL only.
        validate_tenant(tenant)?;
        let ddl = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {tenant}; \
             USE NS {tenant}; \
             DEFINE DATABASE IF NOT EXISTS {DB_NAME};"
        );
        self.db
            .query(ddl)
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        self.db
            .use_ns(tenant.to_string())
            .use_db(DB_NAME)
            .await
            .map_err(map_db_err)?;
        // The migration bookkeeping table is created outside the numbered set so version 0 can be read.
        self.db
            .query(SCHEMA_MIGRATIONS_DDL)
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    /// The highest applied migration version for a tenant (0 on a fresh namespace).
    pub async fn current_version(&self, tenant: &str) -> Result<u32, StoreError> {
        self.select_namespace(tenant).await?;
        let mut resp = self
            .db
            .query("SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1")
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("read schema_migrations: {e}")))?;
        let version = rows
            .first()
            .and_then(|r| r.get("version"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(0);
        Ok(version)
    }

    /// Apply every pending `up` migration in order, recording each version. Idempotent: a second call
    /// with nothing pending is a no-op. Returns the new current version.
    pub async fn migrate_up(&self, tenant: &str) -> Result<u32, StoreError> {
        let mut current = self.current_version(tenant).await?;
        let pending: Vec<&Migration> =
            MIGRATIONS.iter().filter(|m| m.version > current).collect();
        for m in pending {
            let stmts = self.render_up(m);
            self.db
                .query(stmts)
                .await
                .map_err(map_db_err)?
                .check()
                .map_err(map_db_err)?;
            self.db
                .query("CREATE schema_migrations SET version = $v, applied_at = time::now()")
                .bind(("v", m.version as i64))
                .await
                .map_err(map_db_err)?
                .check()
                .map_err(map_db_err)?;
            tracing::info!(target: "recall", version = m.version, slug = m.slug, "migrate.applied");
            current = m.version;
        }
        Ok(current)
    }

    /// Return the statements that `migrate_up` WOULD run for the pending migrations, executing
    /// nothing. The review artefact before a shared-store apply (X7 Internal Logic 2).
    pub async fn dry_run(&self, tenant: &str) -> Result<Vec<String>, StoreError> {
        let current = self.current_version(tenant).await?;
        let mut out = Vec::new();
        for m in MIGRATIONS.iter().filter(|m| m.version > current) {
            out.push(self.render_up(m));
        }
        Ok(out)
    }

    /// Run the `down` pairs from the current version down to (and excluding) `to`. Destructive downs
    /// (the `0001` table drop) only ever target an empty store; this method does not guard population
    /// (the caller — an explicit user action — owns that decision per X7), but logs the intent.
    pub async fn migrate_down(&self, tenant: &str, to: u32) -> Result<(), StoreError> {
        let current = self.current_version(tenant).await?;
        for m in MIGRATIONS
            .iter()
            .rev()
            .filter(|m| m.version > to && m.version <= current)
        {
            tracing::warn!(target: "recall", version = m.version, slug = m.slug, "migrate.down");
            self.db
                .query(m.down)
                .await
                .map_err(map_db_err)?
                .check()
                .map_err(map_db_err)?;
            self.db
                .query("DELETE schema_migrations WHERE version = $v")
                .bind(("v", m.version as i64))
                .await
                .map_err(map_db_err)?
                .check()
                .map_err(map_db_err)?;
        }
        Ok(())
    }

    /// Render an `up` migration with the `<dim>` HNSW placeholder substituted by `RECALL_EMBED_DIM`.
    fn render_up(&self, m: &Migration) -> String {
        m.up.replace("<dim>", &self.embed_dim.to_string())
    }
}

/// DDL for the migration bookkeeping table (X7 Data Model). Idempotent.
const SCHEMA_MIGRATIONS_DDL: &str = "\
DEFINE TABLE IF NOT EXISTS schema_migrations SCHEMAFULL; \
DEFINE FIELD IF NOT EXISTS version    ON schema_migrations TYPE int; \
DEFINE FIELD IF NOT EXISTS applied_at ON schema_migrations TYPE datetime; \
DEFINE INDEX IF NOT EXISTS sm_version ON schema_migrations FIELDS version UNIQUE;";

/// Reject a tenant id that is not a safe SurrealDB namespace identifier. Tenant ids originate from a
/// validated OIDC claim (C3); this is a defensive second check before interpolating into DDL where
/// parameter binding of an identifier is not available.
pub fn validate_tenant(tenant: &str) -> Result<(), StoreError> {
    if tenant.is_empty() || tenant.len() > 128 {
        return Err(StoreError::Validation("tenant length".into()));
    }
    if !tenant
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(StoreError::Validation("tenant characters".into()));
    }
    Ok(())
}

/// Map a SurrealDB error to a `StoreError`. A timeout maps to `Timeout`; everything else from the
/// engine is treated as the store being unavailable (the migration left the namespace at its prior
/// version; X7 error table).
pub fn map_db_err(e: surrealdb::Error) -> StoreError {
    let msg = e.to_string();
    if msg.to_lowercase().contains("timeout") {
        StoreError::Timeout
    } else {
        StoreError::Unavailable(msg)
    }
}
