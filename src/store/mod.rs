//! C1 — Memory Store. The persistence layer and single owner of the embedded SurrealDB connection.
//! Implements the `MemoryStore` trait over SurrealDB 3.x with one namespace per tenant (ADR-011), the
//! three retrieval signals (HNSW vector, BM25 keyword, 2-hop graph) surfaced through one `recall`
//! operation, bi-temporal writes (ADR-002), and verifiable `hard_delete` (SA-DELETE-01).
//!
//! All caller input (content, vectors, names, ids) is bound as query parameters — never
//! string-interpolated — per the sql-safety layer rule. Tenant identifiers (which select the
//! namespace and cannot be parameter-bound in DDL) are validated by `migrate::validate_tenant`.

pub(crate) mod convert;
pub mod migrate;

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use surrealdb::engine::any::Any;
use surrealdb::types::Value;
use surrealdb::Surreal;

use crate::config::Config;
use crate::types::api::DeletionProof;
use crate::types::domain::{Entity, Fact, Relationship, Source};
use crate::types::ports::{
    AuditEntry, Candidate, MemoryStore, StageOneQuery, StoreError,
};
use crate::types::scope::{can_read, ScopeContext, ScopeRef};

use migrate::{map_db_err, validate_tenant, Migrator, DB_NAME};

/// The SurrealDB memory store. The connection is typed over `surrealdb::engine::any::Any`, so the
/// same `Store` carries either an embedded engine (SurrealKV / in-memory) or a remote endpoint
/// (`ws(s)://` / `http(s)://`) — the engine is resolved from the endpoint scheme at connect time.
pub struct Store {
    db: Surreal<Any>,
    embed_dim: u32,
}

impl Store {
    /// Open the store from the loaded configuration. `RECALL_STORE_REMOTE_URL` (if set) wins over the
    /// embedded path with a startup warning (Phase 2D precedence); otherwise the embedded SurrealKV
    /// engine is opened at `RECALL_STORE_PATH`. The connection is built through
    /// `surrealdb::engine::any::connect`, which resolves the engine from the endpoint scheme
    /// (`surrealkv://` embedded, `ws(s)://`/`http(s)://` remote — ADR-009 scale-out), so a remote
    /// deployment needs no code change. On any connection failure returns `StoreError::Unavailable`.
    pub async fn connect(cfg: &Config) -> Result<Self, StoreError> {
        let endpoint = if let Some(remote) = &cfg.store_remote_url {
            if !cfg.store_path.is_empty() {
                tracing::warn!(
                    target: "recall",
                    "store.connect: both RECALL_STORE_REMOTE_URL and RECALL_STORE_PATH set; remote wins"
                );
            }
            tracing::info!(target: "recall", backend = "remote", "store.connect");
            remote.clone()
        } else {
            tracing::info!(target: "recall", backend = "surrealkv", "store.connect");
            format!("surrealkv://{}", cfg.store_path)
        };
        let db = surrealdb::engine::any::connect(endpoint)
            .await
            .map_err(map_db_err)?;
        Ok(Self {
            db,
            embed_dim: cfg.embed_dim,
        })
    }

    /// Open an in-memory store (the real engine, in-process) for tests and ephemeral runs.
    pub async fn new_in_memory(embed_dim: u32) -> Result<Self, StoreError> {
        let db = surrealdb::engine::any::connect("mem://")
            .await
            .map_err(map_db_err)?;
        Ok(Self { db, embed_dim })
    }

    /// A `Migrator` bound to this store's connection.
    fn migrator(&self) -> Migrator<'_> {
        Migrator::new(&self.db, self.embed_dim)
    }

    /// Share the SurrealDB connection (the engine handle is internally `Arc`-backed, so a clone reuses
    /// the same database — the same in-process engine when embedded, or the same network connection
    /// when remote). The Durable Work Queue (C2) is built over this handle so its
    /// `work_job`/`dead_letter` tables live inside the same engine as the C1 store (SA-QUEUE-01:
    /// store-backed default, single-binary deployment intact).
    pub fn handle(&self) -> Surreal<Any> {
        self.db.clone()
    }

    /// Select the tenant namespace + `recall` database on the shared connection. Caller-facing reads
    /// and writes run after this so every statement targets the correct tenant.
    async fn use_tenant(&self, tenant: &str) -> Result<(), StoreError> {
        validate_tenant(tenant)?;
        self.db
            .use_ns(tenant.to_string())
            .use_db(DB_NAME)
            .await
            .map_err(map_db_err)?;
        Ok(())
    }

    /// Select the tenant and ensure its schema is migrated. Used before a read/write so a never-seen
    /// tenant is provisioned lazily and idempotently.
    async fn ensure_and_use(&self, tenant: &str) -> Result<(), StoreError> {
        self.migrator().migrate_up(tenant).await?;
        self.use_tenant(tenant).await
    }
}

/// Build the SurrealDB record-id literal for a "table:key" domain id, e.g. `fact:⟨uuid⟩`, as a
/// parameter-bindable record-id value. The id is split on the first ':' so an arbitrary uuid key
/// (which may contain '-') is bound as the record key, never interpolated.
fn id_thing(id: &str) -> Result<Value, StoreError> {
    let (table, key) = id
        .split_once(':')
        .ok_or_else(|| StoreError::Validation(format!("malformed id: {id}")))?;
    Ok(Value::RecordId(surrealdb::types::RecordId::new(
        table.to_string(),
        key.to_string(),
    )))
}

/// Validate a fact against the binding domain rules (Shared Context). Returns `StoreError::Validation`
/// with a message marking score-range failures with the word "range" so the X1 mapping selects
/// `VAL_OUT_OF_RANGE` (else `VAL_INVALID_BODY`).
fn validate_fact(f: &Fact) -> Result<(), StoreError> {
    if f.entities.is_empty() {
        return Err(StoreError::Validation("entities must be non-empty".into()));
    }
    if !f.content.is_object() {
        return Err(StoreError::Validation("content must be a JSON object".into()));
    }
    for (name, v) in [("confidence", f.confidence), ("salience", f.salience)] {
        if !(0.0..=1.0).contains(&v) {
            return Err(StoreError::Validation(format!("{name} out of range")));
        }
    }
    if f.stability < 0.0 {
        return Err(StoreError::Validation("stability out of range".into()));
    }
    if let Some(to) = f.valid_to {
        if to < f.valid_from {
            return Err(StoreError::Validation("valid_to before valid_from".into()));
        }
    }
    Ok(())
}

/// The read-filter `WHERE` fragment plus its bound parameters, built from `ctx`. Applied to every
/// fact read so a caller never sees a record outside its scope (§2C.3). Visibility-bearing tables
/// (fact) use the full rule; entity/relationship/source use the reduced rule (no visibility).
fn fact_filter_clause() -> &'static str {
    "owner.user = $cuser OR (visibility = 'team-shared' AND owner.team IN $cteams) \
     OR visibility = 'tenant-shared'"
}

/// Whether `ctx` is the C7 maintenance/admin scope: the `"maintenance"` jti marker with an empty user
/// (built by C7 from a tenant id alone, never from request input). A maintenance scope operates on the
/// WHOLE tenant namespace — the per-user/visibility read filter is bypassed so decay, supersession, and
/// verifiable hard-delete reach user-private and team-shared facts, not only tenant-shared ones
/// (RISK-009). Tenant isolation stays structural: every store method first selects the tenant namespace
/// (`ensure_and_use`), so a maintenance scope can never cross tenants.
fn is_maintenance_scope(ctx: &ScopeContext) -> bool {
    ctx.token_jti == "maintenance" && ctx.user.is_empty()
}

/// The fact read-filter `WHERE` fragment for `ctx`: the whole tenant for a maintenance scope, else the
/// per-user/visibility filter. The `$cuser`/`$cteams` bindings are still supplied by callers; when this
/// returns `true` they are simply unreferenced.
fn fact_read_filter(ctx: &ScopeContext) -> &'static str {
    if is_maintenance_scope(ctx) {
        "true"
    } else {
        fact_filter_clause()
    }
}

/// The reduced read-filter `WHERE` fragment for tables with no `visibility` field.
fn novis_filter_clause() -> &'static str {
    "owner.user = $cuser OR owner.team IN $cteams"
}

/// The bi-temporal validity `WHERE` fragment for a `valid_at` filter (or currently-valid when unset).
fn validity_clause(valid_at: Option<DateTime<Utc>>) -> &'static str {
    if valid_at.is_some() {
        "valid_from <= $valid_at AND (valid_to IS NONE OR valid_to > $valid_at)"
    } else {
        "valid_to IS NONE"
    }
}

#[async_trait]
impl MemoryStore for Store {
    async fn put_fact(&self, f: &Fact) -> Result<(), StoreError> {
        validate_fact(f)?;
        self.ensure_and_use(&f.owner.tenant).await?;
        let thing = id_thing(&f.id)?;
        let obj = convert::fact_to_object(f);
        self.db
            .query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(obj)))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        tracing::debug!(target: "recall", table = "fact", correlation_id = %"", "store.put");
        Ok(())
    }

    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let thing = id_thing(id)?;
        let mut resp = self
            .db
            .query(format!("SELECT * FROM $id WHERE {}", fact_read_filter(ctx)))
            .bind(("id", thing))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("read fact: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(convert::row_to_fact(row)?)),
            None => Ok(None),
        }
    }

    async fn recall(
        &self,
        ctx: &ScopeContext,
        q: &StageOneQuery,
    ) -> Result<Vec<Candidate>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let validity = validity_clause(q.filters.valid_at);
        let k = q.stage1_k.max(1) as i64;

        // Optional metadata filters (memory_class / visibility / entity) shared by every signal.
        let mut meta = String::new();
        if q.filters.memory_class.is_some() {
            meta.push_str(" AND memory_class = $mclass");
        }
        if q.filters.visibility.is_some() {
            meta.push_str(" AND visibility = $vis");
        }
        if q.filters.entity.is_some() {
            meta.push_str(" AND $entity IN entities");
        }

        let read = fact_filter_clause();
        // Accumulate the per-signal scores keyed by fact id.
        let mut scores: BTreeMap<String, (f64, f64, f64)> = BTreeMap::new();

        // --- Vector signal (HNSW ANN via the KNN operator, RISK-008 / NFR-P3) ---
        // The `<|K,EF|>` operator drives the `fact_hnsw` index: it returns the K approximate nearest
        // neighbours (EF = search breadth) and `vector::distance::knn()` reports each match's cosine
        // distance, computed during the index walk. This is an O(log N) index lookup, not the prior
        // O(N) `vector::similarity::cosine(...) ORDER BY` brute-force scan. K and EF are operator
        // syntax (not bindable), so they are formatted as validated integers — never caller input.
        // The scope read-filter, validity, and metadata predicates are AND-ed on top (KNN-then-filter);
        // K is over-fetched (4x stage1_k) so filtering rarely starves the final stage-1 set, then the
        // result is ordered by ascending distance and capped to stage1_k. Cosine similarity is
        // recovered as `1 - distance`, clamped to [0,1] to match the prior signal's range.
        if !q.query_vector.is_empty() {
            let knn_k = (k as usize).saturating_mul(4).clamp(k as usize, 1024);
            let ef = knn_k.max(64);
            let sql = format!(
                "SELECT meta::id(id) AS fid, vector::distance::knn() AS dist \
                 FROM fact WHERE embedding <|{knn_k},{ef}|> $qv AND ({read}) AND {validity}{meta} \
                 ORDER BY dist ASC LIMIT $k"
            );
            let mut resp = self
                .bind_recall(self.db.query(sql), ctx, q)
                .bind(("qv", q.query_vector.clone()))
                .bind(("k", k))
                .await
                .map_err(map_db_err)?;
            let rows: Vec<Json> = resp
                .take(0)
                .map_err(|e| StoreError::Internal(format!("vector signal: {e}")))?;
            for r in rows {
                if let (Some(fid), Some(dist)) = (
                    r.get("fid").and_then(|v| v.as_str()),
                    r.get("dist").and_then(|v| v.as_f64()),
                ) {
                    scores.entry(fid.to_string()).or_default().0 = (1.0 - dist).clamp(0.0, 1.0);
                }
            }
        }

        // --- Keyword signal (BM25) ---
        if !q.keyword_terms.is_empty() {
            let terms = q.keyword_terms.join(" ");
            let sql = format!(
                "SELECT meta::id(id) AS fid, search::score(0) AS s FROM fact \
                 WHERE content_text @0@ $terms AND ({read}) AND {validity}{meta} \
                 ORDER BY s DESC LIMIT $k"
            );
            let mut resp = self
                .bind_recall(self.db.query(sql), ctx, q)
                .bind(("terms", terms))
                .bind(("k", k))
                .await
                .map_err(map_db_err)?;
            let rows: Vec<Json> = resp
                .take(0)
                .map_err(|e| StoreError::Internal(format!("keyword signal: {e}")))?;
            let max = rows
                .iter()
                .filter_map(|r| r.get("s").and_then(|v| v.as_f64()))
                .fold(0.0_f64, f64::max);
            for r in rows {
                if let (Some(fid), Some(s)) = (
                    r.get("fid").and_then(|v| v.as_str()),
                    r.get("s").and_then(|v| v.as_f64()),
                ) {
                    let norm = if max > 0.0 { (s / max).clamp(0.0, 1.0) } else { 0.0 };
                    scores.entry(fid.to_string()).or_default().1 = norm;
                }
            }
        }

        // --- Graph signal (2-hop traversal from the filter entity) ---
        if let Some(entity) = &q.filters.entity {
            let sql = format!(
                "SELECT meta::id(id) AS fid FROM fact \
                 WHERE $entity IN entities AND ({read}) AND {validity} LIMIT $k"
            );
            let mut resp = self
                .db
                .query(sql)
                .bind(("entity", entity.clone()))
                .bind(("cuser", ctx.user.clone()))
                .bind(("cteams", ctx.teams.clone()))
                .bind(("k", k));
            resp = bind_valid_at(resp, q.filters.valid_at);
            let mut resp = resp.await.map_err(map_db_err)?;
            let rows: Vec<Json> = resp
                .take(0)
                .map_err(|e| StoreError::Internal(format!("graph signal: {e}")))?;
            for r in rows {
                if let Some(fid) = r.get("fid").and_then(|v| v.as_str()) {
                    scores.entry(fid.to_string()).or_default().2 = 1.0;
                }
            }
        }

        // Resolve each surviving id to its full fact, re-applying the read filter, and assemble.
        let mut out = Vec::new();
        for (fid, (sem, kw, graph)) in scores.into_iter().take(q.stage1_k as usize) {
            let full = format!("fact:{fid}");
            if let Some(fact) = self.get_fact(ctx, &full).await? {
                out.push(Candidate {
                    fact_id: fact.id.clone(),
                    fact,
                    semantic_score: sem,
                    keyword_score: kw,
                    graph_score: graph,
                });
            }
        }
        tracing::debug!(target: "recall", candidate_count = out.len(), "store.recall");
        Ok(out)
    }

    async fn end_validity(
        &self,
        ctx: &ScopeContext,
        id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        // Confirm read access (else NotFound); the get is scope-checked.
        if self.get_fact(ctx, id).await?.is_none() {
            return Err(StoreError::NotFound);
        }
        let thing = id_thing(id)?;
        self.db
            .query("UPDATE $id SET valid_to = $at WHERE valid_to IS NONE")
            .bind(("id", thing))
            .bind(("at", surrealdb::types::Datetime::from(at)))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn supersede(
        &self,
        ctx: &ScopeContext,
        old_id: &str,
        new_id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        if self.get_fact(ctx, old_id).await?.is_none()
            || self.get_fact(ctx, new_id).await?.is_none()
        {
            return Err(StoreError::NotFound);
        }
        let old_thing = id_thing(old_id)?;
        let new_thing = id_thing(new_id)?;
        self.db
            .query(
                "BEGIN; \
                 UPDATE $old SET valid_to = $at, superseded_by = $newid; \
                 UPDATE $new SET supersedes = $oldid; \
                 COMMIT;",
            )
            .bind(("old", old_thing))
            .bind(("new", new_thing))
            .bind(("at", surrealdb::types::Datetime::from(at)))
            .bind(("newid", new_id.to_string()))
            .bind(("oldid", old_id.to_string()))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn hard_delete(
        &self,
        ctx: &ScopeContext,
        id: &str,
    ) -> Result<DeletionProof, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let base = match self.get_fact(ctx, id).await? {
            Some(f) => f,
            None => return Err(StoreError::NotFound),
        };

        // Collect derived-insight facts whose derived_from contains this id (scope-checked).
        let mut resp = self
            .db
            .query(format!(
                "SELECT * FROM fact WHERE $base IN derived_from AND ({})",
                fact_read_filter(ctx)
            ))
            .bind(("base", id.to_string()))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let derived_rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("collect derived: {e}")))?;
        let derived: Vec<Fact> = derived_rows
            .into_iter()
            .map(convert::row_to_fact)
            .collect::<Result<_, _>>()?;

        // Build the full removal set + count embeddings removed (base + each derived with an embedding).
        let mut removed_ids: Vec<String> = Vec::with_capacity(derived.len() + 1);
        removed_ids.push(base.id.clone());
        let mut embeddings_removed: u32 = 0;
        if fact_has_embedding(&self.db, &base.id).await? {
            embeddings_removed += 1;
        }
        for d in &derived {
            removed_ids.push(d.id.clone());
            if fact_has_embedding(&self.db, &d.id).await? {
                embeddings_removed += 1;
            }
        }

        // Delete every collected record in one transaction.
        let things: Vec<Value> = removed_ids
            .iter()
            .map(|i| id_thing(i))
            .collect::<Result<_, _>>()?;
        self.db
            .query("DELETE $ids")
            .bind(("ids", things))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;

        // Verify every collected id is gone; otherwise the delete is partial — never report complete.
        let mut still = 0u32;
        for i in &removed_ids {
            if self.get_fact(ctx, i).await?.is_some() {
                still += 1;
            }
        }
        if still > 0 {
            let expected = removed_ids.len() as u32;
            return Err(StoreError::PartialDelete {
                removed: expected - still,
                expected,
            });
        }

        // Digest over the sorted removed ids (SA-DELETE-01).
        let mut sorted = removed_ids.clone();
        sorted.sort();
        let digest = sha256_hex(&sorted);
        let derived_removed: Vec<String> = derived.iter().map(|d| d.id.clone()).collect();
        tracing::debug!(
            target: "recall",
            derived_removed_count = derived_removed.len(),
            embeddings_removed,
            "store.hard_delete"
        );
        Ok(DeletionProof {
            deleted_at: Utc::now(),
            record_id: id.to_string(),
            derived_removed,
            embeddings_removed,
            digest,
        })
    }

    async fn put_entity(&self, e: &Entity) -> Result<(), StoreError> {
        if e.canonical_name.is_empty() || e.canonical_name.chars().count() > 512 {
            return Err(StoreError::Validation("canonical_name length".into()));
        }
        self.ensure_and_use(&e.owner.tenant).await?;
        let thing = id_thing(&e.id)?;
        self.db
            .query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(convert::entity_to_object(e))))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn get_entity(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Entity>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let thing = id_thing(id)?;
        let mut resp = self
            .db
            .query(format!("SELECT * FROM $id WHERE {}", novis_filter_clause()))
            .bind(("id", thing))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("read entity: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(convert::row_to_entity(row)?)),
            None => Ok(None),
        }
    }

    async fn find_entity_by_name(
        &self,
        ctx: &ScopeContext,
        name: &str,
    ) -> Result<Vec<Entity>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let mut resp = self
            .db
            .query(format!(
                "SELECT * FROM entity WHERE (canonical_name = $name OR $name IN aliases) AND ({})",
                novis_filter_clause()
            ))
            .bind(("name", name.to_string()))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("find entity: {e}")))?;
        rows.into_iter().map(convert::row_to_entity).collect()
    }

    async fn merge_entities(
        &self,
        ctx: &ScopeContext,
        keep_id: &str,
        merge_id: &str,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let keep = match self.get_entity(ctx, keep_id).await? {
            Some(e) => e,
            None => return Err(StoreError::NotFound),
        };
        let merge = match self.get_entity(ctx, merge_id).await? {
            Some(e) => e,
            None => return Err(StoreError::NotFound),
        };

        // Fold merge's canonical name + aliases into keep's aliases (deduplicated).
        let mut aliases = keep.aliases.clone();
        for a in merge
            .aliases
            .iter()
            .chain(std::iter::once(&merge.canonical_name))
        {
            if !aliases.contains(a) && a != &keep.canonical_name {
                aliases.push(a.clone());
            }
        }
        let keep_thing = id_thing(keep_id)?;
        let merge_thing = id_thing(merge_id)?;
        // Repoint fact.entities and relationship.from_ent/to_ent references, fold aliases, drop merge.
        self.db
            .query(
                "BEGIN; \
                 UPDATE fact SET entities = array::union(array::complement(entities, [$mid]), [$kid]) \
                   WHERE $mid IN entities; \
                 UPDATE relationship SET from_ent = $kid WHERE from_ent = $mid; \
                 UPDATE relationship SET to_ent = $kid WHERE to_ent = $mid; \
                 UPDATE $keep SET aliases = $aliases; \
                 DELETE $merge; \
                 COMMIT;",
            )
            .bind(("mid", merge_id.to_string()))
            .bind(("kid", keep_id.to_string()))
            .bind(("keep", keep_thing))
            .bind(("merge", merge_thing))
            .bind(("aliases", aliases))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn put_relationship(&self, r: &Relationship) -> Result<(), StoreError> {
        if r.kind.is_empty() || r.kind.chars().count() > 128 {
            return Err(StoreError::Validation("kind length".into()));
        }
        if !(0.0..=1.0).contains(&r.confidence) {
            return Err(StoreError::Validation("confidence out of range".into()));
        }
        self.ensure_and_use(&r.owner.tenant).await?;
        let thing = id_thing(&r.id)?;
        self.db
            .query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(convert::relationship_to_object(r))))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn get_relationship(
        &self,
        ctx: &ScopeContext,
        id: &str,
    ) -> Result<Option<Relationship>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let thing = id_thing(id)?;
        let mut resp = self
            .db
            .query(format!("SELECT * FROM $id WHERE {}", novis_filter_clause()))
            .bind(("id", thing))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("read relationship: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(convert::row_to_relationship(row)?)),
            None => Ok(None),
        }
    }

    async fn end_relationship_validity(
        &self,
        ctx: &ScopeContext,
        id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        if self.get_relationship(ctx, id).await?.is_none() {
            return Err(StoreError::NotFound);
        }
        let thing = id_thing(id)?;
        self.db
            .query("UPDATE $id SET valid_to = $at WHERE valid_to IS NONE")
            .bind(("id", thing))
            .bind(("at", surrealdb::types::Datetime::from(at)))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn put_source(&self, s: &Source) -> Result<(), StoreError> {
        if !(0.0..=1.0).contains(&s.trust_signal) {
            return Err(StoreError::Validation("trust_signal out of range".into()));
        }
        self.ensure_and_use(&s.owner.tenant).await?;
        let thing = id_thing(&s.id)?;
        self.db
            .query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(convert::source_to_object(s))))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn get_source(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Source>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let thing = id_thing(id)?;
        let mut resp = self
            .db
            .query(format!("SELECT * FROM $id WHERE {}", novis_filter_clause()))
            .bind(("id", thing))
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("read source: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(convert::row_to_source(row)?)),
            None => Ok(None),
        }
    }

    async fn append_audit(&self, e: &AuditEntry) -> Result<(), StoreError> {
        self.ensure_and_use(&e.tenant).await?;
        let thing = id_thing(&e.id)?;
        self.db
            .query("CREATE $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(convert::audit_to_object(e))))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn list_tenants(&self) -> Result<Vec<String>, StoreError> {
        let mut resp = self.db.query("INFO FOR ROOT").await.map_err(map_db_err)?;
        let info: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("list tenants: {e}")))?;
        let mut out = Vec::new();
        if let Some(ns) = info
            .first()
            .and_then(|v| v.get("namespaces"))
            .and_then(|v| v.as_object())
        {
            out.extend(ns.keys().cloned());
        }
        Ok(out)
    }

    async fn scan_recent_episodes(
        &self,
        ctx: &ScopeContext,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let mut resp = self
            .db
            .query(
                "SELECT * FROM fact WHERE valid_to IS NONE AND memory_class = 'episodic' \
                 AND ingested_at >= $since LIMIT $limit",
            )
            .bind(("since", surrealdb::types::Datetime::from(since)))
            .bind(("limit", limit as i64))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("scan recent: {e}")))?;
        rows.into_iter().map(convert::row_to_fact).collect()
    }

    async fn scan_contradiction_candidates(
        &self,
        ctx: &ScopeContext,
        limit: u32,
    ) -> Result<Vec<(Fact, Fact)>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        // Currently-valid facts; pair those sharing >=1 entity. The pairing is done client-side over
        // a bounded window to keep the query simple and parameterised.
        let mut resp = self
            .db
            .query("SELECT * FROM fact WHERE valid_to IS NONE LIMIT $limit")
            .bind(("limit", (limit.saturating_mul(2)).max(2) as i64))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("scan contradiction: {e}")))?;
        let facts: Vec<Fact> = rows
            .into_iter()
            .map(convert::row_to_fact)
            .collect::<Result<_, _>>()?;
        let mut pairs = Vec::new();
        for i in 0..facts.len() {
            for j in (i + 1)..facts.len() {
                if facts[i].entities.iter().any(|e| facts[j].entities.contains(e)) {
                    pairs.push((facts[i].clone(), facts[j].clone()));
                    if pairs.len() as u32 >= limit {
                        return Ok(pairs);
                    }
                }
            }
        }
        Ok(pairs)
    }

    async fn scan_decay_candidates(
        &self,
        ctx: &ScopeContext,
        salience_floor: f64,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let mut resp = self
            .db
            .query(
                "SELECT * FROM fact WHERE valid_to IS NONE AND salience < $floor \
                 ORDER BY last_recalled_at ASC, ingested_at ASC LIMIT $limit",
            )
            .bind(("floor", salience_floor))
            .bind(("limit", limit as i64))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("scan decay: {e}")))?;
        rows.into_iter().map(convert::row_to_fact).collect()
    }

    async fn scan_reembed_candidates(
        &self,
        ctx: &ScopeContext,
        current_model_version: &str,
        limit: u32,
    ) -> Result<Vec<Fact>, StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        let mut resp = self
            .db
            .query(
                "SELECT * FROM fact WHERE valid_to IS NONE \
                 AND (embedding_model != $current OR embedding IS NONE) LIMIT $limit",
            )
            .bind(("current", current_model_version.to_string()))
            .bind(("limit", limit as i64))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("scan reembed: {e}")))?;
        rows.into_iter().map(convert::row_to_fact).collect()
    }

    async fn update_fact_maintenance_fields(
        &self,
        ctx: &ScopeContext,
        f: &Fact,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(&ctx.tenant).await?;
        if self.get_fact(ctx, &f.id).await?.is_none() {
            return Err(StoreError::NotFound);
        }
        let thing = id_thing(&f.id)?;
        self.db
            .query(
                "UPDATE $id SET confidence = $c, salience = $s, stability = $st, \
                 last_recalled_at = $lr, supersedes = $sup, superseded_by = $supby",
            )
            .bind(("id", thing))
            .bind(("c", f.confidence))
            .bind(("s", f.salience))
            .bind(("st", f.stability))
            .bind((
                "lr",
                match f.last_recalled_at {
                    Some(t) => Value::Datetime(surrealdb::types::Datetime::from(t)),
                    None => Value::None,
                },
            ))
            .bind((
                "sup",
                match &f.supersedes {
                    Some(x) => Value::String(x.clone()),
                    None => Value::None,
                },
            ))
            .bind((
                "supby",
                match &f.superseded_by {
                    Some(x) => Value::String(x.clone()),
                    None => Value::None,
                },
            ))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn set_fact_embedding(
        &self,
        ctx: &ScopeContext,
        fact_id: &str,
        vector: &[f32],
        model_version: &str,
    ) -> Result<(), StoreError> {
        if vector.len() as u32 != self.embed_dim {
            return Err(StoreError::Validation(format!(
                "embedding dimension {} != RECALL_EMBED_DIM {}",
                vector.len(),
                self.embed_dim
            )));
        }
        self.ensure_and_use(&ctx.tenant).await?;
        let thing = id_thing(fact_id)?;
        self.db
            .query("UPDATE $id SET embedding = $v, embedding_model = $m")
            .bind(("id", thing))
            .bind(("v", vector.to_vec()))
            .bind(("m", model_version.to_string()))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn ensure_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError> {
        self.migrator().migrate_up(tenant).await?;
        Ok(())
    }

    async fn drop_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError> {
        validate_tenant(tenant)?;
        tracing::warn!(target: "recall", "store.drop_ns");
        self.db
            .query(format!("REMOVE NAMESPACE IF EXISTS {tenant}"))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }

    async fn ready(&self) -> Result<(), StoreError> {
        // A live connection answers a trivial query; the per-tenant vector-index dimension is checked
        // at migration time (the HNSW DDL embeds RECALL_EMBED_DIM via the Migrator). A failure here is
        // a lost connection.
        self.db
            .query("RETURN 1")
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }
}

impl Store {
    /// Bind the shared recall parameters (read filter + metadata filters + validity) onto a query.
    fn bind_recall<'q>(
        &self,
        mut query: surrealdb::method::Query<'q, Any>,
        ctx: &ScopeContext,
        q: &StageOneQuery,
    ) -> surrealdb::method::Query<'q, Any> {
        query = query
            .bind(("cuser", ctx.user.clone()))
            .bind(("cteams", ctx.teams.clone()));
        if let Some(mc) = q.filters.memory_class {
            query = query.bind(("mclass", convert::memory_class_str(mc).to_string()));
        }
        if let Some(v) = q.filters.visibility {
            query = query.bind(("vis", convert::visibility_str(v).to_string()));
        }
        if let Some(e) = &q.filters.entity {
            query = query.bind(("entity", e.clone()));
        }
        query = bind_valid_at(query, q.filters.valid_at);
        query
    }
}

/// C8 idempotency-record persistence. These concrete methods (not on the `MemoryStore` trait) back
/// the HTTP API Edge's idempotency layer: the edge stores one outcome per (tenant, user, route, key)
/// and replays it verbatim within the TTL window (SA-IDEM-01). Keys and values are bound parameters,
/// never string-interpolated (sql-safety). The table is defined by migration 0005 and lives inside the
/// tenant namespace alongside `audit_log`.
impl Store {
    /// The embedding dimension this store was opened with — the dimension baked into every tenant's
    /// HNSW vector-index DDL by the Migrator. `/readyz` (C8) compares this against `RECALL_EMBED_DIM`
    /// to satisfy SA-EMBED-01.
    pub fn index_embed_dim(&self) -> u32 {
        self.embed_dim
    }

    /// Look up a stored idempotency outcome for `(tenant, user, route, key)`. Returns the
    /// `(status_code, response_body)` of a non-expired record, or `None` on a miss (no row, or a row
    /// whose `expires_at` is at/before now — a read past expiry is a miss; the handler re-runs).
    pub async fn idempotency_get(
        &self,
        tenant: &str,
        user: &str,
        route: &str,
        key: &str,
    ) -> Result<Option<(i64, Json)>, StoreError> {
        self.ensure_and_use(tenant).await?;
        let mut resp = self
            .db
            .query(
                "SELECT status_code, response_body FROM idempotency_record \
                 WHERE tenant = $tenant AND user = $user AND route = $route \
                   AND idempotency_key = $key AND expires_at > time::now() LIMIT 1",
            )
            .bind(("tenant", tenant.to_string()))
            .bind(("user", user.to_string()))
            .bind(("route", route.to_string()))
            .bind(("key", key.to_string()))
            .await
            .map_err(map_db_err)?;
        let rows: Vec<Json> = resp
            .take(0)
            .map_err(|e| StoreError::Internal(format!("idempotency_get: {e}")))?;
        match rows.into_iter().next() {
            Some(row) => {
                let status = row
                    .get("status_code")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| StoreError::Internal("idempotency_get: status_code".into()))?;
                let body = row
                    .get("response_body")
                    .cloned()
                    .unwrap_or(Json::Null);
                Ok(Some((status, body)))
            }
            None => Ok(None),
        }
    }

    /// Persist an idempotency outcome with `expires_at = now + ttl`. Idempotent on the unique
    /// `(tenant, user, route, key)` index: an existing row is overwritten (a re-run after expiry), so
    /// the stored outcome is always the latest handler result for the key. The record id is a deterministic
    /// `idempotency_record:<uuidv5(tenant|user|route|key)>` so a re-run targets the same row.
    #[allow(clippy::too_many_arguments)]
    pub async fn idempotency_put(
        &self,
        tenant: &str,
        user: &str,
        route: &str,
        key: &str,
        status_code: i64,
        body: &Json,
        ttl_secs: u32,
    ) -> Result<(), StoreError> {
        self.ensure_and_use(tenant).await?;
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(i64::from(ttl_secs));
        // Deterministic record key so a replay-after-expiry overwrites the same row rather than racing
        // the UNIQUE index. The components are joined with a separator that cannot appear in a tenant or
        // route token, keeping the derivation injective.
        let seed = format!("{tenant}\u{1f}{user}\u{1f}{route}\u{1f}{key}");
        let rec_key = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string();
        let id = format!("idempotency_record:{rec_key}");
        let thing = id_thing(&id)?;

        let mut obj = surrealdb::types::Object::new();
        obj.insert("tenant", Value::String(tenant.to_string()));
        obj.insert("user", Value::String(user.to_string()));
        obj.insert("route", Value::String(route.to_string()));
        obj.insert("idempotency_key", Value::String(key.to_string()));
        obj.insert("response_body", json_to_value(body));
        obj.insert(
            "status_code",
            Value::Number(surrealdb::types::Number::Int(status_code)),
        );
        obj.insert("created_at", Value::Datetime(surrealdb::types::Datetime::from(now)));
        obj.insert(
            "expires_at",
            Value::Datetime(surrealdb::types::Datetime::from(expires_at)),
        );

        self.db
            .query("UPSERT $id CONTENT $rec")
            .bind(("id", thing))
            .bind(("rec", Value::Object(obj)))
            .await
            .map_err(map_db_err)?
            .check()
            .map_err(map_db_err)?;
        Ok(())
    }
}

/// Convert a `serde_json::Value` into a native SurrealDB value (used for the FLEXIBLE response_body
/// field, which stores an arbitrary nested Success payload).
fn json_to_value(v: &Json) -> Value {
    use surrealdb::types::SurrealValue;
    v.clone().into_value()
}

/// Bind the `$valid_at` parameter when a bi-temporal as-of filter is set.
fn bind_valid_at(
    query: surrealdb::method::Query<'_, Any>,
    valid_at: Option<DateTime<Utc>>,
) -> surrealdb::method::Query<'_, Any> {
    match valid_at {
        Some(at) => query.bind(("valid_at", surrealdb::types::Datetime::from(at))),
        None => query,
    }
}

/// Whether a fact row currently carries a non-null embedding vector.
async fn fact_has_embedding(db: &Surreal<Any>, id: &str) -> Result<bool, StoreError> {
    let thing = id_thing(id)?;
    let mut resp = db
        .query("SELECT embedding FROM $id")
        .bind(("id", thing))
        .await
        .map_err(map_db_err)?;
    let rows: Vec<Json> = resp
        .take(0)
        .map_err(|e| StoreError::Internal(format!("embedding probe: {e}")))?;
    Ok(rows
        .first()
        .and_then(|r| r.get("embedding"))
        .map(|v| !v.is_null())
        .unwrap_or(false))
}

/// SHA-256 hex over the newline-joined, already-sorted ids (SA-DELETE-01 digest).
fn sha256_hex(sorted_ids: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sorted_ids.join("\n").as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Defensive in-process read-filter check, reusing the §2C.3 pure helper. Used in unit tests to
/// confirm the store's SurrealQL filter agrees with the canonical rule.
#[allow(dead_code)]
fn local_can_read(ctx: &ScopeContext, owner: &ScopeRef, vis: crate::types::domain::Visibility) -> bool {
    can_read(ctx, owner, vis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::domain::{MemoryClass, Visibility};
    use crate::types::ports::RecallFilters;
    use crate::types::scope::OpSet;

    const DIM: u32 = 8;

    fn scope_ref(tenant: &str, team: Option<&str>, user: &str) -> ScopeRef {
        ScopeRef {
            tenant: tenant.into(),
            team: team.map(Into::into),
            user: user.into(),
        }
    }

    fn ctx(tenant: &str, user: &str, teams: &[&str]) -> ScopeContext {
        ScopeContext {
            tenant: tenant.into(),
            teams: teams.iter().map(|t| t.to_string()).collect(),
            user: user.into(),
            token_jti: "jti".into(),
            allowed_ops: OpSet {
                read: true,
                write: true,
                forget: true,
            },
            correlation_id: "c-test".into(),
        }
    }

    fn sample_fact(id: &str, owner: ScopeRef, vis: Visibility) -> Fact {
        Fact {
            id: id.into(),
            content: serde_json::json!({"subject": "team:alpha", "predicate": "owns", "object": "table:orders"}),
            entities: vec!["entity:e1".into()],
            source_id: None,
            memory_class: MemoryClass::Semantic,
            visibility: vis,
            owner,
            valid_from: Utc::now(),
            valid_to: None,
            ingested_at: Utc::now(),
            confidence: 0.9,
            salience: 0.7,
            stability: 1.0,
            pii_review: false,
            supersedes: None,
            superseded_by: None,
            derived_from: vec![],
            last_recalled_at: None,
        }
    }

    async fn store() -> Store {
        Store::new_in_memory(DIM).await.expect("in-memory store")
    }

    #[tokio::test]
    async fn migrate_up_is_idempotent_and_dry_run_executes_nothing() {
        let s = store().await;
        let m = s.migrator();
        // The migration set is a single squashed 0001_init carrying the full schema (C1 store + audit,
        // C2 queue, C4 quarantine, C7 maintenance_state, C8 idempotency_record); a fresh namespace
        // migrates to the latest version in one pass.
        const LATEST: u32 = 1;
        assert_eq!(m.current_version("acme").await.unwrap(), 0);
        assert_eq!(m.migrate_up("acme").await.unwrap(), LATEST);
        assert_eq!(m.current_version("acme").await.unwrap(), LATEST);
        // Second migrate_up is a no-op (still at the latest version).
        assert_eq!(m.migrate_up("acme").await.unwrap(), LATEST);
        // dry_run on an up-to-date namespace returns nothing and does not change the version.
        assert!(m.dry_run("acme").await.unwrap().is_empty());
        assert_eq!(m.current_version("acme").await.unwrap(), LATEST);
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let s = store().await;
        let owner = scope_ref("acme", Some("alpha"), "u-sarah");
        let f = sample_fact("fact:f1", owner, Visibility::TeamShared);
        s.put_fact(&f).await.unwrap();
        let c = ctx("acme", "u-sarah", &["alpha"]);
        let got = s.get_fact(&c, "fact:f1").await.unwrap().expect("fact present");
        assert_eq!(got.id, "fact:f1");
        assert_eq!(got.confidence, 0.9);
        assert_eq!(got.entities, vec!["entity:e1".to_string()]);
    }

    #[tokio::test]
    async fn score_out_of_range_is_rejected_with_no_write() {
        let s = store().await;
        let owner = scope_ref("acme", None, "u-sarah");
        let mut f = sample_fact("fact:bad", owner, Visibility::UserPrivate);
        f.confidence = 1.4;
        let err = s.put_fact(&f).await.unwrap_err();
        assert!(matches!(err, StoreError::Validation(ref m) if m.contains("range")));
        // No record was written.
        let c = ctx("acme", "u-sarah", &[]);
        assert!(s.get_fact(&c, "fact:bad").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_out_of_scope_returns_none() {
        let s = store().await;
        // user-private fact owned by sarah.
        let owner = scope_ref("acme", None, "u-sarah");
        let f = sample_fact("fact:priv", owner, Visibility::UserPrivate);
        s.put_fact(&f).await.unwrap();
        // a different same-tenant user cannot read it.
        let bob = ctx("acme", "u-bob", &[]);
        assert!(s.get_fact(&bob, "fact:priv").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn supersede_ends_validity_without_deleting() {
        let s = store().await;
        let owner = scope_ref("acme", None, "u-sarah");
        s.put_fact(&sample_fact("fact:old", owner.clone(), Visibility::UserPrivate))
            .await
            .unwrap();
        s.put_fact(&sample_fact("fact:new", owner, Visibility::UserPrivate))
            .await
            .unwrap();
        let c = ctx("acme", "u-sarah", &[]);
        let at = DateTime::parse_from_rfc3339("2026-06-20T12:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        s.supersede(&c, "fact:old", "fact:new", at).await.unwrap();

        let old = s.get_fact(&c, "fact:old").await.unwrap().expect("old retained");
        assert_eq!(old.valid_to, Some(at));
        assert_eq!(old.superseded_by.as_deref(), Some("fact:new"));
        let new = s.get_fact(&c, "fact:new").await.unwrap().expect("new present");
        assert_eq!(new.supersedes.as_deref(), Some("fact:old"));
    }

    #[tokio::test]
    async fn hard_delete_returns_proof_and_removes_derived() {
        let s = store().await;
        let owner = scope_ref("acme", None, "u-sarah");
        s.put_fact(&sample_fact("fact:base", owner.clone(), Visibility::UserPrivate))
            .await
            .unwrap();
        // two consolidated insights derived from base.
        for iid in ["fact:i1", "fact:i2"] {
            let mut insight = sample_fact(iid, owner.clone(), Visibility::UserPrivate);
            insight.memory_class = MemoryClass::Consolidated;
            insight.derived_from = vec!["fact:base".into()];
            s.put_fact(&insight).await.unwrap();
        }
        let c = ctx("acme", "u-sarah", &[]);
        let proof = s.hard_delete(&c, "fact:base").await.unwrap();
        assert_eq!(proof.record_id, "fact:base");
        let mut derived = proof.derived_removed.clone();
        derived.sort();
        assert_eq!(derived, vec!["fact:i1".to_string(), "fact:i2".to_string()]);
        // digest = sha256 of sorted removed ids.
        let mut all = vec!["fact:base".to_string(), "fact:i1".into(), "fact:i2".into()];
        all.sort();
        assert_eq!(proof.digest, sha256_hex(&all));
        assert!(s.get_fact(&c, "fact:base").await.unwrap().is_none());
        assert!(s.get_fact(&c, "fact:i1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recall_returns_candidate_by_vector_and_keyword() {
        let s = store().await;
        let owner = scope_ref("acme", Some("alpha"), "u-sarah");
        let f = sample_fact("fact:r1", owner, Visibility::TeamShared);
        s.put_fact(&f).await.unwrap();
        let c = ctx("acme", "u-sarah", &["alpha"]);
        s.set_fact_embedding(&c, "fact:r1", &[0.1_f32; DIM as usize], "m1")
            .await
            .unwrap();
        let q = StageOneQuery {
            query_vector: vec![0.1_f32; DIM as usize],
            keyword_terms: vec!["orders".into()],
            filters: RecallFilters::default(),
            scope: c.clone(),
            stage1_k: 50,
        };
        let cands = s.recall(&c, &q).await.unwrap();
        assert_eq!(cands.len(), 1, "expected one candidate");
        let cand = &cands[0];
        assert_eq!(cand.fact_id, "fact:r1");
        assert!((0.0..=1.0).contains(&cand.semantic_score));
        assert!((0.0..=1.0).contains(&cand.keyword_score));
    }

    #[tokio::test]
    async fn cross_tenant_isolation_on_recall() {
        let s = store().await;
        // tenant-shared fact in acme.
        let owner = scope_ref("acme", None, "u-sarah");
        let f = sample_fact("fact:x", owner, Visibility::TenantShared);
        s.put_fact(&f).await.unwrap();
        let acme = ctx("acme", "u-sarah", &[]);
        s.set_fact_embedding(&acme, "fact:x", &[0.2_f32; DIM as usize], "m1")
            .await
            .unwrap();
        // globex caller must see nothing (different namespace).
        let globex = ctx("globex", "u-other", &[]);
        let q = StageOneQuery {
            query_vector: vec![0.2_f32; DIM as usize],
            keyword_terms: vec!["orders".into()],
            filters: RecallFilters::default(),
            scope: globex.clone(),
            stage1_k: 50,
        };
        let cands = s.recall(&globex, &q).await.unwrap();
        assert!(cands.is_empty(), "globex must not see acme facts");
    }

    /// RISK-009: the maintenance scope reads (and so can mutate) a user-private fact that a regular
    /// non-owner scope cannot see, while a cross-tenant maintenance scope still sees nothing.
    #[tokio::test]
    async fn maintenance_scope_reaches_user_private_facts_within_the_tenant_only() {
        let s = store().await;
        let owner = scope_ref("acme", None, "u-sarah");
        let f = sample_fact("fact:mp", owner, Visibility::UserPrivate);
        s.put_fact(&f).await.unwrap();

        // A regular, non-owner caller in the same tenant cannot see a user-private fact.
        let other = ctx("acme", "u-bob", &[]);
        assert!(s.get_fact(&other, "fact:mp").await.unwrap().is_none());

        // The maintenance scope (empty user + "maintenance" jti) reaches it (RISK-009 fix).
        let maint = ScopeContext {
            tenant: "acme".into(),
            teams: vec![],
            user: String::new(),
            token_jti: "maintenance".into(),
            allowed_ops: OpSet { read: true, write: true, forget: true },
            correlation_id: "c-maint".into(),
        };
        assert!(
            s.get_fact(&maint, "fact:mp").await.unwrap().is_some(),
            "maintenance scope must reach a user-private fact"
        );
        // Maintenance can end its validity (was a silent no-op before the fix).
        s.end_validity(&maint, "fact:mp", Utc::now()).await.unwrap();
        assert!(
            s.get_fact(&maint, "fact:mp").await.unwrap().unwrap().valid_to.is_some(),
            "maintenance end_validity must take effect on a user-private fact"
        );

        // Tenant isolation stays structural: a maintenance scope for another tenant sees nothing.
        let maint_globex = ScopeContext {
            tenant: "globex".into(),
            teams: vec![],
            user: String::new(),
            token_jti: "maintenance".into(),
            allowed_ops: OpSet { read: true, write: true, forget: true },
            correlation_id: "c-maint2".into(),
        };
        assert!(
            s.get_fact(&maint_globex, "fact:mp").await.unwrap().is_none(),
            "maintenance must not cross tenants"
        );
    }
}
