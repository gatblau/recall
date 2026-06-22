### SPEC: Memory Store

**File:** `src/store` | **Package:** `recall::store` | **Phase:** 1 | **Dependencies:** none

> **Mode:** greenfield
> **derivedFromHld:** 0.5.0

#### Purpose

The Memory Store is `recall`'s persistence layer and the single owner of the embedded SurrealDB
connection. It implements the `MemoryStore` trait (the contract the Write Pipeline, Retrieval Engine,
Maintenance Worker, and HTTP API Edge depend on). It persists Facts, Entities, Relationships, Sources,
and the per-tenant audit log; it enforces hard tenant isolation by targeting one SurrealDB namespace
per tenant (ADR-011); and it provides the three retrieval signals over one store — an HNSW vector
index, a BM25 keyword index, and graph traversal over relationship edges — surfaced through a single
multi-signal stage-1 `recall` operation. It records bi-temporal facts and edges (validity interval plus
ingestion time, ADR-002): supersession and retirement end a record's validity but never delete it; only
`hard_delete` removes data, and it returns a verifiable `DeletionProof` (SA-DELETE-01). The store also
exposes the maintenance scan queries the Maintenance Worker (C7) consumes: decay candidates,
contradiction candidates, and stale-embedding facts.

#### Approach

A single embedded SurrealDB instance backs all three retrieval signals and the graph, chosen because
SurrealDB is the only engine that gives graph + vector (HNSW) + keyword (BM25) over rich bi-temporal
edges in one in-process store (ADR-003/ADR-009), removing the client/server hop from the read-path
latency budget. The component is a thin trait implementation over `SurrealDB`: it owns the connection
lifecycle (embedded SurrealKV or RocksDB selected by `RECALL_STORE_BACKEND`, or a remote
SurrealDB/TiKV cluster when `RECALL_STORE_REMOTE_URL` is set) and per-tenant namespace selection, but
holds no domain policy beyond the read-filter rule and bi-temporal write rules stated below. The
internal pattern is one parameterised SurrealQL query per trait method (no string interpolation of
caller input). Two alternatives were rejected at component scope: building a separate query-builder
abstraction over SurrealQL (rejected — the trait surface is small and fixed, an abstraction adds
indirection without reuse); and caching parsed Facts in-process (rejected — the read path latency
budget is met by the embedded engine and ANN index, and a cache would complicate the eventual-consistency
contract from ADR-004).

#### Shared Context

The following shared types, env vars, and the read-filter rule are duplicated verbatim from Phase 2
(§2C, §2D) and Phase 1 (§1C). A reader implements from this section alone.

##### Domain entities (Phase 2C.2) — persisted by this component

```rust
// src/types/domain.rs   Used by: C1, C4, C6, C7 (and C8 for GET fact).

#[derive(Serialize, Deserialize, Clone)]
pub struct Fact {
    pub id: String,                       // "fact:<uuidv7>" (SA-ID-01)
    pub content: serde_json::Value,       // structured assertion (object), not free text
    pub entities: Vec<String>,            // entity ids this fact connects (>=1)
    pub source_id: Option<String>,        // "source:<uuidv7>" provenance, null for agent-stated
    pub memory_class: MemoryClass,
    pub visibility: Visibility,
    pub owner: ScopeRef,                  // owning (tenant, team, user)
    pub valid_from: DateTime<Utc>,        // RFC3339 ms (SA-TIME-01)
    pub valid_to: Option<DateTime<Utc>>,  // null = currently true (open interval)
    pub ingested_at: DateTime<Utc>,       // server-set
    pub confidence: f64,                  // [0,1] (SA-SCORE-01)
    pub salience: f64,                    // [0,1]
    pub stability: f64,                   // decay stability `s` (SA-DECAY-01), >=0.0
    pub pii_review: bool,                 // true => low-confidence PII flagged for review (SA-PII-01)
    pub supersedes: Option<String>,       // fact id this one replaces
    pub superseded_by: Option<String>,    // fact id replacing this one
    pub derived_from: Vec<String>,        // source fact ids (consolidated insights only)
    pub last_recalled_at: Option<DateTime<Utc>>,
}
// The persisted SurrealDB row additionally carries derived, persistence-only fields not on this
// JSON struct: `embedding: option<array<float>>` (HNSW vector, written by C4, refreshed by C7) and
// `embedding_model: option<string>` (version tag the C7 re-embed scan keys on). See Data Model.

#[derive(Serialize, Deserialize, Clone)]
pub struct Entity {
    pub id: String,                       // "entity:<uuidv7>"
    pub canonical_name: String,           // 1..=512 chars, non-empty
    pub aliases: Vec<String>,
    pub owner: ScopeRef,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Relationship {
    pub id: String,                       // "relationship:<uuidv7>"
    pub kind: String,                     // typed edge label, e.g. "owns" (1..=128 chars)
    pub from: String,                     // entity id
    pub to: String,                       // entity id
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub ingested_at: DateTime<Utc>,
    pub confidence: f64,                  // [0,1]
    pub source_id: Option<String>,
    pub owner: ScopeRef,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Source {
    pub id: String,                       // "source:<uuidv7>"
    pub origin_ref: String,               // document/system handle, opaque to recall
    pub modification_marker: Option<String>, // ETag / Last-Modified token for freshness
    pub trust_signal: f64,                // [0,1] prior trust of this source
    pub owner: ScopeRef,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryClass { Episodic, Semantic, Consolidated }   // procedural rejected (SA-CLASS-01)

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Visibility { UserPrivate, TeamShared, TenantShared } // (SA-VIS-01)
```

**Domain validation (binding at the store boundary):** `entities.len() >= 1`; all score fields
(`confidence`, `salience`, `trust_signal`) in `[0.0, 1.0]`; `stability >= 0.0`; `valid_to` (if present)
`>= valid_from`; `content` MUST be a JSON object; `canonical_name` 1..=512 chars non-empty; `kind`
1..=128 chars. A record that fails these is rejected with `StoreError::Validation` before any write.

##### Scope & auth context (Phase 2C.3)

```rust
// src/types/scope.rs   Used by: C3 (produces), C1/C4/C6/C7/C8 (consume — every query is scoped).

/// The owning scope stored on every record.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef {
    pub tenant: String,            // tenant id -> SurrealDB namespace (ADR-011)
    pub team: Option<String>,      // team id, null for user-only facts
    pub user: String,              // user id, bound to the OIDC subject claim
}

/// The authenticated request context derived by C3 from the validated token.
/// Never constructed from request-body input.
#[derive(Clone)]
pub struct ScopeContext {
    pub tenant: String,
    pub teams: Vec<String>,        // membership claim — teams the user belongs to
    pub user: String,              // = token subject claim
    pub token_jti: String,         // for the audit trail (never the token itself)
    pub allowed_ops: OpSet,        // read / write / forget, from token scopes
    pub correlation_id: String,
}

#[derive(Clone, Copy)]
pub struct OpSet { pub read: bool, pub write: bool, pub forget: bool }
```

**Read filter rule (binding for every store query, copied verbatim from §2C.3):** a caller may read a
Fact/Entity/Relationship iff `record.owner.tenant == ctx.tenant` **and** ( `record.owner.user ==
ctx.user` **or** (`record.visibility == TeamShared` and `record.owner.team ∈ ctx.teams`) **or**
`record.visibility == TenantShared` ). Cross-tenant access is structurally impossible — a different
tenant is a different namespace. Entities and Relationships have no `visibility` field; for them the
filter reduces to `record.owner.tenant == ctx.tenant` **and** ( `record.owner.user == ctx.user` **or**
`record.owner.team ∈ ctx.teams` ).

**Maintenance-scope read filter (RISK-009, resolved):** the C7 Maintenance Worker constructs its
`ScopeContext` from a tenant id alone, with `token_jti == "maintenance"` and an empty `user` (never
from request input). `is_maintenance_scope(ctx)` is `ctx.token_jti == "maintenance" && ctx.user.is_empty()`.
For a maintenance scope, the per-user/visibility predicate would otherwise admit only `tenant-shared`
facts (empty `user`/`teams`), so decay, supersession, and verifiable hard-delete would silently no-op
on the bulk of memory (most facts are `user-private`). To prevent that, the fact read-filter helper
`fact_read_filter(ctx)` returns the SurrealQL literal `true` when `is_maintenance_scope(ctx)` holds —
the per-user/visibility filter is bypassed so the C7 maintenance scope operates on the WHOLE tenant
namespace — and otherwise returns the per-user/visibility clause `fact_filter_clause()`. Tenant
isolation stays structural: every store method first calls `ensure_and_use(ctx.tenant)`, which selects
the tenant namespace, so a maintenance scope can never cross tenants. `fact_read_filter` is applied by
`get_fact` and by `hard_delete`'s derived-collection query; the `recall` signals use the per-user
`fact_filter_clause()` directly (a maintenance scope does not drive interactive recall). The reduced
no-visibility filter `novis_filter_clause()` (`owner.user = $cuser OR owner.team IN $cteams`) is used
for `entity`/`relationship`/`source` reads.

##### DeletionProof (Phase 2C.4) — returned by `hard_delete`

```rust
#[derive(Serialize)]
pub struct DeletionProof {
    pub deleted_at: DateTime<Utc>,
    pub record_id: String,
    pub derived_removed: Vec<String>,        // ids of removed derived summaries
    pub embeddings_removed: u32,
    pub digest: String,                      // sha256 hex over sorted removed ids (SA-DELETE-01)
}
```

##### Trait surface (Phase 2C.6) — implemented by this component

```rust
#[async_trait]
pub trait MemoryStore: Send + Sync {                 // impl: embedded SurrealDB (C1)
    async fn put_fact(&self, f: &Fact) -> Result<(), StoreError>;
    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError>;
    async fn recall(&self, ctx: &ScopeContext, q: &StageOneQuery)
        -> Result<Vec<Candidate>, StoreError>;        // multi-signal stage-1
    async fn end_validity(&self, ctx: &ScopeContext, id: &str, at: DateTime<Utc>)
        -> Result<(), StoreError>;
    async fn hard_delete(&self, ctx: &ScopeContext, id: &str)
        -> Result<DeletionProof, StoreError>;
    // ...entity/relationship/source CRUD, maintenance scans — full surface below.
}
```

##### Environment variables (Phase 2D) — owned or read by this component

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `RECALL_STORE_PATH` | path | `./data/recall.db` | no | Embedded SurrealDB data directory (SurrealKV/RocksDB). |
| `RECALL_STORE_REMOTE_URL` | url | _(unset)_ | no | If set, target a remote SurrealDB/TiKV cluster instead of embedding (ADR-009 scale-out). |
| `RECALL_STORE_BACKEND` | enum `surrealkv\|rocksdb` | `surrealkv` | no | Embedded storage backend. |
| `RECALL_EMBED_DIM` | u32 | `1024` | no | Embedding dimension; must equal the vector-index dimension (SA-EMBED-01). |

**Startup precedence and conditional rules (Phase 2D):** precedence is env var > config file > built-in
default. `RECALL_STORE_REMOTE_URL` and `RECALL_STORE_PATH` are mutually exclusive — remote wins if both
are set, with a startup warning. Startup fails fast if `RECALL_STORE_BACKEND` is not one of
`surrealkv`/`rocksdb`, or if the persisted vector index dimension does not equal `RECALL_EMBED_DIM`
(SA-EMBED-01 readiness check).

##### C1-owned types defined by this spec

```rust
// src/store/types.rs   Defined here; referenced by name in §2C.6 and by C4/C6/C7.

/// Typed error for every MemoryStore operation. Maps to AppError::Store in §2C.7.
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("validation: {1}")]                Validation(ValidationKind, String),    // -> 400 VAL_* (code per ValidationKind)
    #[error("not found")]                       NotFound,              // -> 404 NOT_FOUND
    #[error("scope forbidden")]                ScopeForbidden,        // -> 403 SCOPE_FORBIDDEN
    #[error("store unavailable: {0}")]          Unavailable(String),   // -> 503 STORE_UNAVAILABLE
    #[error("store timeout")]                   Timeout,               // -> 504 STORE_TIMEOUT
    #[error("partial delete: {removed} of {expected} removed")]
                                                PartialDelete { removed: u32, expected: u32 }, // -> 500 INTERNAL
    #[error("internal: {0}")]                   Internal(String),      // -> 500 INTERNAL
}

/// One stage-1 retrieval candidate: a fact id, the fact itself, and the per-signal scores
/// that contributed to its selection. The Retrieval Engine (C6) fuses and reranks these.
#[derive(Clone)]
pub struct Candidate {
    pub fact_id: String,            // "fact:<uuidv7>"
    pub fact: Fact,                 // the resolved fact, already read-filtered
    pub semantic_score: f64,        // cosine similarity from the HNSW vector index, [0,1]; 0.0 if not a vector hit
    pub keyword_score: f64,         // BM25 score, normalised to [0,1]; 0.0 if not a keyword hit
    pub graph_score: f64,           // graph-proximity score, [0,1]; 0.0 if not reached via traversal
}

/// The stage-1 multi-signal query the Retrieval Engine (C6) submits to `recall`.
#[derive(Clone)]
pub struct StageOneQuery {
    pub query_vector: Vec<f32>,         // dim == RECALL_EMBED_DIM; empty disables the vector signal
    pub keyword_terms: Vec<String>,     // BM25 terms; empty disables the keyword signal
    pub filters: RecallFilters,         // metadata filters (memory_class, visibility, entity, valid_at)
    pub scope: ScopeContext,            // the authenticated scope; the read filter is applied to every signal
    pub stage1_k: u16,                  // max candidates to return (SA-RERANK-01, default RECALL_STAGE1_K=50)
}

#[derive(Clone, Default)]
pub struct RecallFilters {
    pub memory_class: Option<MemoryClass>,
    pub visibility: Option<Visibility>,
    pub entity: Option<String>,              // restrict to facts touching this entity id
    pub valid_at: Option<DateTime<Utc>>,     // as-of query into the bi-temporal history
}
```

> Note: `RecallFilters` is the §2C.4 type re-stated here with `Clone, Default` so `StageOneQuery` is
> self-contained for C1; C6/C8 use the §2C.4 definition. The two are field-identical.

#### Public Interface

The full `MemoryStore` trait surface, expanded beyond the abbreviated §2C.6 list. All methods are
`async` and return `Result<_, StoreError>`. Every read/mutate method that takes `ctx: &ScopeContext`
selects the caller's tenant namespace and applies the read-filter rule before returning or mutating any
record. `put_fact`, `put_entity`, `put_relationship`, `put_source` are write-path (C4) entry points and
take the record's own `owner` scope; they validate `owner.tenant` against the active namespace.

```rust
#[async_trait]
pub trait MemoryStore: Send + Sync {
    // --- Fact CRUD + bi-temporal operations ---
    async fn put_fact(&self, f: &Fact) -> Result<(), StoreError>;
    async fn get_fact(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Fact>, StoreError>;
    async fn recall(&self, ctx: &ScopeContext, q: &StageOneQuery) -> Result<Vec<Candidate>, StoreError>;
    async fn end_validity(&self, ctx: &ScopeContext, id: &str, at: DateTime<Utc>) -> Result<(), StoreError>;
    async fn supersede(&self, ctx: &ScopeContext, old_id: &str, new_id: &str, at: DateTime<Utc>)
        -> Result<(), StoreError>; // end old validity at `at`, link old.superseded_by=new & new.supersedes=old
    async fn hard_delete(&self, ctx: &ScopeContext, id: &str) -> Result<DeletionProof, StoreError>;

    // --- Entity CRUD + resolution support ---
    async fn put_entity(&self, e: &Entity) -> Result<(), StoreError>;   // upsert
    async fn get_entity(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Entity>, StoreError>;
    async fn find_entity_by_name(&self, ctx: &ScopeContext, name: &str)
        -> Result<Vec<Entity>, StoreError>; // exact match on canonical_name OR any alias, read-filtered
    async fn merge_entities(&self, ctx: &ScopeContext, keep_id: &str, merge_id: &str)
        -> Result<(), StoreError>; // repoint facts/relationships from merge_id to keep_id, fold aliases, drop merge_id

    // --- Relationship CRUD ---
    async fn put_relationship(&self, r: &Relationship) -> Result<(), StoreError>;
    async fn get_relationship(&self, ctx: &ScopeContext, id: &str)
        -> Result<Option<Relationship>, StoreError>;
    async fn end_relationship_validity(&self, ctx: &ScopeContext, id: &str, at: DateTime<Utc>)
        -> Result<(), StoreError>;

    // --- Source CRUD ---
    async fn put_source(&self, s: &Source) -> Result<(), StoreError>;
    async fn get_source(&self, ctx: &ScopeContext, id: &str) -> Result<Option<Source>, StoreError>;

    // --- Audit trail (SA-AUDIT-01) ---
    async fn append_audit(&self, e: &AuditEntry) -> Result<(), StoreError>; // append-only, synchronous

    // --- Maintenance surface consumed by C7 (all take ctx; ctx.tenant selects the namespace) ---
    /// Currently-valid episodic facts ingested at/after `since`, capped at `limit` — consolidation input.
    async fn scan_recent_episodes(&self, ctx: &ScopeContext, since: DateTime<Utc>, limit: u32)
        -> Result<Vec<Fact>, StoreError>;
    /// Pairs of currently-valid facts sharing >=1 entity, ranked for contradiction review.
    async fn scan_contradiction_candidates(&self, ctx: &ScopeContext, limit: u32)
        -> Result<Vec<(Fact, Fact)>, StoreError>;
    /// Coarse decay prefilter: currently-valid facts with `salience < salience_floor`, ordered by
    /// staleness (oldest `last_recalled_at`/`ingested_at` first), capped at `limit`. C7 owns the
    /// Ebbinghaus retrievability maths and the final prune decision (ADR-006).
    async fn scan_decay_candidates(&self, ctx: &ScopeContext, salience_floor: f64, limit: u32)
        -> Result<Vec<Fact>, StoreError>;
    /// Currently-valid facts whose stored embedding-model version differs from `current_model_version`,
    /// or that have no embedding, eligible for re-embedding.
    async fn scan_reembed_candidates(&self, ctx: &ScopeContext, current_model_version: &str, limit: u32)
        -> Result<Vec<Fact>, StoreError>;
    /// Persist C7's maintenance bookkeeping on a fact: confidence, salience, stability,
    /// last_recalled_at, supersedes, superseded_by (scope-checked; never changes content/owner).
    async fn update_fact_maintenance_fields(&self, ctx: &ScopeContext, f: &Fact)
        -> Result<(), StoreError>;
    /// Write a refreshed embedding vector + model-version tag onto a fact's row (re-embed path).
    async fn set_fact_embedding(&self, ctx: &ScopeContext, fact_id: &str, vector: &[f32],
        model_version: &str) -> Result<(), StoreError>;

    // --- Lifecycle / tenancy ---
    async fn list_tenants(&self) -> Result<Vec<String>, StoreError>;  // enumerate provisioned namespaces (admin)
    async fn ensure_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError>; // idempotent provision
    async fn drop_tenant_namespace(&self, tenant: &str) -> Result<(), StoreError>;   // ADR-011 erasure
    async fn ready(&self) -> Result<(), StoreError>; // connection live + vector-index dim == RECALL_EMBED_DIM
}
```

`AuditEntry` (C1-owned, persisted to the per-tenant `audit_log` table):

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct AuditEntry {
    pub id: String,             // "audit_log:<uuidv7>"
    pub tenant: String,         // selects the namespace; never stored cross-tenant
    pub subject: String,        // ctx.user (OIDC subject)
    pub operation: String,      // "recall" | "remember" | "forget" | "get_fact" | "freshness_check"
    pub scope: ScopeRef,        // the scope the operation targeted
    pub outcome: String,        // "ok" | "denied" | "error:<CODE>"
    pub token_jti: String,      // ctx.token_jti — never the token itself, never PII
    pub correlation_id: String,
    pub at: DateTime<Utc>,      // server-set, RFC3339 ms
}
```

##### Example

`put_fact` then `recall` (vector + keyword signals), within tenant `acme`:

Input to `put_fact`:

```json
{
  "id": "fact:018f9a2b-7c41-7e10-9b3a-1f2e3d4c5b6a",
  "content": {"subject": "team:alpha", "predicate": "owns", "object": "table:orders"},
  "entities": ["entity:018f9a2b-aaaa-7e10-9b3a-000000000001"],
  "source_id": "source:018f9a2b-bbbb-7e10-9b3a-000000000002",
  "memory_class": "semantic",
  "visibility": "team-shared",
  "owner": {"tenant": "acme", "team": "alpha", "user": "u-sarah"},
  "valid_from": "2026-01-04T09:00:00.000Z",
  "valid_to": null,
  "ingested_at": "2026-06-20T12:00:00.000Z",
  "confidence": 0.9, "salience": 0.7, "stability": 1.0,
  "supersedes": null, "superseded_by": null,
  "derived_from": [], "last_recalled_at": null
}
```

`recall(ctx, StageOneQuery{ query_vector: [..1024 f32..], keyword_terms: ["orders","owns"],
filters: {}, scope: ctx(tenant="acme", teams=["alpha"], user="u-sarah"), stage1_k: 50 })` returns:

```rust
vec![ Candidate {
    fact_id: "fact:018f9a2b-7c41-7e10-9b3a-1f2e3d4c5b6a".into(),
    fact: /* the Fact above */,
    semantic_score: 0.83, keyword_score: 0.61, graph_score: 0.0,
} ]
```

The candidate is returned because `owner.tenant == ctx.tenant` and `visibility == TeamShared` with
`owner.team ("alpha") ∈ ctx.teams`. A fact owned by a different tenant cannot appear: it lives in a
different namespace.

#### Internal Logic

**Connection lifecycle (constructor `Store::connect(cfg)`):**

1. Read `RECALL_STORE_REMOTE_URL`, `RECALL_STORE_PATH`, `RECALL_STORE_BACKEND`, `RECALL_EMBED_DIM` from
   the loaded config. The connection is typed over `surrealdb::engine::any::Any` and opened through
   `surrealdb::engine::any::connect(endpoint)`, so a single `Store` carries either an embedded engine
   or a remote endpoint — the engine is resolved from the endpoint scheme (FU-009): `RECALL_STORE_REMOTE_URL`
   verbatim when set (e.g. `ws://host:8000`, `http(s)://…`; log a warning if `RECALL_STORE_PATH` is also
   set — remote wins), otherwise `surrealkv://{RECALL_STORE_PATH}` for the embedded default (in-process
   tests/ephemeral runs use `mem://`). A remote deployment therefore needs no code change. On any
   connection failure return `StoreError::Unavailable`. Log span `store.connect` with field `backend`.
   *(Residual: a secured remote server needs `signin` credentials, which `connect` does not yet pass —
   see the plan's remote-auth follow-up.)*
2. Migrations are applied lazily per tenant through the `Migrator` (see Data Model): every read/write
   method first calls `ensure_and_use(tenant)`, which runs `Migrator::migrate_up(tenant)` (idempotent —
   a no-op once the namespace is at the latest version) then selects the tenant namespace and the
   `recall` database. A SurrealDB timeout maps to `StoreError::Timeout`, any other engine error to
   `StoreError::Unavailable` (see `map_db_err`).
3. `ready()`: issue a trivial `RETURN 1` query against the live connection to confirm the engine
   answers; a failure maps via `map_db_err` (`StoreError::Timeout`/`StoreError::Unavailable`). The
   per-tenant vector-index dimension is fixed at migration time — the HNSW DDL embeds
   `RECALL_EMBED_DIM` via the `Migrator` (the `<dim>` placeholder, SA-EMBED-01) — so it is not
   re-checked here; C8's `/readyz` compares `index_embed_dim()` against `RECALL_EMBED_DIM`.

**`ensure_tenant_namespace(tenant)`:**

4. Delegate to `Migrator::migrate_up(tenant)`. The Migrator validates the tenant id
   (`validate_tenant`), creates the namespace and `recall` database itself with `DEFINE NAMESPACE IF
   NOT EXISTS <tenant>; USE NS <tenant>; DEFINE DATABASE IF NOT EXISTS recall;` (DDL identifiers cannot
   be parameter-bound, so the validated tenant is interpolated), ensures the `schema_migrations`
   bookkeeping table, then applies every pending numbered migration's `DEFINE TABLE`/`DEFINE
   FIELD`/`DEFINE INDEX`/`DEFINE ANALYZER` statements (Data Model). All `DEFINE` statements use `IF NOT
   EXISTS`, so the call is idempotent. Engine errors map via `map_db_err`
   (`StoreError::Timeout`/`StoreError::Unavailable`).

**`put_fact(f)` / `put_entity` / `put_relationship` / `put_source`:**

5. Validate the record against the domain rules in Shared Context (`entities.len() >= 1`; scores in
   `[0,1]`; `stability >= 0.0`; `valid_to >= valid_from`; `content` is a JSON object; string-length
   bounds). On failure return `StoreError::Validation` and do not write.
6. `USE NS <f.owner.tenant>` to select the namespace. If the configured/active namespace would differ
   from `f.owner.tenant`, return `StoreError::ScopeForbidden` (a record may not be written outside its
   own tenant namespace).
7. Execute a parameterised `UPSERT $id CONTENT $rec`, binding the record id as `$id` (a record-id
   value built from the "table:key" id, split on the first `:`) and the native row object as `$rec` —
   never string-interpolated. The row is built by `convert::*_to_object`; for `put_fact` it also
   carries the derived `content_text` BM25 projection (`convert::content_to_text`), the `embedding`
   field (the fact-content vector supplied by C4, written as `NONE` on first write) and an
   `embedding_model` version string, both filled later by C7 re-embedding via `set_fact_embedding`.
   `append_audit` uses `CREATE $id CONTENT $rec` (append-only). On a store error return
   `StoreError::Unavailable`; on per-statement timeout `StoreError::Timeout`. Logged at `store.put`.

**`get_fact(ctx, id)` / `get_entity` / `get_relationship` / `get_source`:**

8. `ensure_and_use(ctx.tenant)` (select + lazily migrate the namespace). Run a parameterised
   `SELECT * FROM $id WHERE <read-filter>` binding the record id as `$id` and the read-filter
   parameters `$cuser` (`ctx.user`) and `$cteams` (`ctx.teams`) — never request-body input. `get_fact`
   uses the maintenance-aware `fact_read_filter(ctx)` (the whole tenant for a maintenance scope, else
   `fact_filter_clause()`); `get_entity`/`get_relationship`/`get_source` use `novis_filter_clause()`.
   If the record exists but the predicate excludes it, return `Ok(None)` (a caller learns nothing about
   records it cannot read; this is the read-side equivalent of not-found). If the record does not
   exist, return `Ok(None)`. The decoded row drops the persistence-only `embedding`, `embedding_model`,
   and `content_text` columns before deserialising into the domain `Fact`.

**`recall(ctx, q)` (multi-signal stage-1, NFR-P3 / SA-LAT-01):**

9. `USE NS <ctx.tenant>`. Run up to three parameterised sub-queries, each constrained by the read-filter
   predicate and the `q.filters` (`memory_class`, `visibility`, `entity`, and `valid_at` — when
   `valid_at` is set, restrict to facts whose `valid_from <= valid_at AND (valid_to IS NONE OR valid_to
   > valid_at)`; when unset, restrict to currently-valid facts `valid_to IS NONE`):
   - **Vector signal** (skipped if `q.query_vector` is empty): SurrealDB HNSW approximate-nearest-
     neighbour search through the `<|K,EF|>` KNN operator, which drives the `fact_hnsw` index — an
     O(log N) index walk, not the prior O(N) `vector::similarity::cosine(...) ORDER BY` brute-force
     scan (RISK-008, resolved). The query is `SELECT meta::id(id) AS fid, vector::distance::knn() AS
     dist FROM fact WHERE embedding <|knn_k,ef|> $qv AND (<read-filter>) AND <validity><meta> ORDER BY
     dist ASC LIMIT $k` with `$qv = q.query_vector`, `$k = q.stage1_k`. `vector::distance::knn()`
     reports each match's cosine distance computed during the index walk. `K` (`knn_k`) and `EF`
     (`ef`) are operator syntax and cannot be parameter-bound, so they are formatted as validated
     integers — never caller input: `knn_k = (4 × stage1_k).clamp(stage1_k, 1024)` (a 4× over-fetch so
     the KNN-then-filter ordering rarely starves the final set), `ef = knn_k.max(64)`. The scope
     read-filter, validity, and metadata predicates are AND-ed on top of the KNN operator
     (KNN-then-filter), the result ordered by ascending distance and capped to `stage1_k`. Cosine
     similarity is recovered as `1 - dist` and clamped to `[0,1]`. The ANN portion must complete in
     ≤ 50 ms (NFR-P3); on per-statement timeout return `StoreError::Timeout`.
   - **Keyword signal** (skipped if `q.keyword_terms` is empty): BM25 full-text search over the
     derived `content_text` projection using the `@0@` full-text match operator and the `search::score(0)`
     relevance function against the `fact_bm25` index, `LIMIT $k`. The query is `SELECT meta::id(id) AS
     fid, search::score(0) AS s FROM fact WHERE content_text @0@ $terms AND (<read-filter>) AND
     <validity><meta> ORDER BY s DESC LIMIT $k`, with `$terms` the caller's `keyword_terms` joined by
     spaces.
   - **Graph signal** (run when `q.filters.entity` is set): select currently-valid, read-filtered
     facts whose `entities` array contains the given entity id, served by the `fact_ent` index —
     `SELECT meta::id(id) AS fid FROM fact WHERE $entity IN entities AND (<read-filter>) AND
     <validity> LIMIT $k`. (`entities` holds "table:key" entity id strings; the `fact_ent` index on
     `FIELDS entities` is load-bearing for this leg. Multi-hop `relationship`-edge traversal beyond
     this direct-membership query is not implemented in this method — see Gaps.)
10. Merge the three result sets, keyed by the bare fact id returned by each sub-query's `meta::id(id)
    AS fid` projection, into `Candidate`s, populating `semantic_score`, `keyword_score`, `graph_score`
    (0.0 for any signal that did not surface that fact; the graph signal contributes a fixed `1.0`).
    Normalise BM25 scores to `[0,1]` by dividing by the maximum `search::score(0)` in the keyword
    result set (0.0 when the set is empty). Resolve each surviving id (prefixed back to `fact:<fid>`)
    to its full `Fact` by calling `get_fact(ctx, id)`, which re-applies the read filter. Return up to
    `q.stage1_k` candidates. No fusion or rerank is done here — that is C6. Logged at `store.recall`
    with `candidate_count`.

**`end_validity(ctx, id, at)` / `end_relationship_validity`:**

11. `USE NS <ctx.tenant>`. Confirm read access via the read filter; if excluded or absent return
    `StoreError::NotFound`. Run a parameterised `UPDATE <table>:⟨id⟩ SET valid_to = $at WHERE valid_to
    IS NONE`. Setting `valid_to` retires the record without deleting it (ADR-002). If the record was
    already retired (`valid_to` already set), the update is a no-op and returns `Ok(())` (idempotent).
    Logged at `store.end_validity`.

**`supersede(ctx, old_id, new_id, at)`:**

12. `USE NS <ctx.tenant>`. Confirm read access to both ids; if either is excluded or absent return
    `StoreError::NotFound`. In one parameterised transaction: `UPDATE fact:⟨old⟩ SET valid_to = $at,
    superseded_by = $new` and `UPDATE fact:⟨new⟩ SET supersedes = $old`. Neither fact is deleted
    (ADR-002). On store failure return `StoreError::Unavailable`. Logged at `store.supersede`.

**`hard_delete(ctx, id)` (SA-DELETE-01):**

13. `ensure_and_use(ctx.tenant)`. Confirm `forget` access path via `get_fact(ctx, id)` (the operation
    is authorised by C8; the store enforces tenant + read-filter scoping); if excluded or absent return
    `StoreError::NotFound`. Collect every consolidated-insight fact whose `derived_from` contains this
    id with a parameterised `SELECT * FROM fact WHERE $base IN derived_from AND (<read-filter>)`; this
    derived-collection query uses the maintenance-aware `fact_read_filter(ctx)` so a maintenance scope
    reaches user-private and team-shared derived insights (RISK-009). Build the removal set — the fact
    itself plus each derived insight — and count embedding vectors removed (the fact's own embedding
    plus any derived-summary embeddings, each probed via a `SELECT embedding FROM $id` non-null check).
14. Delete all collected records in one parameterised `DELETE $ids` (the ids bound as a list of
    record-id values). Compute `digest` as the SHA-256 hex of the newline-joined, sorted removed ids
    and build `DeletionProof { deleted_at: now, record_id: id, derived_removed: [...],
    embeddings_removed: N, digest }`. After deleting, re-read each collected id via `get_fact`; if any
    is still present the removal is partial — return `StoreError::PartialDelete { removed, expected }`
    (`removed = expected - still_present`) and never report the delete complete (SA-DELETE-01). Logged
    at `store.hard_delete` with `derived_removed_count`, `embeddings_removed`.

**`find_entity_by_name(ctx, name)` (entity-resolution support for C4):**

15. `USE NS <ctx.tenant>`. Run a parameterised `SELECT * FROM entity WHERE (canonical_name = $name OR
    $name IN aliases) AND <read-filter>`. Return all matches (the caller resolves ties). Logged at
    `store.find_entity`.

**`append_audit(e)` (SA-AUDIT-01):**

16. `USE NS <e.tenant>`. Run a parameterised `CREATE audit_log:⟨id⟩ CONTENT $entry`. The `audit_log`
    table is append-only (Data Model permissions forbid `UPDATE`/`DELETE`); it stores subject,
    operation, scope, outcome, `token_jti`, correlation id, and timestamp — never the token, never fact
    content. Called synchronously by C8 before the response is returned. On store failure return
    `StoreError::Unavailable`. Logged at `store.audit`.

**Maintenance surface (consumed by C7; each uses `ctx.tenant` to select the namespace):**

17. `scan_decay_candidates(ctx, salience_floor, limit)`: `USE NS <ctx.tenant>`; parameterised `SELECT *
    FROM fact WHERE valid_to IS NONE AND salience < $floor ORDER BY last_recalled_at ASC NUMERIC LIMIT
    $limit` (rows with a NULL `last_recalled_at` order first via `ingested_at`). This is a coarse
    prefilter; C7 computes the Ebbinghaus retrievability `R = exp(-Δt / (stability · k))` and decides
    pruning (ADR-006). (SA-DECAY-01.)
18. `scan_contradiction_candidates(ctx, limit)`: `USE NS <ctx.tenant>`; parameterised query returning
    pairs of currently-valid facts that share at least one entity id, capped at `limit`.
19. `scan_reembed_candidates(ctx, current_model_version, limit)`: `USE NS <ctx.tenant>`; parameterised
    `SELECT * FROM fact WHERE valid_to IS NONE AND (embedding_model != $current OR embedding IS NONE)
    LIMIT $limit`.
19a. `scan_recent_episodes(ctx, since, limit)`: `USE NS <ctx.tenant>`; parameterised `SELECT * FROM fact
    WHERE valid_to IS NONE AND memory_class = 'episodic' AND ingested_at >= $since LIMIT $limit`.
19b. `update_fact_maintenance_fields(ctx, f)`: `USE NS <ctx.tenant>`; confirm read access to `f.id`
    (else `StoreError::NotFound`); parameterised `UPDATE fact:⟨f.id⟩ SET confidence = $c, salience = $s,
    stability = $st, last_recalled_at = $lr, supersedes = $sup, superseded_by = $supby` — content,
    owner, validity, and class are never touched by this method. Logged at `store.update_maint`.
19c. `set_fact_embedding(ctx, fact_id, vector, model_version)`: `USE NS <ctx.tenant>`; assert
    `vector.len() == RECALL_EMBED_DIM` (else `StoreError::Validation`); parameterised `UPDATE
    fact:⟨fact_id⟩ SET embedding = $v, embedding_model = $m`. Logged at `store.set_embedding`.
19d. `merge_entities(ctx, keep_id, merge_id)`: `ensure_and_use(ctx.tenant)`; confirm read access to
    both via `get_entity` (else `StoreError::NotFound`); in one `BEGIN; … COMMIT;` transaction repoint
    every `fact.entities` membership and every `relationship.from_ent`/`to_ent` string reference from
    `merge_id` to `keep_id` (the relationship endpoints are stored as the `from_ent`/`to_ent` string
    fields, not `from`/`to` — see Data Model), fold `merge_id`'s aliases + canonical_name into
    `keep_id.aliases`, then `DELETE $merge`. Logged at `store.merge_entities`.
19e. `list_tenants()`: `INFO FOR ROOT` to enumerate defined namespaces (the `namespaces` object's
    keys); return the tenant ids. Admin-only — takes no `ctx`; callers (C2 reaper, C7 scheduler)
    iterate the result and build a per-tenant `ScopeContext`. Logged at `store.list_tenants`.

**`drop_tenant_namespace(tenant)` (ADR-011 erasure):**

20. `REMOVE NAMESPACE IF EXISTS <tenant>` (the tenant id validated by `validate_tenant` before
    interpolation, since DDL identifiers cannot be parameter-bound) — removes every Fact, Entity, Relationship, Source, audit entry, and
    derived artefact for the tenant in one structural operation, satisfying per-tenant erasure. This is
    a destructive operation; see Data Model rollback considerations. Logged at `store.drop_ns`.

Every external store call carries a per-statement timeout (default 2000 ms, the read-path store budget
within SA-LAT-01); a breach returns `StoreError::Timeout`. All logs are structured and include
`correlation_id` from `ctx` where a `ctx` is present.

#### Data Model

SurrealDB DDL applied per tenant through the `Migrator` (SA-MIG-01). One namespace per tenant
(ADR-011); one database `recall` inside each. The five originally-numbered migrations (init, queue,
quarantine, maintenance, idempotency) were squashed into a single initial migration
`migrations/0001_init.{up,down}.surql` before first release (greenfield, no shipped DB); the
`Migrator` registers exactly one entry (`version: 1, slug: "init"`) and the migration-count test
asserts `LATEST = 1` (FU-018). This component owns the C1 portion of that one migration — the `fact`,
`entity`, `relationship`, `source`, and `audit_log` tables; the same file also carries the C2 queue
(`work_job`/`dead_letter`), C4 `quarantine`, C7 `maintenance_state`, and C8 `idempotency_record`
tables owned by their components. Tables `fact`, `entity`, `relationship`, `source` are
**schemaless** with typed `DEFINE FIELD` constraints and tighten further as the model stabilises
(SA-MIG-01); `audit_log` is **schemafull and append-only** because its shape is fixed and
integrity-critical. All `DEFINE` statements are idempotent (`IF NOT EXISTS`) so the migration is safe
to repeat.

The namespace and database DDL is **not** in the `.surql` file: the `Migrator` selects the namespace
itself before running the migration body — `DEFINE NAMESPACE IF NOT EXISTS <tenant>; USE NS <tenant>;
DEFINE DATABASE IF NOT EXISTS recall;` then `use_ns(tenant).use_db("recall")` — and tracks applied
versions in a `schema_migrations` table created **outside** the numbered set (so version 0 is readable
on a fresh namespace). The HNSW `DIMENSION` is the literal placeholder `<dim>` in the `.surql` file,
substituted by the Migrator (`render_up`) from `RECALL_EMBED_DIM` before execution (SA-EMBED-01).

SurrealDB 3.x adaptations baked into the as-built DDL (FU-008): BM25 uses `FULLTEXT ANALYZER … BM25`
over a derived `content_text` projection (the 2.x `SEARCH ANALYZER` form is rejected); schemafull
tables carrying arbitrary nested JSON use `TYPE object FLEXIBLE`; and domain ids are stored as
"table:key" strings throughout (see `src/store/convert.rs`), so link-shaped fields are `string` /
`array<string>` / `option<string>`, not native record links.

```surql
-- src/store/migrate.rs SCHEMA_MIGRATIONS_DDL — created outside the numbered set so version 0 is readable
DEFINE TABLE IF NOT EXISTS schema_migrations SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS version    ON schema_migrations TYPE int;
DEFINE FIELD IF NOT EXISTS applied_at ON schema_migrations TYPE datetime;
DEFINE INDEX IF NOT EXISTS sm_version ON schema_migrations FIELDS version UNIQUE;
```

```surql
-- migration 0001_init.up.surql (C1 portion) — applied within each tenant namespace/database.
-- The Migrator selects the namespace (USE NS <tenant>; USE DB recall) before running these statements,
-- so the file body contains only table/field/index DDL (no DEFINE NAMESPACE/DATABASE).

-- fact: schemaless body, indexed for the three retrieval signals
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
-- BM25 keyword analyzer + full-text index over the derived content_text projection (3.x syntax)
DEFINE ANALYZER IF NOT EXISTS recall_text TOKENIZERS class FILTERS lowercase, ascii;
DEFINE INDEX IF NOT EXISTS fact_bm25 ON fact FIELDS content_text FULLTEXT ANALYZER recall_text BM25;
-- HNSW vector index; DIMENSION is the <dim> placeholder, substituted from RECALL_EMBED_DIM (SA-EMBED-01)
DEFINE INDEX IF NOT EXISTS fact_hnsw ON fact FIELDS embedding HNSW DIMENSION <dim> DIST COSINE;
-- secondary indexes for scope filtering, the graph leg, and bi-temporal scans
DEFINE INDEX IF NOT EXISTS fact_owner_user ON fact FIELDS owner.user;
DEFINE INDEX IF NOT EXISTS fact_owner_team ON fact FIELDS owner.team;
DEFINE INDEX IF NOT EXISTS fact_valid      ON fact FIELDS valid_to;
DEFINE INDEX IF NOT EXISTS fact_ent        ON fact FIELDS entities;  -- graph-leg membership index

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
-- endpoints stored as plain "table:key" strings in from_ent/to_ent; the domain `from`/`to` clash with
-- SurrealQL reserved words, and ids are strings throughout, so these are TYPE string not record links
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

-- audit_log: schemafull + append-only from the first migration (SA-AUDIT-01)
DEFINE TABLE IF NOT EXISTS audit_log SCHEMAFULL
    PERMISSIONS FOR create FULL FOR select FULL FOR update NONE FOR delete NONE;
DEFINE FIELD IF NOT EXISTS subject        ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS operation      ON audit_log TYPE string;
-- `scope` is TYPE object FLEXIBLE so the audit ScopeRef sub-keys (tenant/team/user) write on 3.x (FU-016)
DEFINE FIELD IF NOT EXISTS scope          ON audit_log TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS outcome        ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS token_jti      ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS correlation_id ON audit_log TYPE string;
DEFINE FIELD IF NOT EXISTS at             ON audit_log TYPE datetime;
DEFINE INDEX IF NOT EXISTS audit_at ON audit_log FIELDS at;

-- The same 0001_init.up.surql also defines the C2 (work_job, dead_letter), C4 (quarantine),
-- C7 (maintenance_state), and C8 (idempotency_record) tables owned by those components.
```

The `content_text` column is a derived flat-keyword projection of the structured `content` object —
every scalar leaf concatenated with spaces, built by `convert::content_to_text` on each `put_fact` and
stored alongside the intact `content` object. The `fact_bm25` index is defined over `content_text`, and
the keyword recall sub-query matches `content_text @0@ $terms`. `content_text` (with `embedding` and
`embedding_model`) is a persistence-only column dropped on read before deserialising the domain `Fact`.

```surql
-- migration 0001_init.down.surql — destructive reversal (the C1 portion shown)
REMOVE INDEX fact_hnsw ON fact;       REMOVE INDEX fact_bm25 ON fact;
REMOVE INDEX fact_owner_user ON fact; REMOVE INDEX fact_owner_team ON fact;
REMOVE INDEX fact_valid ON fact;      REMOVE INDEX fact_ent ON fact;
REMOVE INDEX entity_name ON entity;   REMOVE INDEX entity_owner ON entity;
REMOVE INDEX rel_from ON relationship; REMOVE INDEX rel_to ON relationship;
REMOVE INDEX audit_at ON audit_log;
REMOVE TABLE audit_log; REMOVE TABLE source; REMOVE TABLE relationship; REMOVE TABLE entity; REMOVE TABLE fact;
-- (the down file also drops the C2/C4/C7/C8 tables and indexes from the same squashed migration)
```

**Rollback considerations (sql-safety layer):**

- The single `0001_init` down path `REMOVE TABLE`/`REMOVE INDEX` is **destructive** — it discards every
  fact, entity, relationship, source, and audit record in the namespace (and the C2/C4/C7/C8 tables in
  the same squashed migration) and drops the vector/keyword indexes. It is intended only to unwind a
  failed initial migration on an empty store. `Migrator::migrate_down` logs the intent but does not
  guard population — that decision is the caller's (an explicit user action). It must never run against
  a populated production namespace; applies to a shared store are a user action with a `dry_run` first
  (SA-MIG-01).
- `drop_tenant_namespace` (`REMOVE NAMESPACE IF EXISTS`) is **irreversible and intentionally
  destructive** — it is the ADR-011 per-tenant erasure mechanism. There is no down path; the data is
  gone by design.
- Any future schemaless→schemafull tightening for `fact`/`entity`/`relationship`/`source` would be a
  **later migration** (version 2+) that adds `ASSERT`/type constraints; tightening a populated table can
  reject rows that violate a new constraint, so such a migration must include a forward
  data-validation scan and a down path that widens the field back to schemaless (non-destructive).
  Changing the HNSW `DIMENSION` is destructive (it drops and rebuilds the index and requires
  re-embedding all facts via C7) and is out of scope for `0001_init`; the `<dim>` placeholder fixes the
  dimension from `RECALL_EMBED_DIM` at first migration only.

#### Error Table

| Condition | Status | Code | Response Body |
|-----------|--------|------|---------------|
| Record fails domain validation (empty `entities`, score out of `[0,1]`, `valid_to < valid_from`, non-object `content`, name length) | 400 | VAL_INVALID_BODY | `{"error":{"code":"VAL_INVALID_BODY","message":"record failed validation","correlation_id":"<uuid>"}}` |
| Score field outside `[0.0,1.0]` (`confidence`/`salience`/`trust_signal`) | 400 | VAL_OUT_OF_RANGE | `{"error":{"code":"VAL_OUT_OF_RANGE","message":"score out of range","correlation_id":"<uuid>"}}` |
| `get_fact`/`end_validity`/`hard_delete`/`supersede` target id absent or excluded by the read filter | 404 | NOT_FOUND | `{"error":{"code":"NOT_FOUND","message":"record not found","correlation_id":"<uuid>"}}` |
| `put_*` record `owner.tenant` differs from the active namespace (cross-tenant write attempt) | 403 | SCOPE_FORBIDDEN | `{"error":{"code":"SCOPE_FORBIDDEN","message":"record outside caller tenant","correlation_id":"<uuid>"}}` |
| Store connection lost / engine not reachable | 503 | STORE_UNAVAILABLE | `{"error":{"code":"STORE_UNAVAILABLE","message":"memory store unavailable","correlation_id":"<uuid>"}}` |
| Per-statement timeout (read-path store op exceeds budget; ANN > 50 ms or query > 2000 ms) | 504 | STORE_TIMEOUT | `{"error":{"code":"STORE_TIMEOUT","message":"memory store timed out","correlation_id":"<uuid>"}}` |
| `hard_delete` verification finds a collected record still present (partial delete) | 500 | INTERNAL | `{"error":{"code":"INTERNAL","message":"deletion incomplete","correlation_id":"<uuid>"}}` |

`StoreError` → `code` mapping is owned by the Phase 4 Error Handling spec; this table states the
intended mapping for every failure mode the Internal Logic references. `StoreError::Validation` maps to
`VAL_INVALID_BODY` for structural failures and `VAL_OUT_OF_RANGE` for score-range failures;
`StoreError::PartialDelete` maps to `INTERNAL` because a partial delete is an internal invariant
violation, never a client error.

#### Acceptance Criteria (Gherkin)

```gherkin
Feature: Memory Store

  Scenario: Happy path — put a fact then recall it by vector + keyword
    Given a provisioned tenant namespace "acme"
    And a fact owned by {tenant "acme", team "alpha", user "u-sarah"} with visibility "team-shared" is stored via put_fact
    When recall is called with ctx {tenant "acme", teams ["alpha"], user "u-sarah"}, a query_vector of dimension RECALL_EMBED_DIM, and keyword_terms ["orders"]
    Then a Candidate for that fact is returned
    And its semantic_score and keyword_score are both in [0.0, 1.0]

  Scenario: Edge case — cross-tenant isolation on recall
    Given a fact F owned by tenant "acme" with visibility "tenant-shared"
    And a caller ctx whose tenant is "globex"
    When recall is called with ctx for tenant "globex" matching F's content
    Then F is not in the returned candidates
    And no Candidate references any record outside namespace "globex"

  Scenario: Edge case — supersession ends validity without deleting
    Given a currently-valid fact "fact:old" in tenant "acme" with valid_to NONE
    And a replacement fact "fact:new" in tenant "acme"
    When supersede is called with old "fact:old", new "fact:new", at "2026-06-20T12:00:00.000Z"
    Then get_fact for "fact:old" still returns the record
    And "fact:old".valid_to equals "2026-06-20T12:00:00.000Z" and "fact:old".superseded_by equals "fact:new"
    And "fact:new".supersedes equals "fact:old"

  Scenario: Edge case — verifiable hard delete removes derived summaries and embeddings
    Given a fact "fact:base" in tenant "acme" with two consolidated insights whose derived_from includes "fact:base"
    When hard_delete is called for "fact:base"
    Then a DeletionProof is returned with derived_removed listing both insight ids
    And embeddings_removed equals the count of removed embedding vectors
    And digest equals the sha256 hex of the sorted removed record ids
    And get_fact for "fact:base" returns None

  Scenario: Error path — store unavailable
    Given the embedded store connection has been lost
    When get_fact is called
    Then the call returns StoreError::Unavailable
    And the mapped response is 503 with code STORE_UNAVAILABLE

  Scenario: Error path — score out of range on write
    Given a fact whose confidence is 1.4
    When put_fact is called
    Then the call returns StoreError::Validation
    And the mapped response is 400 with code VAL_OUT_OF_RANGE
    And no record is written
```

#### Performance, Security, Observability

- **Performance targets:** ANN-only vector similarity search ≤ 50 ms at target corpus size (NFR-P3, the
  vector portion of step 9). The full `recall` stage-1 operation and every other read-path store op fit
  the SA-LAT-01 stage-1 sub-budget (stage-1 ANN ≤ 50 ms within NFR-P2 p95 ≤ 200 ms). Per-statement
  timeout default 2000 ms; embedded in-process access removes any client/server hop (ADR-009). Memory:
  the HNSW index is held by the embedded engine; no per-request fact cache is kept in this component.
- **Security:** every read/mutate method takes a `ScopeContext` and applies the read-filter rule;
  cross-tenant access is structurally impossible (namespace per tenant, ADR-011). Scope values come
  only from `ScopeContext` (derived by C3 from the validated token), never from request-body input. All
  queries are parameterised — caller content (`content`, `query_vector`, `keyword_terms`, names) is
  bound as parameters, never string-interpolated (no SQL/SurrealQL injection). The `audit_log` table is
  append-only by table permission and never stores the token or fact content (SA-AUDIT-01). No secrets,
  tokens, or raw PII are logged.
- **Observability:** metrics `store_op_duration_seconds{op,table,outcome}`,
  `store_recall_candidates{tenant_present}`, `store_ann_duration_seconds`,
  `store_hard_delete_removed_total{kind}`. Log fields: `correlation_id`, `op`, `table`, `tenant_present`
  (boolean — never the tenant id at debug-and-below to avoid identifying data in operational logs),
  `outcome`. Trace spans: `store.connect`, `store.ensure_ns`, `store.put`, `store.get`, `store.recall`
  (child spans `store.recall.vector`, `store.recall.keyword`, `store.recall.graph`),
  `store.end_validity`, `store.supersede`, `store.hard_delete`, `store.audit`, `store.scan.decay`,
  `store.scan.contradiction`, `store.scan.reembed`, `store.scan.recent_episodes`,
  `store.update_maint`, `store.set_embedding`, `store.merge_entities`, `store.list_tenants`,
  `store.drop_ns`.

#### Gaps

None. (Reconciliation notes — no open gaps: FU-008 SurrealDB 3.x DDL drift, RISK-008 / FU-014
HNSW-KNN recall, RISK-009 maintenance-scope read filter, and FU-018 single squashed `0001_init`
migration are all reconciled to the as-built code above. The `recall` graph signal is the direct
`entities`-membership query over the `fact_ent` index, not multi-hop `relationship`-edge traversal;
this is the as-built behaviour, documented in the Vector/Keyword/Graph signal description.)
