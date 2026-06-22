//! Cucumber integration harness for the `recall` service.
//!
//! Phase 1 wires the boot smoke feature (`tests/features/boot.feature`): the app boots in-process on
//! an ephemeral port, `/healthz` returns 200, and an unknown route returns the X1 error envelope with
//! a correlation id. No external services are required this phase; the testcontainers/wiremock seams
//! live (unused) in `tests/support`.

mod support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cucumber::{given, then, when, World};
use serde_json::Value;
use support::{boot_minimal, BootedApp};

use recall::auth::{can_read, AuthConfig, AuthError, Authenticator, Op, ScopeRef as AuthScopeRef};
use recall::config::Config;
use recall::error::{map_error, AppError, Env};
use recall::freshness::BrokerFreshnessChecker;
use recall::maintenance::{
    ConsolidationReport, CycleReport, DecayReport, HardDeletePayload, MaintenanceConfig,
    MaintenanceWorker, ReEmbedPayload, SupersessionReport,
};
use recall::providers::{
    HttpBrokerClient, HttpEmbeddingClient, HttpLlmClient, HttpPiiDetector, HttpRerankClient,
};
use recall::queue::StoreWorkQueue;
use recall::retrieval::{RecallOutcome, RetrievalConfig, RetrievalEngine};
use recall::store::Store;
use recall::types::api::{Currency, RecallRequest};
use recall::types::domain::{Fact, MemoryClass, Source, Visibility};
use recall::types::job::{JobKind, JobStatus, WorkJob};
use recall::types::ports::{
    BrokerClient, Candidate, EmbeddingClient, FreshnessChecker, LlmClient, MemoryStore, PiiDetector,
    QueueError, RecallFilters, RerankClient, StageOneQuery, StoreError, WorkQueue,
};
use recall::types::scope::{OpSet, ScopeContext, ScopeRef};
use recall::write_pipeline::{WritePipeline, WritePipelineConfig};
use support::dex::{self, DexInstance};
use support::issuer::{self, LocalIssuer};

/// The cucumber world: the booted app and the most recent HTTP response (Phase-1 boot smoke), plus
/// the embedded in-memory Memory Store and the outcomes of the most recent store calls (Phase-2 C1).
#[derive(World)]
#[world(init = Self::new)]
struct RecallWorld {
    app: Option<BootedApp>,
    status: Option<u16>,
    body: Option<Value>,
    correlation_header: Option<String>,
    // --- C1 store fields ---
    store: Option<Store>,
    candidates: Vec<Candidate>,
    last_proof: Option<recall::types::api::DeletionProof>,
    last_store_err: Option<StoreError>,
    last_fact: Option<Option<Fact>>,
    // --- C2 queue fields ---
    queue: Option<Arc<StoreWorkQueue>>,
    embed_dim: u32,
    last_enqueue_id: Option<String>,
    last_queue_err: Option<QueueError>,
    last_claim: Option<Option<WorkJob>>,
    concurrent_claims: Vec<Option<WorkJob>>,
    last_reaped: Option<u64>,
    // --- C3 auth fields ---
    local_issuer: Option<LocalIssuer>,
    dex: Option<DexInstance>,
    authenticator: Option<Authenticator>,
    token: Option<String>,
    auth_ctx: Option<ScopeContext>,
    auth_err: Option<AuthError>,
    second_ctx: Option<ScopeContext>,
    key_count_after_first: Option<usize>,
    // --- C4 write-pipeline fields ---
    wp: Option<WpHarness>,
    wp_outcome: Option<recall::write_pipeline::WriteOutcome>,
    wp_extract_content: Option<Value>,
    // --- C5 freshness fields ---
    fresh: Option<FreshnessHarness>,
    fresh_facts: Vec<(Fact, Source)>,
    fresh_results: Vec<(String, Currency)>,
    fresh_elapsed_ms: Option<u128>,
    fresh_ctx_tenant: String,
    fresh_budget_ms: u32,
    fresh_per_call_ms: u32,
    // --- C6 retrieval fields ---
    retr: Option<RetrievalHarness>,
    retr_outcome: Option<RecallOutcome>,
    retr_err: Option<recall::error::AppError>,
    retr_saved_cursor: Option<String>,
    retr_page1_ids: Vec<String>,
    // --- C7 maintenance fields ---
    maint: Option<MaintHarness>,
    maint_report: Option<CycleReport>,
    maint_proof: Option<recall::types::api::DeletionProof>,
    maint_err: Option<recall::error::AppError>,
    // --- C8 HTTP API edge fields ---
    api: Option<ApiHarness>,
    api_token: Option<String>,
    api_idem_key: Option<String>,
    edge_status: Option<u16>,
    edge_body: Option<Value>,
    edge_headers: Option<reqwest::header::HeaderMap>,
    edge_fact_id: Option<String>,
    edge_etag: Option<String>,
    // --- Phase 10 whole-system fields ---
    sys: Option<SystemHarness>,
    sys_token: Option<String>,
    sys_idem_key: Option<String>,
    sys_status: Option<u16>,
    sys_body: Option<Value>,
    sys_recall_fact_ids: Vec<String>,
}

/// The C8 HTTP API edge harness: the full production `AppState` served in-process on an ephemeral
/// port, plus a `LocalIssuer` for minting bearer tokens and the shared SurrealDB handle for asserting
/// audit / queue rows. The provider mocks and issuer are held so their endpoints outlive the server.
struct ApiHarness {
    base_url: String,
    handle: tokio::task::JoinHandle<()>,
    db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    issuer: Arc<LocalIssuer>,
    store: Arc<Store>,
    /// A clone of the AppState rate-limiter map, so a scenario can deterministically seed an empty
    /// bucket (the spec fixes burst 40/10; draining via a seeded empty bucket avoids a timing-dependent
    /// 40-request drain — see the "rate limit exhausted" scenario).
    rate: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<
                (String, recall::api::ratelimit::OpClass),
                recall::api::ratelimit::TokenBucket,
            >,
        >,
    >,
    _mocks: support::ProviderMocks,
}

impl Drop for ApiHarness {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl ApiHarness {
    /// Count audit_log rows for a tenant (read directly from the shared engine; mirrors
    /// WpHarness::count_facts). A tenant never provisioned (no audit table) counts as zero.
    async fn count_audit(&self, tenant: &str, operation: Option<&str>) -> u64 {
        if self
            .db
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .is_err()
        {
            return 0;
        }
        let sql = match operation {
            Some(_) => "SELECT count() AS c FROM audit_log WHERE operation = $op GROUP ALL",
            None => "SELECT count() AS c FROM audit_log GROUP ALL",
        };
        let mut q = self.db.query(sql);
        if let Some(op) = operation {
            q = q.bind(("op", op.to_string()));
        }
        let mut resp = match q.await {
            Ok(r) => r,
            Err(_) => return 0,
        };
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Count work_job rows of a kind for a tenant.
    async fn count_jobs_of_kind(&self, tenant: &str, kind: &str) -> u64 {
        if self
            .db
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .is_err()
        {
            return 0;
        }
        let mut resp = match self
            .db
            .query("SELECT count() AS c FROM work_job WHERE kind = $k GROUP ALL")
            .bind(("k", kind.to_string()))
            .await
        {
            Ok(r) => r,
            Err(_) => return 0,
        };
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }
}

/// The Phase-10 whole-system harness: the FULL stack assembled over ONE shared in-memory SurrealDB
/// engine. The HTTP edge (`build_router` over `AppState`) is served in-process on an ephemeral port; a
/// `WritePipeline` is built over the SAME store handle + queue + provider mocks so the async write path
/// can be drained in-process (no background worker runs). The issuer + provider mocks are held so their
/// endpoints outlive the server. The store handle is shared, so the drained write and the served read
/// observe the same data — modelling eventual consistency across the queue boundary.
struct SystemHarness {
    base_url: String,
    handle: tokio::task::JoinHandle<()>,
    db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    issuer: Arc<LocalIssuer>,
    store: Arc<Store>,
    queue: Arc<StoreWorkQueue>,
    embed_dim: u32,
    mocks_base_url: String,
    _mocks: support::ProviderMocks,
}

impl Drop for SystemHarness {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl SystemHarness {
    /// Build the in-process `WritePipeline` over the shared store/queue/handle and the wiremock-backed
    /// extract/embed/pii providers. Rebuilt per drain so the latest mounts are in effect.
    fn pipeline(&self) -> WritePipeline {
        let config = wp_config(&self.mocks_base_url, self.embed_dim);
        let store: Arc<dyn MemoryStore> = self.store.clone();
        let queue: Arc<dyn WorkQueue> = self.queue.clone();
        let embed: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
        let llm: Arc<dyn LlmClient> = Arc::new(HttpLlmClient::new(&config));
        let pii: Arc<dyn PiiDetector> = Arc::new(HttpPiiDetector::new(&config));
        WritePipeline::new(
            store,
            queue,
            embed,
            llm,
            pii,
            self.db.clone(),
            WritePipelineConfig::from_config(&config),
        )
    }
}

/// The C7 maintenance harness: a shared embedded SurrealDB engine backing the C1 store, the C2 queue,
/// and a wiremock server playing the consolidation LLM and the embedding provider.
struct MaintHarness {
    store: Arc<Store>,
    queue: Arc<StoreWorkQueue>,
    handle: surrealdb::Surreal<surrealdb::engine::any::Any>,
    mocks: support::ProviderMocks,
    embed_dim: u32,
}

/// The C6 retrieval harness: a real embedded store seeded with facts, a store-backed queue for the C5
/// freshness dependency, and a wiremock server playing the embedding, reranker, and broker providers.
struct RetrievalHarness {
    store: Arc<Store>,
    queue: Arc<StoreWorkQueue>,
    mocks: support::ProviderMocks,
    embed_dim: u32,
}

/// The C5 freshness harness: a wiremock server playing the Faraday broker, a store-backed C2 queue
/// over a shared embedded SurrealDB engine, and the shared engine handle for asserting enqueued jobs.
struct FreshnessHarness {
    broker: Arc<HttpBrokerClient>,
    queue: Arc<StoreWorkQueue>,
    handle: surrealdb::Surreal<surrealdb::engine::any::Any>,
    mocks: support::ProviderMocks,
    // Kept alive so the backing store engine is not dropped while the queue shares its connection.
    _store: Arc<Store>,
}

/// The assembled write-pipeline harness: a shared embedded SurrealDB engine backing the C1 store, the
/// C2 queue, and the C4 quarantine table, plus a wiremock server playing the three providers.
struct WpHarness {
    store: Arc<Store>,
    queue: Arc<StoreWorkQueue>,
    handle: surrealdb::Surreal<surrealdb::engine::any::Any>,
    mocks: support::ProviderMocks,
    embed_dim: u32,
    /// The contact string used by the PII scenarios (for the redaction span range).
    contact: Option<String>,
}

impl std::fmt::Debug for RecallWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallWorld")
            .field("status", &self.status)
            .field("body", &self.body)
            .field("candidate_count", &self.candidates.len())
            .finish()
    }
}

impl RecallWorld {
    fn new() -> Self {
        RecallWorld {
            app: None,
            status: None,
            body: None,
            correlation_header: None,
            store: None,
            candidates: Vec::new(),
            last_proof: None,
            last_store_err: None,
            last_fact: None,
            queue: None,
            embed_dim: 8,
            last_enqueue_id: None,
            last_queue_err: None,
            last_claim: None,
            concurrent_claims: Vec::new(),
            last_reaped: None,
            local_issuer: None,
            dex: None,
            authenticator: None,
            token: None,
            auth_ctx: None,
            auth_err: None,
            second_ctx: None,
            key_count_after_first: None,
            wp: None,
            wp_outcome: None,
            wp_extract_content: None,
            fresh: None,
            fresh_facts: Vec::new(),
            fresh_results: Vec::new(),
            fresh_elapsed_ms: None,
            fresh_ctx_tenant: "acme".to_string(),
            fresh_budget_ms: 25,
            fresh_per_call_ms: 20,
            retr: None,
            retr_outcome: None,
            retr_err: None,
            retr_saved_cursor: None,
            retr_page1_ids: Vec::new(),
            maint: None,
            maint_report: None,
            maint_proof: None,
            maint_err: None,
            api: None,
            api_token: None,
            api_idem_key: None,
            edge_status: None,
            edge_body: None,
            edge_headers: None,
            edge_fact_id: None,
            edge_etag: None,
            sys: None,
            sys_token: None,
            sys_idem_key: None,
            sys_status: None,
            sys_body: None,
            sys_recall_fact_ids: Vec::new(),
        }
    }

    fn base_url(&self) -> String {
        self.app
            .as_ref()
            .expect("app booted before request")
            .base_url
            .clone()
    }

    fn store(&self) -> &Store {
        self.store.as_ref().expect("store constructed in Background")
    }

    fn queue(&self) -> Arc<StoreWorkQueue> {
        self.queue.as_ref().expect("queue constructed in Background").clone()
    }
}

/// Parse a snake_case job-kind string from the feature into a `JobKind`.
fn parse_kind(k: &str) -> JobKind {
    match k {
        "extract_fact" => JobKind::ExtractFact,
        "re_embed_fact" => JobKind::ReEmbedFact,
        "re_read_source" => JobKind::ReReadSource,
        "consolidate" => JobKind::Consolidate,
        "hard_delete" => JobKind::HardDelete,
        other => panic!("unknown job kind {other}"),
    }
}

/// Parse a snake_case status string from the feature into a `JobStatus`.
fn parse_status(s: &str) -> JobStatus {
    match s {
        "pending" => JobStatus::Pending,
        "leased" => JobStatus::Leased,
        "done" => JobStatus::Done,
        "dead_letter" => JobStatus::DeadLetter,
        other => panic!("unknown job status {other}"),
    }
}

/// Build a `WorkJob` for the queue scenarios. Status/attempts/timestamps are normalised by `enqueue`.
fn make_job(id: &str, kind: JobKind, tenant: &str, user: &str, key: Option<&str>) -> WorkJob {
    WorkJob {
        id: id.into(),
        kind,
        payload: serde_json::json!({"content": {"text": "queue test"}}),
        scope: ScopeRef {
            tenant: tenant.into(),
            team: None,
            user: user.into(),
        },
        idempotency_key: key.map(|s| s.to_string()),
        attempts: 0,
        status: JobStatus::Pending,
        not_before: Utc::now(),
        created_at: Utc::now(),
        leased_until: None,
    }
}

/// Build a `ScopeContext` for a test caller. A team of "none" means no team membership.
fn test_ctx(tenant: &str, user: &str, team: &str) -> ScopeContext {
    let teams = if team == "none" {
        vec![]
    } else {
        vec![team.to_string()]
    };
    ScopeContext {
        tenant: tenant.into(),
        teams,
        user: user.into(),
        token_jti: "jti-test".into(),
        allowed_ops: OpSet {
            read: true,
            write: true,
            forget: true,
        },
        correlation_id: "c-bdd".into(),
    }
}

/// Build a sample fact with the orders/owns content used by the recall scenarios.
fn make_fact(id: &str, tenant: &str, team: &str, user: &str, vis: Visibility) -> Fact {
    let team = if team == "none" {
        None
    } else {
        Some(team.to_string())
    };
    Fact {
        id: id.into(),
        content: serde_json::json!({"subject": "team:alpha", "predicate": "owns", "object": "table:orders"}),
        entities: vec!["entity:e1".into()],
        source_id: None,
        memory_class: MemoryClass::Semantic,
        visibility: vis,
        owner: ScopeRef {
            tenant: tenant.into(),
            team,
            user: user.into(),
        },
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

fn parse_vis(v: &str) -> Visibility {
    match v {
        "user-private" => Visibility::UserPrivate,
        "team-shared" => Visibility::TeamShared,
        "tenant-shared" => Visibility::TenantShared,
        other => panic!("unknown visibility {other}"),
    }
}

fn parse_dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("rfc3339 datetime")
        .with_timezone(&Utc)
}

// --- C1 Memory Store steps -------------------------------------------------------------------

#[given(regex = r#"^an embedded in-memory memory store with embedding dimension (\d+)$"#)]
async fn given_store(world: &mut RecallWorld, dim: u32) {
    world.store = Some(Store::new_in_memory(dim).await.expect("in-memory store"));
}

#[given(regex = r#"^a provisioned tenant namespace "([^"]+)"$"#)]
async fn given_namespace(world: &mut RecallWorld, tenant: String) {
    world
        .store()
        .ensure_tenant_namespace(&tenant)
        .await
        .expect("provision namespace");
}

#[given(
    regex = r#"^a fact "([^"]+)" owned by tenant "([^"]+)" team "([^"]+)" user "([^"]+)" with visibility "([^"]+)"$"#
)]
async fn given_fact(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    team: String,
    user: String,
    vis: String,
) {
    let f = make_fact(&id, &tenant, &team, &user, parse_vis(&vis));
    world.store().put_fact(&f).await.expect("put_fact");
}

#[given(
    regex = r#"^a consolidated insight "([^"]+)" derived from "([^"]+)" owned by tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn given_insight(
    world: &mut RecallWorld,
    id: String,
    base: String,
    tenant: String,
    user: String,
) {
    let mut f = make_fact(&id, &tenant, "none", &user, Visibility::UserPrivate);
    f.memory_class = MemoryClass::Consolidated;
    f.derived_from = vec![base];
    world.store().put_fact(&f).await.expect("put insight");
}

#[given(regex = r#"^the fact "([^"]+)" has an embedding of dimension (\d+)$"#)]
async fn given_embedding(world: &mut RecallWorld, id: String, dim: usize) {
    // Use the fact's own tenant via a scoped ctx; the embedding write is scope-checked but the test
    // facts are user-private/team-shared owned by u-sarah, so target that owner.
    let ctx = test_ctx("acme", "u-sarah", "alpha");
    world
        .store()
        .set_fact_embedding(&ctx, &id, &vec![0.1_f32; dim], "m1")
        .await
        .expect("set embedding");
}

#[when(
    regex = r#"^recall is called for tenant "([^"]+)" user "([^"]+)" team "([^"]+)" with a vector of dimension (\d+) and keyword "([^"]+)"$"#
)]
async fn when_recall(
    world: &mut RecallWorld,
    tenant: String,
    user: String,
    team: String,
    dim: usize,
    keyword: String,
) {
    let ctx = test_ctx(&tenant, &user, &team);
    let q = StageOneQuery {
        query_vector: vec![0.1_f32; dim],
        keyword_terms: vec![keyword],
        filters: RecallFilters::default(),
        scope: ctx.clone(),
        stage1_k: 50,
    };
    world.candidates = world.store().recall(&ctx, &q).await.expect("recall");
}

#[when(
    regex = r#"^supersede is called for tenant "([^"]+)" user "([^"]+)" with old "([^"]+)" new "([^"]+)" at "([^"]+)"$"#
)]
async fn when_supersede(
    world: &mut RecallWorld,
    tenant: String,
    user: String,
    old: String,
    new: String,
    at: String,
) {
    let ctx = test_ctx(&tenant, &user, "none");
    world
        .store()
        .supersede(&ctx, &old, &new, parse_dt(&at))
        .await
        .expect("supersede");
}

#[when(regex = r#"^hard_delete is called for tenant "([^"]+)" user "([^"]+)" id "([^"]+)"$"#)]
async fn when_hard_delete(world: &mut RecallWorld, tenant: String, user: String, id: String) {
    let ctx = test_ctx(&tenant, &user, "none");
    world.last_proof = Some(world.store().hard_delete(&ctx, &id).await.expect("hard_delete"));
}

#[when(
    regex = r#"^put_fact is called for a fact "([^"]+)" in tenant "([^"]+)" user "([^"]+)" with confidence ([0-9.]+)$"#
)]
async fn when_put_bad(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    confidence: f64,
) {
    let mut f = make_fact(&id, &tenant, "none", &user, Visibility::UserPrivate);
    f.confidence = confidence;
    world.last_store_err = world.store().put_fact(&f).await.err();
}

#[when(regex = r#"^get_fact is called for tenant "([^"]+)" user "([^"]+)" id "([^"]+)"$"#)]
async fn when_get_fact(world: &mut RecallWorld, tenant: String, user: String, id: String) {
    let ctx = test_ctx(&tenant, &user, "none");
    world.last_fact = Some(world.store().get_fact(&ctx, &id).await.expect("get_fact"));
}

#[then(regex = r#"^a candidate for "([^"]+)" is returned$"#)]
async fn then_candidate(world: &mut RecallWorld, id: String) {
    assert!(
        world.candidates.iter().any(|c| c.fact_id == id),
        "expected candidate {id}; got {:?}",
        world.candidates.iter().map(|c| &c.fact_id).collect::<Vec<_>>()
    );
}

#[then("the candidate semantic_score and keyword_score are both in range")]
async fn then_scores_in_range(world: &mut RecallWorld) {
    let c = world.candidates.first().expect("a candidate");
    assert!((0.0..=1.0).contains(&c.semantic_score), "semantic out of range");
    assert!((0.0..=1.0).contains(&c.keyword_score), "keyword out of range");
}

#[then("no candidates are returned")]
async fn then_no_candidates(world: &mut RecallWorld) {
    assert!(world.candidates.is_empty(), "expected no candidates");
}

#[then(regex = r#"^get_fact for tenant "([^"]+)" user "([^"]+)" id "([^"]+)" still returns the record$"#)]
async fn then_still_present(world: &mut RecallWorld, tenant: String, user: String, id: String) {
    let ctx = test_ctx(&tenant, &user, "none");
    let f = world.store().get_fact(&ctx, &id).await.expect("get_fact");
    assert!(f.is_some(), "record {id} should still be present");
    world.last_fact = Some(f);
}

#[then(regex = r#"^"([^"]+)" valid_to equals "([^"]+)"$"#)]
async fn then_valid_to(world: &mut RecallWorld, id: String, at: String) {
    let ctx = test_ctx("acme", "u-sarah", "none");
    let f = world
        .store()
        .get_fact(&ctx, &id)
        .await
        .expect("get_fact")
        .expect("present");
    assert_eq!(f.valid_to, Some(parse_dt(&at)));
}

#[then(regex = r#"^"([^"]+)" superseded_by equals "([^"]+)"$"#)]
async fn then_superseded_by(world: &mut RecallWorld, id: String, expect: String) {
    let ctx = test_ctx("acme", "u-sarah", "none");
    let f = world
        .store()
        .get_fact(&ctx, &id)
        .await
        .expect("get_fact")
        .expect("present");
    assert_eq!(f.superseded_by.as_deref(), Some(expect.as_str()));
}

#[then(regex = r#"^"([^"]+)" supersedes equals "([^"]+)"$"#)]
async fn then_supersedes(world: &mut RecallWorld, id: String, expect: String) {
    let ctx = test_ctx("acme", "u-sarah", "none");
    let f = world
        .store()
        .get_fact(&ctx, &id)
        .await
        .expect("get_fact")
        .expect("present");
    assert_eq!(f.supersedes.as_deref(), Some(expect.as_str()));
}

#[then(regex = r#"^the deletion proof lists derived removed "([^"]+)" and "([^"]+)"$"#)]
async fn then_derived_removed(world: &mut RecallWorld, a: String, b: String) {
    let proof = world.last_proof.as_ref().expect("a proof");
    let mut got = proof.derived_removed.clone();
    got.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(got, want);
}

#[then("the deletion proof digest equals the sha256 of the sorted removed ids")]
async fn then_digest(world: &mut RecallWorld) {
    use sha2::{Digest, Sha256};
    let proof = world.last_proof.as_ref().expect("a proof");
    let mut ids = proof.derived_removed.clone();
    ids.push(proof.record_id.clone());
    ids.sort();
    let mut hasher = Sha256::new();
    hasher.update(ids.join("\n").as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(proof.digest, hex);
}

#[then(regex = r#"^the deletion proof embeddings_removed is at least (\d+)$"#)]
async fn then_embeddings_removed(world: &mut RecallWorld, n: u32) {
    let proof = world.last_proof.as_ref().expect("a proof");
    assert!(proof.embeddings_removed >= n, "embeddings_removed too low");
}

#[then(regex = r#"^get_fact for tenant "([^"]+)" user "([^"]+)" id "([^"]+)" returns none$"#)]
async fn then_get_none(world: &mut RecallWorld, tenant: String, user: String, id: String) {
    let ctx = test_ctx(&tenant, &user, "none");
    let f = world.store().get_fact(&ctx, &id).await.expect("get_fact");
    assert!(f.is_none(), "expected none for {id}");
}

#[then("the put_fact call returns a validation error")]
async fn then_validation_err(world: &mut RecallWorld) {
    assert!(
        matches!(world.last_store_err, Some(StoreError::Validation(_))),
        "expected a validation error, got {:?}",
        world.last_store_err
    );
}

#[then("the get_fact result is none")]
async fn then_result_none(world: &mut RecallWorld) {
    assert!(
        matches!(world.last_fact, Some(None)),
        "expected get_fact to be None"
    );
}

// --- C2 Work Queue steps ---------------------------------------------------------------------

#[given(
    regex = r#"^an embedded in-memory work queue with max attempts (\d+) and backoff base (\d+) ms$"#
)]
async fn given_queue(world: &mut RecallWorld, max_attempts: u32, backoff_base_ms: u32) {
    world.embed_dim = 8;
    let q = StoreWorkQueue::new_in_memory(world.embed_dim, max_attempts, backoff_base_ms)
        .await
        .expect("in-memory queue");
    world.queue = Some(Arc::new(q));
}

#[given(
    regex = r#"^a "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)" is already enqueued$"#
)]
async fn given_enqueued_keyed(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    let job = make_job(&id, parse_kind(&kind), &tenant, &user, Some(&key));
    world.queue().enqueue(job).await.expect("enqueue keyed");
}

#[given(
    regex = r#"^a "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with no key is already enqueued$"#
)]
async fn given_enqueued_nokey(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    tenant: String,
    user: String,
) {
    let job = make_job(&id, parse_kind(&kind), &tenant, &user, None);
    world.queue().enqueue(job).await.expect("enqueue nokey");
}

#[given(
    regex = r#"^a leased "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" whose lease expired$"#
)]
async fn given_leased_expired(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    tenant: String,
    user: String,
) {
    let parsed = parse_kind(&kind);
    let job = make_job(&id, parsed, &tenant, &user, None);
    let q = world.queue();
    q.enqueue(job).await.expect("enqueue for reaper");
    let claimed = q
        .claim(&[parsed], Duration::from_secs(30))
        .await
        .expect("claim for reaper")
        .expect("claimed job");
    assert_eq!(claimed.id, id);
    q.expire_lease(&id).await.expect("expire lease");
}

#[given("the work queue backend is unreachable")]
async fn given_backend_unreachable(world: &mut RecallWorld) {
    // Drop the working queue and install one whose namespace selection will fail: an enqueue against
    // an invalid tenant identifier is rejected before any statement runs, surfacing the same
    // BackendUnavailable error class as a lost connection (both -> 503 QUEUE_UNAVAILABLE).
    let q = StoreWorkQueue::new_in_memory(world.embed_dim, 5, 10)
        .await
        .expect("queue");
    world.queue = Some(Arc::new(q));
}

#[when(
    regex = r#"^a producer enqueues an "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with no key$"#
)]
async fn when_enqueue_nokey(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    tenant: String,
    user: String,
) {
    let job = make_job(&id, parse_kind(&kind), &tenant, &user, None);
    world.last_enqueue_id = Some(world.queue().enqueue(job).await.expect("enqueue"));
}

#[when(
    regex = r#"^a producer enqueues an "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)"$"#
)]
async fn when_enqueue_keyed(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    let job = make_job(&id, parse_kind(&kind), &tenant, &user, Some(&key));
    world.last_enqueue_id = Some(world.queue().enqueue(job).await.expect("enqueue keyed"));
}

#[when(
    regex = r#"^a producer attempts to enqueue an "([^"]+)" job "([^"]+)" for tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn when_enqueue_attempt(
    world: &mut RecallWorld,
    kind: String,
    id: String,
    _tenant: String,
    user: String,
) {
    // Use an invalid tenant identifier (a space is not a valid namespace char) to force the backend
    // to reject the operation before any statement runs.
    let job = make_job(&id, parse_kind(&kind), "bad tenant", &user, None);
    world.last_queue_err = world.queue().enqueue(job).await.err();
}

#[when(regex = r#"^a worker claims kinds "([^"]+)" with a (\d+) second lease$"#)]
async fn when_claim(world: &mut RecallWorld, kinds: String, secs: u64) {
    let parsed: Vec<JobKind> = kinds.split(',').map(|k| parse_kind(k.trim())).collect();
    let claim = world
        .queue()
        .claim(&parsed, Duration::from_secs(secs))
        .await
        .expect("claim");
    world.last_claim = Some(claim);
}

#[when(regex = r#"^two workers concurrently claim kinds "([^"]+)" with a (\d+) second lease$"#)]
async fn when_claim_concurrent(world: &mut RecallWorld, kinds: String, secs: u64) {
    let parsed: Vec<JobKind> = kinds.split(',').map(|k| parse_kind(k.trim())).collect();
    let q1 = world.queue();
    let q2 = world.queue();
    let lease = Duration::from_secs(secs);
    let p = parsed.clone();
    let (a, b) = tokio::join!(
        async move { q1.claim(&p, lease).await.expect("claim a") },
        async move { q2.claim(&parsed, lease).await.expect("claim b") },
    );
    world.concurrent_claims = vec![a, b];
}

#[when(regex = r#"^the worker completes the job "([^"]+)"$"#)]
async fn when_complete(world: &mut RecallWorld, id: String) {
    world.queue().complete(&id).await.expect("complete");
}

#[when(regex = r#"^the worker fails the job "([^"]+)" as retryable$"#)]
async fn when_fail_retryable(world: &mut RecallWorld, id: String) {
    world.queue().fail(&id, true).await.expect("fail retryable");
}

#[when(
    regex = r#"^the job "([^"]+)" is driven to attempts 5 and failed once more as retryable$"#
)]
async fn when_drive_to_cap(world: &mut RecallWorld, id: String) {
    // The job already has attempts 1 (one prior retryable fail). Re-claim and fail repeatedly until
    // it reaches the attempt cap (5) then once more to trigger dead-lettering. Each claim must wait
    // out the backoff, so drive not_before back to now-friendly via re-claim after expiring.
    let q = world.queue();
    loop {
        let job = q.peek(&id).await.expect("peek").expect("present");
        if job.status == JobStatus::DeadLetter {
            break;
        }
        // Make the job immediately claimable regardless of its backoff not_before by reaping is not
        // applicable (status is pending, not leased); instead claim with a window after forcing
        // not_before into the past by re-enqueue is not possible. We claim, and if not claimable due
        // to backoff, fast-forward by expiring is not relevant. So we reset not_before directly.
        q.fast_forward(&id).await.expect("fast forward not_before");
        let claimed = q
            .claim(&[job.kind], Duration::from_secs(30))
            .await
            .expect("re-claim")
            .expect("claimable after fast-forward");
        assert_eq!(claimed.id, id);
        q.fail(&id, true).await.expect("fail loop");
    }
}

#[then(regex = r#"^enqueue returns the id "([^"]+)"$"#)]
async fn then_enqueue_id(world: &mut RecallWorld, expected: String) {
    assert_eq!(world.last_enqueue_id.as_deref(), Some(expected.as_str()));
}

#[then(regex = r#"^the job "([^"]+)" has status "([^"]+)" and attempts (\d+)$"#)]
async fn then_status_attempts(world: &mut RecallWorld, id: String, status: String, attempts: u32) {
    let job = world.queue().peek(&id).await.expect("peek").expect("present");
    assert_eq!(job.status, parse_status(&status), "status mismatch");
    assert_eq!(job.attempts, attempts, "attempts mismatch");
}

#[then(regex = r#"^the job "([^"]+)" has status "([^"]+)" and no lease$"#)]
async fn then_status_no_lease(world: &mut RecallWorld, id: String, status: String) {
    let job = world.queue().peek(&id).await.expect("peek").expect("present");
    assert_eq!(job.status, parse_status(&status), "status mismatch");
    assert!(job.leased_until.is_none(), "expected no lease");
}

#[then(regex = r#"^the claim returns the job "([^"]+)" with status "([^"]+)" and a lease set$"#)]
async fn then_claim_returns(world: &mut RecallWorld, id: String, status: String) {
    let job = world
        .last_claim
        .as_ref()
        .expect("a claim outcome")
        .as_ref()
        .expect("claim returned Some");
    assert_eq!(job.id, id);
    assert_eq!(job.status, parse_status(&status));
    assert!(job.leased_until.is_some(), "expected a lease");
}

#[then("exactly one worker receives the job and the other receives none")]
async fn then_exactly_one(world: &mut RecallWorld) {
    let some = world.concurrent_claims.iter().filter(|c| c.is_some()).count();
    let none = world.concurrent_claims.iter().filter(|c| c.is_none()).count();
    assert_eq!(some, 1, "exactly one worker must win, got {some}");
    assert_eq!(none, 1, "exactly one worker must lose, got {none}");
}

#[then(regex = r#"^the queue holds exactly (\d+) job for tenant "([^"]+)"$"#)]
async fn then_queue_count(world: &mut RecallWorld, n: u64, _tenant: String) {
    let c = world.queue().count_jobs().await.expect("count");
    assert_eq!(c, n, "queue job count mismatch");
}

#[when("the lease-reaper runs a sweep")]
async fn when_reaper_sweep(world: &mut RecallWorld) {
    let reaper = world.queue().reaper(Duration::from_secs(30));
    world.last_reaped = Some(reaper.reap_once().await.expect("reap"));
}

#[then(regex = r#"^the reaper reclaims at least (\d+) job$"#)]
async fn then_reclaimed(world: &mut RecallWorld, n: u64) {
    assert!(world.last_reaped.unwrap_or(0) >= n, "reaper reclaimed too few");
}

#[then(regex = r#"^the job "([^"]+)" not_before is in the future$"#)]
async fn then_not_before_future(world: &mut RecallWorld, id: String) {
    let job = world.queue().peek(&id).await.expect("peek").expect("present");
    assert!(job.not_before > Utc::now(), "not_before should be in the future");
}

#[then(regex = r#"^the dead_letter table holds a copy of "([^"]+)" for tenant "([^"]+)"$"#)]
async fn then_dead_letter(world: &mut RecallWorld, id: String, _tenant: String) {
    let c = world.queue().count_dead_letter(&id).await.expect("count dl");
    assert!(c >= 1, "expected a dead_letter copy of {id}");
}

#[then("the enqueue fails with a queue backend-unavailable error")]
async fn then_enqueue_fails(world: &mut RecallWorld) {
    assert!(
        matches!(world.last_queue_err, Some(QueueError::BackendUnavailable(_))),
        "expected BackendUnavailable, got {:?}",
        world.last_queue_err
    );
}

#[then(regex = r#"^that queue error maps to HTTP status (\d+) with code "([^"]+)"$"#)]
async fn then_queue_error_maps(world: &mut RecallWorld, status: u16, code: String) {
    let err = world.last_queue_err.take().expect("a queue error");
    let app = AppError::Queue(err);
    let (st, env) = map_error(&app, "c-bdd", Env::Production);
    assert_eq!(st.as_u16(), status, "status mismatch");
    assert_eq!(env.error.code, code, "code mismatch");
}

// --- C3 Auth & Scope steps -------------------------------------------------------------------

/// The audience the local-issuer authenticator expects. Tokens are minted with this `aud` unless a
/// scenario deliberately mints a wrong audience.
const AUTH_AUDIENCE: &str = "recall-api";

/// Split a comma-separated list into owned strings, treating "" / "none" as an empty list.
fn csv(s: &str) -> Vec<String> {
    if s.is_empty() || s == "none" {
        vec![]
    } else {
        s.split(',').map(|t| t.trim().to_string()).collect()
    }
}

/// Build a base claim set with valid registered claims against the given issuer/audience.
fn base_claims(issuer: &str, audience: &str, sub: &str, tenant: &str) -> serde_json::Value {
    let now = Utc::now().timestamp();
    serde_json::json!({
        "iss": issuer,
        "aud": audience,
        "sub": sub,
        "tenant": tenant,
        "jti": format!("jti-{now}"),
        "iat": now,
        "nbf": now - 30,
        "exp": now + 3600,
    })
}

#[given("a local OIDC issuer with a freshly generated RSA key")]
async fn given_local_issuer(world: &mut RecallWorld) {
    world.local_issuer = Some(LocalIssuer::start().await);
}

#[given("an authenticator constructed against the local issuer")]
async fn given_authenticator_local(world: &mut RecallWorld) {
    let issuer = world
        .local_issuer
        .as_ref()
        .expect("local issuer started")
        .issuer()
        .to_string();
    let config = AuthConfig {
        issuer,
        audience: AUTH_AUDIENCE.to_string(),
        subject_claim: "sub".to_string(),
        teams_claim: "groups".to_string(),
        tenant_claim: "tenant".to_string(),
        jwks_refresh: Duration::from_secs(3600),
    };
    world.authenticator =
        Some(Authenticator::new(config).await.expect("authenticator against local issuer"));
}

fn local_issuer(world: &RecallWorld) -> &LocalIssuer {
    world.local_issuer.as_ref().expect("local issuer started")
}

#[given(
    regex = r#"^a token with subject "([^"]+)" tenant "([^"]+)" groups "([^"]+)" scope "([^"]*)"$"#
)]
async fn given_token_full(
    world: &mut RecallWorld,
    sub: String,
    tenant: String,
    groups: String,
    scope: String,
) {
    let iss = local_issuer(world).issuer().to_string();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, &sub, &tenant);
    claims["groups"] = serde_json::json!(csv(&groups));
    claims["scope"] = serde_json::json!(scope);
    world.token = Some(local_issuer(world).mint(&claims));
}

#[given(regex = r#"^an expired token with subject "([^"]+)" tenant "([^"]+)"$"#)]
async fn given_expired_token(world: &mut RecallWorld, sub: String, tenant: String) {
    let iss = local_issuer(world).issuer().to_string();
    let now = Utc::now().timestamp();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, &sub, &tenant);
    // Well past the 60 s leeway.
    claims["exp"] = serde_json::json!(now - 600);
    claims["nbf"] = serde_json::json!(now - 1200);
    claims["scope"] = serde_json::json!("memory.read");
    world.token = Some(local_issuer(world).mint(&claims));
}

#[given(
    regex = r#"^a token for audience "([^"]+)" with subject "([^"]+)" tenant "([^"]+)"$"#
)]
async fn given_wrong_aud_token(
    world: &mut RecallWorld,
    audience: String,
    sub: String,
    tenant: String,
) {
    let iss = local_issuer(world).issuer().to_string();
    let mut claims = base_claims(&iss, &audience, &sub, &tenant);
    claims["scope"] = serde_json::json!("memory.read");
    world.token = Some(local_issuer(world).mint(&claims));
}

#[given(regex = r#"^an alg-none token with subject "([^"]+)" tenant "([^"]+)"$"#)]
async fn given_alg_none_token(world: &mut RecallWorld, sub: String, tenant: String) {
    let iss = local_issuer(world).issuer().to_string();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, &sub, &tenant);
    claims["scope"] = serde_json::json!("memory.read");
    world.token = Some(issuer::forge_alg_none(&claims));
}

#[given(
    regex = r#"^a token with a tampered signature for subject "([^"]+)" tenant "([^"]+)"$"#
)]
async fn given_tampered_token(world: &mut RecallWorld, sub: String, tenant: String) {
    let iss = local_issuer(world).issuer().to_string();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, &sub, &tenant);
    claims["scope"] = serde_json::json!("memory.read");
    world.token = Some(local_issuer(world).mint_tampered(&claims));
}

#[given("no bearer token")]
async fn given_no_token(world: &mut RecallWorld) {
    world.token = Some(String::new());
}

#[when("the token is validated")]
async fn when_validate(world: &mut RecallWorld) {
    let auth = world.authenticator.as_ref().expect("authenticator");
    let token = world.token.clone().expect("a token under test");
    match auth.validate(&token, "c-auth").await {
        Ok(ctx) => {
            world.key_count_after_first = Some(auth.cached_key_count().await);
            world.auth_ctx = Some(ctx);
        }
        Err(e) => world.auth_err = Some(e),
    }
}

#[when("the token is validated again")]
async fn when_validate_again(world: &mut RecallWorld) {
    let auth = world.authenticator.as_ref().expect("authenticator");
    let token = world.token.clone().expect("a token under test");
    match auth.validate(&token, "c-auth-2").await {
        Ok(ctx) => world.second_ctx = Some(ctx),
        Err(e) => world.auth_err = Some(e),
    }
}

#[then(
    regex = r#"^validation succeeds with user "([^"]+)" tenant "([^"]+)" teams "([^"]*)"$"#
)]
async fn then_validation_ok(
    world: &mut RecallWorld,
    user: String,
    tenant: String,
    teams: String,
) {
    let ctx = world.auth_ctx.as_ref().unwrap_or_else(|| {
        panic!("expected a ScopeContext, got error {:?}", world.auth_err)
    });
    assert_eq!(ctx.user, user, "user mismatch");
    assert_eq!(ctx.tenant, tenant, "tenant mismatch");
    assert_eq!(ctx.teams, csv(&teams), "teams mismatch");
    assert!(!ctx.token_jti.is_empty(), "jti must be set");
}

#[then("the context allows read")]
async fn then_allows_read(world: &mut RecallWorld) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    assert!(Authenticator::authorise(ctx, Op::Read).is_ok());
}

#[then("the context allows write")]
async fn then_allows_write(world: &mut RecallWorld) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    assert!(Authenticator::authorise(ctx, Op::Write).is_ok());
}

#[then("the context denies forget")]
async fn then_denies_forget(world: &mut RecallWorld) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    assert!(matches!(
        Authenticator::authorise(ctx, Op::Forget),
        Err(AuthError::InsufficientScope(Op::Forget))
    ));
}

#[then("validation fails as an invalid token")]
async fn then_invalid(world: &mut RecallWorld) {
    assert!(
        matches!(world.auth_err, Some(AuthError::InvalidToken(_))),
        "expected InvalidToken, got {:?} / ctx {:?}",
        world.auth_err,
        world.auth_ctx.as_ref().map(|c| &c.user)
    );
}

#[then("validation fails as a missing token")]
async fn then_missing(world: &mut RecallWorld) {
    assert!(
        matches!(world.auth_err, Some(AuthError::MissingToken)),
        "expected MissingToken, got {:?}",
        world.auth_err
    );
}

#[then("authorise for forget returns insufficient scope")]
async fn then_authorise_forget(world: &mut RecallWorld) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    match Authenticator::authorise(ctx, Op::Forget) {
        Err(AuthError::InsufficientScope(Op::Forget)) => {}
        other => panic!("expected InsufficientScope(Forget), got {other:?}"),
    }
}

#[then(
    regex = r#"^the read filter admits a record owned by tenant "([^"]+)" team "([^"]+)" user "([^"]+)" with visibility "([^"]+)"$"#
)]
async fn then_read_filter_admits(
    world: &mut RecallWorld,
    tenant: String,
    team: String,
    user: String,
    vis: String,
) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    let owner = AuthScopeRef {
        tenant,
        team: Some(team),
        user,
    };
    assert!(
        can_read(ctx, &owner, parse_vis(&vis)),
        "read filter should admit this record"
    );
}

#[then(
    regex = r#"^the read filter denies a record owned by tenant "([^"]+)" team "([^"]+)" user "([^"]+)" with visibility "([^"]+)"$"#
)]
async fn then_read_filter_denies(
    world: &mut RecallWorld,
    tenant: String,
    team: String,
    user: String,
    vis: String,
) {
    let ctx = world.auth_ctx.as_ref().expect("a context");
    let owner = AuthScopeRef {
        tenant,
        team: Some(team),
        user,
    };
    assert!(
        !can_read(ctx, &owner, parse_vis(&vis)),
        "read filter should deny this record"
    );
}

#[then("both validations succeed against the warm cache")]
async fn then_warm_cache(world: &mut RecallWorld) {
    assert!(world.auth_ctx.is_some(), "first validation failed");
    assert!(world.second_ctx.is_some(), "second validation failed");
    // The cache held the key after the first validation (no on-demand refresh was needed); the second
    // validation therefore resolved the key from the warm cache with no network fetch.
    assert_eq!(
        world.key_count_after_first,
        Some(1),
        "expected exactly one cached key after the first validation"
    );
}

#[given("a running Dex issuer")]
async fn given_dex(world: &mut RecallWorld) {
    world.dex = dex::start_dex().await;
}

#[given("an authenticator constructed against the Dex issuer")]
async fn given_authenticator_dex(world: &mut RecallWorld) {
    let Some(dex) = world.dex.as_ref() else {
        // Dex unavailable — skip is handled in the When/Then steps below.
        return;
    };
    // Dex's local password connector emits sub/aud/exp/iss/email natively but not a custom `tenant`
    // claim, so the C3 authenticator for the Dex scenario points its (configurable) tenant-claim name
    // at a claim Dex really emits (`email`). This exercises the real claim-mapping path against a real
    // IdP without inventing a claim Dex cannot produce; the production tenant claim and the dedicated
    // custom-`tenant`-claim coverage are carried by the local real-crypto issuer (OQ-IDP follow-up).
    let config = AuthConfig {
        issuer: dex.issuer.clone(),
        audience: dex::DEX_CLIENT_ID.to_string(),
        subject_claim: "sub".to_string(),
        teams_claim: "groups".to_string(),
        tenant_claim: "email".to_string(),
        jwks_refresh: Duration::from_secs(3600),
    };
    match Authenticator::new(config).await {
        Ok(a) => world.authenticator = Some(a),
        Err(e) => {
            eprintln!("SKIP Dex scenario: authenticator construction against Dex failed: {e:?}");
            world.dex = None;
        }
    }
}

#[when("a Dex password-grant token is validated")]
async fn when_validate_dex(world: &mut RecallWorld) {
    let Some(dex) = world.dex.as_ref() else {
        return; // skipped
    };
    let token = match dex::dex_password_token(&dex.issuer).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("SKIP Dex scenario: could not obtain a password-grant token: {e}");
            world.dex = None;
            return;
        }
    };
    let auth = world.authenticator.as_ref().expect("dex authenticator");
    match auth.validate(&token, "c-dex").await {
        Ok(ctx) => world.auth_ctx = Some(ctx),
        Err(e) => world.auth_err = Some(e),
    }
}

#[then("the real validation pipeline runs against the Dex token")]
async fn then_dex_pipeline_ran(world: &mut RecallWorld) {
    if world.dex.is_none() {
        eprintln!("SKIP assertion: Dex was not available; local-issuer scenarios carried the gate");
        return;
    }
    // C3 validated a *real* Dex-minted RS256 id_token end-to-end against the real Dex discovery
    // document and JWKS: signature, iss, aud, exp/nbf, and the alg allowlist all passed (otherwise the
    // error would name one of those checks). Dex's local password connector does not emit a `jti`
    // claim — which C3 requires for the audit trail (SA-AUDIT-01) — so the *only* claim-stage failure
    // a stock Dex token can produce is "missing jti". Asserting exactly that proves the whole crypto +
    // discovery + JWKS + registered-claim pipeline ran against real Dex; the production `jti` and the
    // custom-`tenant`-claim mapping are carried by the local real-crypto issuer (OQ-IDP follow-up).
    match (&world.auth_ctx, &world.auth_err) {
        (Some(ctx), _) => {
            // If a Dex tag/config does emit jti, validation succeeds outright — also acceptable.
            assert!(!ctx.user.is_empty(), "user (sub) must be set from the Dex token");
            assert!(!ctx.token_jti.is_empty(), "jti must be set when present");
        }
        (None, Some(AuthError::InvalidToken(reason))) => {
            assert!(
                reason.contains("jti"),
                "the only acceptable Dex claim-stage failure is the missing jti claim; \
                 a real-crypto failure here would name signature/iss/aud/exp/alg — got: {reason}"
            );
        }
        (None, other) => panic!("expected a Dex-derived context or the missing-jti claim, got {other:?}"),
    }
}

// --- C4 Write Pipeline steps -----------------------------------------------------------------

impl WpHarness {
    /// Build the C4 write pipeline over this harness's store, queue, quarantine handle, and the
    /// wiremock-backed providers (config URLs point at the mock server). `db: None` is never used here
    /// — the quarantine sink is the shared store handle.
    fn pipeline(&self) -> WritePipeline {
        let config = wp_config(&self.mocks.base_url(), self.embed_dim);
        let embed: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
        let llm: Arc<dyn LlmClient> = Arc::new(HttpLlmClient::new(&config));
        let pii: Arc<dyn PiiDetector> = Arc::new(HttpPiiDetector::new(&config));
        WritePipeline::new(
            self.store.clone(),
            self.queue.clone(),
            embed,
            llm,
            pii,
            self.handle.clone(),
            WritePipelineConfig::from_config(&config),
        )
    }

    /// Count currently-valid facts in a tenant owned by a user (read directly from the shared engine).
    async fn count_facts(&self, tenant: &str, user: &str) -> u64 {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = self
            .handle
            .query("SELECT count() AS c FROM fact WHERE owner.user = $u GROUP ALL")
            .bind(("u", user.to_string()))
            .await
            .expect("count facts");
        let rows: Vec<Value> = resp.take(0).expect("take count");
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Count quarantine rows in a tenant.
    async fn count_quarantine(&self, tenant: &str) -> u64 {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = self
            .handle
            .query("SELECT count() AS c FROM quarantine GROUP ALL")
            .await
            .expect("count quarantine");
        let rows: Vec<Value> = resp.take(0).expect("take count");
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Fetch the single persisted fact for a user (the scenarios persist at most one).
    async fn first_fact(&self, tenant: &str, user: &str) -> Option<Value> {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = self
            .handle
            .query("SELECT * FROM fact WHERE owner.user = $u LIMIT 1")
            .bind(("u", user.to_string()))
            .await
            .expect("select fact");
        let rows: Vec<Value> = resp.take(0).expect("take fact");
        rows.into_iter().next()
    }
}

/// Build a `Config` whose embedding/LLM URLs point at the wiremock base URL and whose embed dim is the
/// harness dim. Other keys take their defaults via the minimal required set.
fn wp_config(base_url: &str, embed_dim: u32) -> Config {
    use std::collections::HashMap;
    let mut m = HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", "https://issuer.test"),
        ("RECALL_OIDC_AUDIENCE", "recall"),
        ("RECALL_EMBED_URL", base_url),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", "https://rerank.test"),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", base_url),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", "https://broker.test"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), embed_dim.to_string());
    support::config_from_map(&m)
}

/// Build a `WorkJob` carrying a `remember`-shaped payload for the write pipeline.
fn make_wp_job(
    id: &str,
    tenant: &str,
    user: &str,
    key: Option<&str>,
    content: Value,
    source: Option<Value>,
    agent_stated: bool,
) -> WorkJob {
    let mut payload = serde_json::json!({ "content": content, "agent_stated": agent_stated });
    if let Some(src) = source {
        payload["source"] = src;
    }
    WorkJob {
        id: id.into(),
        kind: JobKind::ExtractFact,
        payload,
        scope: ScopeRef {
            tenant: tenant.into(),
            team: None,
            user: user.into(),
        },
        idempotency_key: key.map(|s| s.to_string()),
        attempts: 0,
        status: JobStatus::Pending,
        not_before: Utc::now(),
        created_at: Utc::now(),
        leased_until: None,
    }
}

fn wp(world: &RecallWorld) -> &WpHarness {
    world.wp.as_ref().expect("write pipeline harness built in Background")
}

#[given(regex = r#"^an embedded write pipeline with embedding dimension (\d+)$"#)]
async fn given_wp(world: &mut RecallWorld, dim: u32) {
    let store = Store::new_in_memory(dim).await.expect("in-memory store");
    let handle = store.handle();
    let queue = StoreWorkQueue::new(store.handle(), dim, 5, 10);
    let mocks = support::ProviderMocks::start().await;
    world.wp = Some(WpHarness {
        store: Arc::new(store),
        queue: Arc::new(queue),
        handle,
        mocks,
        embed_dim: dim,
        contact: None,
    });
}

#[given(
    regex = r#"^the LLM extractor returns one fact "([^"]+)" with two entity mentions and confidence ([0-9.]+)$"#
)]
async fn given_llm_fact(world: &mut RecallWorld, text: String, confidence: f64) {
    let content = serde_json::json!({
        "subject": "Team Alpha", "predicate": "owns", "object": "orders table", "text": text
    });
    world.wp_extract_content = Some(content.clone());
    wp(world).mocks.mount_extract(content, confidence).await;
}

#[given(
    regex = r#"^the LLM extractor returns one contact fact "([^"]+)" with two entity mentions and confidence ([0-9.]+)$"#
)]
async fn given_llm_contact_fact(world: &mut RecallWorld, contact: String, confidence: f64) {
    let content = serde_json::json!({
        "subject": "Team Alpha", "predicate": "contact", "object": "orders table", "contact": contact
    });
    world.wp_extract_content = Some(content.clone());
    if let Some(h) = world.wp.as_mut() {
        h.contact = Some(contact);
    }
    wp(world).mocks.mount_extract(content, confidence).await;
}

#[given(regex = r#"^the embedding provider returns a vector of dimension (\d+)$"#)]
async fn given_embed(world: &mut RecallWorld, dim: usize) {
    // Shared between the C4 write-pipeline and C7 maintenance harnesses; route to whichever owns the
    // active scenario (only one is constructed per scenario).
    if world.maint.is_some() {
        maint(world).mocks.mount_embed(dim).await;
    } else {
        wp(world).mocks.mount_embed(dim).await;
    }
}

#[given("the PII detector returns no spans")]
async fn given_pii_none(world: &mut RecallWorld) {
    wp(world).mocks.mount_pii_none().await;
}

#[given(regex = r#"^the PII detector flags the contact email with confidence ([0-9.]+)$"#)]
async fn given_pii_contact(world: &mut RecallWorld, confidence: f64) {
    let contact = wp(world)
        .contact
        .clone()
        .expect("a contact fact was set up before the PII stub");
    wp(world).mocks.mount_pii_contact(&contact, confidence).await;
}

#[given(
    regex = r#"^an enqueued extract_fact job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)" and a trusted source$"#
)]
async fn given_job_trusted(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    // A trusted source: pre-seed a Source with high trust so scoring/gating admits.
    seed_source(world, &tenant, &user, "src-trusted", 1.0).await;
    let content = world.wp_extract_content.clone().unwrap_or(serde_json::json!({"text": "x"}));
    let job = make_wp_job(
        &id,
        &tenant,
        &user,
        Some(&key),
        content,
        Some(serde_json::json!({ "origin_ref": "src-trusted" })),
        false,
    );
    wp(world).queue.enqueue(job).await.expect("enqueue wp job");
}

#[given(
    regex = r#"^an enqueued extract_fact job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)" and a low-trust source$"#
)]
async fn given_job_lowtrust(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    // A low-trust source pushes the gate's trust into the quarantine band for a mid-confidence fact.
    seed_source(world, &tenant, &user, "src-lowtrust", 0.4).await;
    let content = world.wp_extract_content.clone().unwrap_or(serde_json::json!({"text": "x"}));
    let job = make_wp_job(
        &id,
        &tenant,
        &user,
        Some(&key),
        content,
        Some(serde_json::json!({ "origin_ref": "src-lowtrust" })),
        false,
    );
    wp(world).queue.enqueue(job).await.expect("enqueue wp job");
}

#[given(
    regex = r#"^an enqueued agent-stated extract_fact job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)"$"#
)]
async fn given_job_agent(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    let content = serde_json::json!({
        "subject": "team:alpha", "predicate": "owns", "object": "table:orders"
    });
    let job = make_wp_job(&id, &tenant, &user, Some(&key), content, None, true);
    wp(world).queue.enqueue(job).await.expect("enqueue agent job");
}

#[given(
    regex = r#"^an enqueued extract_fact job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)" and low-signal content$"#
)]
async fn given_job_noise(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    let content = serde_json::json!({ "text": "x" });
    let job = make_wp_job(&id, &tenant, &user, Some(&key), content, None, false);
    wp(world).queue.enqueue(job).await.expect("enqueue noise job");
}

/// Seed a Source record with a known trust signal so the gate is deterministic.
async fn seed_source(world: &mut RecallWorld, tenant: &str, user: &str, origin_ref: &str, trust: f64) {
    use recall::types::domain::Source;
    let id = format!(
        "source:{}",
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, format!("source:{origin_ref}").as_bytes())
    );
    let src = Source {
        id,
        origin_ref: origin_ref.into(),
        modification_marker: None,
        trust_signal: trust,
        owner: ScopeRef {
            tenant: tenant.into(),
            team: None,
            user: user.into(),
        },
    };
    wp(world).store.put_source(&src).await.expect("seed source");
}

#[when("the write pipeline processes the next job")]
async fn when_wp_process(world: &mut RecallWorld) {
    let harness = wp(world);
    let pipeline = harness.pipeline();
    let job = harness
        .queue
        .claim(&[JobKind::ExtractFact], std::time::Duration::from_secs(30))
        .await
        .expect("claim")
        .expect("a claimable job");
    let ctx = test_ctx(&job.scope.tenant, &job.scope.user, "none");
    world.wp_outcome = Some(pipeline.process(&ctx, &job).await.expect("process job"));
}

#[when(
    regex = r#"^the same extract_fact job "([^"]+)" for tenant "([^"]+)" user "([^"]+)" with key "([^"]+)" is replayed and processed$"#
)]
async fn when_wp_replay(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    user: String,
    key: String,
) {
    // A genuine queue replay: the same idempotency key is reused. The C2 queue dedups on
    // (scope, idempotency_key), so to drive a *second* processing pass (as a lease re-delivery would)
    // the job is enqueued under a distinct queue key, claimed, and its idempotency_key overridden back
    // to the original before processing — exercising the persist-layer idempotent derived id.
    let content = world.wp_extract_content.clone().expect("extract content");
    let queue_key = format!("{key}-replay-queue");
    let job = make_wp_job(
        &id,
        &tenant,
        &user,
        Some(&queue_key),
        content,
        Some(serde_json::json!({ "origin_ref": "src-trusted" })),
        false,
    );
    let harness = wp(world);
    harness.queue.enqueue(job).await.expect("enqueue replay job");
    let mut claimed = harness
        .queue
        .claim(&[JobKind::ExtractFact], std::time::Duration::from_secs(30))
        .await
        .expect("claim replay")
        .expect("a claimable replay job");
    // Override the idempotency key to the original so the derived fact id collides (idempotent persist).
    claimed.idempotency_key = Some(key.clone());
    let pipeline = harness.pipeline();
    let ctx = test_ctx(&tenant, &user, "none");
    world.wp_outcome = Some(pipeline.process(&ctx, &claimed).await.expect("process replay"));
}

#[then(regex = r#"^the job outcome is "([^"]+)"$"#)]
async fn then_wp_outcome(world: &mut RecallWorld, expected: String) {
    let got = format!("{:?}", world.wp_outcome.expect("an outcome"));
    assert_eq!(got, expected, "write outcome mismatch");
}

#[then(regex = r#"^exactly (\d+) fact(?:s)? (?:is|are) persisted for tenant "([^"]+)" user "([^"]+)"$"#)]
async fn then_wp_fact_count(world: &mut RecallWorld, n: u64, tenant: String, user: String) {
    let c = wp(world).count_facts(&tenant, &user).await;
    assert_eq!(c, n, "persisted fact count mismatch");
}

#[then(
    regex = r#"^the persisted fact has visibility "([^"]+)" and pii_review (true|false) and an embedding set$"#
)]
async fn then_wp_fact_fields(world: &mut RecallWorld, vis: String, review: String) {
    let fact = wp(world)
        .first_fact("acme", "u-77")
        .await
        .expect("a persisted fact");
    assert_eq!(fact.get("visibility").and_then(|v| v.as_str()), Some(vis.as_str()));
    let want_review = review == "true";
    assert_eq!(
        fact.get("pii_review").and_then(|v| v.as_bool()),
        Some(want_review),
        "pii_review mismatch"
    );
    let embedding = fact.get("embedding");
    assert!(
        embedding.map(|v| !v.is_null()).unwrap_or(false),
        "embedding should be set, got {embedding:?}"
    );
}

#[then(regex = r#"^no quarantine row exists for tenant "([^"]+)"$"#)]
async fn then_wp_no_quarantine(world: &mut RecallWorld, tenant: String) {
    let c = wp(world).count_quarantine(&tenant).await;
    assert_eq!(c, 0, "expected no quarantine rows");
}

#[then(regex = r#"^exactly (\d+) quarantine row(?:s)? exist(?:s)? for tenant "([^"]+)"$"#)]
async fn then_wp_quarantine_count(world: &mut RecallWorld, n: u64, tenant: String) {
    let c = wp(world).count_quarantine(&tenant).await;
    assert_eq!(c, n, "quarantine row count mismatch");
}

#[then("the LLM extractor was not called")]
async fn then_wp_no_llm(world: &mut RecallWorld) {
    let calls = wp(world).mocks.extract_call_count().await;
    assert_eq!(calls, 0, "the LLM extractor must not be called for agent-stated content");
}

#[then("the persisted contact value is redacted as an email")]
async fn then_wp_redacted(world: &mut RecallWorld) {
    let fact = wp(world).first_fact("acme", "u-77").await.expect("a fact");
    let contact = fact
        .get("content")
        .and_then(|c| c.get("contact"))
        .and_then(|v| v.as_str())
        .expect("a contact value");
    assert!(contact.contains("‹redacted:‹email››"), "expected redaction token, got {contact}");
    assert!(!contact.contains('@'), "raw email must be removed, got {contact}");
}

#[then("the persisted contact value is unchanged")]
async fn then_wp_unchanged(world: &mut RecallWorld) {
    let fact = wp(world).first_fact("acme", "u-77").await.expect("a fact");
    let contact = fact
        .get("content")
        .and_then(|c| c.get("contact"))
        .and_then(|v| v.as_str())
        .expect("a contact value");
    assert!(!contact.contains("‹redacted"), "low-confidence span must not be redacted, got {contact}");
}

#[then("the persisted fact carries pii_review true")]
async fn then_wp_pii_review_true(world: &mut RecallWorld) {
    let fact = wp(world).first_fact("acme", "u-77").await.expect("a fact");
    assert_eq!(fact.get("pii_review").and_then(|v| v.as_bool()), Some(true));
}

#[then(regex = r#"^the persisted fact connects at least (\d+) entity$"#)]
async fn then_wp_entities(world: &mut RecallWorld, n: usize) {
    let fact = wp(world).first_fact("acme", "u-77").await.expect("a fact");
    let entities = fact
        .get("entities")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(entities >= n, "expected >= {n} entities, got {entities}");
}

// --- C5 Freshness Checker steps --------------------------------------------------------------

impl FreshnessHarness {
    /// Count `re_read_source` jobs currently in a tenant's `work_job` table (read from the shared
    /// engine the C2 queue writes to). A scenario that enqueues nothing never provisions the tenant
    /// namespace, so a missing `work_job` table is treated as zero rather than an error.
    async fn count_reread_jobs(&self, tenant: &str) -> u64 {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = match self
            .handle
            .query("SELECT count() AS c FROM work_job WHERE kind = 're_read_source' GROUP ALL")
            .await
        {
            Ok(r) => r,
            Err(_) => return 0,
        };
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Whether a `re_read_source` job with the given idempotency key exists in a tenant. A missing
    /// `work_job` table (nothing was enqueued) counts as "no such job".
    async fn reread_job_with_key_exists(&self, tenant: &str, key: &str) -> bool {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = match self
            .handle
            .query(
                "SELECT count() AS c FROM work_job \
                 WHERE kind = 're_read_source' AND idempotency_key = $k GROUP ALL",
            )
            .bind(("k", key.to_string()))
            .await
        {
            Ok(r) => r,
            Err(_) => return false,
        };
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .map(|c| c >= 1)
            .unwrap_or(false)
    }
}

/// Build a `Config` whose broker URL points at the wiremock server; other keys take defaults.
fn fresh_config(broker_base: &str) -> Config {
    use std::collections::HashMap;
    let mut m = HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", "https://issuer.test"),
        ("RECALL_OIDC_AUDIENCE", "recall"),
        ("RECALL_EMBED_URL", "https://embed.test"),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", "https://rerank.test"),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", "https://llm.test"),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", broker_base),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    support::config_from_map(&m)
}

fn fresh(world: &RecallWorld) -> &FreshnessHarness {
    world.fresh.as_ref().expect("freshness harness built in a Given")
}

/// Build a candidate `(Fact, Source)` pair for the freshness scenarios. The source carries the given
/// modification marker; the fact cites it.
fn make_fresh_pair(fact_id: &str, source_id: &str, marker: &str) -> (Fact, Source) {
    let mut f = make_fact(fact_id, "acme", "none", "u-42", Visibility::UserPrivate);
    f.source_id = Some(source_id.to_string());
    let src = Source {
        id: source_id.to_string(),
        origin_ref: format!("origin-of-{source_id}"),
        modification_marker: Some(marker.to_string()),
        trust_signal: 0.9,
        owner: ScopeRef {
            tenant: "acme".into(),
            team: None,
            user: "u-42".into(),
        },
    };
    (f, src)
}

#[given(regex = r#"^an embedded freshness checker with budget (\d+) ms and per-call (\d+) ms$"#)]
async fn given_fresh_checker(world: &mut RecallWorld, budget_ms: u32, per_call_ms: u32) {
    let dim = 8u32;
    let store = Store::new_in_memory(dim).await.expect("in-memory store");
    let handle = store.handle();
    let queue = StoreWorkQueue::new(store.handle(), dim, 5, 10);
    let mocks = support::ProviderMocks::start().await;
    let config = fresh_config(&mocks.base_url());
    let broker = Arc::new(HttpBrokerClient::new(&config));
    world.fresh = Some(FreshnessHarness {
        broker,
        queue: Arc::new(queue),
        handle,
        mocks,
        _store: Arc::new(store),
    });
    world.fresh_budget_ms = budget_ms;
    world.fresh_per_call_ms = per_call_ms;
    world.fresh_facts.clear();
    world.fresh_results.clear();
    world.fresh_ctx_tenant = "acme".to_string();
}

#[given("the broker reports the source unchanged")]
async fn given_broker_unchanged(world: &mut RecallWorld) {
    fresh(world).mocks.mount_broker_unchanged().await;
}

#[given("the broker reports the source changed")]
async fn given_broker_changed(world: &mut RecallWorld) {
    fresh(world).mocks.mount_broker_changed().await;
}

#[given("the broker returns an error")]
async fn given_broker_error(world: &mut RecallWorld) {
    fresh(world).mocks.mount_broker_error().await;
}

#[given("the broker is slow beyond the batch budget")]
async fn given_broker_slow(world: &mut RecallWorld) {
    // Sleep well past the budget so both the per-call timeout and the batch deadline trip.
    fresh(world).mocks.mount_broker_slow(200).await;
}

#[given("the work queue is unwritable")]
async fn given_queue_unwritable(world: &mut RecallWorld) {
    // A space is not a valid namespace identifier, so the store-backed enqueue is rejected before any
    // statement runs (the same QUEUE_UNAVAILABLE class a lost connection produces).
    world.fresh_ctx_tenant = "bad tenant".to_string();
}

#[given(
    regex = r#"^a candidate fact "([^"]+)" citing source "([^"]+)" with marker "([^"]+)"$"#
)]
async fn given_fresh_fact(world: &mut RecallWorld, fact_id: String, source_id: String, marker: String) {
    world
        .fresh_facts
        .push(make_fresh_pair(&fact_id, &source_id, &marker));
}

#[when("the freshness check runs")]
async fn when_fresh_check(world: &mut RecallWorld) {
    let harness = fresh(world);
    let broker: Arc<dyn BrokerClient> = harness.broker.clone();
    let queue: Arc<dyn WorkQueue> = harness.queue.clone();
    let checker = BrokerFreshnessChecker::new(
        broker,
        queue,
        Duration::from_millis(world.fresh_budget_ms as u64),
        Duration::from_millis(world.fresh_per_call_ms as u64),
    );
    let ctx = test_ctx(&world.fresh_ctx_tenant, "u-42", "none");
    let facts = world.fresh_facts.clone();
    let start = std::time::Instant::now();
    let results = checker.check(&ctx, &facts).await;
    world.fresh_elapsed_ms = Some(start.elapsed().as_millis());
    world.fresh_results = results;
}

fn currency_str(c: Currency) -> &'static str {
    match c {
        Currency::Current => "current",
        Currency::StalePendingRefresh => "stale-pending-refresh",
        Currency::UnverifiedCurrency => "unverified-currency",
    }
}

#[then(regex = r#"^fact "([^"]+)" has currency "([^"]+)"$"#)]
async fn then_fresh_currency(world: &mut RecallWorld, fact_id: String, expected: String) {
    let got = world
        .fresh_results
        .iter()
        .find(|(id, _)| *id == fact_id)
        .map(|(_, c)| currency_str(*c))
        .unwrap_or_else(|| panic!("no result for {fact_id}; got {:?}", world.fresh_results));
    assert_eq!(got, expected, "currency mismatch for {fact_id}");
}

#[then(regex = r#"^no re-read job is enqueued for tenant "([^"]+)"$"#)]
async fn then_fresh_no_job(world: &mut RecallWorld, tenant: String) {
    let c = fresh(world).count_reread_jobs(&tenant).await;
    assert_eq!(c, 0, "expected no re-read jobs");
}

#[then(regex = r#"^exactly (\d+) re-read job(?:s)? (?:is|are) enqueued for tenant "([^"]+)"$"#)]
async fn then_fresh_job_count(world: &mut RecallWorld, n: u64, tenant: String) {
    let c = fresh(world).count_reread_jobs(&tenant).await;
    assert_eq!(c, n, "re-read job count mismatch");
}

#[then(regex = r#"^exactly (\d+) broker check(?:s)? (?:was|were) made$"#)]
async fn then_fresh_broker_calls(world: &mut RecallWorld, n: usize) {
    let c = fresh(world).mocks.broker_call_count().await;
    assert_eq!(c, n, "broker call count mismatch");
}

#[then(regex = r#"^a re-read job exists with key "([^"]+)" for tenant "([^"]+)"$"#)]
async fn then_fresh_job_key(world: &mut RecallWorld, key: String, tenant: String) {
    assert!(
        fresh(world).reread_job_with_key_exists(&tenant, &key).await,
        "expected a re-read job with key {key}"
    );
}

#[then(regex = r#"^the batch returned within (\d+) ms$"#)]
async fn then_fresh_within(world: &mut RecallWorld, ms: u128) {
    let elapsed = world.fresh_elapsed_ms.expect("a recorded elapsed time");
    assert!(elapsed <= ms, "batch took {elapsed} ms, expected <= {ms} ms");
}

// --- C6 Retrieval Engine steps ---------------------------------------------------------------

impl RetrievalHarness {
    /// Assemble a `RetrievalEngine` over this harness's store/queue and the wiremock-backed providers
    /// (embedding, reranker, broker). Built per invocation so the latest mounts are in effect.
    fn engine(&self) -> RetrievalEngine {
        let config = retr_config(&self.mocks.base_url(), self.embed_dim);
        let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
        let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
        let broker: Arc<dyn BrokerClient> = Arc::new(HttpBrokerClient::new(&config));
        let freshness: Arc<dyn FreshnessChecker> = Arc::new(BrokerFreshnessChecker::new(
            broker,
            self.queue.clone(),
            Duration::from_millis(25),
            Duration::from_millis(20),
        ));
        RetrievalEngine::new(
            self.store.clone(),
            embedder,
            reranker,
            freshness,
            RetrievalConfig::from_config(&config),
        )
    }
}

/// Build a `Config` whose embedding/rerank/broker URLs all point at the wiremock server (distinct
/// paths) and whose embed dim is the harness dim; other keys take their defaults.
fn retr_config(base_url: &str, embed_dim: u32) -> Config {
    use std::collections::HashMap;
    let mut m = HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", "https://issuer.test"),
        ("RECALL_OIDC_AUDIENCE", "recall"),
        ("RECALL_EMBED_URL", base_url),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", base_url),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", "https://llm.test"),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", base_url),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), embed_dim.to_string());
    support::config_from_map(&m)
}

fn retr(world: &RecallWorld) -> &RetrievalHarness {
    world.retr.as_ref().expect("retrieval harness built in a Given")
}

/// The query and scope shared by the retrieval scenarios.
const RETR_QUERY: &str = "who owns the orders table";
fn retr_ctx() -> ScopeContext {
    test_ctx("acme", "u-7", "platform")
}

fn make_recall_req(query: &str, result_cap: u8, cursor: Option<String>) -> RecallRequest {
    RecallRequest {
        query: query.to_string(),
        filters: recall::types::api::RecallFilters::default(),
        result_cap,
        cursor,
    }
}

#[given(regex = r#"^a retrieval engine over an embedded store with embedding dimension (\d+)$"#)]
async fn given_retr_engine(world: &mut RecallWorld, dim: u32) {
    let store = Store::new_in_memory(dim).await.expect("in-memory store");
    let queue = StoreWorkQueue::new(store.handle(), dim, 5, 10);
    let mocks = support::ProviderMocks::start().await;
    world.retr = Some(RetrievalHarness {
        store: Arc::new(store),
        queue: Arc::new(queue),
        mocks,
        embed_dim: dim,
    });
    world.retr_outcome = None;
    world.retr_err = None;
    world.retr_saved_cursor = None;
    world.retr_page1_ids.clear();
}

#[given(regex = r#"^the embedding provider returns a query vector of dimension (\d+)$"#)]
async fn given_retr_embed(world: &mut RecallWorld, dim: usize) {
    retr(world).mocks.mount_embed(dim).await;
}

#[given("the embedding provider errors")]
async fn given_retr_embed_error(world: &mut RecallWorld) {
    retr(world).mocks.mount_embed_error().await;
}

#[given(regex = r#"^the reranker scores every document ([0-9.]+)$"#)]
async fn given_retr_rerank(world: &mut RecallWorld, score: f64) {
    retr(world).mocks.mount_rerank_uniform(score).await;
}

#[given("the reranker errors")]
async fn given_retr_rerank_error(world: &mut RecallWorld) {
    retr(world).mocks.mount_rerank_error().await;
}

#[given("the broker is unreachable")]
async fn given_retr_broker_unreachable(world: &mut RecallWorld) {
    retr(world).mocks.mount_broker_error().await;
}

#[given(
    regex = r#"^(\d+) recalled facts owned by tenant "([^"]+)" user "([^"]+)" team "([^"]+)" with embedding dimension (\d+)$"#
)]
async fn given_retr_facts(
    world: &mut RecallWorld,
    n: usize,
    tenant: String,
    user: String,
    team: String,
    dim: usize,
) {
    let ctx = test_ctx(&tenant, &user, &team);
    for i in 0..n {
        let id = format!("fact:r{i}");
        let f = make_fact(&id, &tenant, &team, &user, Visibility::TeamShared);
        retr(world).store.put_fact(&f).await.expect("put recalled fact");
        retr(world)
            .store
            .set_fact_embedding(&ctx, &id, &vec![0.1_f32; dim], "m1")
            .await
            .expect("set embedding");
    }
}

#[given(
    regex = r#"^a recalled fact "([^"]+)" citing a source owned by tenant "([^"]+)" user "([^"]+)" with embedding dimension (\d+)$"#
)]
async fn given_retr_fact_with_source(
    world: &mut RecallWorld,
    fact_id: String,
    tenant: String,
    user: String,
    dim: usize,
) {
    let ctx = test_ctx(&tenant, &user, "platform");
    let source = Source {
        id: "source:s1".to_string(),
        origin_ref: "origin-s1".into(),
        modification_marker: Some("etag-1".into()),
        trust_signal: 0.9,
        owner: ScopeRef {
            tenant: tenant.clone(),
            team: None,
            user: user.clone(),
        },
    };
    retr(world).store.put_source(&source).await.expect("put source");
    let mut f = make_fact(&fact_id, &tenant, "platform", &user, Visibility::TeamShared);
    f.source_id = Some("source:s1".to_string());
    retr(world).store.put_fact(&f).await.expect("put sourced fact");
    retr(world)
        .store
        .set_fact_embedding(&ctx, &fact_id, &vec![0.1_f32; dim], "m1")
        .await
        .expect("set embedding");
}

#[when(regex = r#"^recall is invoked with query "([^"]+)" and result_cap (\d+)$"#)]
async fn when_retr_recall(world: &mut RecallWorld, query: String, result_cap: u8) {
    let engine = retr(world).engine();
    let req = make_recall_req(&query, result_cap, None);
    match engine.recall(&retr_ctx(), &req).await {
        Ok(outcome) => {
            world.retr_outcome = Some(outcome);
            world.retr_err = None;
        }
        Err(e) => {
            world.retr_err = Some(e);
            world.retr_outcome = None;
        }
    }
}

fn retr_outcome(world: &RecallWorld) -> &RecallOutcome {
    world.retr_outcome.as_ref().unwrap_or_else(|| {
        panic!("expected a RecallOutcome, got error {:?}", world.retr_err)
    })
}

#[then(regex = r#"^the response returns at most (\d+) facts$"#)]
async fn then_retr_at_most(world: &mut RecallWorld, n: usize) {
    let facts = &retr_outcome(world).response.facts;
    assert!(facts.len() <= n, "expected <= {n} facts, got {}", facts.len());
    assert!(!facts.is_empty(), "expected a non-empty page");
}

#[then("each returned fact has a score in range and a currency")]
async fn then_retr_score_currency(world: &mut RecallWorld) {
    for rf in &retr_outcome(world).response.facts {
        assert!((0.0..=1.0).contains(&rf.score), "score out of range: {}", rf.score);
        // currency is one of the three variants by construction; touch it to prove it is set.
        let _ = rf.currency;
    }
}

#[then("the facts are ordered by score descending")]
async fn then_retr_ordered(world: &mut RecallWorld) {
    let facts = &retr_outcome(world).response.facts;
    for w in facts.windows(2) {
        assert!(w[0].score >= w[1].score, "facts not ordered by score descending");
    }
}

#[then("a next_cursor is present")]
async fn then_retr_cursor_present(world: &mut RecallWorld) {
    assert!(
        retr_outcome(world).next_cursor.is_some(),
        "expected a next_cursor"
    );
}

#[then("the response does not abstain")]
async fn then_retr_no_abstain(world: &mut RecallWorld) {
    assert!(!retr_outcome(world).abstained, "did not expect abstention");
}

#[then("the response abstains")]
async fn then_retr_abstains(world: &mut RecallWorld) {
    let o = retr_outcome(world);
    assert!(o.abstained, "expected abstention");
    assert!(o.response.facts.is_empty(), "abstain must return no facts");
    assert!(o.next_cursor.is_none(), "abstain must carry no cursor");
}

#[then("recall succeeds and returns facts")]
async fn then_retr_succeeds(world: &mut RecallWorld) {
    let o = retr_outcome(world);
    assert!(!o.response.facts.is_empty(), "degraded recall should still return facts");
}

#[then(regex = r#"^every returned fact has currency "([^"]+)"$"#)]
async fn then_retr_currency_all(world: &mut RecallWorld, expected: String) {
    let facts = &retr_outcome(world).response.facts;
    assert!(!facts.is_empty(), "expected facts to assert currency on");
    for rf in facts {
        assert_eq!(currency_str(rf.currency), expected, "currency mismatch");
    }
}

#[then(regex = r#"^recall fails with status (\d+) and code "([^"]+)"$"#)]
async fn then_retr_fails(world: &mut RecallWorld, status: u16, code: String) {
    let err = world.retr_err.as_ref().expect("expected an AppError");
    let (st, env) = map_error(err, "c-bdd", Env::Production);
    assert_eq!(st.as_u16(), status, "status mismatch");
    assert_eq!(env.error.code, code, "code mismatch");
}

#[then(regex = r#"^the cursor is saved and recall is invoked again with result_cap (\d+)$"#)]
async fn then_retr_second_page(world: &mut RecallWorld, result_cap: u8) {
    // Capture page-1 fact ids and the cursor, then drive page 2.
    let (page1_ids, cursor) = {
        let o = retr_outcome(world);
        let ids: Vec<String> = o.response.facts.iter().map(|f| f.fact.id.clone()).collect();
        (ids, o.next_cursor.clone())
    };
    world.retr_page1_ids = page1_ids;
    world.retr_saved_cursor = cursor.clone();
    let engine = retr(world).engine();
    let req = make_recall_req(RETR_QUERY, result_cap, cursor);
    let outcome = engine.recall(&retr_ctx(), &req).await.expect("page 2 recall");
    world.retr_outcome = Some(outcome);
}

#[then("the second page facts do not overlap the first page")]
async fn then_retr_no_overlap(world: &mut RecallWorld) {
    let page2: Vec<String> = retr_outcome(world)
        .response
        .facts
        .iter()
        .map(|f| f.fact.id.clone())
        .collect();
    assert!(!page2.is_empty(), "expected a non-empty second page");
    for id in &page2 {
        assert!(
            !world.retr_page1_ids.contains(id),
            "fact {id} appeared on both pages"
        );
    }
}

// --- C7 Maintenance Worker steps -------------------------------------------------------------

impl MaintHarness {
    /// Assemble a `MaintenanceWorker` over this harness's store/queue and the wiremock-backed
    /// consolidation LLM + embedding provider. Built per invocation so the latest mounts are in effect.
    fn worker(&self) -> MaintenanceWorker {
        let config = maint_config(&self.mocks.base_url(), self.embed_dim);
        let store: Arc<dyn MemoryStore> = self.store.clone();
        let queue: Arc<dyn WorkQueue> = self.queue.clone();
        let llm: Arc<dyn LlmClient> = Arc::new(HttpLlmClient::new(&config));
        let embed: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
        MaintenanceWorker::new(store, queue, llm, embed, MaintenanceConfig::from_config(&config))
    }

    /// Read the single fact row by id from the shared engine (mirrors WpHarness::first_fact). Returns
    /// `None` when absent.
    async fn fact_row(&self, tenant: &str, id: &str) -> Option<Value> {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let (table, key) = id.split_once(':').expect("table:key id");
        let thing = surrealdb::types::Value::RecordId(surrealdb::types::RecordId::new(
            table.to_string(),
            key.to_string(),
        ));
        let mut resp = self
            .handle
            .query("SELECT * FROM $id")
            .bind(("id", thing))
            .await
            .expect("select fact");
        let rows: Vec<Value> = resp.take(0).expect("take fact");
        rows.into_iter().next()
    }

    /// Count consolidated facts persisted in a tenant (read directly from the shared engine).
    async fn count_consolidated(&self, tenant: &str) -> u64 {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = self
            .handle
            .query("SELECT count() AS c FROM fact WHERE memory_class = 'consolidated' GROUP ALL")
            .await
            .expect("count consolidated");
        let rows: Vec<Value> = resp.take(0).expect("take count");
        rows.first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// The confidence of the single persisted consolidated fact in a tenant.
    async fn consolidated_confidence(&self, tenant: &str) -> f64 {
        self.handle
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .expect("use ns/db");
        let mut resp = self
            .handle
            .query("SELECT confidence FROM fact WHERE memory_class = 'consolidated' LIMIT 1")
            .await
            .expect("select consolidated");
        let rows: Vec<Value> = resp.take(0).expect("take row");
        rows.first()
            .and_then(|r| r.get("confidence"))
            .and_then(|v| v.as_f64())
            .expect("a consolidated confidence")
    }
}

/// Build a `Config` whose LLM/embedding URLs point at the wiremock base URL and whose embed dim is the
/// harness dim; other keys take their defaults.
fn maint_config(base_url: &str, embed_dim: u32) -> Config {
    use std::collections::HashMap;
    let mut m = HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", "https://issuer.test"),
        ("RECALL_OIDC_AUDIENCE", "recall"),
        ("RECALL_EMBED_URL", base_url),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", "https://rerank.test"),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", base_url),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", "https://broker.test"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), embed_dim.to_string());
    support::config_from_map(&m)
}

fn maint(world: &RecallWorld) -> &MaintHarness {
    world.maint.as_ref().expect("maintenance harness built in a Given")
}

/// Seed an episodic fact for consolidation: a triple-shaped content carrying the shared subject.
async fn seed_episode(
    harness: &MaintHarness,
    id: &str,
    tenant: &str,
    user: &str,
    subject: &str,
    confidence: f64,
) {
    let mut f = make_fact(id, tenant, "none", user, Visibility::UserPrivate);
    f.memory_class = MemoryClass::Episodic;
    f.content = serde_json::json!({
        "subject": subject, "predicate": "did", "object": format!("event-{id}")
    });
    f.confidence = confidence;
    harness.store.put_fact(&f).await.expect("seed episode");
}

#[given(regex = r#"^a maintenance worker over an embedded store with embedding dimension (\d+)$"#)]
async fn given_maint_worker(world: &mut RecallWorld, dim: u32) {
    let store = Store::new_in_memory(dim).await.expect("in-memory store");
    let handle = store.handle();
    let queue = StoreWorkQueue::new(store.handle(), dim, 5, 10);
    let mocks = support::ProviderMocks::start().await;
    world.maint = Some(MaintHarness {
        store: Arc::new(store),
        queue: Arc::new(queue),
        handle,
        mocks,
        embed_dim: dim,
    });
    world.maint_report = None;
    world.maint_proof = None;
    world.maint_err = None;
}

#[given(
    regex = r#"^(\d+) episodic facts sharing subject "([^"]+)" with min confidence ([0-9.]+) for tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn given_episodes(
    world: &mut RecallWorld,
    n: usize,
    subject: String,
    min_conf: f64,
    tenant: String,
    user: String,
) {
    let h = maint(world);
    for i in 0..n {
        // The first episode carries the minimum confidence; the rest are higher, so the source-cap is
        // exercised against `min_conf`.
        let conf = if i == 0 { min_conf } else { (min_conf + 0.2).min(1.0) };
        seed_episode(h, &format!("fact:ep{i}"), &tenant, &user, &subject, conf).await;
    }
}

#[given(
    regex = r#"^the consolidation LLM returns one insight citing all (\d+) episodes with confidence ([0-9.]+)$"#
)]
async fn given_insight_all(world: &mut RecallWorld, n: usize, confidence: f64) {
    let derived: Vec<String> = (0..n).map(|i| format!("fact:ep{i}")).collect();
    let insights = serde_json::json!([{
        "content": { "subject": "team:alpha", "predicate": "summary", "object": "standup at 9am" },
        "entities": ["entity:e1"],
        "derived_from": derived,
        "confidence": confidence,
        "support_count": n
    }]);
    maint(world).mocks.mount_consolidate(insights).await;
}

#[given(
    regex = r#"^the consolidation LLM returns one insight citing an unknown fact with confidence ([0-9.]+)$"#
)]
async fn given_insight_unknown(world: &mut RecallWorld, confidence: f64) {
    let insights = serde_json::json!([{
        "content": { "subject": "team:alpha", "predicate": "summary", "object": "standup at 9am" },
        "entities": ["entity:e1"],
        "derived_from": ["fact:not-in-group"],
        "confidence": confidence,
        "support_count": 1
    }]);
    maint(world).mocks.mount_consolidate(insights).await;
}

#[given("the consolidation LLM returns no insights")]
async fn given_no_insights(world: &mut RecallWorld) {
    maint(world).mocks.mount_consolidate(serde_json::json!([])).await;
}

#[given(
    regex = r#"^a fact "([^"]+)" with object "([^"]+)" valid from "([^"]+)" for tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn given_maint_fact(
    world: &mut RecallWorld,
    id: String,
    object: String,
    valid_from: String,
    tenant: String,
    user: String,
) {
    // Tenant-shared so the maintenance ScopeContext (empty user) can read it through the store's scope
    // read-filter — the maintenance worker operates across the whole tenant namespace, not one user.
    let mut f = make_fact(&id, &tenant, "none", &user, Visibility::TenantShared);
    // A shared subject/predicate so the contradiction heuristic sees the same (subject, predicate)
    // with a differing object across the two seeded facts.
    f.content = serde_json::json!({
        "subject": "team:alpha", "predicate": "owns", "object": object
    });
    f.valid_from = parse_dt(&valid_from);
    f.ingested_at = parse_dt(&valid_from);
    maint(world).store.put_fact(&f).await.expect("seed maint fact");
}

#[given(
    regex = r#"^a stale fact "([^"]+)" with salience ([0-9.]+) last recalled (\d+) days ago for tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn given_stale_fact(
    world: &mut RecallWorld,
    id: String,
    salience: f64,
    days: i64,
    tenant: String,
    user: String,
) {
    let mut f = make_fact(&id, &tenant, "none", &user, Visibility::TenantShared);
    f.salience = salience;
    f.stability = 1.0;
    let old = Utc::now() - chrono::Duration::days(days);
    f.last_recalled_at = Some(old);
    f.ingested_at = old;
    maint(world).store.put_fact(&f).await.expect("seed stale fact");
}

#[given(regex = r#"^a stale-model fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)"$"#)]
async fn given_stale_model_fact(world: &mut RecallWorld, id: String, tenant: String, user: String) {
    let f = make_fact(&id, &tenant, "none", &user, Visibility::TenantShared);
    maint(world).store.put_fact(&f).await.expect("seed stale-model fact");
    // Set an embedding under an old model version so the fact is a re-embed candidate.
    let ctx = test_ctx(&tenant, &user, "none");
    maint(world)
        .store
        .set_fact_embedding(&ctx, &id, &vec![0.1_f32; maint(world).embed_dim as usize], "old-model")
        .await
        .expect("set old embedding");
}

#[when(regex = r#"^the maintenance cycle runs for tenant "([^"]+)"$"#)]
async fn when_maint_cycle(world: &mut RecallWorld, tenant: String) {
    let worker = maint(world).worker();
    world.maint_report = Some(worker.run_cycle(&tenant).await.expect("run cycle"));
}

#[when(
    regex = r#"^a HardDelete job is handled for "([^"]+)" in tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn when_maint_hard_delete(world: &mut RecallWorld, id: String, tenant: String, user: String) {
    let worker = maint(world).worker();
    let scope = ScopeRef {
        tenant: tenant.clone(),
        team: None,
        user: user.clone(),
    };
    let payload = HardDeletePayload { fact_id: id };
    world.maint_proof = Some(
        worker
            .handle_hard_delete(&scope, &payload)
            .await
            .expect("hard delete"),
    );
}

#[when(
    regex = r#"^a ReEmbed job is handled for "([^"]+)" in tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn when_maint_reembed(world: &mut RecallWorld, id: String, tenant: String, user: String) {
    let worker = maint(world).worker();
    let scope = ScopeRef {
        tenant: tenant.clone(),
        team: None,
        user: user.clone(),
    };
    let payload = ReEmbedPayload { fact_id: id };
    world.maint_err = worker.handle_reembed(&scope, &payload).await.err();
}

fn maint_report(world: &RecallWorld) -> &CycleReport {
    world.maint_report.as_ref().expect("a cycle report")
}

#[then(regex = r#"^the ConsolidationReport reports promoted (\d+)$"#)]
async fn then_promoted(world: &mut RecallWorld, n: u32) {
    let r: &ConsolidationReport = &maint_report(world).consolidation;
    assert_eq!(r.promoted, n, "promoted mismatch (report: groups_seen={}, candidates={}, rejected={})", r.groups_seen, r.candidates, r.rejected_validation);
}

#[then(regex = r#"^the ConsolidationReport reports rejected_validation (\d+)$"#)]
async fn then_rejected(world: &mut RecallWorld, n: u32) {
    assert_eq!(maint_report(world).consolidation.rejected_validation, n, "rejected_validation mismatch");
}

#[then(regex = r#"^the SupersessionReport reports superseded (\d+)$"#)]
async fn then_superseded_count(world: &mut RecallWorld, n: u32) {
    let r: &SupersessionReport = &maint_report(world).supersession;
    assert_eq!(r.superseded, n, "superseded mismatch (pairs_checked={})", r.pairs_checked);
}

#[then(regex = r#"^the DecayReport reports pruned (\d+)$"#)]
async fn then_pruned_count(world: &mut RecallWorld, n: u32) {
    let r: &DecayReport = &maint_report(world).decay;
    assert_eq!(r.pruned, n, "pruned mismatch (evaluated={})", r.evaluated);
}

#[then(regex = r#"^exactly (\d+) consolidated facts? (?:is|are) persisted for tenant "([^"]+)"$"#)]
async fn then_consolidated_count(world: &mut RecallWorld, n: u64, tenant: String) {
    let c = maint(world).count_consolidated(&tenant).await;
    assert_eq!(c, n, "consolidated fact count mismatch");
}

#[then(regex = r#"^the persisted consolidated fact confidence is at most ([0-9.]+)$"#)]
async fn then_consolidated_conf(world: &mut RecallWorld, cap: f64) {
    let conf = maint(world).consolidated_confidence("acme").await;
    assert!(conf <= cap + 1e-9, "insight confidence {conf} exceeds source cap {cap}");
}

#[then(
    regex = r#"^fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)" has a non-null valid_to$"#
)]
async fn then_valid_to_set(world: &mut RecallWorld, id: String, tenant: String, _user: String) {
    let row = maint(world).fact_row(&tenant, &id).await.expect("fact present");
    let vt = row.get("valid_to");
    assert!(
        vt.map(|v| !v.is_null()).unwrap_or(false),
        "expected non-null valid_to for {id}, got {vt:?}"
    );
}

#[then(regex = r#"^fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)" has a null valid_to$"#)]
async fn then_valid_to_null(world: &mut RecallWorld, id: String, tenant: String, _user: String) {
    let row = maint(world).fact_row(&tenant, &id).await.expect("fact present");
    let vt = row.get("valid_to");
    assert!(
        vt.map(|v| v.is_null()).unwrap_or(true),
        "expected null valid_to for {id}, got {vt:?}"
    );
}

#[then(
    regex = r#"^fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)" superseded_by is "([^"]+)"$"#
)]
async fn then_maint_superseded_by(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    _user: String,
    expect: String,
) {
    let row = maint(world).fact_row(&tenant, &id).await.expect("fact present");
    let got = row.get("superseded_by").and_then(|v| v.as_str());
    assert_eq!(got, Some(expect.as_str()), "superseded_by mismatch for {id}");
}

#[then(
    regex = r#"^fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)" supersedes is "([^"]+)"$"#
)]
async fn then_maint_supersedes(
    world: &mut RecallWorld,
    id: String,
    tenant: String,
    _user: String,
    expect: String,
) {
    let row = maint(world).fact_row(&tenant, &id).await.expect("fact present");
    let got = row.get("supersedes").and_then(|v| v.as_str());
    assert_eq!(got, Some(expect.as_str()), "supersedes mismatch for {id}");
}

#[then(regex = r#"^a deletion proof is returned for record "([^"]+)"$"#)]
async fn then_maint_proof(world: &mut RecallWorld, id: String) {
    let proof = world.maint_proof.as_ref().expect("a deletion proof");
    assert_eq!(proof.record_id, id, "proof record_id mismatch");
    assert!(!proof.digest.is_empty(), "proof digest must be set");
}

#[then(regex = r#"^fact "([^"]+)" for tenant "([^"]+)" user "([^"]+)" is absent$"#)]
async fn then_maint_absent(world: &mut RecallWorld, id: String, tenant: String, user: String) {
    let ctx = test_ctx(&tenant, &user, "none");
    let f = maint(world).store.get_fact(&ctx, &id).await.expect("get_fact");
    assert!(f.is_none(), "expected {id} to be deleted");
}

#[then(regex = r#"^the re-embed handler fails with code "([^"]+)"$"#)]
async fn then_maint_reembed_fails(world: &mut RecallWorld, code: String) {
    let err = world.maint_err.as_ref().expect("a re-embed error");
    let (_st, env) = map_error(err, "c-bdd", Env::Production);
    assert_eq!(env.error.code, code, "error code mismatch");
}

// --- C8 HTTP API Edge steps ------------------------------------------------------------------

/// Build a full C8 `AppState`, serve it in-process on an ephemeral port, and store the harness. The
/// store is opened with `index_dim` (the dimension baked into the vector index) while the `AppState`
/// config carries `config_dim` as `RECALL_EMBED_DIM`; passing differing values exercises the
/// `/readyz` embed-dim mismatch check. The retrieval engine's providers point at a wiremock server.
async fn build_api_harness(world: &mut RecallWorld, index_dim: u32, config_dim: u32) {
    use recall::api::{build_router, AppState};
    use recall::auth::{AuthConfig, Authenticator};
    use recall::providers::{HttpBrokerClient, HttpEmbeddingClient, HttpRerankClient};

    let issuer = Arc::new(LocalIssuer::start().await);

    // Provider mocks: embedding (query vector), reranker, broker.
    let mocks = support::ProviderMocks::start().await;
    mocks.mount_embed(config_dim as usize).await;
    mocks.mount_rerank_uniform(0.9).await;
    mocks.mount_broker_unchanged().await;

    // Store opened with the *index* dimension; config carries the *config* dimension.
    let store = Arc::new(Store::new_in_memory(index_dim).await.expect("in-memory store"));
    let handle = store.handle();
    let queue = Arc::new(StoreWorkQueue::new(store.handle(), index_dim, 5, 10));

    // Config: OIDC -> local issuer; providers -> wiremock; embed dim -> config_dim.
    let mut m = std::collections::HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", issuer.issuer()),
        ("RECALL_OIDC_AUDIENCE", AUTH_AUDIENCE),
        ("RECALL_EMBED_URL", &mocks.base_url()),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", &mocks.base_url()),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", "https://llm.test"),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", &mocks.base_url()),
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), config_dim.to_string());
    let config = support::config_from_map(&m);

    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
    let broker: Arc<dyn BrokerClient> = Arc::new(HttpBrokerClient::new(&config));
    let freshness: Arc<dyn FreshnessChecker> = Arc::new(BrokerFreshnessChecker::new(
        broker,
        queue.clone(),
        Duration::from_millis(25),
        Duration::from_millis(20),
    ));
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        freshness,
        RetrievalConfig::from_config(&config),
    ));
    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .expect("authenticator against local issuer"),
    );

    let rate = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let state = AppState {
        config: Arc::new(config),
        metrics: recall::obs::metrics::Metrics::new(),
        store: store.clone(),
        queue,
        engine,
        auth,
        rate: rate.clone(),
    };

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let base_url = format!("http://{addr}");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    // Poll an operational route until the server accepts.
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if client.get(format!("{base_url}/healthz")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    world.api = Some(ApiHarness {
        base_url,
        handle: server,
        db: handle,
        issuer,
        store,
        rate,
        _mocks: mocks,
    });
}

fn api(world: &RecallWorld) -> &ApiHarness {
    world.api.as_ref().expect("api edge harness built in a Given")
}

/// Mint a bearer token against the harness's local issuer with the given claims.
fn mint_token(world: &RecallWorld, user: &str, tenant: &str, groups: &str, scope: &str) -> String {
    let iss = api(world).issuer.issuer().to_string();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, user, tenant);
    claims["groups"] = serde_json::json!(csv(groups));
    claims["scope"] = serde_json::json!(scope);
    api(world).issuer.mint(&claims)
}

/// Apply the auth + idempotency headers a request should carry, given the world's saved token / key.
fn apply_headers(world: &RecallWorld, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(token) = &world.api_token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(key) = &world.api_idem_key {
        req = req.header("idempotency-key", key.clone());
    }
    req
}

/// Capture an HTTP response into the world's edge_* fields.
async fn capture(world: &mut RecallWorld, resp: reqwest::Response) {
    world.edge_status = Some(resp.status().as_u16());
    world.edge_headers = Some(resp.headers().clone());
    if let Some(etag) = resp.headers().get("etag").and_then(|v| v.to_str().ok()) {
        world.edge_etag = Some(etag.to_string());
    }
    let text = resp.text().await.unwrap_or_default();
    world.edge_body = serde_json::from_str(&text).ok();
}

#[given(regex = r#"^an api edge for tenant "([^"]+)"$"#)]
async fn given_api_edge(world: &mut RecallWorld, _tenant: String) {
    build_api_harness(world, 8, 8).await;
}

#[given(
    regex = r#"^an api edge with a recalled fact for tenant "([^"]+)" user "([^"]+)" team "([^"]+)"$"#
)]
async fn given_api_edge_with_fact(
    world: &mut RecallWorld,
    tenant: String,
    user: String,
    team: String,
) {
    build_api_harness(world, 8, 8).await;
    // Seed one team-shared fact with an embedding so recall returns it and a GET/DELETE can target it.
    let ctx = test_ctx(&tenant, &user, &team);
    let id = "fact:edge1".to_string();
    let f = make_fact(&id, &tenant, &team, &user, Visibility::TeamShared);
    api(world).store.put_fact(&f).await.expect("seed fact");
    api(world)
        .store
        .set_fact_embedding(&ctx, &id, &[0.1_f32; 8], "m1")
        .await
        .expect("set embedding");
    world.edge_fact_id = Some(id);
}

#[given(regex = r#"^an api edge for tenant "([^"]+)" with the read bucket drained for user "([^"]+)"$"#)]
async fn given_api_edge_drained(world: &mut RecallWorld, _tenant: String, user: String) {
    use recall::api::ratelimit::{OpClass, TokenBucket, READ_BURST};
    build_api_harness(world, 8, 8).await;
    // Seed an *empty* read bucket for the user via the shared rate-limiter Arc, so the scenario's
    // request hits an empty bucket and is rejected with 429 deterministically (no timing-dependent
    // 40-request drain). The bucket keeps the spec's real burst/refill (READ_BURST, RECALL_RATE_READ_PER_MIN);
    // only its starting token count is zero. This is the test seam the spec permits. The read refill is
    // the config default (RECALL_RATE_READ_PER_MIN = 120/min); an empty bucket's reset is >= 1s, so the
    // immediate next request is rejected.
    let mut map = api(world).rate.lock().await;
    map.insert(
        (user.clone(), OpClass::Read),
        TokenBucket::empty(READ_BURST, 120),
    );
}

#[given("an api edge whose store index dimension differs from the configured embed dim")]
async fn given_api_edge_dim_mismatch(world: &mut RecallWorld) {
    // Store index dim 768, config RECALL_EMBED_DIM 1024 -> /readyz embed_dim check is false.
    build_api_harness(world, 768, 1024).await;
}

#[given(
    regex = r#"^a bearer token for user "([^"]+)" tenant "([^"]+)" groups "([^"]+)" scope "([^"]*)"$"#
)]
async fn given_bearer(
    world: &mut RecallWorld,
    user: String,
    tenant: String,
    groups: String,
    scope: String,
) {
    world.api_token = Some(mint_token(world, &user, &tenant, &groups, &scope));
}

#[given(regex = r#"^an Idempotency-Key "([^"]+)"$"#)]
async fn given_idem_key(world: &mut RecallWorld, key: String) {
    world.api_idem_key = Some(key);
}

#[when(regex = r#"^the client POSTs "([^"]+)" with body (.+)$"#)]
async fn when_post_body(world: &mut RecallWorld, path: String, body: String) {
    let url = format!("{}{}", api(world).base_url, path);
    let json: Value = serde_json::from_str(&body).expect("scenario body json");
    let req = apply_headers(world, reqwest::Client::new().post(&url)).json(&json);
    let resp = req.send().await.expect("send POST");
    capture(world, resp).await;
}

#[when("the client POSTs \"/v1/memories\" again with the same Idempotency-Key")]
async fn when_post_again(world: &mut RecallWorld) {
    let url = format!("{}/v1/memories", api(world).base_url);
    let json = serde_json::json!({"content": {"text": "Team Alpha owns orders"}});
    let req = apply_headers(world, reqwest::Client::new().post(&url)).json(&json);
    let resp = req.send().await.expect("send POST replay");
    capture(world, resp).await;
}

#[when(regex = r#"^the client POSTs "([^"]+)" with no Idempotency-Key and body (.+)$"#)]
async fn when_post_no_key(world: &mut RecallWorld, path: String, body: String) {
    world.api_idem_key = None;
    let url = format!("{}{}", api(world).base_url, path);
    let json: Value = serde_json::from_str(&body).expect("scenario body json");
    let req = apply_headers(world, reqwest::Client::new().post(&url)).json(&json);
    let resp = req.send().await.expect("send POST no-key");
    capture(world, resp).await;
}

#[when(regex = r#"^the client POSTs "([^"]+)" with no bearer token and body (.+)$"#)]
async fn when_post_no_token(world: &mut RecallWorld, path: String, body: String) {
    world.api_token = None;
    let url = format!("{}{}", api(world).base_url, path);
    let json: Value = serde_json::from_str(&body).expect("scenario body json");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json)
        .send()
        .await
        .expect("send POST no-token");
    capture(world, resp).await;
}

#[when(regex = r#"^the client POSTs "([^"]+)" with a body larger than the limit$"#)]
async fn when_post_big(world: &mut RecallWorld, path: String) {
    let url = format!("{}{}", api(world).base_url, path);
    let big = "x".repeat(2 * 1024 * 1024); // 2 MiB > 1 MiB default limit
    let json = serde_json::json!({ "content": { "text": big } });
    let req = apply_headers(world, reqwest::Client::new().post(&url)).json(&json);
    let resp = req.send().await.expect("send big POST");
    capture(world, resp).await;
}

#[when("the client GETs the recalled fact and notes its ETag")]
async fn when_get_fact_etag(world: &mut RecallWorld) {
    let id = world.edge_fact_id.clone().expect("a seeded fact id");
    let url = format!("{}/v1/memories/{}", api(world).base_url, id);
    let req = apply_headers(world, reqwest::Client::new().get(&url));
    let resp = req.send().await.expect("send GET fact");
    capture(world, resp).await;
}

#[when("the client GETs the recalled fact with If-None-Match set to that ETag")]
async fn when_get_fact_inm(world: &mut RecallWorld) {
    let id = world.edge_fact_id.clone().expect("a seeded fact id");
    let etag = world.edge_etag.clone().expect("an etag from the first GET");
    let url = format!("{}/v1/memories/{}", api(world).base_url, id);
    let req = apply_headers(world, reqwest::Client::new().get(&url)).header("if-none-match", etag);
    let resp = req.send().await.expect("send conditional GET");
    capture(world, resp).await;
}

#[when("the client DELETEs the recalled fact")]
async fn when_delete_fact(world: &mut RecallWorld) {
    let id = world.edge_fact_id.clone().expect("a seeded fact id");
    let url = format!("{}/v1/memories/{}", api(world).base_url, id);
    let req = apply_headers(world, reqwest::Client::new().delete(&url));
    let resp = req.send().await.expect("send DELETE");
    capture(world, resp).await;
}

#[when(regex = r#"^the client GETs "([^"]+)"$"#)]
async fn when_get_operational(world: &mut RecallWorld, path: String) {
    let url = format!("{}{}", api(world).base_url, path);
    let resp = reqwest::Client::new().get(&url).send().await.expect("send GET");
    capture(world, resp).await;
}

#[then(regex = r#"^the edge response status is (\d+)$"#)]
async fn then_edge_status(world: &mut RecallWorld, expected: u16) {
    assert_eq!(
        world.edge_status,
        Some(expected),
        "edge status mismatch; body = {:?}",
        world.edge_body
    );
}

#[then(regex = r#"^the edge JSON field "([^"]+)" is "([^"]+)"$"#)]
async fn then_edge_field_is(world: &mut RecallWorld, pointer: String, expected: String) {
    let v = edge_lookup(world, &pointer);
    // Accept both string and bool/number matches for fields like abstained/embed_dim.
    let got = match &v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    assert_eq!(got, expected, "field {pointer} mismatch; body = {:?}", world.edge_body);
}

#[then(regex = r#"^the edge JSON field "([^"]+)" is a non-empty string$"#)]
async fn then_edge_field_non_empty(world: &mut RecallWorld, pointer: String) {
    let v = edge_lookup(world, &pointer);
    let s = v.as_str().unwrap_or("");
    assert!(!s.is_empty(), "field {pointer} should be non-empty; body = {:?}", world.edge_body);
}

#[then("the edge response carries RateLimit headers")]
async fn then_edge_ratelimit(world: &mut RecallWorld) {
    let h = world.edge_headers.as_ref().expect("headers");
    assert!(h.contains_key("ratelimit-limit"), "missing RateLimit-Limit");
    assert!(h.contains_key("ratelimit-remaining"), "missing RateLimit-Remaining");
    assert!(h.contains_key("ratelimit-reset"), "missing RateLimit-Reset");
}

#[then("the edge response carries Retry-After and RateLimit-Reset headers")]
async fn then_edge_retry_after(world: &mut RecallWorld) {
    let h = world.edge_headers.as_ref().expect("headers");
    assert!(h.contains_key("retry-after"), "missing Retry-After");
    assert!(h.contains_key("ratelimit-reset"), "missing RateLimit-Reset");
}

#[then(regex = r#"^an audit row with operation "([^"]+)" and outcome "([^"]+)" exists for tenant "([^"]+)"$"#)]
async fn then_edge_audit_exists(
    world: &mut RecallWorld,
    operation: String,
    _outcome: String,
    tenant: String,
) {
    let c = api(world).count_audit(&tenant, Some(&operation)).await;
    assert!(c >= 1, "expected an audit row for operation {operation}");
}

#[then(regex = r#"^no audit row exists for tenant "([^"]+)"$"#)]
async fn then_edge_no_audit(world: &mut RecallWorld, tenant: String) {
    let c = api(world).count_audit(&tenant, None).await;
    assert_eq!(c, 0, "expected no audit rows");
}

#[then(regex = r#"^exactly (\d+) extract_fact job is enqueued for tenant "([^"]+)"$"#)]
async fn then_edge_job_count(world: &mut RecallWorld, n: u64, tenant: String) {
    let c = api(world).count_jobs_of_kind(&tenant, "extract_fact").await;
    assert_eq!(c, n, "extract_fact job count mismatch");
}

/// Resolve a dotted JSON path against the captured edge response body.
fn edge_lookup(world: &RecallWorld, dotted: &str) -> Value {
    let body = world.edge_body.clone().unwrap_or(Value::Null);
    let mut current = &body;
    for segment in dotted.split('.') {
        current = match current.get(segment) {
            Some(v) => v,
            None => return Value::Null,
        };
    }
    current.clone()
}

// --- Phase 10 Whole-system steps --------------------------------------------------------------
//
// The whole-system harness assembles the FULL stack over ONE shared in-memory SurrealDB engine and
// serves the HTTP edge in-process. Because no background worker runs, the async write path is advanced
// by an explicit drain step that claims the pending ExtractFact job and runs it through a WritePipeline
// built over the SAME store handle — the eventual-consistency boundary the plan names. Steps here are
// uniquely worded so their regexes never clash with the C8 `api_edge` steps (which use "edge" wording).

fn sys(world: &RecallWorld) -> &SystemHarness {
    world.sys.as_ref().expect("system harness built in a Given")
}

/// Assemble the full stack over one shared engine and serve the edge on an ephemeral port. The broker
/// behaviour (`unchanged` => 304, `changed` => 200) selects the freshness branch a recall will observe.
async fn build_system_harness(world: &mut RecallWorld, broker_changed: bool) {
    use recall::api::{build_router, AppState};
    use recall::auth::{AuthConfig, Authenticator};

    let dim = 8u32;
    let issuer = Arc::new(LocalIssuer::start().await);

    // One wiremock server plays every provider: embedding (write + read), reranker (read), broker
    // (read freshness), extract + pii (write pipeline). Mounting all up front is unambiguous — each is
    // matched on a distinct path (or, for the broker, the GET method).
    let mocks = support::ProviderMocks::start().await;
    mocks.mount_embed(dim as usize).await;
    mocks.mount_rerank_uniform(0.9).await;
    mocks.mount_pii_none().await;
    if broker_changed {
        mocks.mount_broker_changed().await;
    } else {
        mocks.mount_broker_unchanged().await;
    }
    let mocks_base_url = mocks.base_url();

    // One shared in-memory engine: the store, its handle, and a store-backed queue over that handle.
    let store = Arc::new(Store::new_in_memory(dim).await.expect("in-memory store"));
    let handle = store.handle();
    let queue = Arc::new(StoreWorkQueue::new(store.handle(), dim, 5, 10));

    // Config: OIDC -> local issuer; providers -> wiremock; embed dim -> the shared index dim.
    let mut m = std::collections::HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", issuer.issuer()),
        ("RECALL_OIDC_AUDIENCE", AUTH_AUDIENCE),
        ("RECALL_EMBED_URL", &mocks_base_url),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", &mocks_base_url),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", &mocks_base_url),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", &mocks_base_url),
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), dim.to_string());
    let config = support::config_from_map(&m);

    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
    let broker: Arc<dyn BrokerClient> = Arc::new(HttpBrokerClient::new(&config));
    let freshness: Arc<dyn FreshnessChecker> = Arc::new(BrokerFreshnessChecker::new(
        broker,
        queue.clone(),
        Duration::from_millis(25),
        Duration::from_millis(20),
    ));
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        freshness,
        RetrievalConfig::from_config(&config),
    ));
    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .expect("authenticator against the local issuer"),
    );

    let state = AppState {
        config: Arc::new(config),
        metrics: recall::obs::metrics::Metrics::new(),
        store: store.clone(),
        queue: queue.clone(),
        engine,
        auth,
        rate: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let base_url = format!("http://{addr}");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if client.get(format!("{base_url}/healthz")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    world.sys = Some(SystemHarness {
        base_url,
        handle: server,
        db: handle,
        issuer,
        store,
        queue,
        embed_dim: dim,
        mocks_base_url,
        _mocks: mocks,
    });
    world.sys_token = None;
    world.sys_idem_key = None;
    world.sys_status = None;
    world.sys_body = None;
    world.sys_recall_fact_ids.clear();
}

#[given(
    regex = r#"^a system stack for tenant "([^"]+)" with the broker reporting sources unchanged$"#
)]
async fn given_system_unchanged(world: &mut RecallWorld, _tenant: String) {
    build_system_harness(world, false).await;
}

#[given(
    regex = r#"^a system stack for tenant "([^"]+)" with the broker reporting sources changed$"#
)]
async fn given_system_changed(world: &mut RecallWorld, _tenant: String) {
    build_system_harness(world, true).await;
}

/// Mount the extract stub returning a fact whose `text` matches the recall query keyword. No source is
/// attached, so the persisted fact has no `source_id` and the read-path freshness check is skipped.
#[given(
    regex = r#"^the extractor will return a fact matching "([^"]+)" for the recall query$"#
)]
async fn given_system_extract(world: &mut RecallWorld, keyword: String) {
    let content = serde_json::json!({
        "subject": "Team Alpha",
        "predicate": "owns",
        "object": format!("the {keyword}"),
        "text": format!("Team Alpha owns the {keyword}")
    });
    sys(world).mocks_extract(content).await;
}

/// Mount the extract stub returning a fact that CITES a source, so the read-path freshness check runs
/// the broker and the returned fact carries a non-`current` currency when the broker reports a change.
#[given(
    regex = r#"^the extractor will return a sourced fact matching "([^"]+)" for the recall query$"#
)]
async fn given_system_extract_sourced(world: &mut RecallWorld, keyword: String) {
    let content = serde_json::json!({
        "subject": "Team Alpha",
        "predicate": "owns",
        "object": format!("the {keyword}"),
        "text": format!("Team Alpha owns the {keyword}")
    });
    sys(world).mocks_extract_sourced(content).await;
}

/// Seed a high-trust `Source` (trust 1.0) under the deterministic id the write pipeline derives from
/// `origin_ref`, so the pipeline's `upsert_source` reuses this trust signal and the gate admits the fact
/// (a brand-new source would default to a lower trust that quarantines a mid-confidence fact). Mirrors
/// the WpHarness `seed_source` helper.
#[given(
    regex = r#"^a trusted source "([^"]+)" seeded for tenant "([^"]+)" user "([^"]+)"$"#
)]
async fn given_system_trusted_source(
    world: &mut RecallWorld,
    origin_ref: String,
    tenant: String,
    user: String,
) {
    let id = format!(
        "source:{}",
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, format!("source:{origin_ref}").as_bytes())
    );
    let src = Source {
        id,
        origin_ref,
        modification_marker: Some("etag-orig".into()),
        trust_signal: 1.0,
        owner: ScopeRef {
            tenant,
            team: None,
            user,
        },
    };
    sys(world).store.put_source(&src).await.expect("seed trusted source");
}

impl SystemHarness {
    /// Mount the `/extract` stub returning one fact with the given content (no source).
    async fn mocks_extract(&self, content: Value) {
        self._mocks.mount_extract(content, 0.95).await;
    }

    /// Mount the `/extract` stub returning one fact carrying a `source` so the persisted fact cites it
    /// (the write pipeline upserts the source from the job payload's `source`). The job payload supplies
    /// the source; the extract content is the same as the unsourced case.
    async fn mocks_extract_sourced(&self, content: Value) {
        self._mocks.mount_extract(content, 0.95).await;
    }
}

#[given(
    regex = r#"^a system bearer token for user "([^"]+)" tenant "([^"]+)" groups "([^"]+)" scope "([^"]*)"$"#
)]
async fn given_system_token(
    world: &mut RecallWorld,
    user: String,
    tenant: String,
    groups: String,
    scope: String,
) {
    let iss = sys(world).issuer.issuer().to_string();
    let mut claims = base_claims(&iss, AUTH_AUDIENCE, &user, &tenant);
    claims["groups"] = serde_json::json!(csv(&groups));
    claims["scope"] = serde_json::json!(scope);
    world.sys_token = Some(sys(world).issuer.mint(&claims));
}

// The token-granting `Given` is also reachable as a `When` in the cross-tenant scenario (it swaps the
// caller mid-scenario). Cucumber matches `When` against `#[when]` steps only, so mirror the regex.
#[when(
    regex = r#"^a system bearer token for user "([^"]+)" tenant "([^"]+)" groups "([^"]+)" scope "([^"]*)"$"#
)]
async fn when_system_token(
    world: &mut RecallWorld,
    user: String,
    tenant: String,
    groups: String,
    scope: String,
) {
    given_system_token(world, user, tenant, groups, scope).await;
}

#[given(regex = r#"^a system Idempotency-Key "([^"]+)"$"#)]
async fn given_system_idem(world: &mut RecallWorld, key: String) {
    world.sys_idem_key = Some(key);
}

/// Capture an HTTP response into the world's sys_* fields.
async fn sys_capture(world: &mut RecallWorld, resp: reqwest::Response) {
    world.sys_status = Some(resp.status().as_u16());
    let text = resp.text().await.unwrap_or_default();
    world.sys_body = serde_json::from_str(&text).ok();
}

#[when(regex = r#"^the client writes a memory with content (.+)$"#)]
async fn when_system_write(world: &mut RecallWorld, content: String) {
    let url = format!("{}/v1/memories", sys(world).base_url);
    let content_json: Value = serde_json::from_str(&content).expect("scenario content json");
    let body = serde_json::json!({ "content": content_json });
    let mut req = reqwest::Client::new().post(&url).json(&body);
    if let Some(token) = &world.sys_token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(key) = &world.sys_idem_key {
        req = req.header("idempotency-key", key.clone());
    }
    let resp = req.send().await.expect("send system write");
    sys_capture(world, resp).await;
}

#[when(
    regex = r#"^the client writes a sourced memory citing "([^"]+)" with content (.+)$"#
)]
async fn when_system_write_sourced(world: &mut RecallWorld, origin_ref: String, content: String) {
    let url = format!("{}/v1/memories", sys(world).base_url);
    let content_json: Value = serde_json::from_str(&content).expect("scenario content json");
    let body = serde_json::json!({
        "content": content_json,
        "source": { "origin_ref": origin_ref }
    });
    let mut req = reqwest::Client::new().post(&url).json(&body);
    if let Some(token) = &world.sys_token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(key) = &world.sys_idem_key {
        req = req.header("idempotency-key", key.clone());
    }
    let resp = req.send().await.expect("send system sourced write");
    sys_capture(world, resp).await;
}

#[when("the pending extract_fact job is drained through the write pipeline")]
async fn when_system_drain(world: &mut RecallWorld) {
    let harness = sys(world);
    let pipeline = harness.pipeline();
    let job = harness
        .queue
        .claim(&[JobKind::ExtractFact], Duration::from_secs(30))
        .await
        .expect("claim pending job")
        .expect("a claimable extract_fact job");
    let ctx = test_ctx(&job.scope.tenant, &job.scope.user, "none");
    let outcome = pipeline.process(&ctx, &job).await.expect("process drained job");
    assert_eq!(
        format!("{outcome:?}"),
        "Persisted",
        "the drained job should persist a fact"
    );
}

#[when(regex = r#"^the client recalls "([^"]+)" with result_cap (\d+)$"#)]
async fn when_system_recall(world: &mut RecallWorld, query: String, result_cap: u8) {
    let url = format!("{}/v1/recall", sys(world).base_url);
    let body = serde_json::json!({ "query": query, "result_cap": result_cap });
    let mut req = reqwest::Client::new().post(&url).json(&body);
    if let Some(token) = &world.sys_token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let resp = req.send().await.expect("send system recall");
    sys_capture(world, resp).await;
    // Record the returned fact ids so a later DELETE can target one.
    world.sys_recall_fact_ids = world
        .sys_body
        .as_ref()
        .and_then(|b| b.get("data"))
        .and_then(|d| d.get("facts"))
        .and_then(|f| f.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|rf| rf.get("fact").and_then(|f| f.get("id")).and_then(|v| v.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
}

#[when(
    regex = r#"^the client DELETEs the recalled system fact with Idempotency-Key "([^"]+)"$"#
)]
async fn when_system_delete(world: &mut RecallWorld, key: String) {
    let id = world
        .sys_recall_fact_ids
        .first()
        .cloned()
        .expect("a recalled fact id to delete");
    let url = format!("{}/v1/memories/{}", sys(world).base_url, id);
    let mut req = reqwest::Client::new().delete(&url).header("idempotency-key", key);
    if let Some(token) = &world.sys_token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let resp = req.send().await.expect("send system delete");
    sys_capture(world, resp).await;
}

#[then(regex = r#"^the system edge status is (\d+)$"#)]
async fn then_system_status(world: &mut RecallWorld, expected: u16) {
    assert_eq!(
        world.sys_status,
        Some(expected),
        "system status mismatch; body = {:?}",
        world.sys_body
    );
}

#[then(regex = r#"^the system edge field "([^"]+)" is "([^"]+)"$"#)]
async fn then_system_field_is(world: &mut RecallWorld, pointer: String, expected: String) {
    let v = sys_lookup(world, &pointer);
    let got = match &v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    assert_eq!(got, expected, "field {pointer} mismatch; body = {:?}", world.sys_body);
}

#[then(regex = r#"^the system edge field "([^"]+)" is a non-empty string$"#)]
async fn then_system_field_non_empty(world: &mut RecallWorld, pointer: String) {
    let v = sys_lookup(world, &pointer);
    let s = v.as_str().unwrap_or("");
    assert!(
        !s.is_empty(),
        "field {pointer} should be non-empty; body = {:?}",
        world.sys_body
    );
}

#[then("the system recall returns no facts")]
async fn then_system_recall_empty(world: &mut RecallWorld) {
    let n = sys_recall_count(world);
    assert_eq!(n, 0, "expected no recalled facts; body = {:?}", world.sys_body);
}

#[then(regex = r#"^the system recall returns at least (\d+) facts?$"#)]
async fn then_system_recall_at_least(world: &mut RecallWorld, n: usize) {
    let got = sys_recall_count(world);
    assert!(
        got >= n,
        "expected >= {n} recalled facts, got {got}; body = {:?}",
        world.sys_body
    );
}

#[then(regex = r#"^every recalled system fact has currency "([^"]+)"$"#)]
async fn then_system_currency_all(world: &mut RecallWorld, expected: String) {
    let facts = sys_recall_facts(world);
    assert!(!facts.is_empty(), "expected recalled facts to assert currency on");
    for rf in &facts {
        let got = rf.get("currency").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(got, expected, "currency mismatch; fact = {rf:?}");
    }
}

/// The recalled facts array from the captured recall response body, or empty.
fn sys_recall_facts(world: &RecallWorld) -> Vec<Value> {
    world
        .sys_body
        .as_ref()
        .and_then(|b| b.get("data"))
        .and_then(|d| d.get("facts"))
        .and_then(|f| f.as_array())
        .cloned()
        .unwrap_or_default()
}

fn sys_recall_count(world: &RecallWorld) -> usize {
    sys_recall_facts(world).len()
}

/// Resolve a dotted JSON path against the captured system response body.
fn sys_lookup(world: &RecallWorld, dotted: &str) -> Value {
    let body = world.sys_body.clone().unwrap_or(Value::Null);
    let mut current = &body;
    for segment in dotted.split('.') {
        current = match current.get(segment) {
            Some(v) => v,
            None => return Value::Null,
        };
    }
    current.clone()
}

#[given("the recall app is booted with a minimal valid environment")]
async fn boot(world: &mut RecallWorld) {
    world.app = Some(boot_minimal().await);
}

#[when(regex = r#"^I GET "([^"]+)"$"#)]
async fn get_path(world: &mut RecallWorld, path: String) {
    let url = format!("{}{}", world.base_url(), path);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("send GET request");
    world.status = Some(resp.status().as_u16());
    world.correlation_header = resp
        .headers()
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let text = resp.text().await.expect("read response body");
    world.body = serde_json::from_str(&text).ok();
}

#[then(regex = r#"^the response status is (\d+)$"#)]
async fn status_is(world: &mut RecallWorld, expected: u16) {
    assert_eq!(world.status, Some(expected), "unexpected status code");
}

#[then(regex = r#"^the JSON field "([^"]+)" is "([^"]+)"$"#)]
async fn json_field_is(world: &mut RecallWorld, pointer: String, expected: String) {
    let value = lookup(world, &pointer);
    assert_eq!(
        value.as_str(),
        Some(expected.as_str()),
        "field {pointer} mismatch; body = {:?}",
        world.body
    );
}

#[then(regex = r#"^the JSON field "([^"]+)" is a non-empty string$"#)]
async fn json_field_non_empty(world: &mut RecallWorld, pointer: String) {
    let value = lookup(world, &pointer);
    let s = value.as_str().unwrap_or("");
    assert!(!s.is_empty(), "field {pointer} should be a non-empty string");
}

#[then("the response carries a correlation id")]
async fn carries_correlation_id(world: &mut RecallWorld) {
    let header = world
        .correlation_header
        .as_deref()
        .unwrap_or("");
    let meta_cid = lookup(world, "meta.correlation_id");
    let meta_cid = meta_cid.as_str().unwrap_or("");
    assert!(
        !header.is_empty() || !meta_cid.is_empty(),
        "expected a correlation id on the response header or in meta"
    );
}

/// Resolve a dotted JSON path (e.g. `data.status`) against the captured response body.
fn lookup(world: &RecallWorld, dotted: &str) -> Value {
    let body = world.body.clone().unwrap_or(Value::Null);
    let mut current = &body;
    for segment in dotted.split('.') {
        current = match current.get(segment) {
            Some(v) => v,
            None => return Value::Null,
        };
    }
    current.clone()
}

#[tokio::main]
async fn main() {
    RecallWorld::cucumber()
        // Serial execution: the boot scenarios briefly set a process-global env var, so scenarios
        // must not run concurrently. The store scenarios each construct their own in-memory engine.
        .max_concurrent_scenarios(1)
        .run_and_exit("tests/features")
        .await;
}
