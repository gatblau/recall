//! Shared test support for the integration suite.
//!
//! Phase 1 needs only the in-process boot helper (no external services). The testcontainers and
//! wiremock helper stubs below are deliberately unused this phase (`#[allow(dead_code)]`) so the
//! dev-dependencies compile and the seams are ready for Phase 2 (real SurrealDB + Dex via
//! testcontainers, provider/broker stubs via wiremock).

#![allow(dead_code)]

pub mod dex;
pub mod issuer;

use std::collections::HashMap;
use std::net::SocketAddr;

use recall::config::Config;

/// A minimal valid configuration source for boot tests: the nine §X6 required keys plus an
/// ephemeral bind address. Returned as a `KEY=value` map, written to a temp file the app loads via
/// `RECALL_CONFIG_FILE` so no process-global env mutation is needed.
pub fn minimal_env() -> HashMap<String, String> {
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
        ("RECALL_BROKER_URL", "https://broker.test"),
        // Bind to an ephemeral port; the actual bound port is read back from the listener.
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m
}

/// Build a `Config` directly from a `KEY=value` map by writing it to a temp file and pointing
/// `RECALL_CONFIG_FILE` at it for the duration of the load. The env var is removed immediately
/// after the load returns, keeping the mutation window tiny.
pub fn config_from_map(map: &HashMap<String, String>) -> Config {
    let mut path = std::env::temp_dir();
    path.push(format!("recall-test-config-{}.env", uuid_like()));
    let contents: String = map
        .iter()
        .map(|(k, v)| format!("{k}={v}\n"))
        .collect();
    std::fs::write(&path, contents).expect("write temp config file");

    std::env::set_var("RECALL_CONFIG_FILE", &path);
    let config = Config::load().expect("load minimal valid config");
    std::env::remove_var("RECALL_CONFIG_FILE");
    let _ = std::fs::remove_file(&path);
    config
}

/// A small unique suffix for temp filenames without pulling `uuid` into the test directly through a
/// public dependency boundary. Uses the current time in nanos plus the thread id hash.
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}

/// Boot the recall app in-process on an ephemeral port and return its base URL plus a shutdown
/// handle. The server runs on a spawned tokio task; dropping the returned `BootedApp` aborts it.
pub struct BootedApp {
    pub base_url: String,
    pub addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for BootedApp {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Boot the app with a minimal valid environment, binding an ephemeral port and serving on a task.
///
/// The full C8 `AppState` is constructed via [`build_test_state`]: a `LocalIssuer` so the real
/// `Authenticator::new` succeeds against a genuine JWKS, an in-memory `Store`, a store-backed queue, a
/// retrieval engine over wiremock-backed providers, and the in-process rate-limiter. The boot smoke
/// only hits `/healthz` + an unknown route, so the deps just need to construct.
pub async fn boot_minimal() -> BootedApp {
    let issuer = issuer::LocalIssuer::start().await;
    let config = config_from_map(&boot_env(issuer.issuer()));
    recall::obs::log::init_logging(&config);
    let (state, _keepalive) = build_test_state(config, issuer.issuer()).await;
    // Keep the issuer + provider mocks alive for the life of the booted app.
    let keepalive = TestStateKeepAlive {
        issuer,
        mocks: _keepalive,
    };

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let base_url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        // Hold the issuer + mocks for the server's lifetime, then run until the task is aborted.
        let _keepalive = keepalive;
        let _ = recall::serve_on_listener(listener, state).await;
    });

    // Give the server a moment to start accepting; poll the listener address.
    wait_until_ready(&base_url).await;

    BootedApp {
        base_url,
        addr,
        handle,
    }
}

/// Keeps the local issuer + provider mocks alive while the booted app serves (dropping either would
/// tear down the JWKS / provider endpoints the running state holds connections/config for).
struct TestStateKeepAlive {
    issuer: issuer::LocalIssuer,
    mocks: ProviderMocks,
}

/// A minimal valid configuration for the full-stack boot, pointing OIDC at the supplied local issuer
/// and binding an ephemeral port. The embedded store is in-memory (`RECALL_STORE_PATH` is unused —
/// `build_test_state` constructs the store directly).
fn boot_env(issuer: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", issuer),
        ("RECALL_OIDC_AUDIENCE", "recall-api"),
        ("RECALL_EMBED_URL", "https://embed.test"),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", "https://rerank.test"),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_LLM_URL", "https://llm.test"),
        ("RECALL_LLM_API_KEY", "test-llm-key"),
        ("RECALL_BROKER_URL", "https://broker.test"),
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
        ("RECALL_EMBED_DIM", "8"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m
}

/// Build the full C8 `AppState` over an in-memory store + store-backed queue + retrieval engine +
/// authenticator (against the supplied issuer). Returns the state plus the `ProviderMocks` whose
/// lifetime must outlive the state (the engine's provider URLs point at it). Shared by `boot_minimal`
/// and the `api_edge` harness.
pub async fn build_test_state(
    config: recall::config::Config,
    _issuer: &str,
) -> (recall::api::AppState, ProviderMocks) {
    use std::collections::HashMap as Map;
    use std::sync::Arc;

    use recall::auth::{AuthConfig, Authenticator};
    use recall::providers::{HttpBrokerClient, HttpEmbeddingClient, HttpRerankClient};
    use recall::queue::StoreWorkQueue;
    use recall::retrieval::{RetrievalConfig, RetrievalEngine};
    use recall::store::Store;
    use recall::types::ports::{
        BrokerClient, EmbeddingClient, FreshnessChecker, MemoryStore, RerankClient,
    };

    let embed_dim = config.embed_dim;
    let store = Arc::new(Store::new_in_memory(embed_dim).await.expect("in-memory store"));
    let queue = Arc::new(StoreWorkQueue::new(store.handle(), embed_dim, 5, 10));

    // Provider mocks: a wiremock server playing embedding + rerank + broker. The engine config points
    // its URLs at this server.
    let mocks = ProviderMocks::start().await;
    mocks.mount_embed(embed_dim as usize).await;
    mocks.mount_rerank_uniform(0.9).await;
    mocks.mount_broker_unchanged().await;

    // Re-derive a config whose provider URLs point at the mock server, preserving the OIDC issuer.
    let mut overrides: Map<String, String> = Map::new();
    overrides.insert("RECALL_OIDC_ISSUER".into(), config.oidc_issuer.clone());
    overrides.insert("RECALL_OIDC_AUDIENCE".into(), config.oidc_audience.clone());
    overrides.insert("RECALL_EMBED_URL".into(), mocks.base_url());
    overrides.insert("RECALL_EMBED_API_KEY".into(), "test-embed-key".into());
    overrides.insert("RECALL_RERANK_URL".into(), mocks.base_url());
    overrides.insert("RECALL_RERANK_API_KEY".into(), "test-rerank-key".into());
    overrides.insert("RECALL_LLM_URL".into(), "https://llm.test".into());
    overrides.insert("RECALL_LLM_API_KEY".into(), "test-llm-key".into());
    overrides.insert("RECALL_BROKER_URL".into(), mocks.base_url());
    overrides.insert("RECALL_HTTP_ADDR".into(), "127.0.0.1:0".into());
    overrides.insert("RECALL_ENV".into(), "development".into());
    overrides.insert("RECALL_EMBED_DIM".into(), embed_dim.to_string());
    let prov_config = config_from_map(&overrides);

    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&prov_config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&prov_config));
    let broker: Arc<dyn BrokerClient> = Arc::new(HttpBrokerClient::new(&prov_config));
    let freshness: Arc<dyn FreshnessChecker> =
        Arc::new(recall::freshness::BrokerFreshnessChecker::new(
            broker,
            queue.clone(),
            std::time::Duration::from_millis(25),
            std::time::Duration::from_millis(20),
        ));
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        freshness,
        RetrievalConfig::from_config(&prov_config),
    ));

    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .expect("authenticator against the local issuer"),
    );

    let state = recall::api::AppState {
        config: Arc::new(config),
        metrics: recall::obs::metrics::Metrics::new(),
        store,
        queue,
        engine,
        auth,
        rate: Arc::new(tokio::sync::Mutex::new(Map::new())),
    };
    (state, mocks)
}

/// Poll `/healthz` until it answers or a short deadline elapses, so a test does not race the bind.
async fn wait_until_ready(base_url: &str) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/healthz");
    for _ in 0..50 {
        if client.get(&url).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// --- Phase-2-ready stubs (unused in Phase 1) -------------------------------------------------

/// Placeholder for the Phase-2 SurrealDB testcontainer fixture. Phase 2 replaces this with a real
/// `testcontainers`-managed `surrealdb/surrealdb:v2.x` container (in-memory backend for the inner
/// loop). Kept as a stub so the dev-dependency compiles and the seam is named.
pub struct SurrealFixture;

impl SurrealFixture {
    pub fn placeholder() -> Self {
        SurrealFixture
    }
}

/// Placeholder for the Phase-2 wiremock provider/broker stub server. Phase 2+ replaces this with a
/// real `wiremock::MockServer` honouring each provider wire contract.
pub struct ProviderStub;

impl ProviderStub {
    pub fn placeholder() -> Self {
        ProviderStub
    }
}

// --- C4 Write Pipeline wiremock provider stubs (Phase 5) -------------------------------------------
//
// A single `wiremock::MockServer` plays all three providers the write pipeline consumes, honouring the
// wire contracts documented in `src/providers/mod.rs`:
//   * POST /embeddings  -> { "embeddings": [[f32; dim]] }
//   * POST /extract     -> { "facts": [ { content, entity_mentions, memory_class, extractor_confidence } ] }
//   * POST /pii/scan    -> { "spans": [ { json_pointer, start, end, pii_type, confidence } ] }
// The server's base URL is used as RECALL_EMBED_URL / RECALL_LLM_URL so the HTTP adapters POST here.

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A wiremock server stubbing the embedding, LLM-extract, and PII providers for the write pipeline.
pub struct ProviderMocks {
    pub server: MockServer,
}

impl ProviderMocks {
    /// Start a fresh mock server with no mounts yet.
    pub async fn start() -> Self {
        Self {
            server: MockServer::start().await,
        }
    }

    /// The base URL the provider HTTP adapters should POST to.
    pub fn base_url(&self) -> String {
        self.server.uri()
    }

    /// Mount an `/embeddings` stub returning one vector of `dim` for each input text.
    pub async fn mount_embed(&self, dim: usize) {
        // Return a single vector; the adapter takes the first. A fixed-length vector of the configured
        // dim satisfies the SA-EMBED-01 length assertion.
        let body = serde_json::json!({ "embeddings": [vec![0.1_f32; dim]] });
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Mount an `/extract` stub returning one fact with the given content and confidence plus two
    /// entity mentions (subject/object), `memory_class = semantic`.
    pub async fn mount_extract(&self, content: serde_json::Value, confidence: f64) {
        let body = serde_json::json!({
            "facts": [{
                "content": content,
                "entity_mentions": [
                    { "surface_form": "Team Alpha", "mention_type": "team" },
                    { "surface_form": "orders table", "mention_type": "thing" }
                ],
                "memory_class": "semantic",
                "extractor_confidence": confidence
            }]
        });
        Mock::given(method("POST"))
            .and(path("/extract"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Mount a `/pii/scan` stub returning no spans.
    pub async fn mount_pii_none(&self) {
        let body = serde_json::json!({ "spans": [] });
        Mock::given(method("POST"))
            .and(path("/pii/scan"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Mount a `/pii/scan` stub flagging the `/contact` string with the given confidence and the full
    /// string range as the span.
    pub async fn mount_pii_contact(&self, contact: &str, confidence: f64) {
        let body = serde_json::json!({
            "spans": [{
                "json_pointer": "/contact",
                "start": 0,
                "end": contact.len(),
                "pii_type": "email",
                "confidence": confidence
            }]
        });
        Mock::given(method("POST"))
            .and(path("/pii/scan"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Mount a `/consolidate` stub returning the given `insights` array (the C7 consolidation wire
    /// contract in `src/providers/mod.rs`): `POST /consolidate -> { "insights": [ { content, entities,
    /// derived_from, confidence, support_count } ] }`.
    pub async fn mount_consolidate(&self, insights_json: serde_json::Value) {
        let body = serde_json::json!({ "insights": insights_json });
        Mock::given(method("POST"))
            .and(path("/consolidate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// The number of recorded requests to `/consolidate` (proves the LLM was / was not called).
    pub async fn consolidate_call_count(&self) -> usize {
        self.server
            .received_requests()
            .await
            .map(|reqs| reqs.iter().filter(|r| r.url.path() == "/consolidate").count())
            .unwrap_or(0)
    }

    /// The number of recorded requests to `/extract` (proves the LLM was / was not called).
    pub async fn extract_call_count(&self) -> usize {
        self.server
            .received_requests()
            .await
            .map(|reqs| {
                reqs.iter()
                    .filter(|r| r.url.path() == "/extract")
                    .count()
            })
            .unwrap_or(0)
    }

    // --- C5 Freshness Checker broker stubs (Phase 6) -----------------------------------------
    //
    // The Faraday broker conditional check is `GET /sources/{origin_ref}/freshness` honouring the
    // wire contract in `src/providers/mod.rs`: `304 Not Modified` -> unchanged, `200 OK` -> changed,
    // any other status -> a provider error C5 absorbs into UnverifiedCurrency. Each freshness scenario
    // mounts exactly one broker behaviour, so matching on the GET method alone is unambiguous.

    /// Broker reports every source unchanged (`304`).
    pub async fn mount_broker_unchanged(&self) {
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(304))
            .mount(&self.server)
            .await;
    }

    /// Broker reports every source changed (`200`).
    pub async fn mount_broker_changed(&self) {
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&self.server)
            .await;
    }

    /// Broker is down / errors (`503`) — C5 maps this to UnverifiedCurrency.
    pub async fn mount_broker_error(&self) {
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&self.server)
            .await;
    }

    /// Broker answers, but only after `delay_ms` — used to trip the per-call / batch deadline.
    pub async fn mount_broker_slow(&self, delay_ms: u64) {
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(304)
                    .set_delay(std::time::Duration::from_millis(delay_ms)),
            )
            .mount(&self.server)
            .await;
    }

    // --- C6 Retrieval Engine provider stubs (Phase 7) ---------------------------------------
    //
    // The read path consumes the embedding provider (query vector) and the cross-encoder reranker,
    // honouring the wire contracts in `src/providers/mod.rs`:
    //   * POST /embeddings -> { "embeddings": [[f32; dim]] }   (reused: mount_embed / below)
    //   * POST /rerank     -> { "scores": [f64, ..] }          (positionally aligned with documents)

    /// Mount a `/rerank` stub returning the same score for up to 128 documents (≥ RECALL_STAGE1_K),
    /// so the positional alignment holds regardless of the candidate count.
    pub async fn mount_rerank_uniform(&self, score: f64) {
        let body = serde_json::json!({ "scores": vec![score; 128] });
        Mock::given(method("POST"))
            .and(path("/rerank"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Mount a `/rerank` stub that errors (`503`) — C6 degrades to stage-1 order.
    pub async fn mount_rerank_error(&self) {
        Mock::given(method("POST"))
            .and(path("/rerank"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&self.server)
            .await;
    }

    /// Mount an `/embeddings` stub that errors (`503`) — C6 fails fast with a provider error.
    pub async fn mount_embed_error(&self) {
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&self.server)
            .await;
    }

    /// The number of recorded broker freshness requests (proves the per-source de-duplication).
    pub async fn broker_call_count(&self) -> usize {
        self.server
            .received_requests()
            .await
            .map(|reqs| {
                reqs.iter()
                    .filter(|r| r.url.path().ends_with("/freshness"))
                    .count()
            })
            .unwrap_or(0)
    }
}
