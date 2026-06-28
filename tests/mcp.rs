//! C10 — MCP API Edge integration suite.
//!
//! Serves the `recall-mcp` edge in-process on an ephemeral port over the real C9 [`Service`], an
//! in-memory store, wiremock-backed embedding + rerank providers, and the local RS256 issuer — the
//! same way the C8 `api_edge` BDD harness assembles its stack (`tests/support`). A `reqwest` JSON-RPC
//! client drives the streamable-HTTP request/response subset and asserts the handshake, tool discovery,
//! a read tool call, a write tool call, and the missing-bearer error-code parity with the REST edge.
//!
//! Determinism: the store is empty, so `recall` returns an abstained/empty result with no provider
//! variability; the rerank/embed mocks return fixed responses; no Docker or external service is needed.

mod support;

use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};

use recall::config::Config;
use recall::mcp::serve_mcp_on_listener;
use recall::store::Store;
use recall::types::domain::{Fact, MemoryClass, Visibility};
use recall::types::ports::MemoryStore;
use recall::types::scope::ScopeRef;

use support::issuer::LocalIssuer;
use support::ProviderMocks;

/// The audience the local-issuer authenticator expects (mirrors the BDD harness constant).
const AUTH_AUDIENCE: &str = "recall-api";

/// A served MCP edge plus the handles a test needs to mint tokens and tear down. Dropping it aborts
/// the server task and tears down the issuer + provider mocks.
struct McpHarness {
    base_url: String,
    path: String,
    issuer: Arc<LocalIssuer>,
    handle: tokio::task::JoinHandle<()>,
    _mocks: ProviderMocks,
}

impl Drop for McpHarness {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl McpHarness {
    /// The absolute MCP endpoint URL.
    fn endpoint(&self) -> String {
        format!("{}{}", self.base_url, self.path)
    }

    /// Mint a bearer against the harness's local issuer with the given scope.
    fn mint(&self, user: &str, tenant: &str, groups: &str, scope: &str) -> String {
        let iss = self.issuer.issuer().to_string();
        let now = Utc::now().timestamp();
        let claims = json!({
            "iss": iss,
            "aud": AUTH_AUDIENCE,
            "sub": user,
            "tenant": tenant,
            "jti": format!("jti-{now}"),
            "iat": now,
            "nbf": now - 30,
            "exp": now + 3600,
            "groups": groups.split(',').filter(|s| !s.is_empty()).collect::<Vec<_>>(),
            "scope": scope,
        });
        self.issuer.mint(&claims)
    }
}

/// Build the C9 `Service` over an in-memory store + wiremock providers + the local issuer, then serve
/// the MCP edge in-process on an ephemeral port. Mirrors the C8 `build_api_harness` assembly.
async fn build_mcp_harness(embed_dim: u32) -> McpHarness {
    use recall::auth::{AuthConfig, Authenticator};
    use recall::providers::{HttpEmbeddingClient, HttpRerankClient};
    use recall::queue::StoreWorkQueue;
    use recall::retrieval::{RetrievalConfig, RetrievalEngine};
    use recall::types::ports::{EmbeddingClient, MemoryStore, RerankClient};

    let issuer = Arc::new(LocalIssuer::start().await);

    // Provider mocks: embedding (query vector) + reranker.
    let mocks = ProviderMocks::start().await;
    mocks.mount_embed(embed_dim as usize).await;
    mocks.mount_rerank_uniform(0.9).await;

    let store = Arc::new(Store::new_in_memory(embed_dim).await.expect("in-memory store"));
    let queue = Arc::new(StoreWorkQueue::new(store.handle(), embed_dim, 5, 10));

    // Config: OIDC -> local issuer; providers -> wiremock; env -> development; ephemeral MCP addr.
    let mut m = std::collections::HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", issuer.issuer()),
        ("RECALL_OIDC_AUDIENCE", AUTH_AUDIENCE),
        ("RECALL_EMBED_URL", &mocks.base_url()),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", &mocks.base_url()),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_MCP_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), embed_dim.to_string());
    let config: Config = support::config_from_map(&m);
    let env = config.env;
    let mcp_path = config.mcp_path.clone();
    let max_body = config.max_body_bytes as usize;

    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        RetrievalConfig::from_config(&config),
    ));
    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .expect("authenticator against local issuer"),
    );

    let state = recall::api::AppState {
        config: Arc::new(config),
        metrics: recall::obs::metrics::Metrics::new(),
        store,
        queue,
        engine,
        auth,
        rate: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };
    let service = Arc::new(state.service());

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let base_url = format!("http://{addr}");

    let path_for_serve = mcp_path.clone();
    let handle = tokio::spawn(async move {
        let _ = serve_mcp_on_listener(listener, service, max_body, &path_for_serve, env).await;
    });

    // Poll the MCP endpoint with an `initialize` until the server accepts (it is the only route).
    let client = reqwest::Client::new();
    let endpoint = format!("{base_url}{mcp_path}");
    for _ in 0..50 {
        let ok = client
            .post(&endpoint)
            .json(&rpc("initialize", json!({}), 0))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        if ok {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    McpHarness {
        base_url,
        path: mcp_path,
        issuer,
        handle,
        _mocks: mocks,
    }
}

/// A served MCP edge **and** a served REST edge over the **same** [`AppState`]/store, plus the store
/// handle and the issuer. This is the ADR-016 parity rig: the two surfaces share one core, so a test
/// can hit both and assert byte-for-byte agreement on identity, error codes, audit, and isolation.
/// Dropping it aborts both server tasks and tears down the issuer + provider mocks.
struct ParityHarness {
    mcp: McpHarness,
    rest_base_url: String,
    rest_handle: tokio::task::JoinHandle<()>,
    store: Arc<Store>,
}

impl Drop for ParityHarness {
    fn drop(&mut self) {
        self.rest_handle.abort();
    }
}

impl ParityHarness {
    /// Mint a bearer against the shared local issuer (delegates to the MCP harness's issuer — the same
    /// issuer backs both edges, so a token minted here authenticates on REST and MCP alike).
    fn mint(&self, user: &str, tenant: &str, groups: &str, scope: &str) -> String {
        self.mcp.mint(user, tenant, groups, scope)
    }

    /// Count `audit_log` rows for a tenant with the given operation, reading the shared SurrealDB handle
    /// directly (mirrors `bdd.rs::count_audit`). A tenant with no provisioned namespace counts as zero.
    async fn count_audit(&self, tenant: &str, operation: &str) -> u64 {
        let db = self.store.handle();
        if db
            .use_ns(tenant.to_string())
            .use_db("recall")
            .await
            .is_err()
        {
            return 0;
        }
        let mut resp = match db
            .query("SELECT count() AS c FROM audit_log WHERE operation = $op GROUP ALL")
            .bind(("op", operation.to_string()))
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

/// Build the parity rig: a single C9 `Service`/store assembly (mirroring `build_mcp_harness`) served on
/// **two** ephemeral ports — the MCP edge via `serve_mcp_on_listener` and the REST edge via
/// `build_router(state.clone())`. The `AppState` is cloned **before** `state.service()` consumes a clone
/// for the MCP server, so both edges share the same store, queue, engine, auth, and rate-limiter map.
async fn build_parity_harness(embed_dim: u32) -> ParityHarness {
    use recall::api::{build_router, AppState};
    use recall::auth::{AuthConfig, Authenticator};
    use recall::providers::{HttpEmbeddingClient, HttpRerankClient};
    use recall::queue::StoreWorkQueue;
    use recall::retrieval::{RetrievalConfig, RetrievalEngine};
    use recall::types::ports::{EmbeddingClient, MemoryStore, RerankClient};

    let issuer = Arc::new(LocalIssuer::start().await);

    let mocks = ProviderMocks::start().await;
    mocks.mount_embed(embed_dim as usize).await;
    mocks.mount_rerank_uniform(0.9).await;

    let store = Arc::new(Store::new_in_memory(embed_dim).await.expect("in-memory store"));
    let queue = Arc::new(StoreWorkQueue::new(store.handle(), embed_dim, 5, 10));

    let mut m = std::collections::HashMap::new();
    for (k, v) in [
        ("RECALL_OIDC_ISSUER", issuer.issuer()),
        ("RECALL_OIDC_AUDIENCE", AUTH_AUDIENCE),
        ("RECALL_EMBED_URL", &mocks.base_url()),
        ("RECALL_EMBED_API_KEY", "test-embed-key"),
        ("RECALL_RERANK_URL", &mocks.base_url()),
        ("RECALL_RERANK_API_KEY", "test-rerank-key"),
        ("RECALL_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_MCP_HTTP_ADDR", "127.0.0.1:0"),
        ("RECALL_ENV", "development"),
    ] {
        m.insert(k.to_string(), v.to_string());
    }
    m.insert("RECALL_EMBED_DIM".to_string(), embed_dim.to_string());
    let config: Config = support::config_from_map(&m);
    let env = config.env;
    let mcp_path = config.mcp_path.clone();
    let max_body = config.max_body_bytes as usize;

    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        RetrievalConfig::from_config(&config),
    ));
    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .expect("authenticator against local issuer"),
    );

    let state = AppState {
        config: Arc::new(config),
        metrics: recall::obs::metrics::Metrics::new(),
        store: store.clone(),
        queue,
        engine,
        auth,
        rate: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };

    // Clone the AppState for REST BEFORE the MCP server consumes a Service built from it. Both edges
    // then share the same store/queue/engine/auth/rate via the Arc-backed handles.
    let rest_state = state.clone();
    let service = Arc::new(state.service());

    // --- MCP edge on an ephemeral port ---
    let mcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral MCP port");
    let mcp_addr = mcp_listener.local_addr().expect("MCP local addr");
    let mcp_base_url = format!("http://{mcp_addr}");
    let path_for_serve = mcp_path.clone();
    let mcp_handle = tokio::spawn(async move {
        let _ = serve_mcp_on_listener(mcp_listener, service, max_body, &path_for_serve, env).await;
    });

    // --- REST edge on a second ephemeral port, same state ---
    let rest_router = build_router(rest_state);
    let rest_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral REST port");
    let rest_addr = rest_listener.local_addr().expect("REST local addr");
    let rest_base_url = format!("http://{rest_addr}");
    let rest_handle = tokio::spawn(async move {
        let _ = axum::serve(rest_listener, rest_router).await;
    });

    // Poll both edges until they accept.
    let client = reqwest::Client::new();
    let mcp_endpoint = format!("{mcp_base_url}{mcp_path}");
    for _ in 0..50 {
        let ok = client
            .post(&mcp_endpoint)
            .json(&rpc("initialize", json!({}), 0))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        if ok {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    for _ in 0..50 {
        if client
            .get(format!("{rest_base_url}/healthz"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let mcp = McpHarness {
        base_url: mcp_base_url,
        path: mcp_path,
        issuer,
        handle: mcp_handle,
        _mocks: mocks,
    };

    ParityHarness {
        mcp,
        rest_base_url,
        rest_handle,
        store,
    }
}

/// Build a JSON-RPC 2.0 request envelope.
fn rpc(method: &str, params: Value, id: u64) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

/// POST a JSON-RPC request (optionally with a bearer) and return the parsed JSON-RPC response body.
async fn call(harness: &McpHarness, body: &Value, bearer: Option<&str>) -> Value {
    let client = reqwest::Client::new();
    let mut req = client.post(harness.endpoint()).json(body);
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let resp = req.send().await.expect("send JSON-RPC request");
    assert!(
        resp.status().is_success(),
        "transport status should be 200 for the request/response subset, got {}",
        resp.status()
    );
    resp.json::<Value>().await.expect("JSON-RPC response body")
}

#[tokio::test]
async fn initialize_succeeds() {
    let harness = build_mcp_harness(8).await;
    let resp = call(&harness, &rpc("initialize", json!({}), 1), None).await;

    let result = resp.get("result").expect("initialize returns a result");
    assert!(
        result.get("protocolVersion").and_then(|v| v.as_str()).is_some(),
        "protocolVersion must be advertised"
    );
    assert!(
        result.pointer("/capabilities/tools").is_some(),
        "the tools capability must be advertised"
    );
    assert_eq!(
        result.pointer("/serverInfo/name").and_then(|v| v.as_str()),
        Some("recall-mcp"),
        "serverInfo.name must be recall-mcp"
    );
}

#[tokio::test]
async fn tools_list_returns_the_six_tools_each_with_an_input_schema() {
    let harness = build_mcp_harness(8).await;
    let resp = call(&harness, &rpc("tools/list", json!({}), 2), None).await;

    let tools = resp
        .pointer("/result/tools")
        .and_then(|t| t.as_array())
        .expect("tools/list returns a tools array");
    assert_eq!(tools.len(), 6, "expected exactly six tools, got {}", tools.len());

    let names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .collect();
    for expected in ["recall", "remember", "get", "retire", "delete", "capabilities"] {
        assert!(names.contains(&expected), "tools/list missing {expected}");
    }
    for t in tools {
        assert!(
            t.get("inputSchema").is_some(),
            "tool {:?} carries no inputSchema",
            t.get("name")
        );
    }
}

#[tokio::test]
async fn recall_tool_with_valid_bearer_returns_ranked_facts() {
    let harness = build_mcp_harness(8).await;
    let token = harness.mint("u-sarah", "acme", "alpha", "memory.read memory.write");

    let params = json!({
        "name": "recall",
        "arguments": { "query": "who owns the orders table", "result_cap": 5 }
    });
    let resp = call(&harness, &rpc("tools/call", params, 3), Some(&token)).await;

    // No application error: the call authenticated, authorised, and ran the read path.
    assert!(
        resp.get("error").is_none(),
        "recall should succeed with a valid bearer, got error {:?}",
        resp.get("error")
    );
    let structured = resp
        .pointer("/result/structuredContent")
        .expect("recall tool result carries structuredContent");
    // The store is empty, so recall returns deterministically: an empty, abstained ranked set.
    let facts = structured
        .get("facts")
        .and_then(|f| f.as_array())
        .expect("structuredContent.facts is an array");
    assert!(facts.is_empty(), "an empty store yields no ranked facts");
    assert_eq!(
        structured.get("abstained").and_then(|a| a.as_bool()),
        Some(true),
        "an empty store yields an abstained recall"
    );
}

#[tokio::test]
async fn remember_tool_returns_an_accepted_ack() {
    let harness = build_mcp_harness(8).await;
    let token = harness.mint("u-sarah", "acme", "alpha", "memory.read memory.write");

    let params = json!({
        "name": "remember",
        "arguments": {
            "content": { "subject": "team:alpha", "predicate": "owns", "object": "table:orders" },
            "idempotency_key": "k-mcp-001"
        }
    });
    let resp = call(&harness, &rpc("tools/call", params, 4), Some(&token)).await;

    assert!(
        resp.get("error").is_none(),
        "remember should succeed with a valid bearer + idempotency_key, got error {:?}",
        resp.get("error")
    );
    let structured = resp
        .pointer("/result/structuredContent")
        .expect("remember tool result carries structuredContent");
    assert_eq!(
        structured.get("status").and_then(|s| s.as_str()),
        Some("accepted"),
        "a first remember returns status accepted, got {structured:?}"
    );
    assert!(
        structured
            .get("job_id")
            .and_then(|j| j.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "the ack carries a job_id"
    );
}

#[tokio::test]
async fn recall_without_bearer_yields_auth_missing_token() {
    let harness = build_mcp_harness(8).await;

    let params = json!({
        "name": "recall",
        "arguments": { "query": "who owns the orders table" }
    });
    let resp = call(&harness, &rpc("tools/call", params, 5), None).await;

    let error = resp.get("error").expect("a no-bearer call is an MCP error");
    let code = error
        .pointer("/data/code")
        .and_then(|c| c.as_str())
        .expect("the MCP error carries the registry code in data.code");
    assert_eq!(
        code, "AUTH_MISSING_TOKEN",
        "a missing bearer must map to the same registry code the REST edge returns at 401"
    );
}

// --- Phase 3: REST <-> MCP parity + governance ----------------------------------------------------

/// A REST response reduced to what the parity assertions need: the HTTP status and the parsed body.
struct RestResponse {
    status: u16,
    body: Value,
}

impl RestResponse {
    /// The registry code from the X1 error envelope (`{ "error": { "code": ... } }`), if present.
    fn error_code(&self) -> Option<&str> {
        self.body.pointer("/error/code").and_then(|c| c.as_str())
    }
}

/// POST/GET the REST edge with an optional bearer + idempotency key, returning the status and body.
async fn rest_request(
    harness: &ParityHarness,
    method: reqwest::Method,
    path: &str,
    bearer: Option<&str>,
    idempotency_key: Option<&str>,
    json_body: Option<&Value>,
) -> RestResponse {
    let client = reqwest::Client::new();
    let url = format!("{}{path}", harness.rest_base_url);
    let mut req = client.request(method, url);
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(key) = idempotency_key {
        req = req.header("idempotency-key", key);
    }
    if let Some(b) = json_body {
        req = req.json(b);
    }
    let resp = req.send().await.expect("send REST request");
    let status = resp.status().as_u16();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    RestResponse { status, body }
}

/// Build a sample fact (a minimal copy of `bdd.rs::make_fact`) for direct store seeding. The content is
/// the orders/owns assertion used across the recall scenarios.
fn make_fact(id: &str, tenant: &str, team: Option<&str>, user: &str, vis: Visibility) -> Fact {
    Fact {
        id: id.into(),
        content: json!({"subject": "team:alpha", "predicate": "owns", "object": "table:orders"}),
        entities: vec!["entity:e1".into()],
        source_id: None,
        memory_class: MemoryClass::Semantic,
        visibility: vis,
        owner: ScopeRef {
            tenant: tenant.into(),
            team: team.map(|t| t.to_string()),
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

/// Scenario 1 — cross-edge error-code parity (the core ADR-016 proof). Three representative bad inputs,
/// each hit on BOTH edges; the MCP `error.data.code` must equal the REST `error.code` for each.
#[tokio::test]
async fn error_code_parity_across_rest_and_mcp() {
    let harness = build_parity_harness(8).await;
    let token = harness.mint("u-sarah", "acme", "alpha", "memory.read memory.write");

    // (i) recall with NO bearer -> AUTH_MISSING_TOKEN on both edges.
    let mcp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({ "name": "recall", "arguments": { "query": "who owns the orders table" } }),
            10,
        ),
        None,
    )
    .await;
    let mcp_code = mcp
        .pointer("/error/data/code")
        .and_then(|c| c.as_str())
        .expect("MCP no-bearer recall carries data.code");
    let rest = rest_request(
        &harness,
        reqwest::Method::POST,
        "/v1/recall",
        None,
        None,
        Some(&json!({ "query": "who owns the orders table" })),
    )
    .await;
    assert_eq!(rest.status, 401, "no-bearer recall is a REST 401");
    assert_eq!(
        mcp_code,
        rest.error_code().expect("REST error envelope carries error.code"),
        "no-bearer recall: MCP data.code must equal REST error.code"
    );
    assert_eq!(mcp_code, "AUTH_MISSING_TOKEN", "expected AUTH_MISSING_TOKEN");

    // (ii) remember with a valid bearer but NO idempotency_key -> VAL_MISSING_IDEMPOTENCY_KEY on both.
    let mcp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({
                "name": "remember",
                "arguments": {
                    "content": { "subject": "team:alpha", "predicate": "owns", "object": "table:orders" }
                }
            }),
            11,
        ),
        Some(&token),
    )
    .await;
    let mcp_code = mcp
        .pointer("/error/data/code")
        .and_then(|c| c.as_str())
        .expect("MCP no-idempotency-key remember carries data.code");
    let rest = rest_request(
        &harness,
        reqwest::Method::POST,
        "/v1/memories",
        Some(&token),
        None, // no Idempotency-Key header
        Some(&json!({
            "content": { "subject": "team:alpha", "predicate": "owns", "object": "table:orders" }
        })),
    )
    .await;
    assert_eq!(
        mcp_code,
        rest.error_code().expect("REST error envelope carries error.code"),
        "no-idempotency-key remember: MCP data.code must equal REST error.code"
    );
    assert_eq!(
        mcp_code, "VAL_MISSING_IDEMPOTENCY_KEY",
        "expected VAL_MISSING_IDEMPOTENCY_KEY"
    );

    // (iii) get a nonexistent fact id with a valid bearer -> NOT_FOUND on both.
    let missing_id = "fact:00000000-0000-0000-0000-000000000000";
    let mcp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({ "name": "get", "arguments": { "id": missing_id } }),
            12,
        ),
        Some(&token),
    )
    .await;
    let mcp_code = mcp
        .pointer("/error/data/code")
        .and_then(|c| c.as_str())
        .expect("MCP get-missing carries data.code");
    let rest = rest_request(
        &harness,
        reqwest::Method::GET,
        &format!("/v1/memories/{missing_id}"),
        Some(&token),
        None,
        None,
    )
    .await;
    assert_eq!(rest.status, 404, "get-missing is a REST 404");
    assert_eq!(
        mcp_code,
        rest.error_code().expect("REST error envelope carries error.code"),
        "get-missing: MCP data.code must equal REST error.code"
    );
    assert_eq!(mcp_code, "NOT_FOUND", "expected NOT_FOUND");
}

/// Scenario 2 — MCP idempotency parity. The same `idempotency_key` replayed returns the original ack
/// (`already-accepted`) with no new side effect, exactly as the REST edge does.
#[tokio::test]
async fn mcp_remember_idempotency_replay_is_already_accepted() {
    let harness = build_parity_harness(8).await;
    let token = harness.mint("u-sarah", "acme", "alpha", "memory.read memory.write");

    let args = json!({
        "name": "remember",
        "arguments": {
            "content": { "subject": "team:alpha", "predicate": "owns", "object": "table:orders" },
            "idempotency_key": "k-parity-idem-001"
        }
    });

    let first = call(&harness.mcp, &rpc("tools/call", args.clone(), 20), Some(&token)).await;
    assert!(
        first.get("error").is_none(),
        "first remember should succeed, got {:?}",
        first.get("error")
    );
    let first_status = first
        .pointer("/result/structuredContent/status")
        .and_then(|s| s.as_str());
    assert_eq!(first_status, Some("accepted"), "first remember is accepted");
    let first_job_id = first
        .pointer("/result/structuredContent/job_id")
        .and_then(|j| j.as_str())
        .expect("first ack carries a job_id")
        .to_string();

    let second = call(&harness.mcp, &rpc("tools/call", args, 21), Some(&token)).await;
    assert!(
        second.get("error").is_none(),
        "replayed remember should succeed, got {:?}",
        second.get("error")
    );
    let second_status = second
        .pointer("/result/structuredContent/status")
        .and_then(|s| s.as_str());
    assert_eq!(
        second_status,
        Some("already-accepted"),
        "a replay with the same idempotency_key returns already-accepted"
    );
    let second_job_id = second
        .pointer("/result/structuredContent/job_id")
        .and_then(|j| j.as_str())
        .expect("replay ack carries the original job_id");
    assert_eq!(
        second_job_id, first_job_id,
        "the replay returns the ORIGINAL job_id (no new side effect)"
    );
}

/// Scenario 3 — MCP audit parity. A valid-bearer `recall` over MCP writes the same `audit_log` row the
/// REST path does: an `operation = "recall"` record for the tenant.
#[tokio::test]
async fn mcp_recall_writes_an_audit_row() {
    let harness = build_parity_harness(8).await;
    let token = harness.mint("u-sarah", "acme", "alpha", "memory.read memory.write");

    let before = harness.count_audit("acme", "recall").await;

    let resp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({ "name": "recall", "arguments": { "query": "who owns the orders table" } }),
            30,
        ),
        Some(&token),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "recall should succeed with a valid bearer, got {:?}",
        resp.get("error")
    );

    let after = harness.count_audit("acme", "recall").await;
    assert!(
        after > before,
        "the MCP recall path must write an audit_log row with operation=recall (before {before}, after {after})"
    );
}

/// Scenario 4 — MCP identity parity. A syntactically invalid bearer yields AUTH_INVALID_TOKEN and writes
/// NO audit row (the auth gate runs before any auditable component work).
#[tokio::test]
async fn mcp_invalid_bearer_is_rejected_with_no_audit() {
    let harness = build_parity_harness(8).await;

    let before = harness.count_audit("acme", "recall").await;

    let resp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({ "name": "recall", "arguments": { "query": "who owns the orders table" } }),
            40,
        ),
        Some("not-a-jwt"),
    )
    .await;
    let code = resp
        .pointer("/error/data/code")
        .and_then(|c| c.as_str())
        .expect("an invalid bearer is an MCP error with data.code");
    assert_eq!(
        code, "AUTH_INVALID_TOKEN",
        "a syntactically invalid bearer maps to AUTH_INVALID_TOKEN"
    );

    let after = harness.count_audit("acme", "recall").await;
    assert_eq!(
        before, after,
        "a rejected (invalid-token) call writes no audit row (before {before}, after {after})"
    );
}

/// Scenario 5 — MCP cross-tenant isolation (NFR-PR1). A fact seeded for tenant "acme" is invisible to a
/// "globex" bearer recalling over MCP: the returned ranked set is empty.
#[tokio::test]
async fn mcp_recall_is_tenant_isolated() {
    let harness = build_parity_harness(8).await;

    // Seed a tenant-shared fact directly into "acme".
    let fact = make_fact(
        "fact:11111111-1111-1111-1111-111111111111",
        "acme",
        Some("alpha"),
        "u-sarah",
        Visibility::TenantShared,
    );
    harness.store.put_fact(&fact).await.expect("seed acme fact");

    // A DIFFERENT tenant ("globex") recalls — it must see none of acme's facts.
    let globex = harness.mint("u-carol", "globex", "beta", "memory.read memory.write");
    let resp = call(
        &harness.mcp,
        &rpc(
            "tools/call",
            json!({ "name": "recall", "arguments": { "query": "who owns the orders table" } }),
            50,
        ),
        Some(&globex),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "the globex recall itself should succeed, got {:?}",
        resp.get("error")
    );
    let facts = resp
        .pointer("/result/structuredContent/facts")
        .and_then(|f| f.as_array())
        .expect("structuredContent.facts is an array");
    assert!(
        facts.is_empty(),
        "a globex recall must NOT see acme's fact (cross-tenant isolation), got {facts:?}"
    );
}
