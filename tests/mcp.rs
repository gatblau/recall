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
