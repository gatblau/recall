//! Phase 1 container smoke test (plan 03 — container packaging & release).
//!
//! Builds the multi-stage `recall:ci` image (carrying BOTH binaries) and boots it twice on a shared,
//! user-defined Docker network against a real `dexidp/dex` issuer:
//!
//!   * as `recall-mcp` — POST a JSON-RPC `tools/list` to the mapped `/mcp` and assert six tools;
//!   * as `recall`     — `GET /healthz` on the mapped REST port and assert `200 {status: live}`.
//!
//! Both binaries do OIDC discovery + a JWKS fetch at boot and FAIL FAST if the issuer is unreachable
//! (`build_state` → C3 `Authenticator`), so Dex must be reachable before either container is started.
//! The recall containers reach Dex by its container name (`dex`) as the in-network DNS alias; the
//! issuer is therefore the in-network URL `http://dex:5556/dex`, which is also the URL minted-token
//! `iss` is validated against.
//!
//! Docker may be absent. The whole test skips with a clear `eprintln!` in that case, mirroring how
//! `support::dex::start_dex` skips.

#![allow(dead_code)]

mod support;

use std::process::Command;
use std::time::Duration;

use serde_json::{json, Value};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use support::dex::{DEX_CLIENT_ID, DEX_IMAGE, DEX_TAG};

/// The tag built and exercised by this test.
const IMAGE: &str = "recall:ci";

/// Dex's in-container HTTP listener port on the shared network.
const DEX_PORT: u16 = 5556;
/// The container name = in-network DNS alias the recall containers resolve to reach Dex.
const DEX_ALIAS: &str = "dex";
/// The in-network issuer URL: reachable by the recall containers AND the value `iss` is validated
/// against. Must match the `issuer:` in the Dex config exactly.
const DEX_ISSUER_IN_NET: &str = "http://dex:5556/dex";

/// recall's in-container REST and MCP listener ports (the image's `ENV` defaults).
const RECALL_HTTP_PORT: u16 = 8080;
const RECALL_MCP_PORT: u16 = 8081;
/// The Dex static client id doubles as the audience the recall containers validate.
const AUDIENCE: &str = DEX_CLIENT_ID;

/// The Dex config document for the in-network issuer. Mirrors the YAML shape of
/// `tests/support/dex.rs` (memory storage, password DB / ROPC, one static client + user) but with the
/// in-network issuer and the listener on `0.0.0.0:5556`.
fn dex_config_in_network() -> String {
    format!(
        r#"issuer: {issuer}
storage:
  type: memory
web:
  http: 0.0.0.0:{port}
oauth2:
  passwordConnector: local
  skipApprovalScreen: true
enablePasswordDB: true
staticPasswords:
  - email: "tester@example.com"
    hash: "$2y$10$IcAwXtH3RsRsQlnPSfThfuQ7ghbofQERhhYolr/WRXlCxmswGvzTW"
    username: "tester"
    userID: "08a8684b-db88-4b73-90a9-3cd1661f5466"
staticClients:
  - id: {client_id}
    secret: recall-test-secret
    name: "Recall Test"
    redirectURIs:
      - "http://127.0.0.1/callback"
"#,
        issuer = DEX_ISSUER_IN_NET,
        port = DEX_PORT,
        client_id = DEX_CLIENT_ID,
    )
}

/// A user-defined Docker network created via the CLI, removed on drop. testcontainers 0.23 has no
/// network-alias API, so the test owns the network explicitly: containers attached to it resolve each
/// other by container name (`docker run --network <name>` provides DNS by name on a user-defined net).
struct Network {
    name: String,
}

impl Network {
    /// Create a uniquely-named bridge network. Returns `None` (skip) if the `docker` CLI is absent or
    /// the create fails (e.g. no daemon).
    fn create() -> Option<Self> {
        let name = format!("recall-ci-net-{}", std::process::id());
        // Best-effort cleanup of a stale network from a previous aborted run.
        let _ = Command::new("docker").args(["network", "rm", &name]).output();
        let out = Command::new("docker")
            .args(["network", "create", &name])
            .output()
            .ok()?;
        if !out.status.success() {
            eprintln!(
                "SKIP container test: could not create docker network ({})",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return None;
        }
        Some(Network { name })
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        // Remove the network on teardown. Containers attached to it are stopped first by their own
        // `ContainerAsync` drops, so this should succeed; ignore any residual error.
        let _ = Command::new("docker")
            .args(["network", "rm", &self.name])
            .output();
    }
}

/// Build the `recall:ci` image once. Returns `false` (skip) if `docker` is absent; panics if the build
/// is attempted but fails (a broken Dockerfile is a hard failure, not a skip).
fn build_image() -> bool {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let probe = Command::new("docker").arg("version").output();
    if probe.is_err() {
        eprintln!("SKIP container test: docker CLI not available");
        return false;
    }

    eprintln!("container test: building {IMAGE} (first build may take 10-25 min: surrealdb + aws-lc-rs)...");
    let started = std::time::Instant::now();
    let status = Command::new("docker")
        .args(["build", "-t", IMAGE, "."])
        .current_dir(manifest_dir)
        .status()
        .expect("spawn docker build");
    assert!(
        status.success(),
        "docker build of {IMAGE} failed (exit {:?})",
        status.code()
    );
    eprintln!(
        "container test: built {IMAGE} in {:.0}s",
        started.elapsed().as_secs_f64()
    );
    true
}

/// Start Dex on the shared network under the `dex` alias, with the in-network issuer. Also maps Dex's
/// HTTP port to an ephemeral host port so the test can wait for discovery from the host before the
/// recall containers (which fail fast on an unreachable issuer) are started.
async fn start_dex_on_network(network: &str) -> ContainerAsync<GenericImage> {
    GenericImage::new(DEX_IMAGE, DEX_TAG)
        .with_wait_for(WaitFor::message_on_stderr("listening on"))
        .with_network(network)
        .with_container_name(DEX_ALIAS)
        // Ephemeral host mapping so the host-side discovery poll can confirm Dex is up.
        .with_mapped_port(0, DEX_PORT.tcp())
        .with_copy_to(
            "/etc/dex/config.docker.yaml",
            dex_config_in_network().into_bytes(),
        )
        .with_cmd(["dex", "serve", "/etc/dex/config.docker.yaml"])
        .start()
        .await
        .expect("start dex container")
}

/// Poll the host-mapped Dex discovery document until it answers, so the issuer is reachable before any
/// recall container (fail-fast on boot) is started.
async fn wait_dex_discovery(host_port: u16) -> bool {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{host_port}/dex/.well-known/openid-configuration");
    for _ in 0..150 {
        if let Ok(r) = client.get(&url).send().await {
            if r.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// Build a `recall:ci` container request on the shared network with the boot env (in-network issuer,
/// dummy lazy provider URLs, a tiny embed dim). The store path is the image default. The named binary
/// is the container command.
fn recall_container(network: &str, command: &str) -> testcontainers::ContainerRequest<GenericImage> {
    GenericImage::new("recall", "ci")
        .with_network(network)
        .with_env_var("RECALL_OIDC_ISSUER", DEX_ISSUER_IN_NET)
        .with_env_var("RECALL_OIDC_AUDIENCE", AUDIENCE)
        // Embedding / reranker clients are lazy (HTTP only on use), so dummy URLs are fine for a boot
        // + discovery smoke; the binary never calls them just to come up.
        .with_env_var("RECALL_EMBED_URL", "http://embed.invalid")
        .with_env_var("RECALL_EMBED_API_KEY", "x")
        .with_env_var("RECALL_RERANK_URL", "http://rerank.invalid")
        .with_env_var("RECALL_RERANK_API_KEY", "x")
        .with_env_var("RECALL_EMBED_DIM", "8")
        .with_cmd([command])
}

/// Poll `GET {base}/healthz` until it returns `200`, then return the parsed body. Panics on timeout.
async fn wait_healthz(base: &str) -> Value {
    let client = reqwest::Client::new();
    let url = format!("{base}/healthz");
    for _ in 0..150 {
        if let Ok(r) = client.get(&url).send().await {
            if r.status().is_success() {
                return r.json::<Value>().await.expect("healthz body is json");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("recall /healthz never became reachable at {url}");
}

/// Poll the MCP endpoint with a `tools/list` JSON-RPC request until it answers `200`, then return the
/// parsed JSON-RPC response. Panics on timeout.
async fn wait_tools_list(endpoint: &str) -> Value {
    let client = reqwest::Client::new();
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} });
    for _ in 0..150 {
        if let Ok(r) = client.post(endpoint).json(&body).send().await {
            if r.status().is_success() {
                return r.json::<Value>().await.expect("tools/list body is json");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("recall-mcp {endpoint} never answered tools/list");
}

#[tokio::test]
async fn image_boots_as_each_binary_against_real_dex() {
    // One-time image build (hard failure if the Dockerfile is broken; skip if docker is absent).
    if !build_image() {
        return;
    }

    // User-defined network so the recall containers can resolve `dex` by name.
    let Some(network) = Network::create() else {
        return;
    };

    // Real Dex on the network, reachable at the in-network issuer.
    let dex = start_dex_on_network(&network.name).await;
    let dex_host_port = dex
        .get_host_port_ipv4(DEX_PORT.tcp())
        .await
        .expect("dex host port");

    // The recall containers fail fast on an unreachable issuer, so wait for Dex discovery first.
    assert!(
        wait_dex_discovery(dex_host_port).await,
        "Dex discovery never became reachable on the host mapping"
    );

    // --- recall-mcp: boots against Dex, answers tools/list with six tools ---------------------
    let mcp = recall_container(&network.name, "recall-mcp")
        .with_mapped_port(0, RECALL_MCP_PORT.tcp())
        .start()
        .await
        .expect("start recall-mcp container");
    let mcp_host_port = mcp
        .get_host_port_ipv4(RECALL_MCP_PORT.tcp())
        .await
        .expect("recall-mcp host port");
    let mcp_endpoint = format!("http://127.0.0.1:{mcp_host_port}/mcp");

    let resp = wait_tools_list(&mcp_endpoint).await;
    let tools = resp
        .pointer("/result/tools")
        .and_then(|t| t.as_array())
        .expect("tools/list returns a tools array");
    assert_eq!(
        tools.len(),
        6,
        "expected exactly six tools from the in-image recall-mcp, got {}",
        tools.len()
    );
    let names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .collect();
    for expected in ["recall", "remember", "get", "retire", "delete", "capabilities"] {
        assert!(names.contains(&expected), "tools/list missing {expected}");
    }
    // Stop the MCP container before booting the REST one (keeps the daemon footprint small).
    drop(mcp);

    // --- recall: boots against Dex, /healthz returns 200 {status: live} -----------------------
    let rest = recall_container(&network.name, "recall")
        .with_mapped_port(0, RECALL_HTTP_PORT.tcp())
        .start()
        .await
        .expect("start recall container");
    let rest_host_port = rest
        .get_host_port_ipv4(RECALL_HTTP_PORT.tcp())
        .await
        .expect("recall host port");
    let rest_base = format!("http://127.0.0.1:{rest_host_port}");

    let health = wait_healthz(&rest_base).await;
    assert_eq!(
        health.pointer("/data/status").and_then(|v| v.as_str()),
        Some("live"),
        "GET /healthz did not report status=live: {health}"
    );

    eprintln!(
        "container test: {IMAGE} booted as recall-mcp (six tools) and recall (/healthz live) against Dex"
    );
    // `rest`, `dex`, and `network` drop here, tearing everything down.
}
