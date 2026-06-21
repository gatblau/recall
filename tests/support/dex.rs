//! Real Dex OIDC issuer for the C3 integration suite, via testcontainers.
//!
//! Starts a genuine `dexidp/dex` container with an in-memory storage config that enables the password
//! connector (ROPC), a static client, and a static bcrypt-hashed user carrying `groups`. C3 then runs
//! its real discovery + JWKS fetch and real RS256 signature/claim validation against Dex-minted
//! `id_token`s. Dex does not natively emit a custom `tenant` claim, so the tenant-claim mapping case
//! is carried by the local issuer (`issuer.rs`); Dex covers crypto, discovery, JWKS, and the
//! sub/groups/aud/exp/alg path.
//!
//! Docker may be absent; callers treat a `None` return from [`start_dex`] as a graceful skip.

#![allow(dead_code)]

use std::collections::HashMap;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Pinned Dex image tag (a real v2.x release).
pub const DEX_IMAGE: &str = "dexidp/dex";
pub const DEX_TAG: &str = "v2.41.1";

/// Fixed host port the Dex web server is mapped to, so the issuer URL is known *before* the container
/// starts (Dex validates that minted-token `iss` matches its configured issuer, so the issuer cannot
/// depend on a post-start ephemeral mapping).
pub const DEX_HOST_PORT: u16 = 35357;
/// Dex's in-container HTTP port (its default `web.http` listener).
pub const DEX_CONTAINER_PORT: u16 = 5556;

/// The static client id (= the expected `aud`) and secret used for the ROPC grant.
pub const DEX_CLIENT_ID: &str = "recall-test";
pub const DEX_CLIENT_SECRET: &str = "recall-test-secret";
/// The static test user's credentials and identity.
pub const DEX_USERNAME: &str = "tester@example.com";
pub const DEX_PASSWORD: &str = "password123";
pub const DEX_USER_ID: &str = "08a8684b-db88-4b73-90a9-3cd1661f5466";

/// A running Dex container plus its resolved issuer URL.
pub struct DexInstance {
    pub issuer: String,
    _container: ContainerAsync<GenericImage>,
}

/// The Dex config document, parameterised by the issuer URL. Enables the password DB (ROPC) and a
/// static client + user. The bcrypt hash is for [`DEX_PASSWORD`] (`$2y$` is accepted by Go's bcrypt).
fn dex_config(issuer: &str) -> String {
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
  - email: "{username}"
    hash: "$2y$10$IcAwXtH3RsRsQlnPSfThfuQ7ghbofQERhhYolr/WRXlCxmswGvzTW"
    username: "tester"
    userID: "{user_id}"
staticClients:
  - id: {client_id}
    secret: {client_secret}
    name: "Recall Test"
    redirectURIs:
      - "http://127.0.0.1/callback"
"#,
        issuer = issuer,
        port = DEX_CONTAINER_PORT,
        username = DEX_USERNAME,
        user_id = DEX_USER_ID,
        client_id = DEX_CLIENT_ID,
        client_secret = DEX_CLIENT_SECRET,
    )
}

/// Start Dex, returning `None` if the container cannot start (Docker absent / image unpullable), so
/// the caller can skip the Dex-specific scenarios gracefully.
pub async fn start_dex() -> Option<DexInstance> {
    let issuer = format!("http://127.0.0.1:{DEX_HOST_PORT}/dex");
    let config = dex_config(&issuer);

    let image = GenericImage::new(DEX_IMAGE, DEX_TAG)
        // Dex v2.41.x logs "listening on" (server=http) to stderr once the web server is bound.
        .with_wait_for(WaitFor::message_on_stderr("listening on"))
        .with_mapped_port(DEX_HOST_PORT, DEX_CONTAINER_PORT.tcp())
        .with_copy_to(
            "/etc/dex/config.docker.yaml",
            config.into_bytes(),
        )
        .with_cmd(["dex", "serve", "/etc/dex/config.docker.yaml"]);

    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "SKIP Dex scenarios: could not start {DEX_IMAGE}:{DEX_TAG} container ({e}); docker may be absent"
            );
            return None;
        }
    };

    // Poll discovery on the fixed host port until Dex answers.
    if !wait_discovery(&issuer).await {
        eprintln!("SKIP Dex scenarios: Dex discovery never became reachable at {issuer}");
        return None;
    }

    Some(DexInstance {
        issuer,
        _container: container,
    })
}

/// Obtain a real `id_token` from Dex via the ROPC token endpoint (`grant_type=password`).
/// Returns the raw JWT string on success.
pub async fn dex_password_token(issuer: &str) -> Result<String, String> {
    let token_url = format!("{issuer}/token");
    let mut form: HashMap<&str, &str> = HashMap::new();
    form.insert("grant_type", "password");
    form.insert("username", DEX_USERNAME);
    form.insert("password", DEX_PASSWORD);
    form.insert("scope", "openid groups email");
    form.insert("client_id", DEX_CLIENT_ID);
    form.insert("client_secret", DEX_CLIENT_SECRET);

    let resp = reqwest::Client::new()
        .post(&token_url)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("token request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("token endpoint returned {status}: {body}"));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("token response not json: {e}"))?;
    body.get("id_token")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "token response missing id_token".to_string())
}

async fn wait_discovery(issuer: &str) -> bool {
    let client = reqwest::Client::new();
    let url = format!("{issuer}/.well-known/openid-configuration");
    for _ in 0..100 {
        if let Ok(r) = client.get(&url).send().await {
            if r.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}
