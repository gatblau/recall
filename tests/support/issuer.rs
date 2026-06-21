//! Real-crypto local OIDC issuer for the C3 integration suite.
//!
//! This is **not** a mock of the validation path: it generates a real RSA keypair in-process, serves
//! a genuine `/.well-known/openid-configuration` and a genuine JWKS over HTTP (so C3 performs a real
//! discovery + JWKS fetch), and mints real RS256-signed JWTs. C3's `Authenticator` then runs its real
//! signature verification, alg-allowlist, iss/aud/exp/nbf checks, and claim mapping against it.
//!
//! It exists to cover the cases a stock Dex tag cannot emit natively — chiefly the custom `tenant`
//! claim — and to give the suite a deterministic, Docker-independent gate carrier. The Dex container
//! test (`tests/support/dex.rs`) still exercises a real production-grade IdP for everything Dex
//! supports (crypto, discovery, JWKS, sub/groups/aud/exp/alg).

#![allow(dead_code)]

use std::net::SocketAddr;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde_json::{json, Value};

/// The `kid` advertised by the local issuer's single signing key.
pub const LOCAL_KID: &str = "local-key-1";

/// A running local issuer: holds the keypair plus the HTTP server handle and its base URL (= issuer).
pub struct LocalIssuer {
    issuer: String,
    encoding_key: EncodingKey,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for LocalIssuer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Shared state served by the issuer's HTTP routes.
#[derive(Clone)]
struct IssuerState {
    issuer: String,
    jwks: Value,
}

impl LocalIssuer {
    /// Generate a fresh RSA-2048 keypair, bind an ephemeral port, and serve discovery + JWKS until
    /// dropped. The issuer URL is `http://127.0.0.1:<port>` (no path suffix).
    pub async fn start() -> LocalIssuer {
        // 2048-bit key: large enough for RS256, small enough to generate quickly in a test.
        let mut rng = rand::thread_rng();
        let private = RsaPrivateKey::new(&mut rng, 2048).expect("generate rsa key");
        let public = RsaPublicKey::from(&private);

        let pem = private
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .expect("encode pkcs1 pem");
        let encoding_key =
            EncodingKey::from_rsa_pem(pem.as_bytes()).expect("build encoding key from pem");

        let jwks = build_jwks(&public);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind local issuer port");
        let addr: SocketAddr = listener.local_addr().expect("local issuer addr");
        let issuer = format!("http://{addr}");

        let state = IssuerState {
            issuer: issuer.clone(),
            jwks,
        };
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks.json", get(jwks_route))
            .with_state(state);

        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Poll discovery until the server is accepting, so a caller does not race the bind.
        wait_ready(&issuer).await;

        LocalIssuer {
            issuer,
            encoding_key,
            handle,
        }
    }

    /// The issuer URL (= the `iss` claim and the discovery base).
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Mint a signed RS256 token with the given claims map. The `kid` header is set to [`LOCAL_KID`].
    /// `alg` lets a caller deliberately mint with a non-allowlisted algorithm header for the negative
    /// path (note: a forged `alg=none` token is produced separately, since it carries no signature).
    pub fn mint(&self, claims: &Value) -> String {
        self.mint_with_alg(claims, Algorithm::RS256)
    }

    /// Mint a token with an explicit algorithm header (still signed by the RSA key). Used to prove the
    /// allowlist rejects a token whose header declares a non-RSA family even when otherwise well-formed.
    pub fn mint_with_alg(&self, claims: &Value, alg: Algorithm) -> String {
        let mut header = Header::new(alg);
        header.kid = Some(LOCAL_KID.to_string());
        jsonwebtoken::encode(&header, claims, &self.encoding_key).expect("sign token")
    }

    /// Mint a token with a tampered signature: a valid RS256 token whose final signature segment has
    /// its last byte flipped, so signature verification must fail.
    pub fn mint_tampered(&self, claims: &Value) -> String {
        let token = self.mint(claims);
        let mut parts: Vec<&str> = token.split('.').collect();
        let sig = parts.pop().expect("signature segment");
        let mut bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(sig)
            .expect("decode signature");
        // Flip a bit in the last byte to invalidate the signature.
        if let Some(last) = bytes.last_mut() {
            *last ^= 0x01;
        }
        let tampered = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        format!("{}.{}.{}", parts[0], parts[1], tampered)
    }
}

/// Forge an unsigned `alg=none` token (header + payload, empty signature segment). This never touches
/// the issuer key — it models the classic downgrade attack the allowlist must reject before any
/// signature work.
pub fn forge_alg_none(claims: &Value) -> String {
    let header = json!({ "alg": "none", "typ": "JWT", "kid": LOCAL_KID });
    let enc = |v: &Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(v).expect("serialise"))
    };
    format!("{}.{}.", enc(&header), enc(claims))
}

/// Build a JWKS document for one RSA public key, advertising RS256 and [`LOCAL_KID`].
fn build_jwks(public: &RsaPublicKey) -> Value {
    let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
    let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
    json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": LOCAL_KID,
            "n": n,
            "e": e,
        }]
    })
}

async fn discovery(State(state): State<IssuerState>) -> Json<Value> {
    Json(json!({
        "issuer": state.issuer,
        "jwks_uri": format!("{}/jwks.json", state.issuer),
        "authorization_endpoint": format!("{}/auth", state.issuer),
        "token_endpoint": format!("{}/token", state.issuer),
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }))
}

async fn jwks_route(State(state): State<IssuerState>) -> Json<Value> {
    Json(state.jwks.clone())
}

async fn wait_ready(issuer: &str) {
    let client = reqwest::Client::new();
    let url = format!("{issuer}/.well-known/openid-configuration");
    for _ in 0..50 {
        if client.get(&url).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
