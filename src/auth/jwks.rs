//! OIDC discovery and JWKS fetch/parse for C3 Auth & Scope.
//!
//! Two network operations live here: discovery (GET `<issuer>/.well-known/openid-configuration` to
//! resolve `jwks_uri`) and the JWKS fetch (GET `jwks_uri`, parse the JWK set, index keys by `kid`).
//! Both run only at startup, on the background refresh tick, and on a rate-limited on-demand refresh —
//! never on the validation hot path. Each request carries a fixed 5 s timeout (a component-local
//! constant, not configuration). Error reasons name the failure kind and never include response bytes.

use std::collections::HashMap;

use jsonwebtoken::jwk::{AlgorithmParameters, Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey};
use serde::Deserialize;

use super::cache::CachedKey;
use super::map_alg_allowlist;

/// The discovery/JWKS HTTP request timeout (component-local constant, SA-JWKS-01).
pub const HTTP_TIMEOUT_SECS: u64 = 5;

/// The subset of the OIDC discovery document this component reads.
#[derive(Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

/// Build the discovery/JWKS HTTP client with the fixed 5 s timeout.
pub fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
        .build()
        // A client built only with a timeout cannot fail to construct in practice; fall back to the
        // default client so construction never panics in a library path.
        .unwrap_or_default()
}

/// Perform OIDC discovery: GET `<issuer>/.well-known/openid-configuration` and return `jwks_uri`.
///
/// The reason string names the failure kind (`status`, `transport`, `malformed`) and never contains
/// the response body bytes.
pub async fn discover_jwks_uri(
    http: &reqwest::Client,
    issuer: &str,
) -> Result<String, String> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("transport: {}", redact_reqwest(&e)))?;
    if !resp.status().is_success() {
        return Err(format!("status: {}", resp.status().as_u16()));
    }
    let doc: DiscoveryDocument = resp
        .json()
        .await
        .map_err(|_| "malformed: discovery document missing jwks_uri".to_string())?;
    if doc.jwks_uri.is_empty() {
        return Err("malformed: empty jwks_uri".to_string());
    }
    Ok(doc.jwks_uri)
}

/// Fetch the JWKS from `jwks_uri`, parse it, and index the supported keys by `kid`.
///
/// Keys whose `alg`/`kty` falls outside the C3 allowlist are skipped (forward compatibility); a key
/// with no `kid` is skipped (it cannot be matched against a token header). An empty or unparseable
/// key set, a non-2xx response, or a transport error is an error.
pub async fn fetch_jwks(
    http: &reqwest::Client,
    jwks_uri: &str,
) -> Result<HashMap<String, CachedKey>, String> {
    let resp = http
        .get(jwks_uri)
        .send()
        .await
        .map_err(|e| format!("transport: {}", redact_reqwest(&e)))?;
    if !resp.status().is_success() {
        return Err(format!("status: {}", resp.status().as_u16()));
    }
    let set: JwkSet = resp
        .json()
        .await
        .map_err(|_| "malformed: unparseable jwk set".to_string())?;
    let keys = index_keys(&set);
    if keys.is_empty() {
        return Err("malformed: jwk set contained no usable keys".to_string());
    }
    Ok(keys)
}

/// Index a parsed JWK set into `kid -> CachedKey`, keeping only allowlisted algorithms.
fn index_keys(set: &JwkSet) -> HashMap<String, CachedKey> {
    let mut out = HashMap::new();
    for jwk in &set.keys {
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        let Some(alg) = jwk_algorithm(jwk) else {
            continue;
        };
        // Restrict to the asymmetric allowlist (RS*/ES*); reject HS*/none defensively at parse time.
        if map_alg_allowlist(alg).is_none() {
            continue;
        }
        let Ok(decoding_key) = DecodingKey::from_jwk(jwk) else {
            continue;
        };
        out.insert(kid, CachedKey { decoding_key, alg });
    }
    out
}

/// Resolve the signing algorithm for a JWK from its declared `alg`, or infer a default per key type
/// when `alg` is absent (RSA -> RS256, EC -> ES256 — the common OIDC defaults).
fn jwk_algorithm(jwk: &Jwk) -> Option<Algorithm> {
    if let Some(alg) = jwk.common.key_algorithm {
        // `KeyAlgorithm`'s Display form is the JWA name (e.g. "RS256"), which `Algorithm` parses.
        use std::str::FromStr;
        return Algorithm::from_str(&alg.to_string()).ok();
    }
    match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::EllipticCurve(_) => Some(Algorithm::ES256),
        _ => None,
    }
}

/// Reduce a reqwest error to a coarse class, never surfacing a URL, header, or body.
fn redact_reqwest(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else if e.is_decode() {
        "decode"
    } else {
        "request"
    }
}
