//! Thin `reqwest`-based HTTP adapters for the provider traits (§2B: provider adapters carry no
//! domain logic; their contracts are the traits in §2C.6). recall is LLM-free (ADR-015) and makes no
//! outbound broker call (ADR-014): the only outbound providers are the embedding and reranker model
//! inferences. PII detection is in-process (`LocalPiiDetector`), not an HTTP adapter.
//!
//! ## Wire contracts (the JSON shapes the wiremock stubs honour)
//!
//! Each POSTs to the configured base URL with `Content-Type: application/json` and, where an API key
//! is configured, an `Authorization: Bearer <key>` header. The per-call timeout is uniform
//! ([`DEFAULT_TIMEOUT`]); a `reqwest` timeout maps to `ProviderError::Timeout`, a non-2xx status to
//! `ProviderError::Status(code)`, a transport failure to `ProviderError::Transport`, and a malformed
//! / unexpected body to `ProviderError::Malformed`.
//!
//! ### Embedding — `POST {RECALL_EMBED_URL}/embeddings`
//! Request:  `{ "model": "<RECALL_EMBED_MODEL_VERSION>", "input": ["text", ...] }`
//! Response: `{ "embeddings": [[f32, ...], ...] }` — one vector per input, each of `RECALL_EMBED_DIM`.
//!
//! ### Rerank — `POST {RECALL_RERANK_URL}/rerank`
//! Request:  `{ "query": "..", "documents": ["..", ..] }`
//! Response: `{ "scores": [f64, ..] }` — one relevance score per document, positionally aligned.
//!
//! (Fact extraction and consolidation are agent-side; PII detection is in-process — no HTTP contracts.)
//!

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;

use crate::config::Config;
use crate::types::ports::{
    EmbeddingClient, PiiDetector, PiiSpan, ProviderError, RerankClient,
};

/// Default per-call timeout for a provider HTTP request. Each external call carries its own timeout
/// (C4 Performance note) so the per-job budget is bounded even when a provider hangs.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a `reqwest` client with a per-call timeout. Used by every adapter so timeouts are uniform.
fn build_client(timeout: Duration) -> Client {
    Client::builder()
        .timeout(timeout)
        .build()
        // A client-build failure is a bootstrap-level fault; fall back to a default client so the
        // process can still start and surface the real error on first use.
        .unwrap_or_default()
}

/// Map a `reqwest` transport/timeout error to a `ProviderError`: a timeout is distinguished (504),
/// every other transport failure is a 502-class `Transport` error.
fn map_reqwest_err(provider: &str, e: reqwest::Error) -> ProviderError {
    if e.is_timeout() {
        ProviderError::Timeout
    } else {
        ProviderError::Transport(format!("{provider}: {e}"))
    }
}

/// HTTP adapter for the embedding provider (C4/C6).
pub struct HttpEmbeddingClient {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl HttpEmbeddingClient {
    pub fn new(config: &Config) -> Self {
        Self {
            client: build_client(DEFAULT_TIMEOUT),
            base_url: config.embed_url.clone(),
            api_key: config.embed_api_key.expose().to_owned(),
            model: config.embed_model_version.clone(),
        }
    }
}

/// The embedding provider's response body: one vector per input text.
#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

#[async_trait]
impl EmbeddingClient for HttpEmbeddingClient {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, ProviderError> {
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));
        let body = json!({ "model": self.model, "input": texts });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| map_reqwest_err("embedding", e))?;
        if !resp.status().is_success() {
            return Err(ProviderError::Status(resp.status().as_u16()));
        }
        let parsed: EmbedResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Malformed(format!("embedding: {e}")))?;
        Ok(parsed.embeddings)
    }
}

/// HTTP adapter for the cross-encoder reranker (C6).
pub struct HttpRerankClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl HttpRerankClient {
    pub fn new(config: &Config) -> Self {
        Self {
            client: build_client(DEFAULT_TIMEOUT),
            base_url: config.rerank_url.clone(),
            api_key: config.rerank_api_key.expose().to_owned(),
        }
    }
}

/// The reranker's response body: one relevance score per input document, positionally aligned.
#[derive(Deserialize)]
struct RerankResponse {
    scores: Vec<f64>,
}

#[async_trait]
impl RerankClient for HttpRerankClient {
    /// Score `docs` against `query` with the cross-encoder (read-path inference #2, C6 step 5).
    ///
    /// `POST {RECALL_RERANK_URL}/rerank` with `{ "query": "...", "documents": ["..", ..] }`; the
    /// response `{ "scores": [f64, ..] }` is positionally aligned with `docs`. A `reqwest` timeout maps
    /// to `ProviderError::Timeout` (C6 degrades to stage-1 order); a non-2xx status to
    /// `ProviderError::Status`; a malformed body to `ProviderError::Malformed`.
    async fn rerank(&self, query: &str, docs: &[String]) -> Result<Vec<f64>, ProviderError> {
        let url = format!("{}/rerank", self.base_url.trim_end_matches('/'));
        let body = json!({ "query": query, "documents": docs });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| map_reqwest_err("rerank", e))?;
        if !resp.status().is_success() {
            return Err(ProviderError::Status(resp.status().as_u16()));
        }
        let parsed: RerankResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Malformed(format!("rerank: {e}")))?;
        Ok(parsed.scores)
    }
}

// HttpLlmClient removed by ADR-015 — recall is LLM-free: no fact extraction and no consolidation LLM
// call. The agent extracts/consolidates and submits structured agent-asserted content.
// HttpBrokerClient removed by ADR-014 — recall makes no outbound broker call; freshness is agent-side.

/// In-process deterministic PII detector (C4, ADR-015). `recall` holds no LLM and makes no outbound
/// call for PII: this scans each string leaf of the content for structured identifiers (email,
/// phone, credit-card-via-Luhn) by pattern, emitting a [`PiiSpan`] per match with a confidence keyed
/// to pattern strength. The `PiiDetector` trait remains a DI seam so a model-backed detector can be
/// injected; this is the default impl. Free-text names/addresses are out of scope (handled agent-side).
pub struct LocalPiiDetector;

impl LocalPiiDetector {
    pub fn new() -> Self {
        LocalPiiDetector
    }
}

impl Default for LocalPiiDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Compiled detectors, built once. `email` and a phone/card *candidate* matcher; phone vs card is
/// disambiguated by digit count (phone 7..=11, card 13..=19 + Luhn) so a card never reads as a phone.
fn pii_email_re() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}").unwrap())
}

/// A run of digits with common separators (spaces, dashes, dots, parens, leading +). Digit count is
/// checked in code to classify phone vs card and to reject short noise.
fn pii_digitrun_re() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\+?[0-9][0-9 .()\-]{5,}[0-9]").unwrap())
}

/// Luhn checksum over the digits of `s` (used to confirm a credit-card candidate).
fn luhn_ok(digits: &[u8]) -> bool {
    let mut sum = 0u32;
    let mut double = false;
    for &d in digits.iter().rev() {
        let mut v = u32::from(d);
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    sum.is_multiple_of(10)
}

/// Append a `PiiSpan` for every detector hit inside `s` located at `pointer`. Email and card are
/// high-confidence (redacted); phone is low-confidence (flagged for review). Overlapping lower-priority
/// matches are dropped so a single value is never double-spanned.
fn scan_string(pointer: &str, s: &str, out: &mut Vec<PiiSpan>) {
    let mut taken: Vec<(usize, usize)> = Vec::new();
    let overlaps = |taken: &[(usize, usize)], a: usize, b: usize| {
        taken.iter().any(|&(x, y)| a < y && x < b)
    };

    // Email — confidence 0.95 (redact).
    for m in pii_email_re().find_iter(s) {
        taken.push((m.start(), m.end()));
        out.push(PiiSpan {
            json_pointer: pointer.to_string(),
            start: m.start() as u32,
            end: m.end() as u32,
            pii_type: "email".to_string(),
            confidence: 0.95,
        });
    }

    // Digit runs → credit card (13..=19 digits + Luhn, 0.95 redact) or phone (7..=11 digits, 0.6 flag).
    for m in pii_digitrun_re().find_iter(s) {
        if overlaps(&taken, m.start(), m.end()) {
            continue;
        }
        let digits: Vec<u8> = m.as_str().bytes().filter(|b| b.is_ascii_digit()).map(|b| b - b'0').collect();
        let n = digits.len();
        let (pii_type, confidence) = if (13..=19).contains(&n) && luhn_ok(&digits) {
            ("credit_card", 0.95)
        } else if (7..=11).contains(&n) {
            ("phone", 0.6)
        } else {
            continue;
        };
        taken.push((m.start(), m.end()));
        out.push(PiiSpan {
            json_pointer: pointer.to_string(),
            start: m.start() as u32,
            end: m.end() as u32,
            pii_type: pii_type.to_string(),
            confidence,
        });
    }
}

/// Escape a JSON object key per RFC 6901 (`~` → `~0`, `/` → `~1`) for the pointer path.
fn rfc6901_escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

/// Walk the content, scanning every string leaf and building its RFC 6901 pointer.
fn collect_pii_spans(value: &serde_json::Value, pointer: &str, out: &mut Vec<PiiSpan>) {
    match value {
        serde_json::Value::String(s) => scan_string(pointer, s, out),
        serde_json::Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                collect_pii_spans(item, &format!("{pointer}/{i}"), out);
            }
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                collect_pii_spans(v, &format!("{pointer}/{}", rfc6901_escape(k)), out);
            }
        }
        _ => {}
    }
}

#[async_trait]
impl PiiDetector for LocalPiiDetector {
    async fn scan(&self, content: &serde_json::Value) -> Result<Vec<PiiSpan>, ProviderError> {
        let mut spans = Vec::new();
        collect_pii_spans(content, "", &mut spans);
        Ok(spans)
    }
}
