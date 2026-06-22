//! Thin `reqwest`-based HTTP adapters for the five provider traits (§2B: provider adapters carry no
//! domain logic; their contracts are the traits in §2C.6).
//!
//! Phase 1 wired the adapter structs, their construction (base URL, API key, per-call timeout) and
//! the trait surface. Phase 5 (C4 Write Pipeline) implements the wire bodies for the three providers
//! the write pipeline consumes — `EmbeddingClient::embed`, `LlmClient::extract`,
//! `PiiDetector::scan` — as real `reqwest` POST calls with concrete, documented JSON request/response
//! shapes (these ARE the wire contract the integration-suite wiremock stubs honour). Phase 7
//! (C6 Retrieval Engine) wires `RerankClient::rerank` (the cross-encoder); Phase 8 (C7 Maintenance
//! Worker) wires `LlmClient::consolidate` (episodic->semantic consolidation). Every provider is now
//! wired — none remains a skeleton. (The Faraday broker adapter was removed by ADR-014 — freshness is
//! agent-side; recall makes no outbound broker call.)
//!
//! ## Wire contracts (the JSON shapes the wiremock stubs honour)
//!
//! All three POST to the configured base URL with `Content-Type: application/json` and, where an API
//! key is configured, an `Authorization: Bearer <key>` header. The per-call timeout is uniform
//! ([`DEFAULT_TIMEOUT`]); a `reqwest` timeout maps to `ProviderError::Timeout`, a non-2xx status to
//! `ProviderError::Status(code)`, a transport failure to `ProviderError::Transport`, and a malformed
//! / unexpected body to `ProviderError::Malformed`.
//!
//! ### Embedding — `POST {RECALL_EMBED_URL}/embeddings`
//! Request:  `{ "model": "<RECALL_EMBED_MODEL_VERSION>", "input": ["text", ...] }`
//! Response: `{ "embeddings": [[f32, ...], ...] }` — one vector per input, each of `RECALL_EMBED_DIM`.
//!
//! ### LLM extract — `POST {RECALL_LLM_URL}/extract`
//! Request:  `{ "content": <json object> }`
//! Response: `{ "facts": [ { "content": <json object>,
//!                            "entity_mentions": [ { "surface_form": "..",
//!                                                   "mention_type": "person"|null } ],
//!                            "memory_class": "episodic"|"semantic"|"consolidated",
//!                            "asserted_valid_from": "<rfc3339>"|null,
//!                            "extractor_confidence": f64 } ] }`
//!
//! ### PII scan — `POST {RECALL_PII_URL or RECALL_LLM_URL}/pii/scan`
//! Request:  `{ "content": <json object> }`
//! Response: `{ "spans": [ { "json_pointer": "/path", "start": u32, "end": u32,
//!                            "pii_type": "email"|"phone"|.., "confidence": f64 } ] }`
//!
//! ### Rerank — `POST {RECALL_RERANK_URL}/rerank`
//! Request:  `{ "query": "..", "documents": ["..", ..] }`
//! Response: `{ "scores": [f64, ..] }` — one relevance score per document, positionally aligned.
//!
//! ### LLM consolidate — `POST {RECALL_LLM_URL}/consolidate`
//! Request:  `{ "episodes": [<Fact as JSON>, ...] }` — the recent episodic facts of one subject group.
//! Response: `{ "insights": [ { "content": <json object>,
//!                              "entities": ["entity:..", ..],
//!                              "derived_from": ["fact:..", ..],
//!                              "confidence": f64,
//!                              "support_count": u32 } ] }` — one proposed semantic insight per element,
//!           mapped onto [`InsightCandidate`]; the worker validates each against its sources before
//!           promotion (C7).
//!

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use crate::config::Config;
use crate::types::domain::{Fact, MemoryClass};
use crate::types::ports::{
    EmbeddingClient, EntityMention, ExtractedFact, InsightCandidate, LlmClient, PiiDetector, PiiSpan,
    ProviderError, RerankClient,
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

/// HTTP adapter for the extraction/consolidation LLM (C4/C7, async path only).
pub struct HttpLlmClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl HttpLlmClient {
    pub fn new(config: &Config) -> Self {
        Self {
            client: build_client(DEFAULT_TIMEOUT),
            base_url: config.llm_url.clone(),
            api_key: config.llm_api_key.expose().to_owned(),
        }
    }
}

/// The LLM extract response body: a list of extracted facts in the wire shape.
#[derive(Deserialize)]
struct ExtractResponse {
    facts: Vec<WireExtractedFact>,
}

/// The wire shape of one extracted fact. Mapped onto the canonical [`ExtractedFact`] domain type,
/// whose field names differ (the C4 spec names `entities`/`confidence`; the wire uses the spec's
/// public-interface names `entity_mentions`/`extractor_confidence`/`memory_class`/`asserted_valid_from`).
#[derive(Deserialize)]
struct WireExtractedFact {
    content: serde_json::Value,
    #[serde(default)]
    entity_mentions: Vec<WireEntityMention>,
    #[serde(default)]
    memory_class: Option<String>,
    #[serde(default)]
    extractor_confidence: f64,
}

/// The wire shape of one entity mention.
#[derive(Deserialize)]
struct WireEntityMention {
    surface_form: String,
    #[serde(default)]
    mention_type: Option<String>,
}

/// The LLM consolidate response body: a list of proposed semantic insights in the wire shape.
#[derive(Deserialize)]
struct ConsolidateResponse {
    #[serde(default)]
    insights: Vec<WireInsight>,
}

/// The wire shape of one consolidated insight (field names match the [`InsightCandidate`] type).
#[derive(Deserialize)]
struct WireInsight {
    content: serde_json::Value,
    #[serde(default)]
    entities: Vec<String>,
    #[serde(default)]
    derived_from: Vec<String>,
    #[serde(default)]
    confidence: f64,
    #[serde(default)]
    support_count: u32,
}

/// Parse a kebab-case `memory_class` string from the wire; defaults to `episodic` when absent or
/// unrecognised (the safest class — episodic facts decay rather than persisting as durable semantics).
fn parse_memory_class(raw: Option<&str>) -> MemoryClass {
    match raw {
        Some("semantic") => MemoryClass::Semantic,
        Some("consolidated") => MemoryClass::Consolidated,
        _ => MemoryClass::Episodic,
    }
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    async fn extract(
        &self,
        content: &serde_json::Value,
    ) -> Result<Vec<ExtractedFact>, ProviderError> {
        let url = format!("{}/extract", self.base_url.trim_end_matches('/'));
        let body = json!({ "content": content });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| map_reqwest_err("llm.extract", e))?;
        if !resp.status().is_success() {
            return Err(ProviderError::Status(resp.status().as_u16()));
        }
        let parsed: ExtractResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Malformed(format!("llm.extract: {e}")))?;
        Ok(parsed
            .facts
            .into_iter()
            .map(|wf| ExtractedFact {
                content: wf.content,
                entities: wf
                    .entity_mentions
                    .into_iter()
                    .map(|m| EntityMention {
                        surface: m.surface_form,
                        canonical_name: m.mention_type,
                    })
                    .collect(),
                memory_class: parse_memory_class(wf.memory_class.as_deref()),
                confidence: wf.extractor_confidence,
            })
            .collect())
    }

    /// Distil a group of recent episodic facts into proposed semantic insights (C7 consolidation duty).
    ///
    /// `POST {RECALL_LLM_URL}/consolidate` with `{ "episodes": [<Fact as JSON>, ...] }`; the response
    /// `{ "insights": [ { content, entities, derived_from, confidence, support_count } ] }` is mapped
    /// onto [`InsightCandidate`]. A `reqwest` timeout maps to `ProviderError::Timeout`; a non-2xx status
    /// to `ProviderError::Status`; a malformed body to `ProviderError::Malformed`. The worker validates
    /// each candidate against its sources before promotion.
    async fn consolidate(
        &self,
        episodes: &[Fact],
    ) -> Result<Vec<InsightCandidate>, ProviderError> {
        let url = format!("{}/consolidate", self.base_url.trim_end_matches('/'));
        let body = json!({ "episodes": episodes });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| map_reqwest_err("llm.consolidate", e))?;
        if !resp.status().is_success() {
            return Err(ProviderError::Status(resp.status().as_u16()));
        }
        let parsed: ConsolidateResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Malformed(format!("llm.consolidate: {e}")))?;
        Ok(parsed
            .insights
            .into_iter()
            .map(|wi| InsightCandidate {
                content: wi.content,
                entities: wi.entities,
                derived_from: wi.derived_from,
                confidence: wi.confidence,
                support_count: wi.support_count,
            })
            .collect())
    }
}

// HttpBrokerClient removed by ADR-014 — recall makes no outbound broker call; freshness is agent-side.

/// PII detector adapter (C4). Model/heuristic adapter; HTTP-backed. There is no dedicated PII config
/// key in §2D, so the detector POSTs to the LLM provider base URL under a `/pii/scan` path with the
/// LLM API key (the PII model is co-located with the extraction model in v1).
pub struct HttpPiiDetector {
    client: Client,
    base_url: String,
    api_key: String,
}

impl HttpPiiDetector {
    pub fn new(config: &Config) -> Self {
        Self {
            client: build_client(DEFAULT_TIMEOUT),
            base_url: config.llm_url.clone(),
            api_key: config.llm_api_key.expose().to_owned(),
        }
    }
}

/// The PII scan response body: the detected spans.
#[derive(Deserialize)]
struct PiiResponse {
    #[serde(default)]
    spans: Vec<WirePiiSpan>,
}

/// The wire shape of one PII span (field names match the [`PiiSpan`] domain type exactly).
#[derive(Deserialize)]
struct WirePiiSpan {
    json_pointer: String,
    start: u32,
    end: u32,
    pii_type: String,
    confidence: f64,
}

#[async_trait]
impl PiiDetector for HttpPiiDetector {
    async fn scan(&self, content: &serde_json::Value) -> Result<Vec<PiiSpan>, ProviderError> {
        let url = format!("{}/pii/scan", self.base_url.trim_end_matches('/'));
        let body = json!({ "content": content });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| map_reqwest_err("pii", e))?;
        if !resp.status().is_success() {
            return Err(ProviderError::Status(resp.status().as_u16()));
        }
        let parsed: PiiResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Malformed(format!("pii: {e}")))?;
        Ok(parsed
            .spans
            .into_iter()
            .map(|w| PiiSpan {
                json_pointer: w.json_pointer,
                start: w.start,
                end: w.end,
                pii_type: w.pii_type,
                confidence: w.confidence,
            })
            .collect())
    }
}
