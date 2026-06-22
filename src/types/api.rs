//! API request/response payloads (§2C.4).
//!
//! Used by: C8 (HTTP), C6 (recall), C4 (remember).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::domain::{Fact, MemoryClass, Visibility};

#[derive(Deserialize)]
pub struct RecallRequest {
    /// 1..=4096 chars, non-empty.
    pub query: String,
    #[serde(default)]
    pub filters: RecallFilters,
    /// [1,50], default 10 (SA-CAP-01).
    #[serde(default = "default_result_cap")]
    pub result_cap: u8,
    /// Opaque, from a prior meta.next_cursor.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Opt-in: attach each sourced fact's `origin_ref` + `modification_marker` so the agent can
    /// check source freshness itself (SA-PROV-01, ADR-014). Default false keeps the response lean.
    #[serde(default)]
    pub include_provenance: bool,
}

#[derive(Deserialize, Default)]
pub struct RecallFilters {
    pub memory_class: Option<MemoryClass>,
    pub visibility: Option<Visibility>,
    /// Restrict to facts touching this entity id.
    pub entity: Option<String>,
    /// As-of query into the bi-temporal history.
    pub valid_at: Option<DateTime<Utc>>,
}

/// Wrapped in `Success<RecallResponse>`.
#[derive(Serialize)]
pub struct RecallResponse {
    pub facts: Vec<RankedFact>,
}

#[derive(Serialize)]
pub struct RankedFact {
    pub fact: Fact,
    /// Final ranking score [0,1].
    pub score: f64,
    /// Present only when the request set `include_provenance` and the fact cites a source
    /// (SA-PROV-01, ADR-014). Lets the agent run its own source-freshness check; recall asserts no
    /// freshness verdict of its own.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceProvenance>,
}

/// Returned per sourced fact when `include_provenance` is set (SA-PROV-01, ADR-014).
#[derive(Serialize)]
pub struct SourceProvenance {
    /// Document/system handle the agent resolves.
    pub origin_ref: String,
    /// ETag / Last-Modified token captured at write time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modification_marker: Option<String>,
}

#[derive(Deserialize)]
pub struct RememberRequest {
    /// Raw content to extract a fact from.
    pub content: serde_json::Value,
    pub source: Option<SourceInput>,
    /// True => caller asserts this directly.
    #[serde(default)]
    pub agent_stated: bool,
}

#[derive(Deserialize)]
pub struct SourceInput {
    pub origin_ref: String,
    pub modification_marker: Option<String>,
}

/// Async (ADR-004).
#[derive(Serialize)]
pub struct WriteAck {
    pub job_id: String,
    pub status: JobAckStatus,
}

/// AlreadyAccepted => idempotent replay.
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum JobAckStatus {
    Accepted,
    AlreadyAccepted,
}

/// Acknowledgement returned by `POST /v1/memories/{id}/retire` (C8). A replay within the idempotency
/// window returns the original `retired_at`.
#[derive(Serialize)]
pub struct RetireAck {
    pub record_id: String,
    pub retired_at: DateTime<Utc>,
}

/// Service capabilities returned by `GET /v1` (C8). `openapi` is the absolute URL of `/openapi.json`.
#[derive(Serialize)]
pub struct Capabilities {
    pub service: String,
    pub version: String,
    pub operations: Vec<String>,
    pub openapi: String,
}

#[derive(Serialize)]
pub struct DeletionProof {
    pub deleted_at: DateTime<Utc>,
    pub record_id: String,
    /// Ids of removed derived summaries.
    pub derived_removed: Vec<String>,
    pub embeddings_removed: u32,
    /// Sha256 hex over sorted removed ids (SA-DELETE-01).
    pub digest: String,
}

fn default_result_cap() -> u8 {
    10
}
