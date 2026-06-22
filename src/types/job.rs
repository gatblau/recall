//! Work queue types (§2C.5).
//!
//! Used by: C2 (transport), C4/C5/C7/C8 (produce/consume).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::scope::ScopeRef;

#[derive(Serialize, Deserialize, Clone)]
pub struct WorkJob {
    /// "work_job:<uuidv7>".
    pub id: String,
    pub kind: JobKind,
    /// Kind-specific, validated by the consumer.
    pub payload: serde_json::Value,
    pub scope: ScopeRef,
    /// Present for API-originated writes.
    pub idempotency_key: Option<String>,
    pub attempts: u32,
    pub status: JobStatus,
    /// Backoff schedule (SA-QUEUE-02).
    pub not_before: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// Claim lease; null when unclaimed.
    pub leased_until: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    ExtractFact,
    ReEmbedFact,
    Consolidate,
    HardDelete,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Leased,
    Done,
    DeadLetter,
}
