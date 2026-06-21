//! Domain entities (§2C.2).
//!
//! Used by: C1, C4, C6, C7 (and C8 for GET fact).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::scope::ScopeRef;

#[derive(Serialize, Deserialize, Clone)]
pub struct Fact {
    /// "fact:<uuidv7>" (SA-ID-01).
    pub id: String,
    /// Structured assertion (object), not free text.
    pub content: serde_json::Value,
    /// Entity ids this fact connects (>=1).
    pub entities: Vec<String>,
    /// "source:<uuidv7>" provenance, null for agent-stated.
    pub source_id: Option<String>,
    pub memory_class: MemoryClass,
    pub visibility: Visibility,
    /// Owning (tenant, team, user).
    pub owner: ScopeRef,
    /// RFC3339 ms (SA-TIME-01).
    pub valid_from: DateTime<Utc>,
    /// Null = currently true (open interval).
    pub valid_to: Option<DateTime<Utc>>,
    /// Server-set.
    pub ingested_at: DateTime<Utc>,
    /// [0,1] (SA-SCORE-01).
    pub confidence: f64,
    /// [0,1].
    pub salience: f64,
    /// Decay stability `s` (SA-DECAY-01), >=0.0.
    pub stability: f64,
    /// True => low-confidence PII flagged for review (SA-PII-01).
    pub pii_review: bool,
    /// Fact id this one replaces.
    pub supersedes: Option<String>,
    /// Fact id replacing this one.
    pub superseded_by: Option<String>,
    /// Source fact ids (consolidated insights only).
    pub derived_from: Vec<String>,
    pub last_recalled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Entity {
    /// "entity:<uuidv7>".
    pub id: String,
    /// 1..=512 chars, non-empty.
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub owner: ScopeRef,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Relationship {
    /// "relationship:<uuidv7>".
    pub id: String,
    /// Typed edge label, e.g. "owns" (1..=128 chars).
    pub kind: String,
    /// Entity id.
    pub from: String,
    /// Entity id.
    pub to: String,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub ingested_at: DateTime<Utc>,
    /// [0,1].
    pub confidence: f64,
    pub source_id: Option<String>,
    pub owner: ScopeRef,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Source {
    /// "source:<uuidv7>".
    pub id: String,
    /// Document/system handle, opaque to recall.
    pub origin_ref: String,
    /// ETag / Last-Modified token for freshness.
    pub modification_marker: Option<String>,
    /// [0,1] prior trust of this source.
    pub trust_signal: f64,
    pub owner: ScopeRef,
}

/// Procedural rejected (SA-CLASS-01).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryClass {
    Episodic,
    Semantic,
    Consolidated,
}

/// (SA-VIS-01).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Visibility {
    UserPrivate,
    TeamShared,
    TenantShared,
}
