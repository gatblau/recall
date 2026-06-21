//! Row <-> domain conversion helpers for the SurrealDB 3.x store.
//!
//! SurrealDB 3.x dropped the serde-based bind/take ergonomics: `bind` takes a value implementing
//! `SurrealValue` and `take` returns a `SurrealValue`. `serde_json::Value` implements `SurrealValue`,
//! so it is the bridge in both directions:
//!
//! * **Write** — domain records are turned into a native `surrealdb::types::Object` so datetime,
//!   numeric, and embedding fields are stored as their native SurrealDB kinds (native `datetime` is
//!   required for the bi-temporal `valid_to IS NONE` / `valid_from <= valid_at` comparisons to order
//!   correctly). Caller content is stored verbatim as an object.
//! * **Read** — a row is taken as `serde_json::Value` (SurrealDB renders `datetime` -> RFC3339 string,
//!   `record id` -> "table:key" string), then deserialised into the domain struct via serde; chrono's
//!   `DateTime<Utc>` parses the RFC3339 strings natively.

use serde_json::Value as Json;
use surrealdb::types::{Datetime, Number, Object, Value};

use crate::types::domain::{Entity, Fact, Relationship, Source};
use crate::types::ports::{AuditEntry, StoreError};

/// Convert a chrono UTC timestamp into a native SurrealDB datetime value.
fn dt(at: chrono::DateTime<chrono::Utc>) -> Value {
    Value::Datetime(Datetime::from(at))
}

/// Convert an `f64` into a native SurrealDB float value.
fn f(v: f64) -> Value {
    Value::Number(Number::Float(v))
}

/// Convert an owned string into a native SurrealDB string value.
fn s(v: String) -> Value {
    Value::String(v)
}

/// Convert an optional string into a native SurrealDB value (`NONE` when absent).
fn opt_s(v: Option<String>) -> Value {
    match v {
        Some(x) => Value::String(x),
        None => Value::None,
    }
}

/// Convert a list of strings into a native SurrealDB array value.
fn arr_s(v: &[String]) -> Value {
    Value::Array(surrealdb::types::Array::from(v.to_vec()))
}

/// Convert the owning scope into a native SurrealDB object value.
fn owner_obj(owner: &crate::types::scope::ScopeRef) -> Value {
    let mut o = Object::new();
    o.insert("tenant", Value::String(owner.tenant.clone()));
    o.insert(
        "team",
        match &owner.team {
            Some(t) => Value::String(t.clone()),
            None => Value::None,
        },
    );
    o.insert("user", Value::String(owner.user.clone()));
    Value::Object(o)
}

/// Convert a caller-supplied `serde_json::Value` (an assertion object) into a native SurrealDB value.
fn json_to_value(j: &Json) -> Value {
    use surrealdb::types::SurrealValue;
    j.clone().into_value()
}

/// Render a JSON content object to a flat keyword string for the BM25 `content_text` index: every
/// scalar leaf concatenated with spaces. The original structured `content` object is stored intact.
pub fn content_to_text(content: &Json) -> String {
    let mut out = String::new();
    fn walk(v: &Json, out: &mut String) {
        match v {
            Json::String(x) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(x);
            }
            Json::Number(x) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(&x.to_string());
            }
            Json::Bool(x) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(if *x { "true" } else { "false" });
            }
            Json::Array(a) => a.iter().for_each(|e| walk(e, out)),
            Json::Object(m) => m.values().for_each(|e| walk(e, out)),
            Json::Null => {}
        }
    }
    walk(content, &mut out);
    out
}

/// Build the native row object for a `Fact` (without the `id`, which is the record key). The optional
/// `embedding`/`embedding_model` columns are written only when present on the persisted struct path;
/// `put_fact` writes them as `NONE` and C4/C7 fill them via `set_fact_embedding`.
pub fn fact_to_object(fc: &Fact) -> Object {
    let mut o = Object::new();
    o.insert("content", json_to_value(&fc.content));
    o.insert("content_text", s(content_to_text(&fc.content)));
    o.insert("entities", arr_s(&fc.entities));
    o.insert("source_id", opt_s(fc.source_id.clone()));
    o.insert("memory_class", s(memory_class_str(fc.memory_class).to_string()));
    o.insert("visibility", s(visibility_str(fc.visibility).to_string()));
    o.insert("owner", owner_obj(&fc.owner));
    o.insert("valid_from", dt(fc.valid_from));
    o.insert(
        "valid_to",
        match fc.valid_to {
            Some(t) => dt(t),
            None => Value::None,
        },
    );
    o.insert("ingested_at", dt(fc.ingested_at));
    o.insert("confidence", f(fc.confidence));
    o.insert("salience", f(fc.salience));
    o.insert("stability", f(fc.stability));
    o.insert("pii_review", Value::Bool(fc.pii_review));
    o.insert("supersedes", opt_s(fc.supersedes.clone()));
    o.insert("superseded_by", opt_s(fc.superseded_by.clone()));
    o.insert("derived_from", arr_s(&fc.derived_from));
    o.insert(
        "last_recalled_at",
        match fc.last_recalled_at {
            Some(t) => dt(t),
            None => Value::None,
        },
    );
    o
}

/// Build the native row object for an `Entity`.
pub fn entity_to_object(e: &Entity) -> Object {
    let mut o = Object::new();
    o.insert("canonical_name", s(e.canonical_name.clone()));
    o.insert("aliases", arr_s(&e.aliases));
    o.insert("owner", owner_obj(&e.owner));
    o
}

/// Build the native row object for a `Relationship`. `from`/`to` are persisted as `from_ent`/`to_ent`
/// plain string entity ids (the spec's `from`/`to` collide with SurrealQL reserved words).
pub fn relationship_to_object(r: &Relationship) -> Object {
    let mut o = Object::new();
    o.insert("kind", s(r.kind.clone()));
    o.insert("from_ent", s(r.from.clone()));
    o.insert("to_ent", s(r.to.clone()));
    o.insert("valid_from", dt(r.valid_from));
    o.insert(
        "valid_to",
        match r.valid_to {
            Some(t) => dt(t),
            None => Value::None,
        },
    );
    o.insert("ingested_at", dt(r.ingested_at));
    o.insert("confidence", f(r.confidence));
    o.insert("source_id", opt_s(r.source_id.clone()));
    o.insert("owner", owner_obj(&r.owner));
    o
}

/// Build the native row object for a `Source`.
pub fn source_to_object(src: &Source) -> Object {
    let mut o = Object::new();
    o.insert("origin_ref", s(src.origin_ref.clone()));
    o.insert("modification_marker", opt_s(src.modification_marker.clone()));
    o.insert("trust_signal", f(src.trust_signal));
    o.insert("owner", owner_obj(&src.owner));
    o
}

/// Build the native row object for an `AuditEntry`.
pub fn audit_to_object(e: &AuditEntry) -> Object {
    let mut o = Object::new();
    o.insert("subject", s(e.subject.clone()));
    o.insert("operation", s(e.operation.clone()));
    o.insert("scope", owner_obj(&e.scope));
    o.insert("outcome", s(e.outcome.clone()));
    o.insert("token_jti", s(e.token_jti.clone()));
    o.insert("correlation_id", s(e.correlation_id.clone()));
    o.insert("at", dt(e.at));
    o
}

/// The canonical kebab-case string for a `MemoryClass` (matches the serde rename used on the wire).
pub fn memory_class_str(c: crate::types::domain::MemoryClass) -> &'static str {
    use crate::types::domain::MemoryClass::*;
    match c {
        Episodic => "episodic",
        Semantic => "semantic",
        Consolidated => "consolidated",
    }
}

/// The canonical kebab-case string for a `Visibility` (matches the serde rename used on the wire).
pub fn visibility_str(v: crate::types::domain::Visibility) -> &'static str {
    use crate::types::domain::Visibility::*;
    match v {
        UserPrivate => "user-private",
        TeamShared => "team-shared",
        TenantShared => "tenant-shared",
    }
}

/// Normalise a row's `id` JSON value to the canonical "table:key" string. SurrealDB renders a record
/// id as that string already, so this strips any surrounding angle brackets the SQL form may add.
fn normalise_id(raw: &Json) -> Option<String> {
    raw.as_str()
        .map(|s| s.replace(['⟨', '⟩', '`'], "").trim().to_string())
}

/// Deserialise a fact row (taken as JSON) into a domain `Fact`. The persistence-only `embedding`,
/// `embedding_model`, and `content_text` columns are not part of the JSON struct and are ignored.
pub fn row_to_fact(mut row: Json) -> Result<Fact, StoreError> {
    fix_id(&mut row);
    if let Some(obj) = row.as_object_mut() {
        obj.remove("embedding");
        obj.remove("embedding_model");
        obj.remove("content_text");
    }
    serde_json::from_value(row).map_err(|e| StoreError::Internal(format!("decode fact: {e}")))
}

/// Deserialise an entity row into a domain `Entity`.
pub fn row_to_entity(mut row: Json) -> Result<Entity, StoreError> {
    fix_id(&mut row);
    serde_json::from_value(row).map_err(|e| StoreError::Internal(format!("decode entity: {e}")))
}

/// Deserialise a relationship row into a domain `Relationship`, mapping `from_ent`/`to_ent` back to
/// the struct's `from`/`to` fields.
pub fn row_to_relationship(mut row: Json) -> Result<Relationship, StoreError> {
    fix_id(&mut row);
    if let Some(obj) = row.as_object_mut() {
        if let Some(v) = obj.remove("from_ent") {
            obj.insert("from".to_string(), v);
        }
        if let Some(v) = obj.remove("to_ent") {
            obj.insert("to".to_string(), v);
        }
    }
    serde_json::from_value(row)
        .map_err(|e| StoreError::Internal(format!("decode relationship: {e}")))
}

/// Deserialise a source row into a domain `Source`.
pub fn row_to_source(mut row: Json) -> Result<Source, StoreError> {
    fix_id(&mut row);
    serde_json::from_value(row).map_err(|e| StoreError::Internal(format!("decode source: {e}")))
}

/// Normalise the `id` field of a row in place to a clean "table:key" string.
fn fix_id(row: &mut Json) {
    if let Some(obj) = row.as_object_mut() {
        if let Some(id) = obj.get("id") {
            if let Some(norm) = normalise_id(id) {
                obj.insert("id".to_string(), Json::String(norm));
            }
        }
    }
}
