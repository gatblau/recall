//! C8 — `GET /openapi.json`. A hand-built OpenAPI 3.1 document enumerating the public routes
//! (SA-VER-01). It is the contract document, so it is NOT wrapped in the success envelope and is served
//! with `content-type: application/json`. The document is deliberately hand-authored rather than
//! derived from the handler types (a documented C8 deviation / follow-up); it enumerates each route,
//! its method, its operation id, and its success status.

use serde_json::{json, Value};

/// Build the OpenAPI 3.1 document for the `recall` HTTP surface.
pub fn document(service: &str, version: &str) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": service,
            "version": version,
            "description": "recall — agentic memory service HTTP API"
        },
        "paths": {
            "/v1": {
                "get": {
                    "operationId": "capabilities",
                    "summary": "Service capabilities",
                    "responses": { "200": { "description": "Success<Capabilities>" } }
                }
            },
            "/v1/recall": {
                "post": {
                    "operationId": "recall",
                    "summary": "Recall ranked facts (synchronous read path)",
                    "responses": {
                        "200": { "description": "Success<RecallResponse>" },
                        "400": { "description": "VAL_INVALID_BODY | VAL_OUT_OF_RANGE | VAL_UNSUPPORTED_CLASS" },
                        "401": { "description": "AUTH_MISSING_TOKEN | AUTH_INVALID_TOKEN" },
                        "403": { "description": "AUTH_INSUFFICIENT_SCOPE" },
                        "413": { "description": "VAL_BODY_TOO_LARGE" },
                        "429": { "description": "RATE_LIMITED" }
                    }
                }
            },
            "/v1/memories": {
                "post": {
                    "operationId": "remember",
                    "summary": "Enqueue a fact-extraction job (async)",
                    "responses": {
                        "202": { "description": "Success<WriteAck>" },
                        "400": { "description": "VAL_INVALID_BODY | VAL_MISSING_IDEMPOTENCY_KEY" },
                        "503": { "description": "QUEUE_UNAVAILABLE" }
                    }
                }
            },
            "/v1/memories/{id}": {
                "get": {
                    "operationId": "get_fact",
                    "summary": "Fetch a fact by id (conditional, ETag)",
                    "responses": {
                        "200": { "description": "Success<Fact>" },
                        "304": { "description": "Not Modified" },
                        "404": { "description": "NOT_FOUND" }
                    }
                },
                "delete": {
                    "operationId": "delete",
                    "summary": "Verifiable hard delete (returns a deletion proof)",
                    "responses": {
                        "200": { "description": "Success<DeletionProof>" },
                        "404": { "description": "NOT_FOUND" }
                    }
                }
            },
            "/v1/memories/{id}/retire": {
                "post": {
                    "operationId": "retire",
                    "summary": "End a fact's validity (non-destructive)",
                    "responses": {
                        "200": { "description": "Success<RetireAck>" },
                        "404": { "description": "NOT_FOUND" }
                    }
                }
            },
            "/openapi.json": {
                "get": {
                    "operationId": "openapi",
                    "summary": "This OpenAPI document",
                    "responses": { "200": { "description": "OpenAPI 3.1 JSON" } }
                }
            }
        }
    })
}
