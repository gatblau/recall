//! C10 — MCP API Edge. The second externally-reachable surface of `recall` (ADR-016), shipped as the
//! separate binary `recall-mcp`.
//!
//! It terminates a minimal **JSON-RPC 2.0 over HTTP** transport (the MCP streamable-HTTP request/
//! response subset — a single POST endpoint that accepts a JSON-RPC request and returns a JSON-RPC
//! response; no SSE, no server-initiated messages), advertises the six service operations through
//! native MCP tool discovery (`tools/list`), dispatches each `tools/call` to the matching C9
//! [`Service`](crate::service::Service) method, and renders the typed result or [`AppError`] as an MCP
//! tool result or MCP error.
//!
//! It carries the same broker-injected `Authorization: Bearer <OIDC JWT>` as the REST edge and passes
//! it straight to C9, so identity, scope, rate limiting, idempotency, and audit are exactly those of
//! the REST edge — there is no second copy of that logic (SA-SVC-01). It holds no domain logic and no
//! orchestration: every tool call is a call into C9. The edge adds no auth/rate/idempotency/audit.
//!
//! Correlation-id minting stays at the edge (per the C9 contract): a valid inbound `x-correlation-id`
//! is honoured, otherwise a fresh UUIDv4 is minted, and it is passed into C9 via [`CallContext`].

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{AppError, Env, ValidationKind};
use crate::service::{CallContext, CallError, CallResult, Service};

/// Header carrying the per-request correlation id (mirrors the REST edge constant).
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// JSON-RPC 2.0 error codes used by the edge. The MCP/registry `code` (the C9 `AppError::code()`
/// string) rides in `error.data.code`; the numeric `error.code` follows the JSON-RPC convention.
mod jsonrpc {
    /// Invalid Request — the envelope is not a well-formed JSON-RPC 2.0 request.
    pub const INVALID_REQUEST: i64 = -32600;
    /// Method not found — the JSON-RPC `method` is not one this edge serves.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Invalid params — `tools/call` named an unknown tool or supplied malformed params.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Application error — a tool executed and returned an [`super::AppError`]; the registry `code`
    /// is carried in `error.data.code`.
    pub const APPLICATION_ERROR: i64 = -32000;
}

/// The MCP protocol version advertised by the `initialize` handshake.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// The shared handler state: the C9 Service plus the transport-local knobs the edge needs.
#[derive(Clone)]
struct McpState {
    /// The transport-agnostic orchestration core (C9). Every tool call routes through it.
    service: Arc<Service>,
    /// The deployment environment, gating verbose error detail in the rendered MCP error message.
    env: Env,
    /// `RECALL_MAX_BODY_BYTES`. Enforced inside the handler (not only by the tower layer) so an
    /// oversize body is rendered as a JSON-RPC error carrying `VAL_BODY_TOO_LARGE` rather than a bare
    /// transport-level 413 (SPEC Internal Logic step 2(a) / Error Table).
    max_body: usize,
}

/// Build the MCP axum router: one POST route at `path` carrying the body-size limit, dispatching every
/// JSON-RPC request through [`handle_rpc`]. `max_body` is `RECALL_MAX_BODY_BYTES`; an oversize body is
/// rejected by the tower layer and rendered as an MCP error carrying `VAL_BODY_TOO_LARGE`.
pub fn build_mcp_router(service: Arc<Service>, max_body: usize, path: &str, env: Env) -> Router {
    let state = McpState {
        service,
        env,
        max_body,
    };
    // The tower layer is a structural ceiling set a little above the configured limit so a body that
    // is *just* over `max_body` still reaches the handler, which renders it as a JSON-RPC error
    // carrying `VAL_BODY_TOO_LARGE` (the per-tool transport error the spec mandates). A pathologically
    // large body is still rejected structurally by the layer.
    let structural_ceiling = max_body.saturating_add(STRUCTURAL_MARGIN_BYTES);
    Router::new()
        .route(path, post(handle_rpc))
        .layer(DefaultBodyLimit::max(structural_ceiling))
        .with_state(state)
}

/// Headroom above `RECALL_MAX_BODY_BYTES` for the tower body-size ceiling, so the handler — not the
/// tower layer — renders the `VAL_BODY_TOO_LARGE` JSON-RPC error for a marginally-oversize body.
const STRUCTURAL_MARGIN_BYTES: usize = 64 * 1024;

/// Serve an already-built MCP router on an already-bound listener until shutdown. Exposed so the
/// integration harness can bind an ephemeral port and drive the real MCP edge in-process, mirroring
/// the C8 [`serve_on_listener`](crate::serve_on_listener).
pub async fn serve_mcp_on_listener(
    listener: tokio::net::TcpListener,
    service: Arc<Service>,
    max_body: usize,
    path: &str,
    env: Env,
) -> anyhow::Result<()> {
    use anyhow::Context;
    let router = build_mcp_router(service, max_body, path, env);
    axum::serve(listener, router)
        .with_graceful_shutdown(crate::shutdown::shutdown_signal())
        .await
        .context("serving MCP")?;
    Ok(())
}

/// The single POST handler. Parses the JSON-RPC envelope, dispatches by `method`, and always returns a
/// `200 OK` with a JSON-RPC response body (success or error) — the transport status is HTTP-200; the
/// application outcome rides inside the JSON-RPC payload, per the streamable-HTTP request/response
/// subset.
///
/// A body that exceeded `RECALL_MAX_BODY_BYTES` never reaches here (the tower layer rejected it); that
/// rejection is converted to a `VAL_BODY_TOO_LARGE` MCP error by [`handle_rejection`] via the route's
/// fallthrough. A body present but not valid JSON is rendered as a JSON-RPC Invalid Request error.
async fn handle_rpc(
    State(state): State<McpState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // Step 2(a): enforce the configured body-size limit here so an oversize body is rendered as a
    // JSON-RPC error carrying `VAL_BODY_TOO_LARGE` (not a bare transport 413). A correlation id is
    // minted for the error so the failure is traceable even before any tool is dispatched.
    if body.len() > state.max_body {
        let correlation_id = correlation_from(&headers);
        let err = AppError::Validation(ValidationKind::BodyTooLarge, "request body".into());
        return app_error_response(state.env, Value::Null, &correlation_id, &err);
    }

    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return rpc_error(
                Value::Null,
                jsonrpc::INVALID_REQUEST,
                "invalid JSON-RPC request",
                None,
            )
        }
    };

    // The request id is echoed verbatim on the response; a notification carries no id (null).
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");

    match method {
        "initialize" => initialize_result(id),
        // A notification carries no id and expects no response; we still return a 200 with an empty
        // JSON object body (a JSON-RPC notification has no reply, but the HTTP transport needs a body).
        "notifications/initialized" => (axum::http::StatusCode::OK, Json(json!({}))).into_response(),
        "tools/list" => tools_list_result(id),
        "tools/call" => {
            handle_tools_call(&state, id, req.get("params").cloned().unwrap_or(Value::Null), &headers)
                .await
        }
        _ => rpc_error(
            id,
            jsonrpc::METHOD_NOT_FOUND,
            "method not found",
            None,
        ),
    }
}

/// The MCP `initialize` handshake: advertise the protocol version, the `tools` capability, and the
/// server identity. No negotiation beyond this is needed for the request/response subset.
fn initialize_result(id: Value) -> Response {
    let result = json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "tools": {} },
        "serverInfo": {
            "name": "recall-mcp",
            "version": env!("CARGO_PKG_VERSION"),
        }
    });
    rpc_ok(id, result)
}

/// `tools/list` — the six tool descriptors. Each `inputSchema` is hand-built JSON (the same hand-built
/// style as `src/api/openapi.rs`), mirroring the 2C request types so the MCP and REST contracts cannot
/// drift (SPEC step 4).
fn tools_list_result(id: Value) -> Response {
    rpc_ok(id, json!({ "tools": tool_catalogue() }))
}

/// The hand-built tool catalogue advertised by `tools/list`. Authored from the same 2C DTOs that back
/// the OpenAPI document (`RecallRequest`, `RememberRequest`, the id-bearing forget inputs), kept
/// hand-built and consistent with `src/api/openapi.rs`.
fn tool_catalogue() -> Value {
    json!([
        {
            "name": "recall",
            "description": "Recall ranked facts (synchronous read path).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "1..=4096 chars, non-empty." },
                    "filters": {
                        "type": "object",
                        "properties": {
                            "memory_class": { "type": "string" },
                            "visibility": { "type": "string" },
                            "entity": { "type": "string" },
                            "valid_at": { "type": "string", "format": "date-time" }
                        }
                    },
                    "result_cap": { "type": "integer", "minimum": 1, "maximum": 50, "default": 10 },
                    "cursor": { "type": "string" },
                    "include_provenance": { "type": "boolean", "default": false }
                },
                "required": ["query"]
            }
        },
        {
            "name": "remember",
            "description": "Enqueue a fact-extraction job (async). Requires an idempotency_key.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": { "type": "object", "description": "Structured assertion object (ADR-015)." },
                    "source": {
                        "type": "object",
                        "properties": {
                            "origin_ref": { "type": "string" },
                            "modification_marker": { "type": "string" }
                        },
                        "required": ["origin_ref"]
                    },
                    "memory_class": { "type": "string" },
                    "idempotency_key": { "type": "string", "minLength": 1, "maxLength": 255 }
                },
                "required": ["content", "idempotency_key"]
            }
        },
        {
            "name": "get",
            "description": "Fetch a fact by id.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            }
        },
        {
            "name": "retire",
            "description": "End a fact's validity (non-destructive). Requires an idempotency_key.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "idempotency_key": { "type": "string", "minLength": 1, "maxLength": 255 }
                },
                "required": ["id", "idempotency_key"]
            }
        },
        {
            "name": "delete",
            "description": "Verifiable hard delete (returns a deletion proof). Requires an idempotency_key.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "idempotency_key": { "type": "string", "minLength": 1, "maxLength": 255 }
                },
                "required": ["id", "idempotency_key"]
            }
        },
        {
            "name": "capabilities",
            "description": "Service capabilities.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

/// Dispatch a `tools/call`: extract the tool name + arguments, mint the call context from the HTTP
/// headers, and route to the matching C9 method. An unknown tool is a JSON-RPC Invalid Params error.
async fn handle_tools_call(
    state: &McpState,
    id: Value,
    params: Value,
    headers: &HeaderMap,
) -> Response {
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    // Edge step 2(b)–(c): bearer + correlation id from the HTTP request.
    let bearer = bearer_from(headers);
    let correlation_id = correlation_from(headers);
    // For writes, the idempotency key is a required *argument* (the MCP analogue of the REST header).
    let idempotency_key = arguments
        .get("idempotency_key")
        .and_then(|k| k.as_str())
        .map(|s| s.to_string());

    let cx = CallContext {
        bearer: &bearer,
        correlation_id: &correlation_id,
        idempotency_key: idempotency_key.as_deref(),
    };

    let svc = &state.service;
    match name {
        "capabilities" => render(state.env, &id, &correlation_id, svc.capabilities(cx).await),
        "recall" => {
            let body = serde_json::to_vec(&arguments).unwrap_or_default();
            render_recall(state.env, &id, &correlation_id, svc.recall(cx, &body).await)
        }
        "remember" => {
            let body = serde_json::to_vec(&arguments).unwrap_or_default();
            render(state.env, &id, &correlation_id, svc.remember(cx, &body).await)
        }
        "get" => match arguments.get("id").and_then(|i| i.as_str()) {
            Some(fact_id) => render(
                state.env,
                &id,
                &correlation_id,
                svc.get_fact(cx, fact_id).await,
            ),
            None => invalid_args(id, "missing required argument \"id\""),
        },
        "retire" => match arguments.get("id").and_then(|i| i.as_str()) {
            Some(fact_id) => render(state.env, &id, &correlation_id, svc.retire(cx, fact_id).await),
            None => invalid_args(id, "missing required argument \"id\""),
        },
        "delete" => match arguments.get("id").and_then(|i| i.as_str()) {
            Some(fact_id) => render(state.env, &id, &correlation_id, svc.delete(cx, fact_id).await),
            None => invalid_args(id, "missing required argument \"id\""),
        },
        _ => rpc_error(
            id,
            jsonrpc::INVALID_PARAMS,
            "unknown tool",
            None,
        ),
    }
}

/// Render a C9 `Result<CallResult<T>, CallError>` into a JSON-RPC response.
///
/// `Ok` → an MCP tool result whose `structuredContent` is the typed `data` serialised as JSON, with
/// the correlation id and rate snapshot attached as `_meta`. `Err` → a JSON-RPC application error whose
/// `data.code` carries the registry code (the same string the REST edge maps to a status) and whose
/// `message` is the registry message (fixed in production; detail appended in development).
fn render<T: serde::Serialize>(
    env: Env,
    id: &Value,
    correlation_id: &str,
    outcome: Result<CallResult<T>, CallError>,
) -> Response {
    match outcome {
        Ok(result) => {
            let data = serde_json::to_value(&result.data).unwrap_or(Value::Null);
            let meta = json!({
                "correlation_id": correlation_id,
                "rate": {
                    "limit": result.rate.limit,
                    "remaining": result.rate.remaining,
                    "reset_secs": result.rate.reset_secs,
                },
                "replayed": result.replayed,
            });
            let tool_result = json!({
                "content": [],
                "structuredContent": data,
                "isError": false,
                "_meta": meta,
            });
            rpc_ok(id.clone(), tool_result)
        }
        Err(call_err) => app_error_response(env, id.clone(), correlation_id, &call_err.error),
    }
}

/// Render the `recall` tool's outcome. [`RecallOutcome`](crate::retrieval::RecallOutcome) is not a
/// `Serialize` type (it is C6's internal carrier), so its public payload is assembled here: the
/// `RecallResponse` (`facts`) plus the `abstained` flag and the opaque `next_cursor`, mirroring how the
/// REST edge surfaces `data` + `meta` (SPEC `tools/call — recall`). A failure renders identically to
/// the generic path.
fn render_recall(
    env: Env,
    id: &Value,
    correlation_id: &str,
    outcome: Result<CallResult<crate::retrieval::RecallOutcome>, CallError>,
) -> Response {
    match outcome {
        Ok(result) => {
            let facts =
                serde_json::to_value(&result.data.response.facts).unwrap_or(Value::Array(vec![]));
            let structured = json!({
                "facts": facts,
                "abstained": result.data.abstained,
                "next_cursor": result.data.next_cursor,
            });
            let meta = json!({
                "correlation_id": correlation_id,
                "rate": {
                    "limit": result.rate.limit,
                    "remaining": result.rate.remaining,
                    "reset_secs": result.rate.reset_secs,
                },
                "replayed": result.replayed,
            });
            let tool_result = json!({
                "content": [],
                "structuredContent": structured,
                "isError": false,
                "_meta": meta,
            });
            rpc_ok(id.clone(), tool_result)
        }
        Err(call_err) => app_error_response(env, id.clone(), correlation_id, &call_err.error),
    }
}

/// Build the JSON-RPC application-error response for a C9 [`AppError`], carrying the registry `code`
/// and message. Production uses the fixed registry message; development may append the inner detail —
/// reusing the C9/X1 `map_error` mapping verbatim so the edge invents no codes or messages.
fn app_error_response(env: Env, id: Value, correlation_id: &str, err: &AppError) -> Response {
    let (_status, envelope) = crate::error::map_error(err, correlation_id, env);
    let data = json!({
        "code": envelope.error.code,
        "correlation_id": correlation_id,
    });
    rpc_error(
        id,
        jsonrpc::APPLICATION_ERROR,
        &envelope.error.message,
        Some(data),
    )
}

/// A JSON-RPC Invalid Params error for a malformed/absent tool argument.
fn invalid_args(id: Value, message: &str) -> Response {
    rpc_error(id, jsonrpc::INVALID_PARAMS, message, None)
}

/// Extract the raw bearer token (after `Bearer `) from the `Authorization` header; `""` if absent or
/// not a Bearer scheme. The Service performs the actual validation (C3).
fn bearer_from(headers: &HeaderMap) -> String {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Mint the correlation id: honour a valid inbound `x-correlation-id` (a parseable UUID), else mint a
/// fresh UUIDv4 — identical to the C8 `correlation_id_middleware` rule.
fn correlation_from(headers: &HeaderMap) -> String {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| Uuid::parse_str(s).is_ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// Build a JSON-RPC 2.0 success response body wrapped in a `200 OK`.
fn rpc_ok(id: Value, result: Value) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    });
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

/// Build a JSON-RPC 2.0 error response body wrapped in a `200 OK`. The optional `data` carries the
/// registry `code` + correlation id for the application-error path.
fn rpc_error(id: Value, code: i64, message: &str, data: Option<Value>) -> Response {
    let mut error = json!({
        "code": code,
        "message": message,
    });
    if let Some(d) = data {
        error["data"] = d;
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error,
    });
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn tool_catalogue_lists_the_six_tools_each_with_a_schema() {
        let tools = tool_catalogue();
        let arr = tools.as_array().expect("an array of tools");
        assert_eq!(arr.len(), 6, "expected exactly six tools");
        let names: Vec<&str> = arr
            .iter()
            .map(|t| t.get("name").and_then(|n| n.as_str()).unwrap_or(""))
            .collect();
        for expected in ["recall", "remember", "get", "retire", "delete", "capabilities"] {
            assert!(names.contains(&expected), "missing tool {expected}");
        }
        for t in arr {
            assert!(
                t.get("inputSchema").is_some(),
                "tool {:?} has no inputSchema",
                t.get("name")
            );
        }
    }

    #[test]
    fn bearer_extracts_after_scheme_and_defaults_empty() {
        let mut h = HeaderMap::new();
        assert_eq!(bearer_from(&h), "");
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer eyJ.abc.def"),
        );
        assert_eq!(bearer_from(&h), "eyJ.abc.def");
        // A non-Bearer scheme yields an empty bearer (the Service then maps it to AUTH_MISSING_TOKEN).
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        assert_eq!(bearer_from(&h), "");
    }

    #[test]
    fn correlation_honours_valid_uuid_else_mints_fresh() {
        let mut h = HeaderMap::new();
        // No header → a fresh, parseable UUID.
        let fresh = correlation_from(&h);
        assert!(Uuid::parse_str(&fresh).is_ok());
        // A valid inbound UUID is honoured verbatim.
        let known = "550e8400-e29b-41d4-a716-446655440000";
        h.insert(CORRELATION_ID_HEADER, HeaderValue::from_static(known));
        assert_eq!(correlation_from(&h), known);
        // A non-UUID inbound value is ignored; a fresh UUID is minted instead.
        h.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("not-a-uuid"));
        assert!(Uuid::parse_str(&correlation_from(&h)).is_ok());
        assert_ne!(correlation_from(&h), "not-a-uuid");
    }
}
