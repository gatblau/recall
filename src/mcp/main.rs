//! `recall-mcp` binary entry point (C10, ADR-016). The MCP API Edge — the second externally-reachable
//! surface of `recall`, exposing the same service operations as MCP tools over a minimal JSON-RPC 2.0
//! HTTP transport.
//!
//! Bootstrap mirrors the REST binary (`src/main.rs`): load configuration (X6), initialise logging (X3)
//! and the tracing seam (X5), build the shared application state (`build_state`), construct the
//! transport-agnostic [`Service`](recall::service::Service) (C9), and serve the MCP router on
//! `RECALL_MCP_HTTP_ADDR` at `RECALL_MCP_PATH` until shutdown. Only `main` may panic on unrecoverable
//! bootstrap (X1 / C5 rule); library code never panics.

use std::process::ExitCode;
use std::sync::Arc;

use recall::config::Config;
use recall::build_state;
use recall::mcp::serve_mcp_on_listener;

#[tokio::main]
async fn main() -> ExitCode {
    // X6: load and validate configuration. A failure exits non-zero; the key is named, never the
    // value (so a secret is never logged).
    let config = match Config::load() {
        Ok(config) => config,
        Err(err) => {
            // Logging is not yet initialised; emit to stderr directly with the key only.
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    // X3: initialise structured logging from the validated config.
    recall::obs::log::init_logging(&config);
    tracing::info!(target: "recall-mcp", env = ?config.env, "config.loaded");

    // The MCP listener address, body limit, path, and env are taken from the shared Config before it is
    // moved into `build_state` (both binaries share one configuration model, SA-BIN-01).
    let mcp_addr = config.mcp_http_addr;
    let mcp_path = config.mcp_path.clone();
    let max_body = config.max_body_bytes as usize;
    let env = config.env;

    let state = match build_state(config).await {
        Ok(state) => state,
        Err(err) => {
            tracing::error!(target: "recall-mcp", error = %err, "failed to build application state");
            return ExitCode::FAILURE;
        }
    };

    // Construct the C9 Service over the shared component stack (the same handles back both edges).
    let service = Arc::new(state.service());

    let listener = match tokio::net::TcpListener::bind(mcp_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!(target: "recall-mcp", error = %err, %mcp_addr, "binding MCP address failed");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(target: "recall-mcp", %mcp_addr, path = %mcp_path, "mcp.serving");

    if let Err(err) = serve_mcp_on_listener(listener, service, max_body, &mcp_path, env).await {
        tracing::error!(target: "recall-mcp", error = %err, "MCP server exited with error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
