//! `recall` — agentic memory service.
//!
//! Phase 1 (scaffolding & cross-cutting foundations): shared types (§2C), the X1 error registry,
//! X6 configuration, X3/X4/X5 observability seams, skeleton provider adapters, and a minimal bootable
//! axum server with the operational endpoints. Component logic (C1–C8) lands in later phases.

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod freshness;
pub mod maintenance;
pub mod obs;
pub mod providers;
pub mod queue;
pub mod retrieval;
pub mod shutdown;
pub mod store;
pub mod types;
pub mod write_pipeline;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::api::{build_router, AppState};
use crate::auth::{AuthConfig, Authenticator};
use crate::config::Config;
use crate::obs::metrics::Metrics;
use crate::providers::{HttpBrokerClient, HttpEmbeddingClient, HttpRerankClient};
use crate::queue::StoreWorkQueue;
use crate::retrieval::{RetrievalConfig, RetrievalEngine};
use crate::store::Store;
use crate::types::ports::{BrokerClient, EmbeddingClient, FreshnessChecker, MemoryStore, RerankClient};

/// Build the full production application state from a loaded configuration: the embedded store (C1), the
/// store-backed work queue (C2), the provider HTTP clients, the retrieval engine (C6) over a broker
/// freshness checker (C5), and the authenticator (C3, which performs OIDC discovery + the first JWKS
/// fetch and so fails fast if the IdP is unreachable). Registers the metric catalogue and initialises
/// the optional tracing export seam.
///
/// DEVIATION (documented follow-up): the embedded store is opened with `Store::connect`, which uses the
/// SurrealKV embedded engine at `RECALL_STORE_PATH`. No production "open" constructor beyond
/// `connect`/`new_in_memory` exists, so `connect` is used as-is.
pub async fn build_state(config: Config) -> anyhow::Result<AppState> {
    let config = Arc::new(config);
    obs::trace::init_tracing(&config);

    // C1 — embedded store.
    let store = Arc::new(
        Store::connect(&config)
            .await
            .context("opening the embedded store")?,
    );

    // C2 — store-backed work queue over the shared store handle.
    let queue = Arc::new(StoreWorkQueue::new(
        store.handle(),
        config.embed_dim,
        config.job_max_attempts,
        config.job_backoff_base_ms,
    ));

    // Providers (C4/C5/C6 seams).
    let embedder: Arc<dyn EmbeddingClient> = Arc::new(HttpEmbeddingClient::new(&config));
    let reranker: Arc<dyn RerankClient> = Arc::new(HttpRerankClient::new(&config));
    let broker: Arc<dyn BrokerClient> = Arc::new(HttpBrokerClient::new(&config));
    let freshness: Arc<dyn FreshnessChecker> = Arc::new(crate::freshness::BrokerFreshnessChecker::new(
        broker,
        queue.clone(),
        std::time::Duration::from_millis(u64::from(config.freshness_budget_ms)),
        std::time::Duration::from_millis(u64::from(config.freshness_per_call_ms)),
    ));

    // C6 — retrieval engine. `store.clone()` coerces `Arc<Store>` to `Arc<dyn MemoryStore>`.
    let store_dyn: Arc<dyn MemoryStore> = store.clone();
    let engine = Arc::new(RetrievalEngine::new(
        store_dyn,
        embedder,
        reranker,
        freshness,
        RetrievalConfig::from_config(&config),
    ));

    // C3 — authenticator (OIDC discovery + first JWKS fetch; fails fast).
    let auth = Arc::new(
        Authenticator::new(AuthConfig::from_config(&config))
            .await
            .context("constructing the authenticator")?,
    );

    Ok(AppState {
        config,
        metrics: Metrics::new(),
        store,
        queue,
        engine,
        auth,
        rate: Arc::new(Mutex::new(HashMap::new())),
    })
}

/// Bind the configured HTTP address and serve until a shutdown signal is received.
///
/// Returns the bound [`AppState`] error context on failure. Used by `main`; the BDD harness uses
/// [`serve_on_listener`] with an ephemeral port instead.
pub async fn serve(state: AppState) -> anyhow::Result<()> {
    let addr = state.config.http_addr;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding HTTP address {addr}"))?;
    serve_on_listener(listener, state).await
}

/// Serve the router on an already-bound listener until a shutdown signal is received. Exposed so the
/// integration harness can bind an ephemeral port and drive the real app in-process.
pub async fn serve_on_listener(listener: TcpListener, state: AppState) -> anyhow::Result<()> {
    let router = build_router(state);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
        .context("serving HTTP")?;
    Ok(())
}
