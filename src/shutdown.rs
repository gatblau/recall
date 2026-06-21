//! X13 — Graceful shutdown (Phase 1 minimal form).
//!
//! Phase 1 provides only the signal-wait future used by the axum server's `with_graceful_shutdown`:
//! it completes on the first SIGINT (Ctrl-C) or SIGTERM. The full drain protocol — stop accepting,
//! drain in-flight requests within the deadline, signal workers to release leases, flush + close the
//! store, second-signal force-exit — is Phase 9 (X13 depends on C2/C4/C6/C7/C8, none of which exist
//! yet). This future is the seam those phases build on.

/// Resolve when a shutdown signal (SIGINT or SIGTERM) is received.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        // A failure to install the Ctrl-C handler is a bootstrap fault; in that case we simply never
        // resolve via this arm and rely on the SIGTERM arm (or the process being killed).
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!(target: "recall::shutdown", "shutdown signal received");
}
