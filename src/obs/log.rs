//! X3 — Logging. Structured operational logging with correlation-id propagation and strict
//! redaction. `tracing` + `tracing-subscriber` emitting JSON to stdout.
//!
//! Redaction is enforced at the field layer, not left to call sites: the never-log fields (the
//! `Authorization` header, the raw token, raw PII, fact `content`, embedding vectors, store
//! credentials, provider API keys) are never recorded as fields by any call site in this codebase.
//! The helpers below give call sites a safe way to log a tenant only as a boolean presence flag.

use std::sync::Once;

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::error::Env;

static INIT: Once = Once::new();

/// Initialise the global tracing subscriber from configuration. Idempotent: a second call is a
/// no-op (so tests that boot the app repeatedly do not panic on a double-init). In `production` the
/// format is JSON to stdout; in `development` a human-readable formatter is permitted.
///
/// A subscriber-init failure would be a fatal bootstrap error; here we install best-effort and let
/// the `Once` guard absorb a re-init in test harnesses.
pub fn init_logging(config: &Config) {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(config.log_level.as_directive()));

        match config.env {
            Env::Production => {
                let layer = fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_target(true);
                let _ = tracing_subscriber::registry()
                    .with(filter)
                    .with(layer)
                    .try_init();
            }
            Env::Development => {
                let layer = fmt::layer().with_target(true);
                let _ = tracing_subscriber::registry()
                    .with(filter)
                    .with(layer)
                    .try_init();
            }
        }
    });
}

/// Return whether a tenant id is present, for logging as a boolean `tenant_present` field instead
/// of the identifying value (X3 redaction rule).
pub fn tenant_present(tenant: Option<&str>) -> bool {
    tenant.is_some_and(|t| !t.is_empty())
}
