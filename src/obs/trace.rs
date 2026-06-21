//! X5 — Tracing. Distributed tracing across the API edge, the read path, and the async workers.
//!
//! Phase 1 wires the optional initialisation seam: OTLP export to `RECALL_OTLP_ENDPOINT` is enabled
//! only when that key is set; when unset, tracing export is disabled and this is logged once at
//! startup. The full OpenTelemetry pipeline (OTLP exporter, parent-based sampler, propagator,
//! job-payload context injection) is layered on by later phases that add the `opentelemetry`
//! dependency — Phase 1 deliberately keeps the dependency surface minimal while fixing the seam and
//! the span-naming convention (`<component>.<operation>`, e.g. `api.recall`).

use crate::config::Config;

/// Initialise distributed tracing. Returns whether OTLP export was enabled.
///
/// When `RECALL_OTLP_ENDPOINT` is unset, export is disabled (the HLD non-goal stance: observability
/// disabled if unset) and a single startup line records the fact. When set, later phases install the
/// OTLP exporter against this endpoint; Phase 1 records the intent without panicking.
pub fn init_tracing(config: &Config) -> bool {
    match &config.otlp_endpoint {
        Some(endpoint) if !endpoint.is_empty() => {
            tracing::info!(
                target: "recall::obs::trace",
                otlp_enabled = true,
                "tracing export endpoint configured"
            );
            true
        }
        _ => {
            tracing::info!(
                target: "recall::obs::trace",
                otlp_enabled = false,
                "RECALL_OTLP_ENDPOINT unset; trace export disabled"
            );
            false
        }
    }
}
