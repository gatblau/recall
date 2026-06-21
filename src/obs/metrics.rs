//! X4 — Metrics. The four quality layers (task usage, memory quality, efficiency, governance),
//! exported for scrape at `GET /metrics`.
//!
//! Phase 1 ships a minimal, dependency-free catalogue: the metric **names** match the X4 catalogue
//! exactly so the contract is fixed; counters/gauges are atomics and the Prometheus exposition is
//! rendered by hand. Components increment via the injected [`Metrics`] handle in later phases. Label
//! cardinality is bounded — no tenant id, user id, or fact id is ever a label value.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A single registered counter, rendered in the exposition by its catalogue name.
#[derive(Default)]
struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// The metric catalogue handles, injected into components. Names are the X4 catalogue names.
///
/// Phase 1 registers the catalogue and exposes it; per-label series and histograms are refined as
/// each component lands. Unused handles carry `#[allow(dead_code)]` to keep clippy clean until the
/// owning component wires them in.
#[allow(dead_code)]
#[derive(Default)]
pub struct Metrics {
    // --- Task usage ---
    recall_requests_total: Counter,
    recall_abstain_total: Counter,
    // --- Memory quality ---
    memory_contradictions_superseded_total: Counter,
    memory_facts_stale_pending_refresh_total: Counter,
    memory_facts_total: Counter, // gauge, modelled as a settable counter for Phase 1
    // --- Efficiency / governance counters (histograms/gauges arrive with their owners) ---
    writes_rejected_total: Counter,
    writes_quarantined_total: Counter,
    deletions_total: Counter,
    auth_decisions_total: Counter,
    // --- Self-metric ---
    metrics_scrape_total: Counter,
}

impl Metrics {
    /// Build the catalogue. Held behind an `Arc` and injected.
    pub fn new() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    /// Render the Prometheus text exposition for the registered catalogue. Carries no fact content
    /// and no identifiers. Increments the self-metric `metrics_scrape_total`.
    pub fn render(&self) -> String {
        self.metrics_scrape_total.inc();
        let lines = [
            ("recall_requests_total", self.recall_requests_total.get()),
            ("recall_abstain_total", self.recall_abstain_total.get()),
            (
                "memory_contradictions_superseded_total",
                self.memory_contradictions_superseded_total.get(),
            ),
            (
                "memory_facts_stale_pending_refresh_total",
                self.memory_facts_stale_pending_refresh_total.get(),
            ),
            ("memory_facts_total", self.memory_facts_total.get()),
            ("writes_rejected_total", self.writes_rejected_total.get()),
            ("writes_quarantined_total", self.writes_quarantined_total.get()),
            ("deletions_total", self.deletions_total.get()),
            ("auth_decisions_total", self.auth_decisions_total.get()),
            ("metrics_scrape_total", self.metrics_scrape_total.get()),
        ];
        let mut out = String::new();
        for (name, value) in lines {
            out.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
        }
        out
    }

    /// Increment the request counter (task-usage layer). Wired by C6/C8 in later phases.
    #[allow(dead_code)]
    pub fn inc_recall_requests(&self) {
        self.recall_requests_total.inc();
    }

    /// Increment the abstain counter. Wired by C6 in a later phase.
    #[allow(dead_code)]
    pub fn inc_recall_abstain(&self) {
        self.recall_abstain_total.inc();
    }
}
