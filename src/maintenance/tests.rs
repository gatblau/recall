//! Unit tests for the pure, I/O-free maintenance cores (ADR-010 algorithmic-core unit tests). Each
//! shared path is exercised with at least three table-driven cases.

use super::*;
use crate::types::domain::{MemoryClass, Visibility};

/// Build a minimal fact for the contradiction-detection table tests.
fn fact(
    id: &str,
    entities: &[&str],
    subject: &str,
    predicate: &str,
    object: &str,
    valid_from: DateTime<Utc>,
    confidence: f64,
) -> Fact {
    Fact {
        id: id.into(),
        content: serde_json::json!({
            "subject": subject, "predicate": predicate, "object": object
        }),
        entities: entities.iter().map(|e| e.to_string()).collect(),
        source_id: None,
        memory_class: MemoryClass::Semantic,
        visibility: Visibility::UserPrivate,
        owner: ScopeRef {
            tenant: "acme".into(),
            team: None,
            user: "u-1".into(),
        },
        valid_from,
        valid_to: None,
        ingested_at: valid_from,
        confidence,
        salience: 0.5,
        stability: 1.0,
        pii_review: false,
        supersedes: None,
        superseded_by: None,
        derived_from: vec![],
        last_recalled_at: None,
    }
}

fn dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

#[test]
fn retrievability_cases() {
    // (delta_secs, stability, k, expected_approx)
    let cases = [
        // Spec example: 10 days, s=1, k=10 -> exp(-86400) underflows to ~0.
        (864000.0_f64, 1.0_f64, 10.0_f64, 0.0_f64),
        // No elapsed time -> R = 1.0.
        (0.0, 1.0, 10.0, 1.0),
        // Clock skew (negative delta) clamps to 0 -> R = 1.0.
        (-500.0, 1.0, 10.0, 1.0),
        // One stability-window elapsed (delta = s*k) -> exp(-1) ~= 0.3679.
        (10.0, 1.0, 10.0, std::f64::consts::E.recip()),
    ];
    for (delta, s, k, expected) in cases {
        let r = retrievability(delta, s, k);
        assert!(
            (r - expected).abs() < 1e-4,
            "retrievability({delta},{s},{k}) = {r}, expected ~{expected}"
        );
    }
    // Zero stability must not divide-by-zero / NaN.
    let r0 = retrievability(100.0, 0.0, 10.0);
    assert!(r0.is_finite() && (0.0..=1.0).contains(&r0), "zero stability gave {r0}");
}

#[test]
fn is_prune_candidate_cases() {
    let floor = 0.3;
    let prune_r = 0.05;
    // (r, salience, expected)
    let cases = [
        // Low R, low salience -> prune.
        (0.0, 0.1, true),
        // Low R, high salience -> survives disuse (ADR-006).
        (0.0, 0.9, false),
        // High R, low salience -> not yet a prune candidate.
        (0.5, 0.1, false),
        // Salience exactly at the floor -> not below the floor -> survives.
        (0.0, 0.3, false),
        // R exactly at the prune threshold -> not strictly below -> survives.
        (0.05, 0.1, false),
    ];
    for (r, salience, expected) in cases {
        assert_eq!(
            is_prune_candidate(r, salience, prune_r, floor),
            expected,
            "is_prune_candidate(r={r}, sal={salience})"
        );
    }
}

#[test]
fn insight_confidence_never_outranks_sources() {
    let factor = 0.9;
    // (candidate_conf, sources, expected)
    let cases = [
        // Capped by the minimum source confidence (0.6), then decayed: 0.6 * 0.9 = 0.54.
        (0.95, vec![0.8, 0.6, 0.7], 0.54),
        // Capped by the candidate's own (lower) confidence: 0.4 * 0.9 = 0.36.
        (0.4, vec![0.8, 0.9], 0.36),
        // Equal sources and candidate: 0.5 * 0.9 = 0.45.
        (0.5, vec![0.5, 0.5], 0.45),
    ];
    for (cand, sources, expected) in &cases {
        let got = insight_confidence(*cand, sources, factor);
        assert!((got - expected).abs() < 1e-9, "insight_confidence got {got}, want {expected}");
        // The cap invariant: the insight never exceeds the minimum source confidence.
        let min_source = sources.iter().copied().fold(f64::INFINITY, f64::min);
        assert!(got <= min_source + 1e-9, "insight {got} outranks min source {min_source}");
        assert!(got <= *cand + 1e-9, "insight {got} outranks candidate {cand}");
    }
    // Empty sources: capped only by the candidate, decayed; never panics.
    let got = insight_confidence(0.8, &[], factor);
    assert!((got - 0.72).abs() < 1e-9, "empty-source insight = {got}");
    // Result is always clamped into [0,1].
    assert!((0.0..=1.0).contains(&insight_confidence(2.0, &[2.0], 1.0)));
}

#[test]
fn reinforce_raises_stability_and_resets_clock() {
    let now = dt("2026-06-20T12:00:00.000Z");
    // (stability, gain, expected_stability)
    let cases = [
        (1.0_f64, 0.5_f64, 1.5_f64),
        (2.0, 0.5, 3.0),
        (0.5, 1.0, 1.0),
    ];
    for (s, gain, expected) in cases {
        let (new_s, when) = reinforce(s, gain, now);
        assert!((new_s - expected).abs() < 1e-9, "reinforce({s},{gain}) = {new_s}");
        assert!(new_s >= s, "stability must not fall");
        assert_eq!(when, now, "decay clock must reset to now");
    }
}

#[test]
fn detect_contradiction_no_conflict_cases() {
    let t = dt("2026-06-20T12:00:00.000Z");
    // No shared entity -> no conflict even with the same triple.
    let a = fact("fact:a", &["entity:e1"], "s", "p", "o1", t, 0.5);
    let b = fact("fact:b", &["entity:e2"], "s", "p", "o2", t, 0.5);
    assert_eq!(detect_contradiction(&a, &b), ContradictionVerdict::NoConflict);
    // Same object (compatible content) -> no conflict.
    let c = fact("fact:c", &["entity:e1"], "s", "p", "o", t, 0.5);
    let d = fact("fact:d", &["entity:e1"], "s", "p", "o", t, 0.5);
    assert_eq!(detect_contradiction(&c, &d), ContradictionVerdict::NoConflict);
    // Different predicate -> not the same (subject, predicate) -> no conflict.
    let e = fact("fact:e", &["entity:e1"], "s", "p1", "o1", t, 0.5);
    let g = fact("fact:g", &["entity:e1"], "s", "p2", "o2", t, 0.5);
    assert_eq!(detect_contradiction(&e, &g), ContradictionVerdict::NoConflict);
}

#[test]
fn detect_contradiction_side_selection_cases() {
    let early = dt("2026-06-19T12:00:00.000Z");
    let late = dt("2026-06-20T12:00:00.000Z");

    // valid_from tie-break: a is earlier -> a superseded -> b supersedes a -> Supersedes.
    let a = fact("fact:a", &["entity:e1"], "s", "p", "o1", early, 0.9);
    let b = fact("fact:b", &["entity:e1"], "s", "p", "o2", late, 0.1);
    assert_eq!(detect_contradiction(&a, &b), ContradictionVerdict::Supersedes);

    // a is later -> b superseded -> a supersedes b -> SupersededBy.
    let a2 = fact("fact:a2", &["entity:e1"], "s", "p", "o1", late, 0.1);
    let b2 = fact("fact:b2", &["entity:e1"], "s", "p", "o2", early, 0.9);
    assert_eq!(detect_contradiction(&a2, &b2), ContradictionVerdict::SupersededBy);

    // valid_from tie -> confidence tie-break: a has lower confidence -> a superseded -> Supersedes.
    let a3 = fact("fact:a3", &["entity:e1"], "s", "p", "o1", late, 0.3);
    let b3 = fact("fact:b3", &["entity:e1"], "s", "p", "o2", late, 0.7);
    assert_eq!(detect_contradiction(&a3, &b3), ContradictionVerdict::Supersedes);

    // valid_from + confidence tie -> id tie-break: lexicographically smaller id is superseded.
    // a id "fact:a4" < b id "fact:z4" -> a superseded -> Supersedes.
    let a4 = fact("fact:a4", &["entity:e1"], "s", "p", "o1", late, 0.5);
    let b4 = fact("fact:z4", &["entity:e1"], "s", "p", "o2", late, 0.5);
    assert_eq!(detect_contradiction(&a4, &b4), ContradictionVerdict::Supersedes);
    // Reversed ids: b id "fact:a4" < a id "fact:z4" -> b superseded -> SupersededBy.
    let a5 = fact("fact:z4", &["entity:e1"], "s", "p", "o1", late, 0.5);
    let b5 = fact("fact:a4", &["entity:e1"], "s", "p", "o2", late, 0.5);
    assert_eq!(detect_contradiction(&a5, &b5), ContradictionVerdict::SupersededBy);
}
