//! Unit tests for the C6 ranking, gating, and cursor arithmetic (table-driven where ≥3 cases share a
//! path). The end-to-end pipeline (embed → stage-1 → rerank → recency → gate → page → freshness) is
//! covered by `tests/features/retrieval.feature` against the real store + wiremock providers.

use super::*;

#[test]
fn recency_final_score_table() {
    // (rerank, age_days, w, tau, expected_final)
    let w = 0.15;
    let tau = 30.0;
    let cases = [
        // Fresh fact (age 0): full boost -> rerank * (1 + 0.15).
        (0.80_f64, 0.0_f64, 0.80 * 1.15),
        // Old fact (age >> tau): boost decays to ~0 -> ~= rerank.
        (0.80, 300.0, 0.80 * (1.0 + w * (-300.0_f64 / tau).exp())),
        // Zero score stays zero regardless of boost.
        (0.0, 0.0, 0.0),
    ];
    for (rerank, age, expected) in cases {
        let got = recency_final_score(rerank, age, w, tau);
        assert!((got - expected).abs() < 1e-9, "rerank={rerank} age={age}: {got} != {expected}");
    }
    // A fresher fact always outranks an older one with the same rerank score.
    let fresh = recency_final_score(0.5, 0.0, w, tau);
    let old = recency_final_score(0.5, 100.0, w, tau);
    assert!(fresh > old, "fresher fact must score higher");
}

#[test]
fn cursor_round_trips() {
    let cases = [
        (0.88_f64, "fact:018f9a2b-7c41-7e30-9d22-1a2b3c4d5e6f"),
        (0.0, "fact:zzz"),
        (0.123456789, "fact:01"),
    ];
    for (s, id) in cases {
        let encoded = Cursor { s, id: id.to_string() }.encode().expect("encode");
        // Opaque: base64url-no-pad carries no '=' padding and no '+'/'/'.
        assert!(!encoded.contains('='), "cursor must be unpadded");
        assert!(!encoded.contains('+') && !encoded.contains('/'), "cursor must be url-safe");
        let decoded = Cursor::decode(&encoded).expect("decode");
        assert_eq!(decoded.s, s);
        assert_eq!(decoded.id, id);
    }
}

#[test]
fn cursor_decode_rejects_garbage() {
    // Not valid base64url.
    let err = Cursor::decode("!!!not-base64!!!").unwrap_err();
    assert!(matches!(err, AppError::Validation(ValidationKind::OutOfRange, _)));
    // Valid base64url but not a Cursor JSON.
    let not_json = URL_SAFE_NO_PAD.encode(b"hello world");
    let err = Cursor::decode(&not_json).unwrap_err();
    assert!(matches!(err, AppError::Validation(ValidationKind::OutOfRange, _)));
}

#[test]
fn keyword_terms_tokenises_lowercases_dedupes() {
    let terms = keyword_terms("Who Owns the Orders table, the ORDERS?");
    assert_eq!(
        terms,
        vec![
            "who".to_string(),
            "owns".into(),
            "the".into(),
            "orders".into(),
            "table".into()
        ],
        "split on non-alphanumerics, lowercase, de-duplicate in first-seen order"
    );
    assert!(keyword_terms("   ,. ").is_empty(), "punctuation-only yields no terms");
}
