//! In-memory JWKS cache for C3 Auth & Scope.
//!
//! No DDL, no persistence: the only state is a process-local map of `kid -> verification key`,
//! repopulated by [`crate::auth::Authenticator::new`] on every restart. The key set is replaced
//! wholesale under a write lock on each refresh, so readers never observe a partially-updated set;
//! a failed refresh leaves the previous set intact (the cache invariant from the C3 spec Data Model).

use std::collections::HashMap;
use std::time::Instant;

use jsonwebtoken::{Algorithm, DecodingKey};

/// A single decoded verification key, indexed by its JWK `kid`.
pub struct CachedKey {
    /// Decoding key prepared for `jsonwebtoken` (RSA/EC public key material).
    pub decoding_key: DecodingKey,
    /// The JWS algorithm this key signs with (from the JWK `alg`/`kty`).
    pub alg: Algorithm,
}

/// The cached JWK set, indexed by `kid`.
pub struct JwksCache {
    /// kid -> key; replaced atomically on every refresh.
    pub keys: HashMap<String, CachedKey>,
    /// When the current key set was fetched (for observability). Read by the `auth_jwks` refresh
    /// observability surface wired at the edge (Phase 9); retained now so the cache invariant
    /// ("fetched_at is stamped on every wholesale replace") is enforced from C3 onward.
    #[allow(dead_code)]
    pub fetched_at: Instant,
}

impl JwksCache {
    /// Build a cache from a freshly-indexed key set, stamping `fetched_at` to now.
    pub fn new(keys: HashMap<String, CachedKey>) -> Self {
        Self {
            keys,
            fetched_at: Instant::now(),
        }
    }

    /// Look up a key by its `kid`, if present in the current set.
    pub fn get(&self, kid: &str) -> Option<&CachedKey> {
        self.keys.get(kid)
    }

    /// Current key count, for the `auth_jwks_keys` gauge / observability logs.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the cache holds no keys. Paired with [`JwksCache::len`] so clippy does not flag a
    /// `len`-without-`is_empty` API; used by the fetch path's empty-set guard.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}
