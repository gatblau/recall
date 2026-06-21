//! Scope & auth context (§2C.3).
//!
//! Used by: C3 (produces), C1/C4/C6/C7/C8 (consume — every query is scoped).

use serde::{Deserialize, Serialize};

use crate::types::domain::Visibility;

/// The owning scope stored on every record.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ScopeRef {
    /// Tenant id -> SurrealDB namespace (ADR-011).
    pub tenant: String,
    /// Team id, null for user-only facts.
    pub team: Option<String>,
    /// User id, bound to the OIDC subject claim.
    pub user: String,
}

/// The authenticated request context derived by C3 from the validated token.
/// Never constructed from request-body input.
#[derive(Clone)]
pub struct ScopeContext {
    pub tenant: String,
    /// Membership claim — teams the user belongs to.
    pub teams: Vec<String>,
    /// = token subject claim.
    pub user: String,
    /// For the audit trail (never the token itself).
    pub token_jti: String,
    /// read / write / forget, from token scopes.
    pub allowed_ops: OpSet,
    pub correlation_id: String,
}

#[derive(Clone, Copy)]
pub struct OpSet {
    pub read: bool,
    pub write: bool,
    pub forget: bool,
}

/// Read filter rule (binding for every store query, §2C.3).
///
/// A caller may read a Fact/Entity/Relationship iff
/// `record.owner.tenant == ctx.tenant` **and** (
///   `record.owner.user == ctx.user`
///   **or** (`record.visibility == TeamShared` and `record.owner.team ∈ ctx.teams`)
///   **or** `record.visibility == TenantShared`
/// ).
///
/// Cross-tenant access is structurally impossible — a different tenant is a different namespace —
/// but this helper enforces the tenant match defensively as well.
pub fn can_read(ctx: &ScopeContext, owner: &ScopeRef, visibility: Visibility) -> bool {
    if owner.tenant != ctx.tenant {
        return false;
    }
    if owner.user == ctx.user {
        return true;
    }
    match visibility {
        Visibility::TeamShared => owner
            .team
            .as_ref()
            .is_some_and(|t| ctx.teams.iter().any(|ct| ct == t)),
        Visibility::TenantShared => true,
        Visibility::UserPrivate => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(tenant: &str, user: &str, teams: &[&str]) -> ScopeContext {
        ScopeContext {
            tenant: tenant.to_string(),
            teams: teams.iter().map(|t| t.to_string()).collect(),
            user: user.to_string(),
            token_jti: "jti-1".to_string(),
            allowed_ops: OpSet {
                read: true,
                write: false,
                forget: false,
            },
            correlation_id: "c-1".to_string(),
        }
    }

    fn owner(tenant: &str, team: Option<&str>, user: &str) -> ScopeRef {
        ScopeRef {
            tenant: tenant.to_string(),
            team: team.map(|s| s.to_string()),
            user: user.to_string(),
        }
    }

    #[test]
    fn admits_own_user_private_record() {
        let c = ctx("acme", "alice", &[]);
        let o = owner("acme", None, "alice");
        assert!(can_read(&c, &o, Visibility::UserPrivate));
    }

    #[test]
    fn denies_other_users_private_record() {
        let c = ctx("acme", "alice", &["eng"]);
        let o = owner("acme", Some("eng"), "bob");
        assert!(!can_read(&c, &o, Visibility::UserPrivate));
    }

    #[test]
    fn admits_team_shared_when_member() {
        let c = ctx("acme", "alice", &["eng", "ops"]);
        let o = owner("acme", Some("eng"), "bob");
        assert!(can_read(&c, &o, Visibility::TeamShared));
    }

    #[test]
    fn denies_team_shared_when_not_member() {
        let c = ctx("acme", "alice", &["ops"]);
        let o = owner("acme", Some("eng"), "bob");
        assert!(!can_read(&c, &o, Visibility::TeamShared));
    }

    #[test]
    fn denies_team_shared_with_no_team_on_record() {
        let c = ctx("acme", "alice", &["eng"]);
        let o = owner("acme", None, "bob");
        assert!(!can_read(&c, &o, Visibility::TeamShared));
    }

    #[test]
    fn admits_tenant_shared_to_any_same_tenant_user() {
        let c = ctx("acme", "alice", &[]);
        let o = owner("acme", Some("eng"), "bob");
        assert!(can_read(&c, &o, Visibility::TenantShared));
    }

    #[test]
    fn denies_cross_tenant_even_when_tenant_shared() {
        let c = ctx("acme", "alice", &["eng"]);
        let o = owner("globex", Some("eng"), "alice");
        assert!(!can_read(&c, &o, Visibility::TenantShared));
    }
}
