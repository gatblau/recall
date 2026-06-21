Feature: Auth & Scope (C3)
  The single security boundary: validate the OIDC bearer JWT against a real issuer's JWKS, derive a
  ScopeContext from the verified claims, authorise per operation, and apply the read filter. The
  local-issuer scenarios mint real RS256 tokens (real discovery, real JWKS fetch, real signature
  verify); the Dex scenarios exercise a real production-grade IdP for everything Dex emits natively.

  Background:
    Given a local OIDC issuer with a freshly generated RSA key
    And an authenticator constructed against the local issuer

  Scenario: Happy path — a valid token yields a scoped, authorised context
    Given a token with subject "user-42" tenant "acme" groups "platform,sre" scope "memory.read memory.write"
    When the token is validated
    Then validation succeeds with user "user-42" tenant "acme" teams "platform,sre"
    And the context allows read
    And the context allows write
    And the context denies forget

  Scenario: Claim mapping — the tenant claim maps from a custom claim
    Given a token with subject "u-9" tenant "globex" groups "ops" scope "memory.read"
    When the token is validated
    Then validation succeeds with user "u-9" tenant "globex" teams "ops"

  Scenario: Error path — an expired token is rejected
    Given an expired token with subject "user-42" tenant "acme"
    When the token is validated
    Then validation fails as an invalid token

  Scenario: Error path — a wrong-audience token is rejected
    Given a token for audience "some-other-api" with subject "user-42" tenant "acme"
    When the token is validated
    Then validation fails as an invalid token

  Scenario: Error path — an alg=none token is rejected before any signature work
    Given an alg-none token with subject "user-42" tenant "acme"
    When the token is validated
    Then validation fails as an invalid token

  Scenario: Error path — a tampered signature is rejected
    Given a token with a tampered signature for subject "user-42" tenant "acme"
    When the token is validated
    Then validation fails as an invalid token

  Scenario: Error path — a missing bearer token is reported as missing
    Given no bearer token
    When the token is validated
    Then validation fails as a missing token

  Scenario: Authorisation — a token lacking the op scope is forbidden
    Given a token with subject "user-42" tenant "acme" groups "platform" scope "memory.read"
    When the token is validated
    Then authorise for forget returns insufficient scope

  Scenario: Read filter — admits own, team-shared, tenant-shared and denies cross-tenant
    Given a token with subject "user-42" tenant "acme" groups "platform" scope "memory.read"
    When the token is validated
    Then the read filter admits a record owned by tenant "acme" team "platform" user "user-99" with visibility "team-shared"
    And the read filter denies a record owned by tenant "acme" team "platform" user "user-99" with visibility "user-private"
    And the read filter denies a record owned by tenant "globex" team "platform" user "user-42" with visibility "tenant-shared"

  Scenario: Warm cache — a second validation performs no network fetch
    Given a token with subject "user-42" tenant "acme" groups "platform" scope "memory.read"
    When the token is validated
    And the token is validated again
    Then both validations succeed against the warm cache

  Scenario: Real Dex — the real validation pipeline runs against a Dex-minted token
    Given a running Dex issuer
    And an authenticator constructed against the Dex issuer
    When a Dex password-grant token is validated
    Then the real validation pipeline runs against the Dex token
