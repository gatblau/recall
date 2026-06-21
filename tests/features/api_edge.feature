Feature: HTTP API Edge (C8)
  The edge exposes the /v1 task routes under the binding middleware chain (correlation-id, body-size,
  auth, rate limit, idempotency, audit) and the operational routes, with the single success / error
  envelopes and the registered error codes.

  Scenario: Happy path — recall returns ranked facts in the success envelope
    Given an api edge with a recalled fact for tenant "acme" user "u-7" team "platform"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.read"
    When the client POSTs "/v1/recall" with body {"query":"who owns the orders table","result_cap":5}
    Then the edge response status is 200
    And the edge response carries RateLimit headers
    And the edge JSON field "meta.correlation_id" is a non-empty string
    And an audit row with operation "recall" and outcome "success" exists for tenant "acme"

  Scenario: Happy path — remember enqueues an ExtractFact job and acks 202
    Given an api edge for tenant "acme"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.write"
    And an Idempotency-Key "k-001"
    When the client POSTs "/v1/memories" with body {"content":{"text":"Team Alpha owns orders"}}
    Then the edge response status is 202
    And the edge JSON field "data.status" is "accepted"
    And the edge JSON field "data.job_id" is a non-empty string
    And exactly 1 extract_fact job is enqueued for tenant "acme"

  Scenario: Edge case — idempotent replay returns the original ack without a new job
    Given an api edge for tenant "acme"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.write"
    And an Idempotency-Key "k-replay"
    When the client POSTs "/v1/memories" with body {"content":{"text":"Team Alpha owns orders"}}
    And the client POSTs "/v1/memories" again with the same Idempotency-Key
    Then the edge response status is 202
    And the edge JSON field "data.status" is "already-accepted"
    And exactly 1 extract_fact job is enqueued for tenant "acme"

  Scenario: Edge case — recall abstains when no candidate clears the gate
    Given an api edge for tenant "acme"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.read"
    When the client POSTs "/v1/recall" with body {"query":"a query with no matching facts"}
    Then the edge response status is 200
    And the edge JSON field "meta.abstained" is "true"

  Scenario: Edge case — conditional GET returns 304 when the fact is unchanged
    Given an api edge with a recalled fact for tenant "acme" user "u-7" team "platform"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.read"
    When the client GETs the recalled fact and notes its ETag
    And the client GETs the recalled fact with If-None-Match set to that ETag
    Then the edge response status is 304

  Scenario: Edge case — DELETE returns a deletion proof
    Given an api edge with a recalled fact for tenant "acme" user "u-7" team "platform"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.forget"
    And an Idempotency-Key "d-009"
    When the client DELETEs the recalled fact
    Then the edge response status is 200
    And the edge JSON field "data.record_id" is a non-empty string
    And the edge JSON field "data.digest" is a non-empty string

  Scenario: Error path — write without Idempotency-Key is rejected
    Given an api edge for tenant "acme"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.write"
    When the client POSTs "/v1/memories" with no Idempotency-Key and body {"content":{"text":"x"}}
    Then the edge response status is 400
    And the edge JSON field "error.code" is "VAL_MISSING_IDEMPOTENCY_KEY"

  Scenario: Error path — missing bearer token on a /v1 route
    Given an api edge for tenant "acme"
    When the client POSTs "/v1/recall" with no bearer token and body {"query":"who owns orders"}
    Then the edge response status is 401
    And the edge JSON field "error.code" is "AUTH_MISSING_TOKEN"
    And no audit row exists for tenant "acme"

  Scenario: Error path — rate limit exhausted
    Given an api edge for tenant "acme" with the read bucket drained for user "u-7"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.read"
    When the client POSTs "/v1/recall" with body {"query":"who owns orders"}
    Then the edge response status is 429
    And the edge JSON field "error.code" is "RATE_LIMITED"
    And the edge response carries Retry-After and RateLimit-Reset headers

  Scenario: Error path — body exceeds the size limit
    Given an api edge for tenant "acme"
    And a bearer token for user "u-7" tenant "acme" groups "platform" scope "memory.write"
    And an Idempotency-Key "k-big"
    When the client POSTs "/v1/memories" with a body larger than the limit
    Then the edge response status is 413

  Scenario: Error path — readiness fails when the embedding dimension mismatches the index
    Given an api edge whose store index dimension differs from the configured embed dim
    When the client GETs "/readyz"
    Then the edge response status is 503
    And the edge JSON field "checks.embed_dim" is "false"
