Feature: Whole-system cross-component flows (Phase 10)
  These scenarios exercise the FULL recall stack over one shared in-memory SurrealDB engine — the HTTP
  edge (C8) over the authenticator (C3), the work queue (C2), the write pipeline (C4), the memory store
  (C1), and the retrieval engine (C6) with its broker freshness checker (C5). They cover the flows no
  single-component phase covers: eventual-consistency between an accepted async write and a later recall,
  cross-tenant isolation across the whole edge, a verifiable forget round-trip, and the freshness flag
  surfacing through the /v1/recall response. Because no background worker runs in-process, the async
  write path is advanced by an explicit step that drains and processes the pending ExtractFact job
  through the write pipeline — the eventual-consistency boundary.

  Scenario: Async write becomes recallable only after the pending job is processed (eventual consistency)
    Given a system stack for tenant "acme" with the broker reporting sources unchanged
    And the extractor will return a fact matching "orders table" for the recall query
    And a system bearer token for user "u-77" tenant "acme" groups "platform" scope "memory.read memory.write"
    And a system Idempotency-Key "sys-w-001"
    When the client writes a memory with content {"text":"Team Alpha owns the orders table"}
    Then the system edge status is 202
    And the system edge field "data.status" is "accepted"
    When the client recalls "who owns the orders table" with result_cap 5
    Then the system edge status is 200
    And the system recall returns no facts
    When the pending extract_fact job is drained through the write pipeline
    And the client recalls "who owns the orders table" with result_cap 5
    Then the system edge status is 200
    And the system recall returns at least 1 fact

  Scenario: Cross-tenant isolation — a globex token never sees an acme fact (NFR-PR1)
    Given a system stack for tenant "acme" with the broker reporting sources unchanged
    And the extractor will return a fact matching "orders table" for the recall query
    And a system bearer token for user "u-77" tenant "acme" groups "platform" scope "memory.read memory.write"
    And a system Idempotency-Key "sys-iso-001"
    When the client writes a memory with content {"text":"Team Alpha owns the orders table"}
    And the pending extract_fact job is drained through the write pipeline
    And a system bearer token for user "u-77" tenant "acme" groups "platform" scope "memory.read"
    And the client recalls "who owns the orders table" with result_cap 5
    Then the system recall returns at least 1 fact
    When a system bearer token for user "g-1" tenant "globex" groups "platform" scope "memory.read"
    And the client recalls "who owns the orders table" with result_cap 5
    Then the system edge status is 200
    And the system recall returns no facts

  Scenario: Verifiable forget round-trip — remember, recall, DELETE with proof, then recall is empty
    Given a system stack for tenant "acme" with the broker reporting sources unchanged
    And the extractor will return a fact matching "orders table" for the recall query
    And a system bearer token for user "u-77" tenant "acme" groups "platform" scope "memory.read memory.write memory.forget"
    And a system Idempotency-Key "sys-f-001"
    When the client writes a memory with content {"text":"Team Alpha owns the orders table"}
    And the pending extract_fact job is drained through the write pipeline
    And the client recalls "who owns the orders table" with result_cap 5
    Then the system recall returns at least 1 fact
    When the client DELETEs the recalled system fact with Idempotency-Key "sys-del-001"
    Then the system edge status is 200
    And the system edge field "data.record_id" is a non-empty string
    And the system edge field "data.digest" is a non-empty string
    When the client recalls "who owns the orders table" with result_cap 5
    Then the system edge status is 200
    And the system recall returns no facts

  Scenario: Recall freshness branch — a changed source tags the returned fact stale-pending-refresh
    Given a system stack for tenant "acme" with the broker reporting sources changed
    And a trusted source "wiki-page-7" seeded for tenant "acme" user "u-77"
    And the extractor will return a sourced fact matching "orders table" for the recall query
    And a system bearer token for user "u-77" tenant "acme" groups "platform" scope "memory.read memory.write"
    And a system Idempotency-Key "sys-fr-001"
    When the client writes a sourced memory citing "wiki-page-7" with content {"text":"Team Alpha owns the orders table"}
    And the pending extract_fact job is drained through the write pipeline
    And the client recalls "who owns the orders table" with result_cap 5
    Then the system edge status is 200
    And the system recall returns at least 1 fact
    And every recalled system fact has currency "stale-pending-refresh"
