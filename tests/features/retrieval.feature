Feature: Retrieval Engine
  The synchronous read path (C6). Given an authenticated, scoped recall request it embeds the query,
  delegates stage-1 multi-signal recall to the store, reranks, applies recency weighting and gating,
  and returns a bounded ranked page with an opaque cursor — or abstains. Provider failures degrade
  rather than block; embed and stage-1 failures fail fast with a typed error.

  Scenario: Happy path — ranked facts within the result cap, with a cursor when more survive
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider returns a query vector of dimension 8
    And the reranker scores every document 0.90
    And 4 recalled facts owned by tenant "acme" user "u-7" team "platform" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 3
    Then the response returns at most 3 facts
    And each returned fact has a score in range
    And the facts are ordered by score descending
    And a next_cursor is present
    And the response does not abstain

  Scenario: Edge case — abstain when nothing clears the threshold
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider returns a query vector of dimension 8
    And the reranker scores every document 0.05
    And 3 recalled facts owned by tenant "acme" user "u-7" team "platform" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 5
    Then the response abstains

  Scenario: Edge case — reranker error degrades to stage-1 order
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider returns a query vector of dimension 8
    And the reranker errors
    And 3 recalled facts owned by tenant "acme" user "u-7" team "platform" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 5
    Then recall succeeds and returns facts

  Scenario: Provenance attached only when requested (ADR-014)
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider returns a query vector of dimension 8
    And the reranker scores every document 0.90
    And a recalled fact "fact:src1" citing a source owned by tenant "acme" user "u-7" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 5 with provenance
    Then every returned fact carries source provenance
    When recall is invoked with query "who owns the orders table" and result_cap 5
    Then no returned fact carries source provenance

  Scenario: Error path — embedding provider error fails fast
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider errors
    And 2 recalled facts owned by tenant "acme" user "u-7" team "platform" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 5
    Then recall fails with status 502 and code "PROVIDER_ERROR"

  Scenario: Error path — result_cap out of range
    Given a retrieval engine over an embedded store with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 200
    Then recall fails with status 400 and code "VAL_OUT_OF_RANGE"

  Scenario: Pagination — the cursor resumes after the prior page
    Given a retrieval engine over an embedded store with embedding dimension 8
    And the embedding provider returns a query vector of dimension 8
    And the reranker scores every document 0.90
    And 4 recalled facts owned by tenant "acme" user "u-7" team "platform" with embedding dimension 8
    When recall is invoked with query "who owns the orders table" and result_cap 2
    Then the response returns at most 2 facts
    And a next_cursor is present
    And the cursor is saved and recall is invoked again with result_cap 2
    Then the second page facts do not overlap the first page
