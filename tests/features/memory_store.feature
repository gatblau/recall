Feature: Memory Store

  Background:
    Given an embedded in-memory memory store with embedding dimension 8

  Scenario: Happy path — put a fact then recall it by vector + keyword
    Given a provisioned tenant namespace "acme"
    And a fact "fact:h1" owned by tenant "acme" team "alpha" user "u-sarah" with visibility "team-shared"
    And the fact "fact:h1" has an embedding of dimension 8
    When recall is called for tenant "acme" user "u-sarah" team "alpha" with a vector of dimension 8 and keyword "orders"
    Then a candidate for "fact:h1" is returned
    And the candidate semantic_score and keyword_score are both in range

  Scenario: Edge case — cross-tenant isolation on recall
    Given a provisioned tenant namespace "acme"
    And a fact "fact:c1" owned by tenant "acme" team "none" user "u-sarah" with visibility "tenant-shared"
    And the fact "fact:c1" has an embedding of dimension 8
    When recall is called for tenant "globex" user "u-other" team "none" with a vector of dimension 8 and keyword "orders"
    Then no candidates are returned

  Scenario: Edge case — supersession ends validity without deleting
    Given a provisioned tenant namespace "acme"
    And a fact "fact:old" owned by tenant "acme" team "none" user "u-sarah" with visibility "user-private"
    And a fact "fact:new" owned by tenant "acme" team "none" user "u-sarah" with visibility "user-private"
    When supersede is called for tenant "acme" user "u-sarah" with old "fact:old" new "fact:new" at "2026-06-20T12:00:00.000Z"
    Then get_fact for tenant "acme" user "u-sarah" id "fact:old" still returns the record
    And "fact:old" valid_to equals "2026-06-20T12:00:00.000Z"
    And "fact:old" superseded_by equals "fact:new"
    And "fact:new" supersedes equals "fact:old"

  Scenario: Edge case — verifiable hard delete removes derived summaries and embeddings
    Given a provisioned tenant namespace "acme"
    And a fact "fact:base" owned by tenant "acme" team "none" user "u-sarah" with visibility "user-private"
    And the fact "fact:base" has an embedding of dimension 8
    And a consolidated insight "fact:ins1" derived from "fact:base" owned by tenant "acme" user "u-sarah"
    And a consolidated insight "fact:ins2" derived from "fact:base" owned by tenant "acme" user "u-sarah"
    When hard_delete is called for tenant "acme" user "u-sarah" id "fact:base"
    Then the deletion proof lists derived removed "fact:ins1" and "fact:ins2"
    And the deletion proof digest equals the sha256 of the sorted removed ids
    And the deletion proof embeddings_removed is at least 1
    And get_fact for tenant "acme" user "u-sarah" id "fact:base" returns none

  Scenario: Error path — score out of range on write
    Given a provisioned tenant namespace "acme"
    When put_fact is called for a fact "fact:bad" in tenant "acme" user "u-sarah" with confidence 1.4
    Then the put_fact call returns a validation error
    And get_fact for tenant "acme" user "u-sarah" id "fact:bad" returns none

  Scenario: Error path — get of an out-of-scope record returns none
    Given a provisioned tenant namespace "acme"
    And a fact "fact:p1" owned by tenant "acme" team "none" user "u-sarah" with visibility "user-private"
    When get_fact is called for tenant "acme" user "u-bob" id "fact:p1"
    Then the get_fact result is none
