Feature: Maintenance Worker

  The C7 Maintenance Worker supersedes contradictions non-destructively, decays low-salience stale
  facts, re-embeds stale-model facts, and performs verifiable hard delete — all off the synchronous
  read path.

  Scenario: Two contradicting facts supersede the older non-destructively
    Given a maintenance worker over an embedded store with embedding dimension 8
    And a fact "fact:old" with object "table:orders" valid from "2026-06-19T12:00:00.000Z" for tenant "acme" user "u-1"
    And a fact "fact:new" with object "table:invoices" valid from "2026-06-20T12:00:00.000Z" for tenant "acme" user "u-1"
    When the maintenance cycle runs for tenant "acme"
    Then the SupersessionReport reports superseded 1
    And fact "fact:old" for tenant "acme" user "u-1" has a non-null valid_to
    And fact "fact:old" for tenant "acme" user "u-1" superseded_by is "fact:new"
    And fact "fact:new" for tenant "acme" user "u-1" supersedes is "fact:old"
    And fact "fact:new" for tenant "acme" user "u-1" has a null valid_to

  Scenario: Low-salience stale fact is pruned while high-salience survives
    Given a maintenance worker over an embedded store with embedding dimension 8
    And a stale fact "fact:lowsal" with salience 0.1 last recalled 10 days ago for tenant "acme" user "u-1"
    And a stale fact "fact:highsal" with salience 0.9 last recalled 10 days ago for tenant "acme" user "u-1"
    When the maintenance cycle runs for tenant "acme"
    Then the DecayReport reports pruned 1
    And fact "fact:lowsal" for tenant "acme" user "u-1" has a non-null valid_to
    And fact "fact:highsal" for tenant "acme" user "u-1" has a null valid_to

  Scenario: Verifiable hard delete returns a deletion proof
    Given a maintenance worker over an embedded store with embedding dimension 8
    And a fact "fact:erase" with object "table:orders" valid from "2026-06-20T12:00:00.000Z" for tenant "acme" user "u-1"
    When a HardDelete job is handled for "fact:erase" in tenant "acme" user "u-1"
    Then a deletion proof is returned for record "fact:erase"
    And fact "fact:erase" for tenant "acme" user "u-1" is absent

  Scenario: Re-embed dimension mismatch skips the fact without updating the embedding
    Given a maintenance worker over an embedded store with embedding dimension 8
    And a stale-model fact "fact:reembed" for tenant "acme" user "u-1"
    And the embedding provider returns a vector of dimension 4
    When a ReEmbed job is handled for "fact:reembed" in tenant "acme" user "u-1"
    Then the re-embed handler fails with code "VAL_OUT_OF_RANGE"
