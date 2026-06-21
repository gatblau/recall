Feature: Maintenance Worker

  The C7 Maintenance Worker consolidates episodes into validated insights, supersedes
  contradictions non-destructively, decays low-salience stale facts, re-embeds stale-model facts,
  and performs verifiable hard delete — all off the synchronous read path.

  Scenario: Consolidation promotes a validated insight with source-capped confidence
    Given a maintenance worker over an embedded store with embedding dimension 8
    And 4 episodic facts sharing subject "team:alpha" with min confidence 0.6 for tenant "acme" user "u-1"
    And the consolidation LLM returns one insight citing all 4 episodes with confidence 0.95
    When the maintenance cycle runs for tenant "acme"
    Then the ConsolidationReport reports promoted 1
    And exactly 1 consolidated fact is persisted for tenant "acme"
    And the persisted consolidated fact confidence is at most 0.6

  Scenario: An insight citing a fact outside the scanned group is rejected
    Given a maintenance worker over an embedded store with embedding dimension 8
    And 4 episodic facts sharing subject "team:alpha" with min confidence 0.6 for tenant "acme" user "u-1"
    And the consolidation LLM returns one insight citing an unknown fact with confidence 0.95
    When the maintenance cycle runs for tenant "acme"
    Then the ConsolidationReport reports rejected_validation 1
    And exactly 0 consolidated facts are persisted for tenant "acme"

  Scenario: Two contradicting facts supersede the older non-destructively
    Given a maintenance worker over an embedded store with embedding dimension 8
    And the consolidation LLM returns no insights
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
    And the consolidation LLM returns no insights
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
    And the consolidation LLM returns no insights
    And a stale-model fact "fact:reembed" for tenant "acme" user "u-1"
    And the embedding provider returns a vector of dimension 4
    When a ReEmbed job is handled for "fact:reembed" in tenant "acme" user "u-1"
    Then the re-embed handler fails with code "VAL_OUT_OF_RANGE"

  Scenario: Around ten similar episodes consolidate into one insight
    Given a maintenance worker over an embedded store with embedding dimension 8
    And 10 episodic facts sharing subject "team:alpha" with min confidence 0.6 for tenant "acme" user "u-1"
    And the consolidation LLM returns one insight citing all 10 episodes with confidence 0.8
    When the maintenance cycle runs for tenant "acme"
    Then the ConsolidationReport reports promoted 1
    And exactly 1 consolidated fact is persisted for tenant "acme"
