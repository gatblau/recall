Feature: Freshness Checker
  The read-path conditional source-change check (C5). For each distinct cited source the checker
  issues one broker conditional check under the SA-LAT-01 budget and maps the answer to a per-fact
  Currency, enqueueing one idempotent ReReadSource job on a detected change. Every failure is
  absorbed into a Currency value — the read path is never blocked or failed.

  Scenario: Happy path — unchanged source marks the fact current
    Given an embedded freshness checker with budget 25 ms and per-call 20 ms
    And the broker reports the source unchanged
    And a candidate fact "fact:1" citing source "source:A" with marker "abc"
    When the freshness check runs
    Then fact "fact:1" has currency "current"
    And no re-read job is enqueued for tenant "acme"
    And exactly 1 broker check was made

  Scenario: Changed source — enqueues one idempotent job and flags stale-pending-refresh
    Given an embedded freshness checker with budget 25 ms and per-call 20 ms
    And the broker reports the source changed
    And a candidate fact "fact:1" citing source "source:A" with marker "abc"
    And a candidate fact "fact:2" citing source "source:A" with marker "abc"
    When the freshness check runs
    Then fact "fact:1" has currency "stale-pending-refresh"
    And fact "fact:2" has currency "stale-pending-refresh"
    And exactly 1 re-read job is enqueued for tenant "acme"
    And exactly 1 broker check was made
    And a re-read job exists with key "re-read-source:source:A" for tenant "acme"

  Scenario: Edge case — a single source is checked exactly once
    Given an embedded freshness checker with budget 25 ms and per-call 20 ms
    And the broker reports the source unchanged
    And a candidate fact "fact:3" citing source "source:B" with marker "def"
    When the freshness check runs
    Then fact "fact:3" has currency "current"
    And exactly 1 broker check was made

  Scenario: Error path — broker unreachable returns unverified-currency without blocking
    Given an embedded freshness checker with budget 25 ms and per-call 20 ms
    And the broker returns an error
    And a candidate fact "fact:4" citing source "source:C" with marker "ghi"
    When the freshness check runs
    Then fact "fact:4" has currency "unverified-currency"
    And no re-read job is enqueued for tenant "acme"

  Scenario: Error path — batch deadline breach degrades to unverified-currency
    Given an embedded freshness checker with budget 25 ms and per-call 25 ms
    And the broker is slow beyond the batch budget
    And a candidate fact "fact:5" citing source "source:D" with marker "m5"
    And a candidate fact "fact:6" citing source "source:E" with marker "m6"
    And a candidate fact "fact:7" citing source "source:F" with marker "m7"
    When the freshness check runs
    Then fact "fact:5" has currency "unverified-currency"
    And fact "fact:6" has currency "unverified-currency"
    And fact "fact:7" has currency "unverified-currency"
    And the batch returned within 200 ms

  Scenario: Error path — queue failure on a changed source still flags stale-pending-refresh
    Given an embedded freshness checker with budget 25 ms and per-call 20 ms
    And the broker reports the source changed
    And the work queue is unwritable
    And a candidate fact "fact:8" citing source "source:G" with marker "m8"
    When the freshness check runs
    Then fact "fact:8" has currency "stale-pending-refresh"
