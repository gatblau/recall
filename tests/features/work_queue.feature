Feature: Durable Work Queue
  The store-backed C2 work queue: idempotent enqueue, atomic single-winner claim, backoff then
  dead-letter on failure, and a lease-reaper that reclaims a crashed worker's job. Every scenario
  runs against an embedded in-memory SurrealDB (the real engine, in-process).

  Background:
    Given an embedded in-memory work queue with max attempts 5 and backoff base 10 ms

  Scenario: Happy path — enqueue, claim, complete
    When a producer enqueues an "extract_fact" job "work_job:wq-happy" for tenant "acme" user "u-42" with no key
    Then enqueue returns the id "work_job:wq-happy"
    And the job "work_job:wq-happy" has status "pending" and attempts 0
    When a worker claims kinds "extract_fact" with a 30 second lease
    Then the claim returns the job "work_job:wq-happy" with status "leased" and a lease set
    When the worker completes the job "work_job:wq-happy"
    Then the job "work_job:wq-happy" has status "done" and no lease

  Scenario: Idempotent enqueue deduplicates on (scope, idempotency_key)
    Given a "extract_fact" job "work_job:wq-idem-1" for tenant "acme" user "u-42" with key "k-1" is already enqueued
    When a producer enqueues an "extract_fact" job "work_job:wq-idem-2" for tenant "acme" user "u-42" with key "k-1"
    Then enqueue returns the id "work_job:wq-idem-1"
    And the queue holds exactly 1 job for tenant "acme"

  Scenario: Concurrent claim grants the job to exactly one worker
    Given a "extract_fact" job "work_job:wq-conc" for tenant "acme" user "u-42" with no key is already enqueued
    When two workers concurrently claim kinds "extract_fact" with a 30 second lease
    Then exactly one worker receives the job and the other receives none

  Scenario: Lease-reaper reclaims a crashed worker's job
    Given a leased "re_embed_fact" job "work_job:wq-reap" for tenant "acme" user "u-42" whose lease expired
    When the lease-reaper runs a sweep
    Then the reaper reclaims at least 1 job
    And the job "work_job:wq-reap" has status "pending" and no lease
    When a worker claims kinds "re_embed_fact" with a 30 second lease
    Then the claim returns the job "work_job:wq-reap" with status "leased" and a lease set

  Scenario: Retryable failure backs off then dead-letters at the attempt cap
    Given a "extract_fact" job "work_job:wq-retry" for tenant "acme" user "u-42" with no key is already enqueued
    When a worker claims kinds "extract_fact" with a 30 second lease
    And the worker fails the job "work_job:wq-retry" as retryable
    Then the job "work_job:wq-retry" has status "pending" and attempts 1
    And the job "work_job:wq-retry" not_before is in the future
    When the job "work_job:wq-retry" is driven to attempts 5 and failed once more as retryable
    Then the job "work_job:wq-retry" has status "dead_letter" and no lease
    And the dead_letter table holds a copy of "work_job:wq-retry" for tenant "acme"

  Scenario: Backend unavailable surfaces a queue error mapped to 503 QUEUE_UNAVAILABLE
    Given the work queue backend is unreachable
    When a producer attempts to enqueue an "extract_fact" job "work_job:wq-down" for tenant "acme" user "u-42"
    Then the enqueue fails with a queue backend-unavailable error
    And that queue error maps to HTTP status 503 with code "QUEUE_UNAVAILABLE"
