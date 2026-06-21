Feature: Phase 1 boot smoke
  The scaffolding binary boots from a minimal valid environment, serves the liveness probe,
  and returns the X1 error envelope for an unknown route.

  Scenario: Process boots and serves liveness
    Given the recall app is booted with a minimal valid environment
    When I GET "/healthz"
    Then the response status is 200
    And the JSON field "data.status" is "live"
    And the response carries a correlation id

  Scenario: Unknown route returns the X1 error envelope
    Given the recall app is booted with a minimal valid environment
    When I GET "/no-such-route"
    Then the response status is 404
    And the JSON field "error.code" is "NOT_FOUND"
    And the JSON field "error.correlation_id" is a non-empty string
