---
description: "Add a new integration test for an existing serialization format. Follows the established pattern using KafkaHelper, HostBuilderHelper, and log-based assertions."
---

# Add Integration Test

I need to add a new integration test to `KafkaWorker.IntegrationTests/`.

## Context
- Study existing tests first to match the pattern exactly:
  - Look at test classes in `KafkaWorker.IntegrationTests/` for examples
  - Each test class covers one serialization format
- Tests use `KafkaHelper` for topic setup and `HostBuilderHelper` for host creation
- Assertions are done via `TestLoggerProvider` log message verification
- Each test creates a unique topic via `Guid.NewGuid()` to avoid interference

## Test Structure
1. **Arrange**: Create topic via `KafkaHelper.InitializeTopic()`, publish seed messages
2. **Act**: Build `IHost` via `HostBuilderHelper`, run with timeout via `CancellationTokenSource`
3. **Assert**: Verify expected log messages via `TestLoggerProvider`

## Important
- Docker must be running: `docker compose up -d --wait`
- Run one TFM at a time to avoid contention:
  ```
  dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings -f net10.0
  ```
- Test name format: `Scenario_ExpectedBehavior`
