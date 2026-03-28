---
applyTo: "KafkaWorker.IntegrationTests/**/*.cs"
---

# Integration Test Instructions

## Infrastructure
- Tests run against live Kafka (localhost:9092) and Schema Registry (localhost:8082) via `docker-compose.yml`
- Docker must be running with `docker compose up -d --wait` before executing tests

## Framework & Libraries
- xUnit with `[Fact]` attributes
- NSubstitute for mocking loggers used in assertions
- `KafkaHelper` for topic management (create/delete topics, publish seed messages)
- `HostBuilderHelper` for building `IHost` with the correct serialization format and mocked dependencies
- `TestLoggerProvider` for verifying expected log output

## Test Pattern
1. **Arrange**: Create/clear Kafka topics via `KafkaHelper`, publish seed messages using the appropriate serializer (JSON, Avro, Protobuf, RegistryJson)
2. **Act**: Build and start an `IHost` using `HostBuilderHelper`, let it run with a `CancellationTokenSource` timeout (TestLoggerProvider.WaitTime), then stop
3. **Assert**: Verify behavior through logger assertions (`TestLoggerProvider`) — check that expected log messages were written at expected levels

## Key Helpers
- `KafkaHelper.InitializeTopic(topicName)` — deletes and recreates a topic with retry logic
- `KafkaHelper.PublishJsonMessages(topic, messages)` — publishes plain JSON string messages
- `KafkaHelper.PublishAvroMessages(topic, messages)` — publishes via Schema Registry with Avro serializer
- `HostBuilderHelper.CreateHostWithJson(...)` — builds an IHost configured for plain JSON consumption
- Configuration is injected via `Dictionary<string, string?>` overrides pointing to localhost endpoints

## Naming & Organization
- One test class per serialization format (e.g., `JsonConsumerTests`, `AvroConsumerTests`)
- Test methods follow: `Scenario_ExpectedBehavior`
- Each test creates its own topic `Guid.NewGuid()` to avoid cross-test interference

## Running Integration Tests
```
docker compose up -d --wait
dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings
```
