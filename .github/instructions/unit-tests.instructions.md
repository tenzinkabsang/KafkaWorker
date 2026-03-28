---
applyTo: "KafkaWorker.Tests/**/*.cs"
---

# Unit Test Instructions

## Framework & Libraries
- xUnit with `[Fact]` and `[Theory]` attributes
- NSubstitute for mocking (`Substitute.For<T>()`)
- xUnit built-in `Assert` class for assertions
- `Microsoft.Extensions.Time.Testing.FakeTimeProvider` for time-dependent tests

## Patterns
- One test class per production class (e.g., `ConsumerTests` for `Consumer<TMessage>`)
- Constructor initializes all mocks and shared test data; implement `IDisposable` for cleanup (e.g., `CancellationTokenSource`)
- Use a `CreateConsumer()` (or similar) factory helper method to construct the SUT with all mocked dependencies
- Follow Arrange / Act / Assert structure — use comments to separate sections only when the test is complex
- Test method naming: `MethodName_Scenario_ExpectedBehavior` (e.g., `ExecuteAsync_WhenMessageIsInvalid_PublishesToDlq`)

## Mocking Kafka Types
- Mock `IConsumer<string, TMessage>` and `IProducer<string, TMessage>` via NSubstitute
- Use `new ConsumeResult<string, T> { Message = new Message<string, T> { Value = ..., Headers = new Headers() }, TopicPartitionOffset = ... }` for test data
- Mock `Consume(CancellationToken)` to return test `ConsumeResult` instances
- For header assertions, use `KafkaHeaderExtensions` to read/write headers

## What NOT To Do
- Do not use Moq — this project uses NSubstitute exclusively
- Do not use FluentAssertions — use xUnit `Assert.*` methods
- Do not test Polly retry behavior directly; test the observable outcomes (DLQ publish, log messages)
- Do not add new NuGet packages without asking

## Running Unit Tests
```
dotnet test KafkaWorker.Tests --no-build -c Release
```
