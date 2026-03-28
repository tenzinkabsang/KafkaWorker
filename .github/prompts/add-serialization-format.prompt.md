---
description: "Scaffold a new serialization format extension project (like Avro, Protobuf, JsonSchema). Provides step-by-step guidance for creating the project, ServiceCollectionExtensions, and integration tests."
---

# Add New Serialization Format

I need to add a new serialization format extension for KafkaWorker.

## Context
- Look at the existing extension projects for the pattern to follow:
  - `KafkaWorker.Avro/` — Avro with Schema Registry
  - `KafkaWorker.Protobuf/` — Protobuf with Schema Registry
  - `KafkaWorker.JsonSchema/` — JSON Schema with Schema Registry
- Each project contains a `ServiceCollectionExtensions` class with one public registration method
- Each project targets both `net8.0` and `net10.0`
- Each has a corresponding integration test class in `KafkaWorker.IntegrationTests/`

## Steps
1. Create a new project `KafkaWorker.{FormatName}/` following the exact structure of an existing extension
2. Add a `ServiceCollectionExtensions.cs` with `AddKafkaWorker{FormatName}<TMessage>()` method
3. Add the project to `KafkaWorker.slnx`
4. Add integration tests in `KafkaWorker.IntegrationTests/{FormatName}ConsumerTests.cs`
5. Update the README with the new format's registration example
6. Verify it builds: `dotnet build -c Release`
7. Run existing tests to ensure nothing breaks: `dotnet test KafkaWorker.Tests -c Release`
