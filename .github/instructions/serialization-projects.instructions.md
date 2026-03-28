---
applyTo: "KafkaWorker.Avro/**/*.cs,KafkaWorker.JsonSchema/**/*.cs,KafkaWorker.Protobuf/**/*.cs"
---

# Serialization Extension Projects

These projects provide Schema Registry integration for specific serialization formats. Each follows the same pattern:

## Structure
- Each project contains a `ServiceCollectionExtensions` class with a single public registration method
- Registration methods mirror the core library's pattern but configure format-specific Confluent serializers/deserializers
- Dependencies: `Confluent.SchemaRegistry`, plus format-specific packages (`Confluent.SchemaRegistry.Serdes.Avro`, etc.)

## Rules
- Keep the public API surface minimal — one registration extension method per project
- Schema Registry URL comes from `KafkaWorkerConfig.SchemaRegistryUrl`
- Follow the same DI registration pattern as the core `ServiceCollectionExtensions`
- These projects also target `net8.0` and `net10.0`
