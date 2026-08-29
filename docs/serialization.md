---
layout: default
title: Serialization
nav_order: 5
---

# Serialization Formats
{: .no_toc }

KafkaWorker supports four serialization formats via separate NuGet packages. All formats default to `string` keys — use the 3-type-parameter overload if you need a custom key type.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

| Method | Package | Use Case |
|--------|---------|----------|
| `AddKafkaWorker` | `KafkaWorker` | Plain JSON (System.Text.Json) — no Schema Registry needed |
| `AddKafkaWorkerAvro` | `KafkaWorker.Avro` | Avro messages with Schema Registry |
| `AddKafkaWorkerProtobuf` | `KafkaWorker.Protobuf` | Protobuf messages with Schema Registry (must implement `IMessage<T>`) |
| `AddKafkaWorkerRegistryJson` | `KafkaWorker.JsonSchema` | JSON messages with Schema Registry |

Schema Registry formats require `SchemaRegistryUrls` in the connection config. The registry client is shared automatically when multiple formats are registered in the same host.

---

## Plain JSON

The default format — messages are consumed as raw strings from Kafka and deserialized using `System.Text.Json.JsonSerializer`. No Schema Registry needed.

```bash
dotnet add package KafkaWorker
```

```csharp
builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(builder.Configuration);
```

---

## Avro

Uses Confluent's Avro deserializer with Schema Registry. Your message type must be generated from an Avro schema.

```bash
dotnet add package KafkaWorker.Avro
```

```csharp
builder.Services.AddKafkaWorkerAvro<OrderMessage, OrderMessageHandler>(builder.Configuration);
```

Requires `SchemaRegistryUrls` in the connection config:

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092",
      "SchemaRegistryUrls": "http://schema-registry:8081"
    }
  }
}
```

---

## Protobuf

Uses Confluent's Protobuf deserializer with Schema Registry. Your message type must implement `Google.Protobuf.IMessage<T>`.

```bash
dotnet add package KafkaWorker.Protobuf
```

```csharp
builder.Services.AddKafkaWorkerProtobuf<OrderMessage, OrderMessageHandler>(builder.Configuration);
```

---

## JSON with Schema Registry

Uses Confluent's JSON Schema deserializer with Schema Registry for schema validation.

```bash
dotnet add package KafkaWorker.JsonSchema
```

```csharp
builder.Services.AddKafkaWorkerRegistryJson<OrderMessage, OrderMessageHandler>(builder.Configuration);
```

---

## Serializer Options (DLQ Publishing)

The Schema Registry formats serialize messages when publishing to the dead letter topic. Each registration method accepts an optional `configureSerializer` callback to customize the Confluent serializer config (`AvroSerializerConfig`, `ProtobufSerializerConfig`, or `JsonSerializerConfig`):

```csharp
builder.Services.AddKafkaWorkerAvro<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    configureSerializer: config =>
    {
        config.AutoRegisterSchemas = false;
        config.UseLatestVersion = true;
    });
```

**Why this matters for the DLQ:** Confluent serializers default to `AutoRegisterSchemas = true` with the topic name strategy, so the first message published to your dead letter topic auto-registers a new subject (`{DeadLetterTopic}-value`) in Schema Registry. In environments where clients are not allowed to register schemas, that publish fails — and since DLQ publishing is best-effort, the message is logged at Critical and lost. Either pre-register the schema under the `{DeadLetterTopic}-value` subject (alongside your main topic's subject), or set `AutoRegisterSchemas = false` and `UseLatestVersion = true` as shown above.

---

## Custom Key Types

All registration methods have a 3-type-parameter overload for custom key types:

```csharp
// Plain JSON with Guid keys
builder.Services.AddKafkaWorker<Guid, OrderMessage, OrderMessageHandler>(builder.Configuration);

// Avro with string keys (explicit)
builder.Services.AddKafkaWorkerAvro<string, OrderMessage, OrderMessageHandler>(builder.Configuration);
```

When using the 2-type-parameter overload (`<TMessage, THandler>`), the key type defaults to `string`.

---

## Schema Registry Authentication

For registries that require basic auth (e.g. Confluent Cloud), set `SchemaRegistryUsername` and `SchemaRegistryPassword` in the connection config:

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "<cluster>.confluent.cloud:9092",
      "SchemaRegistryUrls": "https://<schema-registry>.confluent.cloud",
      "SchemaRegistryUsername": "<sr-api-key>",
      "SchemaRegistryPassword": "<sr-api-secret>"
    }
  }
}
```

Both values must be set together — configuration validation fails at startup if only one is present.

---

## Schema Registry Client Sharing

When you register multiple consumers that use Schema Registry (Avro, Protobuf, or JSON Schema), the library automatically shares a single `ISchemaRegistryClient` instance across all registrations. You don't need to configure anything — the first registration creates the client, and subsequent registrations reuse it.

If your application registers its own `ISchemaRegistryClient` (any registration style — instance, type, or factory) **before** calling the `AddKafkaWorker*` methods, the library uses that client instead of creating one.
