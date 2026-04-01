---
layout: default
title: Serialization
nav_order: 4
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

## Schema Registry Client Sharing

When you register multiple consumers that use Schema Registry (Avro, Protobuf, or JSON Schema), the library automatically shares a single `CachedSchemaRegistryClient` instance across all registrations. You don't need to configure anything — the first registration creates the client, and subsequent registrations reuse it.
