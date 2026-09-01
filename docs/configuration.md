---
layout: default
title: Configuration
nav_order: 3
---

# Configuration
{: .no_toc }

All settings are loaded from `IConfiguration` (typically `appsettings.json`). The library validates configuration on startup and fails fast if required values are missing.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Consumer Options

Configure under `KafkaWorker:Consumer` (or a custom section — see [Multiple Consumers]({{ site.baseurl }}/multiple-consumers)).

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `GroupId` | `string` | *(required)* | Kafka consumer group ID |
| `Topic` | `string` | *(required)* | Topic to consume from |
| `MaxRetries` | `int` | `3` | Retry attempts before sending to DLQ. **Set to `0` to disable retries entirely.** Range: 0–5 |
| `DeadLetterTopic` | `string?` | `null` | DLQ topic. **Leave `null` to disable DLQ** — failed messages are logged and skipped |
| `DeadLetterMaxReprocessAttempts` | `int` | `3` | Max times the DLQ consumer retries a message (1–5). Only applies when `DeadLetterTopic` is set |
| `DeadLetterProcessingIntervalMinutes` | `int` | `60` | Minutes between DLQ reprocessing batches. Only applies when `DeadLetterTopic` is set |
| `DeadLetterStartFrom` | `DateTimeOffset?` | `null` | UTC timestamp from which the DLQ consumer should start processing when no committed offsets exist. E.g. `"2025-06-01T00:00:00Z"` |

### Minimal Configuration

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092"
    },
    "Consumer": {
      "GroupId": "my-order-processor",
      "Topic": "orders.v1"
    }
  }
}
```

### With Dead Letter Queue

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092"
    },
    "Consumer": {
      "GroupId": "my-order-processor",
      "Topic": "orders.v1",
      "DeadLetterTopic": "orders.v1.dlq",
      "DeadLetterMaxReprocessAttempts": 3,
      "DeadLetterProcessingIntervalMinutes": 60
    }
  }
}
```

### No Retry, No DLQ

Set `MaxRetries` to `0` and omit `DeadLetterTopic` for a simple consumer that logs failures and moves on:

```json
"Consumer": {
  "GroupId": "my-order-processor",
  "Topic": "orders.v1",
  "MaxRetries": 0
}
```

---

## Connection Settings

Configure under `KafkaWorker:Connection`. Shared by all consumers and producers in the host.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `BootstrapServers` | `string` | *(required)* | Comma-separated list of Kafka broker addresses (e.g. `"broker1:9092,broker2:9092"`) |
| `SchemaRegistryUrls` | `string?` | `null` | Comma-separated Schema Registry URLs. Required when using Avro, Protobuf, or Registry JSON packages |
| `IsSecuredCluster` | `bool` | `false` | Whether the cluster requires SASL/SSL authentication. When `true`, `Username` and `Password` are required |
| `Username` | `string?` | `null` | SASL username (required when `IsSecuredCluster` is `true`) |
| `Password` | `string?` | `null` | SASL password (required when `IsSecuredCluster` is `true`) |
| `SaslMechanism` | `string` | `ScramSha512` | SASL mechanism used when `IsSecuredCluster` is `true`. Accepted values (case-insensitive): `Plain`, `ScramSha256`, `ScramSha512`, `Gssapi`, `OAuthBearer` |
| `SchemaRegistryUsername` | `string?` | `null` | Schema Registry basic-auth username (e.g. a Confluent Cloud Schema Registry API key). When set, `SchemaRegistryPassword` is required |
| `SchemaRegistryPassword` | `string?` | `null` | Schema Registry basic-auth password (e.g. a Confluent Cloud Schema Registry API secret). When set, `SchemaRegistryUsername` is required |

### Secured Cluster Example

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "broker1:9092,broker2:9092",
      "IsSecuredCluster": true,
      "Username": "<username>",
      "Password": "<password>",
      "SchemaRegistryUrls": "http://schema-registry:8081"
    }
  }
}
```

When `IsSecuredCluster` is `true`, the library configures SASL/SSL automatically:
- `SecurityProtocol = SaslSsl`
- `SaslMechanism = ScramSha512` (default — override with the `SaslMechanism` setting)

### Confluent Cloud Example

Confluent Cloud uses SASL `Plain` with API keys, and separate API keys for Schema Registry:

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "<cluster>.confluent.cloud:9092",
      "IsSecuredCluster": true,
      "SaslMechanism": "Plain",
      "Username": "<cluster-api-key>",
      "Password": "<cluster-api-secret>",
      "SchemaRegistryUrls": "https://<schema-registry>.confluent.cloud",
      "SchemaRegistryUsername": "<sr-api-key>",
      "SchemaRegistryPassword": "<sr-api-secret>"
    }
  }
}
```

For security settings not covered by configuration (e.g. custom CA locations), use the `configureConsumer` and `configureProducer` callbacks described below.

---

## ConsumerConfig Overrides

All registration methods accept an optional `Action<ConsumerConfig>` callback to customize the underlying Confluent consumer configuration:

```csharp
builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    configureConsumer: config =>
    {
        config.AutoOffsetReset = AutoOffsetReset.Earliest;
        config.SessionTimeoutMs = 45_000;
        config.MaxPollIntervalMs = 600_000;
    });
```

The callback runs before the library enforces its invariants — after your callback, `EnableAutoOffsetStore` is always `false` (the library stores an offset only after the message is handled) and `EnableAutoCommit` is always `true` (the Kafka client's background auto-commit flushes stored offsets — every `AutoCommitIntervalMs`, on rebalance, and on shutdown).

## ProducerConfig Overrides

All registration methods also accept an optional `Action<ProducerConfig>` callback to customize the producer used for dead letter publishing:

```csharp
builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    configureProducer: config =>
    {
        config.MessageTimeoutMs = 30_000;
        config.EnableIdempotence = true;
    });
```

{: .important }
> The consumer defaults to `AutoOffsetReset.Latest`, meaning a brand-new consumer group (or one with expired offsets) will skip all existing messages and only process new ones. Override to `Earliest` if you need to process historical messages on first deploy.

---

## Configuration Validation

The library validates configuration at startup using .NET's `ValidateDataAnnotations()` and `ValidateOnStart()`:

- **Required fields** — `GroupId`, `Topic`, and `BootstrapServers` must be present
- **Range constraints** — `MaxRetries` must be 0–5, `DeadLetterMaxReprocessAttempts` must be 1–5
- **Conditional validation** — When `IsSecuredCluster` is `true`, `Username` and `Password` are required; `SchemaRegistryUsername` and `SchemaRegistryPassword` must be set together
- **DLQ topic validation** — Calling `AddKafkaWorkerDeadLetter` without a configured `DeadLetterTopic` throws at startup

If any validation fails, the host throws an exception during startup before consuming any messages.
