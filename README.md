# KafkaWorker

[![NuGet Version](https://img.shields.io/nuget/v/KafkaWorker.svg?style=flat)](https://www.nuget.org/packages/KafkaWorker/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/KafkaWorker.svg?style=flat)](https://www.nuget.org/packages/KafkaWorker/)
[![CI](https://github.com/tenzinkabsang/KafkaWorker/actions/workflows/ci.yml/badge.svg)](https://github.com/tenzinkabsang/KafkaWorker/actions/workflows/ci.yml)

A .NET library that abstracts Kafka consumer infrastructure so you can focus on business logic. Implement `IMessageHandler<TMessage>` and the library handles the consume loop, offset management, retry with exponential backoff, dead letter queuing, and DLQ reprocessing.

📖 **[Full Documentation](https://tenzinkabsang.github.io/KafkaWorker/)** — guides, configuration, and examples.

Supports multiple consumers per host, scoped dependency injection per message, Avro / Protobuf / JSON serialization (with or without Schema Registry), custom key types, and OpenTelemetry-compatible metrics — all with sensible defaults and minimal configuration.

**Targets .NET 8.0 and .NET 10.0.** Confluent.Kafka and Polly are pulled in automatically.

## Packages

| Package | Description |
|---------|-------------|
| [`KafkaWorker`](https://www.nuget.org/packages/KafkaWorker/) | Core — consumer abstraction, DI, retry, DLQ, plain JSON (System.Text.Json) |
| [`KafkaWorker.Avro`](https://www.nuget.org/packages/KafkaWorker.Avro/) | Avro deserialization via Confluent Schema Registry |
| [`KafkaWorker.Protobuf`](https://www.nuget.org/packages/KafkaWorker.Protobuf/) | Protobuf deserialization via Confluent Schema Registry |
| [`KafkaWorker.JsonSchema`](https://www.nuget.org/packages/KafkaWorker.JsonSchema/) | JSON Schema deserialization via Confluent Schema Registry |

```bash
dotnet add package KafkaWorker                # plain JSON — no other packages needed
dotnet add package KafkaWorker.Avro           # for Avro + Schema Registry
dotnet add package KafkaWorker.Protobuf       # for Protobuf + Schema Registry
dotnet add package KafkaWorker.JsonSchema     # for JSON + Schema Registry
```

## Features

- **Simple abstraction** — Implement `IMessageHandler<TMessage>` to handle messages
- **Scoped DI per message** — A new DI scope per message, so scoped dependencies like EF Core `DbContext` work naturally
- **Built-in retry with exponential backoff** *(optional)* — Configurable retry attempts (0–5) with jitter. Set `MaxRetries` to `0` to disable
- **Dead letter queue support** *(optional)* — Failed messages are sent to a DLQ. Leave `DeadLetterTopic` null to disable
- **Periodic DLQ reprocessing** *(optional)* — Register `AddKafkaWorkerDeadLetter` to automatically retry failed messages on a schedule
- **Invalid message handling** — Skip retries for messages that will never succeed via `InvalidMessageException`
- **Multiple serialization formats** — Avro, JSON (plain and with Schema Registry), and Protobuf via separate packages
- **Multiple consumers per host** — Register several `AddKafkaWorker` calls with different `TMessage` types, each pointing to its own config section
- **Confluent ConsumerConfig overrides** — Pass an `Action<ConsumerConfig>` callback to customize `AutoOffsetReset`, `SessionTimeoutMs`, and other Confluent settings
- **Built-in observability** — Emits OpenTelemetry-compatible metrics (`System.Diagnostics.Metrics`) for messages processed, processing duration, and DLQ activity
- **Configuration validation on startup** — Bad config fails fast before consuming any messages

## Quick Start

A complete, copy-paste-ready example to get a JSON consumer running.

### 1. Create a new worker service

```bash
dotnet new worker -n MyService
cd MyService
dotnet add package KafkaWorker
```

### 2. Define your message type

```csharp
public record OrderMessage
{
    public required string OrderId { get; init; }
    public required string CustomerId { get; init; }
    public required decimal Total { get; init; }
}
```

### 3. Implement the message handler

This is the **only class you need to write**. The library handles everything else.

```csharp
public class OrderMessageHandler(
    IOrderService orderService,
    ILogger<OrderMessageHandler> logger) : IMessageHandler<OrderMessage>
{
    public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        // Throw InvalidMessageException for messages that will never succeed —
        // they go straight to the DLQ with no retries
        if (string.IsNullOrEmpty(message.OrderId))
            throw new InvalidMessageException("OrderId is required", message);

        // Business logic — any other exception triggers automatic retry
        await orderService.ProcessAsync(message, stoppingToken);

        logger.LogDebug("Processed order {OrderId}", message.OrderId);
    }
}
```

### 4. Register in Program.cs

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(builder.Configuration);

// Optional: enable DLQ reprocessing (requires DeadLetterTopic in config)
// builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(builder.Configuration);

builder.Build().Run();
```

### 5. Add configuration

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092"
    },
    "Consumer": {
      "GroupId": "my-order-processor",
      "Topic": "orders.v1",
      "MaxRetries": 3
    }
  }
}
```

That's it — the simplest setup consumes and retries with zero DLQ config. Add `DeadLetterTopic` when you're ready for dead letter support:

```json
"Consumer": {
  "GroupId": "my-order-processor",
  "Topic": "orders.v1",
  "MaxRetries": 3,
  "DeadLetterTopic": "orders.v1.dlq"
}
```

Then uncomment the `AddKafkaWorkerDeadLetter` line in `Program.cs` to enable periodic reprocessing.

Or set `MaxRetries` to `0` and omit `DeadLetterTopic` for a simple consumer with no retry and no DLQ:

```json
"Consumer": {
  "GroupId": "my-order-processor",
  "Topic": "orders.v1",
  "MaxRetries": 0
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Message Flow                                                   │
│                                                                 │
│  Kafka Topic ──► Consumer ──► IMessageHandler                   │
│                       │                    │                    │
│                       │              ┌─────┴─────┐              │
│                       │              │           │              │
│                       │           Success    Exception          │
│                       │              │           │              │
│                       │              ▼           ▼              │
│                       │           Commit     Retry (n×)         │
│                       │                          │              │
│                       │                     Still fails?        │
│                       │                          │              │
│                       │                          ▼              │
│                       │                    Send to DLQ          │
│                       │                          │              │
│                       ▼                          ▼              │
│              DLQ Consumer ◄──────────────── DLQ Topic           │
│                   │                                             │
│              (every 60 min)                                     │
│                   │                                             │
│                   ▼                                             │
│           Republish to original topic                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Messages flow through your `IMessageHandler<TMessage>`. On success, the offset is committed. On failure, the library retries with exponential backoff. If all retries fail, the message is published to the dead letter topic. The DLQ consumer periodically republishes those messages back to the original topic for another attempt.

Throwing `InvalidMessageException` short-circuits this flow — the message goes directly to the DLQ with no retries, and is permanently skipped during DLQ reprocessing.

## Configuration

### Consumer Options

Configure under `KafkaWorker:Consumer` (or a custom section — see [Multiple Consumers](#multiple-consumers)).

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `GroupId` | `string` | *(required)* | Kafka consumer group ID |
| `Topic` | `string` | *(required)* | Topic to consume from |
| `MaxRetries` | `int` | `3` | Retry attempts before sending to DLQ. **Set to `0` to disable retries entirely.** Range: 0–5 |
| `DeadLetterTopic` | `string?` | `null` | DLQ topic. **Leave `null` to disable DLQ** — failed messages are logged and skipped |
| `DeadLetterMaxReprocessAttempts` | `int` | `3` | Max times the DLQ consumer retries a message (1–5). Only applies when `DeadLetterTopic` is set |
| `DeadLetterProcessingIntervalMinutes` | `int` | `60` | Minutes between DLQ reprocessing batches. Only applies when `DeadLetterTopic` is set |
| `DeadLetterStartFrom` | `DateTimeOffset?` | `null` | UTC timestamp from which the DLQ consumer should start processing when no committed offsets exist. Useful when enabling DLQ after the system has been running. E.g. `"2025-06-01T00:00:00Z"` |

### Connection Settings

Configure under `KafkaWorker:Connection`. Shared by all consumers and producers in the host.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `BootstrapServers` | `string` | *(required)* | Comma-separated list of Kafka broker addresses (e.g. `"broker1:9092,broker2:9092"`) |
| `SchemaRegistryUrls` | `string?` | `null` | Comma-separated Schema Registry URLs. Required when using Avro, Protobuf, or Registry JSON packages |
| `IsSecuredCluster` | `bool` | `false` | Whether the cluster requires SASL/SSL authentication. When `true`, `Username` and `Password` are required |
| `Username` | `string?` | `null` | SASL username (required when `IsSecuredCluster` is `true`) |
| `Password` | `string?` | `null` | SASL password (required when `IsSecuredCluster` is `true`) |

Example with a secured cluster and Schema Registry:

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

## Serialization Formats

Choose the registration method that matches your message format. All methods default to `string` keys — use the `<TKey, TMessage, THandler>` overload if you need a custom key type.

| Method | Package | Use Case |
|--------|---------|----------|
| `AddKafkaWorker` | `KafkaWorker` | Plain JSON (System.Text.Json) — no Schema Registry needed |
| `AddKafkaWorkerAvro` | `KafkaWorker.Avro` | Avro messages with Schema Registry |
| `AddKafkaWorkerProtobuf` | `KafkaWorker.Protobuf` | Protobuf messages with Schema Registry (must implement `IMessage<T>`) |
| `AddKafkaWorkerRegistryJson` | `KafkaWorker.JsonSchema` | JSON messages with Schema Registry |

Schema Registry formats require `SchemaRegistryUrls` in the connection config. The registry client is shared automatically when multiple formats are registered in the same host.

## Testing Your Handler

Your `IMessageHandler<TMessage>` is a plain class — test it directly without any Kafka infrastructure:

```csharp
[Fact]
public async Task HandleMessageAsync_ValidOrder_Succeeds()
{
    var orderService = Substitute.For<IOrderService>();
    var logger = Substitute.For<ILogger<OrderMessageHandler>>();
    var handler = new OrderMessageHandler(orderService, logger);

    var message = new OrderMessage { OrderId = "123", CustomerId = "C1", Total = 99.99m };

    await handler.HandleMessageAsync(message, CancellationToken.None);

    await orderService.Received(1).ProcessAsync(message, Arg.Any<CancellationToken>());
}

[Fact]
public async Task HandleMessageAsync_MissingOrderId_ThrowsInvalidMessageException()
{
    var handler = new OrderMessageHandler(
        Substitute.For<IOrderService>(),
        Substitute.For<ILogger<OrderMessageHandler>>());

    var message = new OrderMessage { OrderId = "", CustomerId = "C1", Total = 0m };

    await Assert.ThrowsAsync<InvalidMessageException>(
        () => handler.HandleMessageAsync(message, CancellationToken.None));
}
```

## Multiple Consumers

Run multiple consumers in a single host by pointing each registration to a different config section:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(
    builder.Configuration,
    configSection: "KafkaWorker:OrderConsumer");

builder.Services.AddKafkaWorker<PaymentMessage, PaymentMessageHandler>(
    builder.Configuration,
    configSection: "KafkaWorker:PaymentConsumer");

builder.Build().Run();
```

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092"
    },
    "OrderConsumer": {
      "GroupId": "order-processor",
      "Topic": "orders.v1",
      "MaxRetries": 3,
      "DeadLetterTopic": "orders.v1.dlq"
    },
    "PaymentConsumer": {
      "GroupId": "payment-processor",
      "Topic": "payments.v1",
      "MaxRetries": 5
    }
  }
}
```

The default `configSection` is `KafkaWorker:Consumer` — if you only have one consumer, you don't need to specify it.

Each `TMessage` type can only be registered once per host. Calling `AddKafkaWorker<OrderMessage, ...>()` twice throws at startup. If you need two consumers for the same data shape, create distinct message types (e.g., `OrderMessageV1`, `OrderMessageV2`).

> **DLQ registration:** When using custom config sections, pass the same `configSection` to `AddKafkaWorkerDeadLetter`:
> ```csharp
> builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(
>     builder.Configuration,
>     configSection: "KafkaWorker:OrderConsumer");
> ```

## Confluent ConsumerConfig Overrides

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

The callback runs before the library enforces its invariants — `EnableAutoCommit` and `EnableAutoOffsetStore` are always set to `false` after your callback, since the library manages offsets manually.

> **Default:** The consumer uses `AutoOffsetReset.Latest`, meaning a brand-new consumer group (or one with expired offsets) will skip all existing messages and only process new ones. Override to `Earliest` if you need to process historical messages on first deploy.

## Metrics

The library emits [OpenTelemetry-compatible metrics](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics) via `System.Diagnostics.Metrics` (BCL — no additional packages needed). When no listener is attached, metrics are zero-cost no-ops.

### Available Metrics

| Instrument | Type | Tags | Description |
|------------|------|------|-------------|
| `kafkaworker.messages.processed` | Counter | `topic`, `status` (`success`, `invalid`, `failed`) | Messages processed |
| `kafkaworker.messages.processing_duration` | Histogram (ms) | `topic` | Processing duration per message |
| `kafkaworker.messages.dlq_published` | Counter | `topic`, `dlq_topic` | Messages published to DLQ |
| `kafkaworker.dlq.messages_reprocessed` | Counter | `topic`, `dlq_topic` | Messages reprocessed from DLQ |
| `kafkaworker.dlq.messages_skipped` | Counter | `topic`, `reason` (`invalid`, `max_attempts`, `missing_topic`) | Messages skipped during DLQ reprocessing |

### Subscribing to Metrics

Add the `KafkaWorker` meter to your OpenTelemetry configuration:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter("KafkaWorker");
        metrics.AddPrometheusExporter(); // or any exporter
    });
```

Metrics work with any `System.Diagnostics.Metrics`-compatible listener — OpenTelemetry, `dotnet-counters`, Prometheus, Azure Monitor, etc.

## Important Notes

- **Scoped DI per message** — `IMessageHandler<TMessage>` is resolved in a new DI scope for each message. Scoped dependencies like EF Core `DbContext` work naturally via constructor injection.
- **DLQ is best-effort from the main consumer** — The main consumer attempts to publish failed messages to the DLQ with Polly retry, but if all attempts fail it logs at `Critical`, commits the offset, and moves on. Processing incoming records takes priority over guaranteeing every failed message reaches the DLQ.
- **DLQ consumer preserves messages on failure** — Unlike the main consumer, if the DLQ consumer fails to republish a message to the original topic, it stops the batch without committing. The message will be retried on the next scheduled run.
- **Single partition DLQ** — For optimal performance, configure the dead letter topic with a single partition.
- **Backpressure** — The consume loop processes messages sequentially, so it naturally applies backpressure — Kafka won't outpace your processor. If you need to throttle calls to a downstream system, add rate limiting inside your `HandleMessageAsync` implementation.

## What the Library Handles

- Consumer subscription, consume loop, and graceful shutdown
- `StoreOffset()` + `Commit()` after every message (success, DLQ publish, or DLQ publish failure)
- Retry with exponential backoff and jitter (Polly)
- Publishing to DLQ with tracking headers (`original-topic`, `error-message`, `invalid-message`, `batch-id`, `reprocessed-attempt`)
- DLQ reprocessing on a timer with loop detection
- Configuration validation on startup

## Requirements

- .NET 8.0 or .NET 10.0
- Confluent.Kafka (pulled in automatically)
- Polly (pulled in automatically)
