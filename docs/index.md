---
layout: default
title: Home
nav_order: 1
permalink: /
---

# KafkaWorker

<p align="center">
  <a href="https://www.nuget.org/packages/KafkaWorker/">
    <img src="https://img.shields.io/nuget/v/KafkaWorker.svg?style=flat-square" alt="KafkaWorker NuGet Version">
  </a>
</p>

A .NET library that abstracts Kafka consumer infrastructure so you can focus on business logic. Implement `IMessageHandler<TMessage>` and the library handles the consume loop, offset management, retry with exponential backoff, dead letter queuing, and DLQ reprocessing.
{: .fs-6 .fw-300 }

[Get Started](#quick-start){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/tenzinkabsang/KafkaWorker){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Packages

| Package | Description |
|---------|-------------|
| **KafkaWorker** | Core — consumer abstraction, DI, retry, DLQ, plain JSON (System.Text.Json) |
| **KafkaWorker.Avro** | Avro deserialization via Confluent Schema Registry |
| **KafkaWorker.Protobuf** | Protobuf deserialization via Confluent Schema Registry |
| **KafkaWorker.JsonSchema** | JSON Schema deserialization via Confluent Schema Registry |

```bash
dotnet add package KafkaWorker                # plain JSON — no other packages needed
dotnet add package KafkaWorker.Avro           # for Avro + Schema Registry
dotnet add package KafkaWorker.Protobuf       # for Protobuf + Schema Registry
dotnet add package KafkaWorker.JsonSchema     # for JSON + Schema Registry
```

## Features

- **Simple abstraction** — Implement `IMessageHandler<TMessage>` to handle messages
- **Scoped DI per message** — A new DI scope per message, so scoped dependencies like EF Core `DbContext` work naturally
- **Built-in retry with exponential backoff** — Configurable retry attempts (0–5) with jitter
- **Dead letter queue support** — Failed messages are sent to a DLQ topic
- **Periodic DLQ reprocessing** — Automatically retry failed messages on a schedule
- **Invalid message handling** — Skip retries for messages that will never succeed via `InvalidMessageException`
- **Poison-message resilient** — Deserialization failures are logged, counted, and skipped — one bad payload can't crash the host
- **Multiple serialization formats** — Avro, JSON (plain and Schema Registry), and Protobuf
- **Multiple consumers per host** — Register several consumers with different `TMessage` types
- **Confluent ConsumerConfig overrides** — Customize `AutoOffsetReset`, `SessionTimeoutMs`, etc.
- **Managed-Kafka ready** — Configurable SASL mechanism and Schema Registry basic auth (Confluent Cloud friendly)
- **Built-in observability** — OpenTelemetry-compatible metrics via `System.Diagnostics.Metrics`
- **Configuration validation on startup** — Bad config fails fast before consuming

**Targets .NET 8.0 and .NET 10.0.** Confluent.Kafka and Polly are pulled in automatically.

## Quick Start

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

That's it — the simplest setup consumes and retries with zero DLQ config. See [Configuration]({{ site.baseurl }}/configuration) for all options and [Dead Letter Queue]({{ site.baseurl }}/dead-letter-queue) for DLQ setup.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Message Flow                                                   │
│                                                                 │
│  Kafka Topic ──► Consumer ──► IMessageHandler                   │
│                       │                    │                     │
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
│            Reprocess via IMessageHandler (in-place)             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Messages flow through your `IMessageHandler<TMessage>`. On success, the offset is committed. On failure, the library retries with exponential backoff. If all retries fail, the message is published to the dead letter topic. The DLQ consumer periodically reprocesses those messages by invoking your handler **in place** so failed messages never reappear on the original topic.

A message that fails deserialization never enters this flow at all — it is logged at `Critical`, counted in metrics, and committed past so the consumer keeps running.

Throwing `InvalidMessageException` short-circuits this flow — the message goes directly to the DLQ with no retries, and is permanently skipped during DLQ reprocessing.
