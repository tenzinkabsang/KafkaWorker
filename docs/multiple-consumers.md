---
layout: default
title: Multiple Consumers
nav_order: 5
---

# Multiple Consumers
{: .no_toc }

Run multiple Kafka consumers in a single host by pointing each registration to a different configuration section.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Registration

Each consumer needs a distinct `TMessage` type and its own config section:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaWorker<OrderMessage, OrderMessageProcessor>(
    builder.Configuration,
    configSection: "KafkaWorker:OrderConsumer");

builder.Services.AddKafkaWorker<PaymentMessage, PaymentMessageProcessor>(
    builder.Configuration,
    configSection: "KafkaWorker:PaymentConsumer");

builder.Build().Run();
```

## Configuration

Each consumer gets its own section. The `Connection` section is shared:

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

---

## DLQ Registration with Custom Sections

When using custom config sections, pass the same `configSection` to `AddKafkaWorkerDeadLetter`:

```csharp
builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(
    builder.Configuration,
    configSection: "KafkaWorker:OrderConsumer");
```

---

## Duplicate Registration Guard

Each `TMessage` type can only be registered once per host. Calling `AddKafkaWorker<OrderMessage, ...>()` twice throws an `InvalidOperationException` at startup.

If you need two consumers for the same data shape, create distinct message types:

```csharp
public record OrderMessageV1 { /* ... */ }
public record OrderMessageV2 { /* ... */ }
```

---

## Mixing Serialization Formats

You can mix serialization formats in the same host:

```csharp
// Plain JSON consumer
builder.Services.AddKafkaWorker<OrderMessage, OrderProcessor>(
    builder.Configuration,
    configSection: "KafkaWorker:OrderConsumer");

// Avro consumer
builder.Services.AddKafkaWorkerAvro<InventoryEvent, InventoryProcessor>(
    builder.Configuration,
    configSection: "KafkaWorker:InventoryConsumer");

// Protobuf consumer
builder.Services.AddKafkaWorkerProtobuf<ShipmentEvent, ShipmentProcessor>(
    builder.Configuration,
    configSection: "KafkaWorker:ShipmentConsumer");
```

Each consumer runs as an independent hosted service with its own Kafka consumer instance, configuration, and lifecycle.
