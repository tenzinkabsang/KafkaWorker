---
layout: default
title: Dead Letter Queue
nav_order: 3
---

# Dead Letter Queue
{: .no_toc }

The library provides automatic dead letter queue (DLQ) support with periodic reprocessing. Failed messages are published to a DLQ topic, and a separate consumer reprocesses them on a configurable schedule.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## How It Works

When a message fails processing after all retry attempts, it is published to the configured dead letter topic. The DLQ consumer then periodically reads these messages and reprocesses them **in place** by invoking your message handler directly — failed messages never reappear on the original topic.

```
Failed message ──► DLQ Topic ──► DLQ Consumer ──► IMessageHandler<T>
                                 (every 60 min)    (in-place)
```

If the handler fails again, the message is re-enqueued to the DLQ topic with an incremented attempt count for a future tick (bounded by `DeadLetterMaxReprocessAttempts`).

### Message Headers

When a message is sent to the DLQ, the library attaches tracking headers:

| Header | Description |
|--------|-------------|
| `original-topic` | The topic the message was originally consumed from (diagnostic) |
| `error-message` | The exception message from the failed processing attempt |
| `invalid-message` | Set to `"true"` if the message was rejected via `InvalidMessageException` |
| `batch-id` | UUID identifying the DLQ reprocessing batch (used for loop detection) |
| `reprocessed-attempt` | Counter tracking how many times this message has been reprocessed from the DLQ |


---

## Setup

### 1. Configure the DLQ topic

Add `DeadLetterTopic` to your consumer configuration:

```json
{
  "KafkaWorker": {
    "Connection": {
      "BootstrapServers": "localhost:9092"
    },
    "Consumer": {
      "GroupId": "my-order-processor",
      "Topic": "orders.v1",
      "MaxRetries": 3,
      "DeadLetterTopic": "orders.v1.dlq"
    }
  }
}
```

With this configuration, failed messages are published to the DLQ but **not** automatically reprocessed. To enable reprocessing, add step 2.

### 2. Register the DLQ consumer

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(builder.Configuration);
builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(builder.Configuration);

builder.Build().Run();
```

The DLQ consumer runs as a hosted service alongside the main consumer. It reprocesses messages **in place** by invoking your `IMessageHandler<TMessage>` directly, so you must register the main consumer (`AddKafkaWorker`) before `AddKafkaWorkerDeadLetter`. If no handler is registered, registration throws at startup.

---

## DLQ Configuration Options

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `DeadLetterTopic` | `string?` | `null` | DLQ topic name. Leave `null` to disable DLQ entirely |
| `DeadLetterMaxReprocessAttempts` | `int` | `3` | Max times a message is reprocessed before being permanently skipped (1–5) |
| `DeadLetterProcessingIntervalMinutes` | `int` | `60` | Minutes between reprocessing batches |
| `DeadLetterStartFrom` | `DateTimeOffset?` | `null` | UTC timestamp to start from when no committed offsets exist |

---

## Reprocessing Behavior

The DLQ consumer creates a DI scope, resolves your `IMessageHandler<TMessage>`, and invokes it directly — the same handler the main consumer uses. Messages **never** reappear on the original topic.

- If the handler succeeds, the offset is committed.
- If the handler throws `InvalidMessageException`, the message is permanently skipped.
- If the handler throws any other exception, the message is re-enqueued to the **DLQ topic** with an incremented `reprocessed-attempt` for a future tick (bounded by `DeadLetterMaxReprocessAttempts`).

Requires an `IMessageHandler<TMessage>` to be registered in the same process — call `AddKafkaWorker` before `AddKafkaWorkerDeadLetter`. If the handler is missing, registration throws at startup.

{: .note }
> **Why in-place** — Republishing to a shared original topic would expose previously failed messages to *every* consumer group on that topic, including services that already processed them successfully. In-place reprocessing keeps failures isolated to the owning consumer.

---

## InvalidMessageException

Throw `InvalidMessageException` from your `IMessageHandler<TMessage>` for messages that will never succeed:

```csharp
public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
{
    if (string.IsNullOrEmpty(message.OrderId))
        throw new InvalidMessageException("OrderId is required", message);

    // ...
}
```

Invalid messages:
- **Skip all retries** — go directly to the DLQ
- **Are permanently skipped during DLQ reprocessing** — they are never reprocessed

---

## DLQ Consumer Behavior

### Processing Cycle

1. The DLQ consumer waits for the configured interval (default: 60 minutes)
2. Creates a temporary Kafka consumer, subscribes to the DLQ topic
3. Reprocesses each message in place by invoking your `IMessageHandler<TMessage>` directly
4. Commits offsets after each successfully reprocessed message
5. Destroys the temporary consumer to avoid broker health-check timeouts during idle periods

### Skip Conditions

A message is skipped (not reprocessed) if:

- It was marked as an **invalid message** (`invalid-message` header is `"true"`)
- It has exceeded the **maximum reprocess attempts** (`reprocessed-attempt` header ≥ configured max)

### Loop Detection

Each reprocessing batch gets a unique `batch-id`. When the consumer encounters a message with the current batch's ID, it knows it has looped back to messages already processed in this batch and stops. This bounds reprocessing when a re-enqueued message is read again within the same tick.

### Error Handling

Unlike the main consumer, the DLQ consumer **preserves messages on failure**. If re-enqueuing a failed message back to the DLQ fails, it stops the batch without committing. The message will be retried on the next scheduled run.

{: .important }
> **Single partition DLQ** — For optimal performance, configure the dead letter topic with a single partition.

---

## DeadLetterStartFrom

When enabling DLQ reprocessing for a system that has been running, you may not want to reprocess all historical DLQ messages. Set `DeadLetterStartFrom` to a UTC timestamp:

```json
"Consumer": {
  "DeadLetterTopic": "orders.v1.dlq",
  "DeadLetterStartFrom": "2025-06-01T00:00:00Z"
}
```

The DLQ consumer uses Kafka's `OffsetsForTimes` API to seek to the first message at or after this timestamp on first startup. Once offsets are committed, this setting has no effect.

---

## Main Consumer DLQ Behavior

The main consumer's DLQ publishing is **best-effort**. If publishing to the DLQ fails after Polly retry, the main consumer:

1. Logs at `Critical` level
2. Commits the offset
3. Continues processing the next message

This design ensures the main consumer (processing millions of records) is never blocked by DLQ publish failures. The DLQ consumer has stricter guarantees — see [Error Handling](#error-handling) above.

---

## Migrating from v1 (republish) to v2 (in-place)

In **v1**, the DLQ consumer republished failed messages back to the original topic, where the main consumer (and any other consumer group on that topic) picked them up again. In **v2**, the DLQ consumer reprocesses messages **in place** by invoking your `IMessageHandler<TMessage>` directly. Failed messages never return to the original topic.

**This is a behavioral breaking change.** After upgrading:

- The `DeadLetterReprocessStrategy` configuration option has been **removed**. If your configuration sets it, delete the key — binding ignores unknown keys, but it no longer has any effect.
- Ensure `AddKafkaWorker` is called before `AddKafkaWorkerDeadLetter` so the handler is available to the DLQ consumer. A standalone DLQ reprocessor with no handler registered now throws at startup.
- If you previously relied on failed messages reappearing on the original topic for a *separate* service to re-consume, that flow is no longer supported. Run the consumer that owns the handler in the same process as the DLQ consumer instead.

