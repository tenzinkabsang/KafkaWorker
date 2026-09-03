---
layout: default
title: Dead Letter Queue
nav_order: 4
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
| `deserialization-failed` | Set to `"true"` on raw-bytes records captured when a message failed deserialization; these are never auto-reprocessed |


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

## Ordering and Idempotency

A dead-lettered message is retried **out of order**: by the time it succeeds, later messages for the same key on the original topic have usually been processed already. This is inherent to any DLQ design — a failed message steps out of the partition's ordered stream.

Design your handler accordingly:

- **Idempotent** — reprocessing a message that (partially) succeeded before must not double-apply effects.
- **Order-tolerant** — a stale message may arrive after newer state was written; guard with timestamps, versions, or upserts where it matters.

If strict per-key ordering is a hard requirement for your domain, don't rely on DLQ recovery for those messages — treat failures as fatal for the key instead (e.g., throw `InvalidMessageException` and handle the key's state out of band).

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
- It is a **tombstone** (null value) — committed past without invoking the handler
- It **cannot be deserialized** — committed past and counted in the `dlq.messages_skipped` metric with reason `deserialization_failed`. A record carrying the `deserialization-failed` header (raw bytes [captured by the main consumer](#poison-message-capture), awaiting manual redrive) is skipped quietly at `Debug`; any other undeserializable record is logged at `Critical`

Tombstones and undeserializable records never end the batch — they are committed past so the DLQ consumer always makes progress.

### Loop Detection

Each reprocessing batch gets a unique `batch-id`. When the consumer encounters a message with the current batch's ID, it knows it has looped back to messages already processed in this batch and stops. This bounds reprocessing when a re-enqueued message is read again within the same tick.

### Error Handling

Unlike the main consumer, the DLQ consumer **preserves messages on failure**. If re-enqueuing a failed message back to the DLQ fails, it stops the batch without committing. The message will be retried on the next scheduled run.

A consume error that carries no record offset (e.g. a transient broker error) also ends the batch without committing; the batch is retried on the next tick.

{: .important }
> **Single partition DLQ** — For optimal performance, configure the dead letter topic with a single partition.

{: .important }
> **Size DLQ retention generously** — the DLQ topic doubles as your failure archive: terminal messages and captured poison records stay in it *only* until the topic's retention expires. Set a long retention on DLQ topics, or make it unlimited:
> ```bash
> kafka-configs --alter --topic orders.v1.dlq --add-config retention.ms=-1
> ```

---

## Poison-Message Capture

A message that fails **deserialization** never reaches your handler and can never be represented as your message type — so it cannot go through the normal typed DLQ flow. Instead, when a `DeadLetterTopic` is configured, the main consumer captures the record's **raw key and value bytes verbatim** to the DLQ, with the usual `original-topic` / `error-message` headers plus `deserialization-failed: true`, and logs at `Error`. Nothing is lost: the record is preserved for inspection and manual redrive (fix the payload or the schema, then republish to the *original* topic).

- The capture uses a plain `byte[]` producer — Schema Registry is **not** involved, so this works even with locked-down registries.
- The DLQ consumer recognizes the `deserialization-failed` header and skips these records quietly — they are never auto-reprocessed (reprocessing can't succeed until the payload or schema is fixed).
- Capture is best-effort: if the capture publish itself fails, or no DLQ is configured, the record is logged at `Critical` and lost — same as the pre-capture behavior.

---

## Terminal Failure Sink

The DLQ topic is transport, not an archive — its retention eventually erases terminal failures, and a Kafka topic can't be queried, annotated, or selectively redriven. If you want terminal failures somewhere durable and queryable (a database table, blob storage, an alerting system), implement `ITerminalFailureSink<TMessage>` and register it — the library calls it at the exact moment it permanently gives up on a message:

```csharp
public class PostgresFailureSink(FailureDbContext db) : ITerminalFailureSink<OrderMessage>
{
    public async Task HandleAsync(TerminalFailure<OrderMessage> failure, CancellationToken ct)
    {
        db.FailedMessages.Add(new FailedMessageRow
        {
            Key = failure.MessageKey?.ToString(),
            Payload = JsonSerializer.Serialize(failure.Message),
            SourceTopic = failure.SourceTopic,
            Reason = failure.Reason.ToString(),
            Error = failure.Error,
            Attempts = failure.ReprocessAttempts,
            FailedAtUtc = DateTime.UtcNow
        });
        await db.SaveChangesAsync(ct);
    }
}
```

```csharp
builder.Services.AddScoped<ITerminalFailureSink<OrderMessage>, PostgresFailureSink>();
```

Any DI lifetime works — the sink is resolved from a fresh scope per call, so scoped dependencies like an EF Core `DbContext` inject naturally. The sink fires **once per terminal message** (`TerminalFailureReason` says why):

| Reason | Where | Meaning |
|--------|-------|---------|
| `InvalidMessage` | DLQ consumer | Marked invalid, or rejected with `InvalidMessageException` during reprocessing |
| `MaxReprocessAttemptsExceeded` | DLQ consumer | Retries exhausted; permanently skipped |
| `DeadLetterPublishFailed` | Main consumer | The best-effort DLQ publish failed — the sink is the message's **last chance** to be persisted anywhere |
| `NoDeadLetterTopicConfigured` | Main consumer | Processing failed with no DLQ configured — also a last-chance call |

A message that reaches the DLQ successfully does *not* fire the sink until it later becomes terminal there. Records that failed deserialization never fire the typed sink — they are [captured as raw bytes](#poison-message-capture) instead.

{: .note }
> The sink is **best-effort**: an exception it throws is logged at `Error` and never crashes the consumer, stops the batch, or prevents the offset from advancing. If the sink write must never be lost, make the sink itself durable (retry or write-ahead internally).

---

## On-Demand Reprocessing

The DLQ consumer normally waits for its configured interval between batches. When you want failed messages retried *right now* — say, a downstream API was down, messages piled into the DLQ, and the API has just been fixed — inject `IDlqReprocessTrigger<TMessage>` (registered automatically by `AddKafkaWorkerDeadLetter`) and call `Trigger()`:

```csharp
// Example: an admin endpoint in a host that also runs ASP.NET
app.MapPost("/admin/dlq/reprocess", (IDlqReprocessTrigger<OrderMessage> trigger) =>
{
    trigger.Trigger();
    return Results.Accepted();
});
```

`Trigger()` wakes the DLQ consumer immediately and runs one normal batch, then the regular schedule resumes. It is safe to call at any time and from any thread:

- Repeated calls while a trigger is already pending **coalesce** into a single batch.
- A trigger fired while a batch is running queues exactly one follow-up batch.
- The entry point is yours to choose — an HTTP endpoint, a console command, a chat-ops bot, a health-check remediation. The library deliberately ships only the injectable service, not an endpoint.

No configuration is involved. If you don't need on-demand retries, a lower `DeadLetterProcessingIntervalMinutes` is the config-only alternative.

---

## Handling Terminal Failures

A message becomes **terminal** when it is marked invalid (`InvalidMessageException`) or exceeds `DeadLetterMaxReprocessAttempts`. Terminal messages are skipped with their offsets committed — but they are **not deleted**: they remain in the DLQ topic until its retention expires. This section is the operational runbook for them. For durable, queryable failure storage beyond the topic's retention, register a [terminal failure sink](#terminal-failure-sink).

### Detect

Alert on the skip metric — it fires exactly when a message becomes terminal:

- `kafkaworker.dlq.messages_skipped` with `reason="max_attempts"` — retries exhausted; likely a persistent downstream or data problem worth a human look.
- `reason="invalid"` — rejected by your own validation; usually indicates a producer bug or schema drift.

### Inspect

Terminal messages sit in the DLQ topic behind the committed offset. Browse the topic with any Kafka tool (kafka-ui, `kcat`, Conduktor) and identify them by their headers: `invalid-message: true`, or `reprocessed-attempt` ≥ your configured max, plus `error-message` and `original-topic` for diagnosis.

{: .note }
> **Size DLQ retention generously** (days to weeks) — the DLQ topic doubles as your terminal-failure archive. Once retention expires, those messages are gone.

### Redrive

After fixing the root cause, republish the message **value** (and key) back to the DLQ topic **without** the `reprocessed-attempt`, `invalid-message`, and `batch-id` headers — the DLQ consumer then treats it as a fresh failure and reprocesses it on the next tick (or immediately via the [on-demand trigger](#on-demand-reprocessing)). Example with `kcat`:

```bash
# 1. Find the terminal message (note its partition/offset, inspect headers)
kcat -C -b localhost:9092 -t orders.v1.dlq -f 'offset=%o headers=%h value=%s\n'

# 2. Republish value+key without the tracking headers
kcat -C -b localhost:9092 -t orders.v1.dlq -o <offset> -c 1 -K $'\t' -f '%k\t%s\n' \
  | kcat -P -b localhost:9092 -t orders.v1.dlq -K $'\t'
```

{: .important }
> Redriving a message that was marked `invalid-message` only makes sense **after a code or schema fix** — by definition it will fail validation again otherwise.

When enabling DLQ reprocessing for a system that has been running, you may not want to reprocess all historical DLQ messages. Set `DeadLetterStartFrom` to a UTC timestamp:

```json
"Consumer": {
  "DeadLetterTopic": "orders.v1.dlq",
  "DeadLetterStartFrom": "2025-06-01T00:00:00Z"
}
```

The DLQ consumer uses Kafka's `OffsetsForTimes` API to seek to the first message at or after this timestamp. The decision is made **per partition**: only partitions with no committed offset seek by timestamp — partitions that already have a committed offset resume from it and are unaffected. If the timestamp is newer than every message in a partition, that partition starts at the end (new messages only).

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

