---
layout: default
title: Batch Processing
nav_order: 5
---

# Batch Processing
{: .no_toc }

Process messages in groups instead of one at a time, so a handler that writes to a database can do one bulk insert per batch rather than one round trip per message.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## What batching actually buys you

Batching does **not** reduce broker round trips. The Kafka client already pre-fetches messages into a local in-memory queue (`QueuedMinMessages` defaults to 100,000 per partition), so reading a message costs roughly a microsecond and involves no network call at all.

The entire win is on the other side of your handler. In single-message mode, every message costs a DI scope, a fresh `DbContext`, and — typically — one round trip to your database. A database round trip is 2–10 **milli**seconds against a ~1 **micro**second Kafka read, so you spend essentially all of your time waiting on the downstream system.

| 10,000 messages, one INSERT each | DI scopes | DB round trips |
|:---|:---|:---|
| Single-message mode | 10,000 | 10,000 |
| Batch mode (`MaxBatchSize` 500) | 20 | 20 bulk inserts |

Both rows issue the same number of broker fetches.

{: .important }
> Batching only pays off if the work **can actually be collapsed into one operation** — a bulk insert, a single transaction, a multi-row upsert, a batch API endpoint. A batch handler that loops over the messages issuing one call each gains nothing and adds complexity for free.

It is also a poor fit for CPU-bound handlers, and for low-volume topics where the consumer will simply wait out `BatchLingerMs` and hand you two or three messages.

## Registration

Implement `IBatchMessageHandler<TMessage>` and register with `AddKafkaWorkerBatch`:

```csharp
public class OrderBatchHandler(OrderDbContext db) : IBatchMessageHandler<OrderMessage>
{
    public async Task HandleBatchAsync(IReadOnlyList<OrderMessage> messages, CancellationToken stoppingToken)
    {
        db.Orders.AddRange(messages.Select(Order.From));
        await db.SaveChangesAsync(stoppingToken); // one round trip for the whole batch
    }
}
```

```csharp
builder.Services.AddKafkaWorkerBatch<OrderMessage, OrderBatchHandler>(builder.Configuration);
```

The handler is registered as a **scoped** service and a new DI scope is created per batch, so a scoped `DbContext` is shared by every message in the batch — which is what makes the single `SaveChangesAsync` possible.

The Schema Registry add-ons have matching methods: `AddKafkaWorkerAvroBatch`, `AddKafkaWorkerProtobufBatch`, and `AddKafkaWorkerRegistryJsonBatch`.

## Configuration

| Setting | Type | Default | Description |
|:---|:---|:---|:---|
| `MaxBatchSize` | `int` | `100` | Maximum messages per handler call. Range: 1–10000 |
| `BatchLingerMs` | `int` | `500` | How long to keep accumulating after the first message arrives. Range: 0–60000 |

```json
{
  "KafkaWorker": {
    "Consumer": {
      "Topic": "orders",
      "GroupId": "order-processor",
      "MaxBatchSize": 500,
      "BatchLingerMs": 250
    }
  }
}
```

`BatchLingerMs` is a **latency** control, not a throughput one. Under load the batch fills from the local pre-fetch queue almost instantly and never waits. On a quiet topic the consumer waits this long and then processes whatever it has. Setting it to `0` means "never wait" — take only what is already buffered locally.

{: .warning }
> `MaxBatchSize` multiplied by your per-message processing time must fit comfortably inside the client's `MaxPollIntervalMs` (default 5 minutes), or the consumer group will evict the consumer mid-batch. This is the most common way to get batch processing wrong.

## Failure handling

A batch that throws tells the library that *something* failed, but never *which* message. Rather than dead lettering the whole batch, the library **re-processes every message in it one at a time** through the ordinary single-message path.

```
batch of 100 → handler throws
             → re-run all 100 individually
                 ├─ 99 succeed          → offsets advance
                 └─ 1 fails             → retried (MaxRetries), then DLQ
```

This means batching never widens the blast radius of a single bad message. Retry counts, `InvalidMessageException` routing, DLQ headers, `ITerminalFailureSink<TMessage>` and per-message metrics all behave exactly as they do in single-message mode.

{: .important }
> **Your batch handler must be idempotent.** Messages that already succeeded inside a failed batch are processed a second time during the fallback. At-least-once delivery already permits duplicates, but batching makes them routine rather than crash-only.

`InvalidMessageException` thrown from a multi-message batch is indistinguishable from any other failure and simply triggers the fallback — where it regains its usual meaning of "skip retries, go straight to the DLQ".

The fallback is visible in the logs:

```
warn: Batch of 100 messages failed; re-processing them one at a time so each is
      retried and dead lettered individually. Topic: orders
```

A steady stream of these means a message is reliably poisoning your batches; the `kafkaworker.batch.fallbacks` counter is the metric to alert on.

## Offsets and commits

Batch consumers manage offsets differently from single-message consumers, and deliberately so.

| | Single-message mode | Batch mode |
|:---|:---|:---|
| `EnableAutoOffsetStore` | `false` | `false` |
| `EnableAutoCommit` | `true` (background, every 5s) | **`false`** |
| Offsets reach the broker | every `AutoCommitIntervalMs` | at every batch boundary |
| Replayed after a hard crash | whatever throughput fit in 5s | **at most one batch** |

In single-message mode a synchronous commit per message would mean a blocking round trip per message, which is why the library relies on the client's background auto-commit. With batching, one synchronous commit per batch costs roughly a hundredth of that — so batch consumers commit explicitly at each batch boundary and get a tighter, tunable redelivery bound in exchange.

This matters more than it first appears. Batching raises throughput, so the same five-second auto-commit window would now contain far more work — a crash could replay tens of thousands of messages instead of a few hundred. Committing per batch replaces that moving target with a number you set via `MaxBatchSize`.

Offsets are tracked **per partition**: a drained batch can span partitions, and the highest offset reached on each one is stored separately. Tombstones (null values) are filtered out before your handler sees them, but their offsets still advance with the batch.

## Metrics

Batch mode adds three instruments alongside the existing per-message ones:

| Metric | Type | Tags | Description |
|:---|:---|:---|:---|
| `kafkaworker.batch.size` | Histogram | `topic` | Messages handed to the handler per call |
| `kafkaworker.batch.processing_duration` | Histogram (ms) | `topic` | Duration of a batch handler call |
| `kafkaworker.batch.fallbacks` | Counter | `topic` | Batches that failed and were re-processed one at a time |

`kafkaworker.messages.processed` still counts individual messages, so the two views stay comparable. See [Metrics]({{ site.baseurl }}/metrics) for wiring these into OpenTelemetry.

## Dead letter queue

`AddKafkaWorkerDeadLetter` works unchanged with a batch consumer. DLQ reprocessing is inherently per-message, so the DLQ consumer invokes your batch handler with single-message batches.

```csharp
builder.Services.AddKafkaWorkerBatch<OrderMessage, OrderBatchHandler>(builder.Configuration);
builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(builder.Configuration);
```

See [Dead Letter Queue]({{ site.baseurl }}/dead-letter-queue) for the full reprocessing model.

## When not to use it

- **CPU-bound handlers** — there is no round trip to amortize.
- **No bulk operation downstream** — if the remote API takes one item per call, batching changes nothing.
- **Low-volume topics** — you pay `BatchLingerMs` of latency to collect a handful of messages.
- **Strict per-message failure isolation with no duplicate tolerance** — the fallback re-runs succeeded messages, so a non-idempotent handler will double-apply them.

In all of these cases, stay on `AddKafkaWorker` and `IMessageHandler<TMessage>`.
