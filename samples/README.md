# KafkaWorker Samples

A complete, runnable demo: a worker that consumes orders, and a producer that sends messages exercising **every failure path** the library handles — successful processing, transient failure with retry, dead-lettering, in-place DLQ reprocessing, invalid messages, and poison (undeserializable) messages.

| Project | What it is |
|---------|------------|
| [`OrderProcessor`](OrderProcessor/) | A worker service using `AddKafkaWorker` + `AddKafkaWorkerDeadLetter`. The handler is ~20 lines — the library does the rest. |
| [`OrderProducer`](OrderProducer/) | A console app that publishes one message for each scenario. |

## Run it

Prerequisites: Docker and the .NET 10 SDK.

```bash
# 1. Start Kafka (from the repo root)
docker compose up -d --wait

# 2. Start the worker (terminal 1) — wait for "Subscribed to kafka topic"
dotnet run --project samples/OrderProcessor

# 3. Send the sample messages (terminal 2)
dotnet run --project samples/OrderProducer
```

## What you'll see in the worker logs

1. **`ORD-001`–`ORD-003`** — processed successfully; offsets committed per message.
2. **`ORD-FLAKY`** — the handler throws a (simulated) transient error. Watch it retry with exponential backoff (`MaxRetries: 2`), then get published to `sample.orders.dlq`. About a minute later (`DeadLetterProcessingIntervalMinutes: 1`) the DLQ consumer reprocesses it **in place** — it fails again, gets re-enqueued with an incremented attempt count, and after `DeadLetterMaxReprocessAttempts: 2` it is terminally skipped (this is when the `dlq.messages_skipped` metric fires with `reason="max_attempts"`).
3. **`ORD-INVALID`** — empty `OrderId`, so the handler throws `InvalidMessageException`: no retries, straight to the DLQ with the `invalid-message` header, permanently skipped by the reprocessor.
4. **`ORD-POISON`** — not valid JSON. The consumer logs it at `Critical`, commits past it, and keeps running. One bad payload can't crash the host or wedge the consumer.

Browse the topics (including the DLQ and its headers) at **http://localhost:888** (kafka-ui).

## Notes

- The sample references the library via `ProjectReference` so it always builds against this repo. In your own application, use the NuGet package: `dotnet add package KafkaWorker`.
- The demo overrides `AutoOffsetReset` to `Earliest` (via the `configureConsumer` callback) so start order doesn't matter. The library default is `Latest`.
- Full documentation: https://tenzinkabsang.github.io/KafkaWorker/
