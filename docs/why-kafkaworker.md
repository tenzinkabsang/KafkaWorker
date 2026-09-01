---
layout: default
title: Why KafkaWorker?
nav_order: 2
---

# Why KafkaWorker?
{: .no_toc }

The same consumer, written twice: once against raw Confluent.Kafka, once with KafkaWorker.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## The problem

`Confluent.Kafka` is an excellent client — but it is deliberately low-level. A production-ready consumer needs a consume loop, manual offset management, retry with backoff, dead-letter publishing, poison-message handling, graceful shutdown, and DI scoping. None of that is your business logic, and every team ends up rewriting it.

## Raw Confluent.Kafka

A minimal-but-honest version of what production consumers look like (and this still omits DLQ *reprocessing*, metrics, and config validation):

```csharp
public class OrderConsumerService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderConsumerService> _logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "order-processor",
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();
        consumer.Subscribe("orders.v1");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    // Poison message: skip it or your service crash-loops forever
                    _logger.LogCritical(ex, "Skipping undeserializable message at {Offset}",
                        ex.ConsumerRecord?.TopicPartitionOffset);
                    if (ex.ConsumerRecord is { } r && r.Offset != Offset.Unset)
                    {
                        consumer.StoreOffset(new TopicPartitionOffset(r.TopicPartition, r.Offset + 1));
                    }
                    continue;
                }

                if (result?.Message?.Value is null) continue;

                OrderMessage? order;
                try
                {
                    order = JsonSerializer.Deserialize<OrderMessage>(result.Message.Value);
                }
                catch (JsonException ex)
                {
                    await PublishToDlqAsync(dlqProducer, result, ex.Message, stoppingToken);
                    consumer.StoreOffset(result);
                    continue;
                }

                // Retry with exponential backoff
                var attempts = 0;
                while (true)
                {
                    try
                    {
                        using var scope = _scopeFactory.CreateScope();
                        var service = scope.ServiceProvider.GetRequiredService<IOrderService>();
                        await service.ProcessAsync(order!, stoppingToken);
                        break;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex) when (attempts < 3)
                    {
                        attempts++;
                        var delay = TimeSpan.FromSeconds(Math.Pow(2, attempts))
                                  + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 1000));
                        _logger.LogWarning(ex, "Attempt {Attempt} failed, retrying in {Delay}", attempts, delay);
                        await Task.Delay(delay, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        await PublishToDlqAsync(dlqProducer, result, ex.Message, stoppingToken);
                        break;
                    }
                }

                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            consumer.Close(); // final auto-commit of stored offsets
        }
    }

    private async Task PublishToDlqAsync(IProducer<string, string> producer,
        ConsumeResult<string, string> result, string error, CancellationToken ct)
    {
        var headers = new Headers
        {
            { "original-topic", Encoding.UTF8.GetBytes(result.Topic) },
            { "error-message", Encoding.UTF8.GetBytes(error) }
        };
        try
        {
            await producer.ProduceAsync("orders.v1.dlq",
                new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "DLQ publish failed — message lost");
        }
    }
}
```

…and you still need a second service that periodically re-consumes the DLQ, tracks attempt counts in headers, detects loops, and skips messages that will never succeed.

## The same consumer with KafkaWorker

```csharp
public class OrderMessageHandler(IOrderService orderService) : IMessageHandler<OrderMessage>
{
    public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        if (string.IsNullOrEmpty(message.OrderId))
            throw new InvalidMessageException("OrderId is required", message);

        await orderService.ProcessAsync(message, stoppingToken);
    }
}
```

```csharp
builder.Services.AddKafkaWorker<OrderMessage, OrderMessageHandler>(builder.Configuration);
builder.Services.AddKafkaWorkerDeadLetter<OrderMessage>(builder.Configuration);
```

Everything else — the loop, offsets, retries, DLQ publishing *and* reprocessing, poison messages, metrics, validation — is handled, tested, and documented.

## What you stop writing

| Concern | Raw Confluent.Kafka | KafkaWorker |
|---------|--------------------|-------------|
| Consume loop & graceful shutdown | You write it | Built in |
| Manual offset management (`StoreOffset` + auto-commit wiring) | You write it | Built in |
| Retry with exponential backoff + jitter | You write it | Built in (`MaxRetries`, Polly) |
| Dead letter publishing with tracking headers | You write it | Built in |
| **Periodic DLQ reprocessing** (in place, attempt-bounded, loop detection) | You write a second service | Built in (`AddKafkaWorkerDeadLetter`) |
| On-demand DLQ reprocessing | You write it | Built in (`IDlqReprocessTrigger<T>`) |
| Poison messages (crash-loop prevention) | You write it | Built in |
| Permanent vs transient failure distinction | You write it | `InvalidMessageException` |
| Scoped DI per message (EF Core `DbContext` etc.) | You write it | Built in |
| OpenTelemetry-compatible metrics | You write it | Built in |
| Config binding + startup validation | You write it | Built in |
| SASL / Schema Registry auth wiring | You write it | Config-driven |

## When you should *not* use KafkaWorker

Honesty matters more than adoption. Use raw `Confluent.Kafka` (or another tool) if you need:

- **Batch or parallel processing** — KafkaWorker processes messages sequentially per consumer, one at a time. That's a feature for ordering and backpressure, but a ceiling for very high-throughput topics.
- **Exactly-once semantics / transactions** — the library is at-least-once by design; handlers must be idempotent.
- **Custom commit strategies** — offsets are stored per message and flushed by the client's background auto-commit; there is no synchronous per-message or commit-every-N mode.
- **Consuming without a consumer group**, manual partition assignment, or other low-level control.

For the common case — "consume a topic, run business logic per message, don't lose anything, don't page me for one bad payload" — that's exactly what KafkaWorker is for.

## Try it in two minutes

The [samples folder](https://github.com/tenzinkabsang/KafkaWorker/tree/main/samples) contains a runnable worker + producer that demonstrate every failure path against a dockerized Kafka:

```bash
docker compose up -d --wait
dotnet run --project samples/OrderProcessor    # terminal 1
dotnet run --project samples/OrderProducer     # terminal 2
```
