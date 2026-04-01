---
layout: default
title: Advanced Topics
nav_order: 7
---

# Advanced Topics
{: .no_toc }

Deeper details on error handling, DI scoping, testing, and deployment behavior.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

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

---

## Scoped Dependency Injection

The library creates a new DI scope for each message. This means scoped dependencies like EF Core `DbContext` work naturally via constructor injection:

```csharp
public class OrderMessageHandler(
    AppDbContext dbContext,  // scoped — fresh instance per message
    ILogger<OrderMessageHandler> logger) : IMessageHandler<OrderMessage>
{
    public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
    {
        dbContext.Orders.Add(new Order { Id = message.OrderId });
        await dbContext.SaveChangesAsync(stoppingToken);
    }
}
```

The scope is disposed after `HandleMessageAsync` completes, which means the `DbContext` is disposed automatically — no need for explicit disposal.

---

## Error Handling Rules

| Exception | Behavior |
|-----------|----------|
| Any `Exception` | Retried with exponential backoff up to `MaxRetries` times |
| `InvalidMessageException` | Sent directly to DLQ — no retries |
| `OperationCanceledException` | Propagated for clean shutdown — do not catch this |

### Retry Strategy

Retries use **exponential backoff with jitter** (via Polly):
- Base delay grows exponentially with each attempt
- Random jitter prevents thundering herd problems
- Maximum of 5 retry attempts (configurable via `MaxRetries`)

---

## Backpressure

The consume loop processes messages sequentially — one at a time. Kafka won't outpace your handler because the next `Consume()` call doesn't happen until the current message is fully processed (including retries and DLQ publish if needed).

If you need to throttle calls to a downstream system, add rate limiting inside your `HandleMessageAsync` implementation.

---

## Failure & Restart Behavior

Both the main consumer and DLQ consumer run as hosted services in the same .NET host. The default `BackgroundServiceExceptionBehavior` in .NET 8+ is `StopHost` — a fatal error from either consumer stops the host, and Kubernetes restarts the pod.

### Offset Management

The library uses manual offset management:
1. `StoreOffset()` — marks the offset locally
2. `Commit()` — commits the stored offset to Kafka

Both are called after every message, whether processing succeeded, the message was sent to the DLQ, or a DLQ publish failed. This ensures the consumer never gets stuck on a single message.

{: .note }
> Confluent.Kafka's internal consumer position advances on each `Consume()` call regardless of offset commits. Not committing an offset only helps on consumer restart or rebalance — not within the current session.

---

## What the Library Handles

You write the `IMessageHandler<TMessage>` — the library handles everything else:

- Consumer subscription, consume loop, and graceful shutdown
- `StoreOffset()` + `Commit()` after every message
- Retry with exponential backoff and jitter (Polly)
- Publishing to DLQ with tracking headers
- DLQ reprocessing on a timer with loop detection
- Configuration validation on startup
- Scoped DI per message

---

## Requirements

- .NET 8.0 or .NET 10.0
- Confluent.Kafka (pulled in automatically)
- Polly (pulled in automatically)
