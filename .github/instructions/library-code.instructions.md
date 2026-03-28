---
applyTo: "KafkaWorker/*.cs"
---

# KafkaWorker Library Code Instructions

## This is a NuGet Package
All changes to files in the `KafkaWorker/` directory affect the public NuGet package consumed by other teams. Exercise extra caution with:
- **Public API surface** — `IMessageHandler<TMessage>`, `InvalidMessageException`, `KafkaWorkerConfig`, `ServiceCollectionExtensions` are public contracts. Breaking changes require a major version bump.
- **Internal classes** — `Consumer<TMessage>`, `DlqConsumer<TMessage>`, `DlqConsumerFactory<TMessage>`, serializers, and header utilities are `internal`. They can be refactored freely.

## Code Patterns
- Use primary constructors for dependency injection
- File-scoped namespaces (`namespace KafkaWorker;`)
- `readonly` fields for Polly `ResiliencePipeline` instances
- Manual offset management: `StoreOffset()` then `Commit()` after every message (success, DLQ publish, or DLQ publish failure)
- `Consume(CancellationToken)` for main consumer; `Consume(TimeSpan)` for DLQ consumer

## Error Handling Reminders
- Main consumer must NEVER crash due to DLQ publish failure — log at `Critical` and commit offset
- DLQ consumer must NOT commit on republish failure — stop the batch and retry next tick
- Catch `OperationCanceledException when (stoppingToken.IsCancellationRequested)` separately from general exceptions
- Re-throw `OperationCanceledException` in inner methods for clean propagation

## Multi-targeting
This library targets both `net8.0` and `net10.0`. Avoid APIs only available in one target without conditional compilation.
