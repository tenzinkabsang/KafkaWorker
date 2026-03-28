# Copilot Instructions

## Project Overview
NuGet package library that abstracts Kafka consumer internals. Consumers implement `IMessageHandler<TMessage>` — retry, DLQ, offset management, and DLQ reprocessing are handled by the library. Supports Avro, JSON (plain + Schema Registry), and Protobuf. Targets .NET 8 and .NET 10.

## Key Design Decisions
- This is a **published NuGet package** — public API changes are breaking changes requiring a major version bump.
- Both consumers run as hosted services in the same process. Fatal errors stop the host; k8s restarts the pod.
- The main consumer processing millions of records is the priority — it must never crash due to DLQ publish failure.
- The DLQ consumer creates/destroys a Kafka consumer per batch to avoid broker health-check timeouts during idle periods.
- Confluent.Kafka's internal consumer position advances on each `Consume()` call regardless of offset commits — not committing an offset only helps on consumer restart/rebalance, not within the current session.

## Code Style
- Prefer inline logic over small private methods when the intent is already clear from context.
- Prefer file scoped namespaces
- Prefer primary constructors