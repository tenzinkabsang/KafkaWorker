# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

### Changed (breaking)

- **DLQ reprocessing is now always in-place.** The DLQ consumer invokes your registered
  `IMessageHandler<TMessage>` directly. Failed messages are never republished back to the
  original topic, which keeps failures isolated to the owning consumer and prevents previously
  failed messages from reappearing on a shared topic for unrelated consumer groups.
- **`AddKafkaWorkerDeadLetter` now throws at startup if no `IMessageHandler<TMessage>` is
  registered.** Call `AddKafkaWorker` before `AddKafkaWorkerDeadLetter` so the handler is
  available to the DLQ consumer.

### Removed (breaking)

- **`DeadLetterReprocessStrategy` enum** and the **`KafkaWorkerConfig.DeadLetterReprocessStrategy`**
  configuration option. The republish-to-original-topic behavior has been removed entirely; there
  is no opt-out.
- The **`failed-consumer-group-id`** DLQ header and the associated multi-consumer-group isolation
  logic, which only existed to support the republish path.

### Behavior

- When in-place reprocessing fails again, the message is re-enqueued to the DLQ topic with an
  incremented `reprocessed-attempt` (bounded by `DeadLetterMaxReprocessAttempts`), reusing the
  existing `batch-id` loop detection.

### Migration

- Remove any `DeadLetterReprocessStrategy` key from your configuration — it no longer has any
  effect.
- Ensure the consumer that owns the `IMessageHandler<TMessage>` is registered in the same process
  as the DLQ consumer (`AddKafkaWorker` before `AddKafkaWorkerDeadLetter`).
- If you previously relied on failed messages reappearing on the original topic for a separate
  service to re-consume, that flow is no longer supported.

See the [Dead Letter Queue documentation](https://tenzinkabsang.github.io/KafkaWorker/dead-letter-queue#migrating-from-v1-republish-to-v2-in-place)
for full migration guidance.

## [1.0.4]

- Last release before the DLQ reprocessing strategy change.

[2.0.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.0.0
[1.0.4]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v1.0.4
