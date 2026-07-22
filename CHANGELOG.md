# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-07-22

### Added

- **`SaslMechanism` connection setting** (default `ScramSha512`, preserving previous behavior).
  Accepted values: `Plain`, `ScramSha256`, `ScramSha512`, `Gssapi`, `OAuthBearer` — enables
  managed-Kafka providers such as Confluent Cloud (SASL `Plain` with API keys).
- **`SchemaRegistryUsername` / `SchemaRegistryPassword` connection settings** for Schema Registry
  basic auth (e.g. Confluent Cloud Schema Registry API keys). Validated together at startup.
- **`configureProducer` callback** on all registration methods (`AddKafkaWorker`,
  `AddKafkaWorkerAvro`, `AddKafkaWorkerProtobuf`, `AddKafkaWorkerRegistryJson`) to customize the
  dead-letter producer's `ProducerConfig`.

### Fixed

- **A message that fails deserialization no longer crashes the host.** Previously a poison message
  threw out of the consume loop, stopped the host, and was re-consumed on restart — a permanent
  crash loop. The consumer now logs it at `Critical`, emits a `deserialization_failed` metric,
  commits past it, and continues. Fatal client errors still propagate.
- **The DLQ consumer no longer wedges permanently** on undeserializable records or tombstones
  (null-value messages). Both are now skipped with their offsets committed; previously they ended
  every batch at the same offset forever without committing.
- **Tombstones on the main topic now commit their offsets** instead of being skipped without commit.
- **Null-valued Kafka headers no longer throw** when read by the library.
- **`DeadLetterStartFrom` is now applied per partition.** Previously, if any partition had a
  committed offset, partitions without one fell back to `AutoOffsetReset.Earliest` and reprocessed
  their entire backlog. Now each partition independently resumes from its committed offset or seeks
  to the configured timestamp.
- **Published assemblies now carry the release version** — the publish workflow passes the tag
  version to the build step, so DLL file/assembly versions match the package version.
- Add-on packages (`KafkaWorker.Avro`, `KafkaWorker.Protobuf`, `KafkaWorker.JsonSchema`) now declare
  the MIT license and publish symbol packages (snupkg), matching the core package.

### Changed

- **Consumer options are now keyed by the message type's full name** (`Type.FullName` instead of
  `Type.Name`), eliminating config collisions between same-named message types in different namespaces.
- **The dead-letter producer is created lazily** — no producer (or broker connection) is created by
  the main consumer until a message is actually dead-lettered. Producer configuration errors on the
  main consumer path now surface at first DLQ publish instead of at startup.
- **Metric tags aligned**: `kafkaworker.messages.dlq_published` now always emits `topic`, `dlq_topic`,
  and `reason` (`processing_failed`, `invalid`, `reprocess_failed`); the `kafkaworker.dlq.messages_skipped`
  tag `topic` was renamed to `dlq_topic` and gains reason `deserialization_failed`.
- **A user-registered `ISchemaRegistryClient` is now honored regardless of registration style**
  (instance, type, or factory). Previously only instance registrations of `CachedSchemaRegistryClient`
  were detected. The main consumer also reuses the same registered `IDeserializer<TMessage>` as the
  DLQ consumer instead of constructing a second instance.
- librdkafka client log/error callbacks are now wired on the main consumer (not just the DLQ
  consumer) and use structured log templates.

### Upgrade notes

- If you have dashboards or alerts on `kafkaworker.dlq.messages_skipped`, update the tag name
  `topic` to `dlq_topic`.
- If you relied on the host crashing when a message failed to deserialize, note the new behavior:
  the message is skipped and committed past, with a `Critical` log and a `deserialization_failed`
  metric to alert on instead.

## [2.0.0] - 2026-06-20

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

[2.1.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.1.0
[2.0.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.0.0
[1.0.4]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v1.0.4
