# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Poison-message capture.** A message that fails deserialization is no longer dropped: when a
  `DeadLetterTopic` is configured, its raw key/value bytes are captured there verbatim (via a plain
  `byte[]` producer — no Schema Registry involvement) with the usual tracking headers plus
  `deserialization-failed: true`, logged at `Error` instead of `Critical`, and counted under
  `dlq_published` reason `deserialization_failed`. The DLQ consumer recognizes the header and skips
  these records quietly — they await manual redrive after a payload/schema fix. Without a DLQ (or
  if the capture publish fails) the record is logged at `Critical` and lost, as before.
- **`ITerminalFailureSink<TMessage>`** — optional extension point invoked exactly when the library
  permanently gives up on a message: the DLQ consumer skips it (invalid, or reprocess attempts
  exhausted), the best-effort DLQ publish fails, or processing fails with no DLQ configured (for
  the last two the sink is the message's last chance to be persisted anywhere). Register any
  implementation in DI (resolved from a fresh scope per call, so an EF Core `DbContext` injects
  naturally) to store terminal failures in a database, blob storage, or an alerting system.
  Best-effort: sink exceptions are logged and never affect the consumer or offsets.

### Fixed

- **Shutdown during a DLQ publish no longer commits the message as handled.** Previously a
  cancellation thrown mid-publish was swallowed by the best-effort catch and the offset advanced,
  silently losing the message; it now propagates so the message is redelivered and dead-lettered
  after restart.

### Documentation

- New **"Poison-Message Capture"** and **"Terminal Failure Sink"** sections in the DLQ docs, and
  explicit guidance to size DLQ topic retention generously (`retention.ms=-1`) since the DLQ topic
  doubles as the failure archive.

### Changed

- **The main consumer no longer commits offsets synchronously after every message.** Offsets are
  still stored (`StoreOffset`) only after a message is handled, but are now flushed by the Kafka
  client's background auto-commit (`EnableAutoCommit = true` + `EnableAutoOffsetStore = false`,
  the pattern Confluent recommends) — every `AutoCommitIntervalMs` (default 5s), on rebalance, and
  on shutdown. This removes a blocking broker round trip per message and means a commit failure
  (e.g. after a group eviction) surfaces through the client error callback instead of crashing the
  host. Delivery semantics remain at-least-once; the only behavioral difference is after a *hard*
  crash (kill -9, node loss), where messages processed since the last background flush are
  redelivered — up to ~5s of messages instead of at most one. Graceful shutdown and rebalances
  commit final offsets exactly as before. Tune the window with `AutoCommitIntervalMs` via
  `configureConsumer` if needed. The DLQ consumer is unchanged (it keeps per-message synchronous
  commits, which its stop-the-batch semantics rely on).
- The duplicate-consumer registration error no longer suggests registering the same message type
  with different key types; consumer options are keyed by message type, so that path would bind
  both consumers to the same configuration. Use a distinct message type per consumer.
- Updated dependencies: the net10.0 target now references `Microsoft.Extensions.*` 10.0.11
  (servicing patches; the net8.0 target stays on 8.0.x).

### Fixed

- **The net8.0 target no longer forces the Microsoft.Extensions 10.x stack onto consumers.** The
  net8.0 build again references `Microsoft.Extensions.*` 8.0.x (an accidental bump in 2.1.0 had
  raised it to 10.0.10, lifting the whole extensions dependency graph of net8 LTS applications).

## [2.3.0] - 2026-08-29

### Added

- **`configureSerializer` callback** on the Schema Registry add-ons (`AddKafkaWorkerAvro`,
  `AddKafkaWorkerProtobuf`, `AddKafkaWorkerRegistryJson`) to customize the serializer used for dead
  letter publishing (`AutoRegisterSchemas`, `UseLatestVersion`, `SubjectNameStrategy`, …). With
  Confluent defaults, the first DLQ publish auto-registers a `{DeadLetterTopic}-value` subject in
  Schema Registry; registries that deny client-side registration can now disable auto-registration
  instead of losing the message when the best-effort DLQ publish fails.

### Changed

- **The DLQ consumer now resolves the dead-letter producer lazily.** Previously registering
  `AddKafkaWorkerDeadLetter` created the producer client at host startup even if no message was ever
  re-enqueued, defeating the main consumer's lazy producer creation (both share one singleton). No
  producer (or broker connection) is created until a message is actually dead-lettered or re-enqueued.

### Documentation

- Trimmed redundant `"MaxRetries": 3` (the default) from minimal configuration examples; the setting
  remains documented in the configuration reference.
- New **"Serializer Options (DLQ Publishing)"** section in the serialization docs covering the DLQ
  subject auto-registration behavior and how to pre-register or disable it.

## [2.2.0] - 2026-07-23

### Added

- **`IDlqReprocessTrigger<TMessage>`** — injectable, zero-configuration service (registered by
  `AddKafkaWorkerDeadLetter`) that wakes the DLQ consumer to run a reprocessing batch immediately
  instead of waiting for the next scheduled tick. Repeated triggers coalesce; the regular schedule
  is unaffected.

### Documentation

- New **"Handling Terminal Failures" runbook** in the DLQ docs: detecting terminal messages via the
  `dlq.messages_skipped` metric, inspecting them in the DLQ topic, and redriving them by
  republishing without the tracking headers.
- New **ordering and idempotency guidance**: dead-lettered messages are retried out of order, so
  handlers should be idempotent and order-tolerant.

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

[2.3.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.3.0
[2.2.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.2.0
[2.1.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.1.0
[2.0.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.0.0
[1.0.4]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v1.0.4
