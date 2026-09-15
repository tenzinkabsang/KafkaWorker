# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **The DLQ sweep now terminates on a high-watermark snapshot instead of the `batch-id` header.**
  Because the DLQ consumer appends to the very topic it is draining, the end of the log runs away
  from it as it works; `batch-id` handled that by tagging each re-enqueue so the sweep could
  recognise its own output coming back around. Each sweep now queries the high watermark of every
  assigned partition once, before handling anything, and treats it as a fixed finish line — records
  at or beyond it were appended during the sweep and are left uncommitted for the next tick.

  The practical gains: a sweep ends deterministically when it reaches the finish line instead of
  idling out the 5-second consume timeout, and **multi-partition dead letter topics are now handled
  correctly**. Previously a `batch-id` match broke out of the entire sweep, stranding unprocessed
  messages on every other partition; each partition now carries its own finish line and is paused as
  it is reached, so the sweep ends only once all of them are drained. The long-standing advice to give the dead letter
  topic a single partition has been dropped from the docs along with it — it was compensating
  for this bug rather than a performance recommendation, and it left every extra app instance's
  DLQ consumer idle.

  `batch-id` is still stamped on every re-enqueue as a diagnostic breadcrumb — it just no longer
  drives control flow. `reprocessed-attempt`, `invalid-message` and `deserialization-failed` are
  unchanged.

- A partition assigned to the DLQ consumer mid-sweep by a rebalance no longer counts towards the
  sweep's completion check. It has no finish line of its own and is deliberately left to the next
  tick, but it was being tallied alongside the snapshotted partitions, so it could stand in for one
  that had not finished and end the sweep early. Nothing was lost — the stranded partition was
  picked up on the following tick — but a sweep could silently do less than it should.

- A DLQ record flagged `IsPartitionEOF` is now skipped rather than ending the sweep. The DLQ consumer
  has never set `EnablePartitionEof`, so this branch was inert; had it ever been enabled, ending on
  EOF would have been wrong, since EOF tracks the *current* end of the log and so includes the
  sweep's own re-enqueues.

### Fixed

- A failed offset commit no longer ends the DLQ sweep. Every commit in the sweep was unguarded, so a
  rebalance revoking a partition mid-sweep — an ordinary event — surfaced as a `Critical` log and
  abandoned the partitions that were still draining. Commit failures are now logged at `Error` and
  the sweep continues; the affected messages are simply re-read on the next tick, which in-place
  reprocessing is already required to tolerate. This matches how the batch consumer has handled
  commit failures since 2.5.0.

## [2.5.0] - 2026-09-13

### Added

- **Batch processing.** Implement `IBatchMessageHandler<TMessage>` and register with
  `AddKafkaWorkerBatch` (or `AddKafkaWorkerAvroBatch` / `AddKafkaWorkerProtobufBatch` /
  `AddKafkaWorkerRegistryJsonBatch`) to receive messages in groups instead of one at a time. A new DI
  scope is created per batch, so a scoped `DbContext` is shared across the batch and a handler that
  wrote one row per message can now write them all in a single round trip. Batch size and latency are
  controlled by the new `MaxBatchSize` (default 100) and `BatchLingerMs` (default 500) settings.

  Batching is a *downstream* optimization, not a Kafka one: the client already pre-fetches messages
  into a local in-memory queue, so consuming is already effectively free and draining a batch issues
  no extra broker round trips. It pays off only when the batch collapses into one operation — a bulk
  insert, one transaction, a batch API call.

  **Failure handling is unchanged from single-message mode.** A batch that throws identifies no
  particular message, so every message in it is re-processed individually through the existing
  per-message path: retries, `InvalidMessageException` routing, DLQ headers, `ITerminalFailureSink`
  and per-message metrics all behave exactly as before, and one bad message is still dead lettered on
  its own rather than dragging its batch with it. Because of that fallback, **a batch handler must be
  idempotent** — messages that already succeeded inside a failed batch run a second time.

  New metrics: `kafkaworker.batch.size`, `kafkaworker.batch.processing_duration`, and
  `kafkaworker.batch.fallbacks`. `AddKafkaWorkerDeadLetter` works unchanged with a batch consumer —
  DLQ reprocessing is inherently per-message, so it invokes the batch handler with single-message
  batches.

### Changed

- **Batch consumers commit offsets synchronously at each batch boundary** rather than relying on the
  client's background auto-commit (`EnableAutoCommit` is forced to `false` for them; single-message
  consumers are unaffected and keep background auto-commit). One round trip per batch is roughly a
  hundredth of the per-message cost that made background auto-commit worthwhile in 2.4.0, and it
  bounds redelivery after a hard crash to a single batch — a number you set via `MaxBatchSize` —
  instead of however much throughput fit inside `AutoCommitIntervalMs`. This matters precisely
  because batching raises throughput: the same 5-second window would otherwise hold far more work.
- Updated dependencies: `Confluent.Kafka` and the three `Confluent.SchemaRegistry.Serdes.*`
  add-ons to 2.15.1, and the net10.0 target to `Microsoft.Extensions.*` 10.0.12 (servicing
  patches; the net8.0 target stays on 8.0.x).

## [2.4.0] - 2026-09-03

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

- **Shutdown during a DLQ publish no longer commits the message as handled.** Previously a
  cancellation thrown mid-publish was swallowed by the best-effort catch and the offset advanced,
  silently losing the message; it now propagates so the message is redelivered and dead-lettered
  after restart.
- **The net8.0 target no longer forces the Microsoft.Extensions 10.x stack onto consumers.** The
  net8.0 build again references `Microsoft.Extensions.*` 8.0.x (an accidental bump in 2.1.0 had
  raised it to 10.0.10, lifting the whole extensions dependency graph of net8 LTS applications).

### Documentation

- New **"Poison-Message Capture"** and **"Terminal Failure Sink"** sections in the DLQ docs, and
  explicit guidance to size DLQ topic retention generously (`retention.ms=-1`) since the DLQ topic
  doubles as the failure archive.

### Upgrade notes

- **Deserialization failures now log at `Error`, not `Critical`, when captured to the DLQ** — which
  is the common case whenever a `DeadLetterTopic` is configured. Alerts keyed on the `Critical`
  poison-message log will stop firing. Re-key them on the `deserialization_failed` value of the
  `kafkaworker.messages.processed` `status` tag (or the new `dlq_published` `reason` of the same
  name). The `Critical` log remains for the genuinely lossy cases: no DLQ configured, or the
  capture publish itself failed.
- **Your DLQ topic will start receiving records that do not deserialize.** Captured poison records
  hold the original raw bytes and carry `deserialization-failed: true`. The library's own DLQ
  consumer skips them safely, but external DLQ tooling — Schema Registry-aware consumers, redrive
  scripts, dashboards — must tolerate them. There is no opt-out short of leaving `DeadLetterTopic`
  unset.
- **A hard crash now redelivers more messages.** With background auto-commit, a `kill -9` or node
  loss redelivers everything processed since the last flush (up to `AutoCommitIntervalMs`, default
  5s) instead of at most one message. Graceful shutdown and rebalances are unaffected. Handlers
  were already required to be idempotent; lower `AutoCommitIntervalMs` via `configureConsumer` to
  narrow the window.
- **net8.0 consumers:** the `Microsoft.Extensions.*` floor drops back to 8.0.x. This only relaxes
  the constraint, but if you were relying on KafkaWorker to pull the 10.x stack into a net8 app,
  reference those packages explicitly.

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

[2.5.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.5.0
[2.4.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.4.0
[2.3.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.3.0
[2.2.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.2.0
[2.1.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.1.0
[2.0.0]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v2.0.0
[1.0.4]: https://github.com/tenzinkabsang/KafkaWorker/releases/tag/v1.0.4
