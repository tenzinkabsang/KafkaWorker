using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;

namespace KafkaWorker;

/// <summary>
/// Periodically processes messages from the dead letter queue and reprocesses them in place.
/// </summary>
/// <remarks>
/// The consumer runs on a configurable interval (default: 60 minutes) and processes all pending
/// DLQ messages in each sweep. Messages marked as invalid (via <see cref="InvalidMessageException"/>)
/// or that have exceeded the maximum reprocess attempts are skipped.
/// <para>
/// Messages are reprocessed in place by invoking the registered <see cref="IMessageHandler{TMessage}"/>.
/// A message that fails again is re-enqueued to the dead letter topic with an incremented attempt for a
/// future tick, so failed messages never reappear on the original topic.
/// </para>
/// <para>
/// Because the consumer appends to the very topic it is draining, each sweep snapshots the end of the
/// log per partition before handling anything and stops there. Re-enqueues made during a sweep land
/// beyond that finish line and are left for the next tick, which is what keeps a message from
/// exhausting its reprocess attempts in a single pass.
/// </para>
/// <para>
/// Partition count needs no special consideration: each partition has its own finish line and is
/// paused as it is reached, so the sweep ends once they are all drained.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The type of message key being consumed.</typeparam>
/// <typeparam name="TMessage">The type of message being consumed.</typeparam>
internal sealed partial class DlqConsumer<TKey, TMessage>(
    Lazy<IProducer<TKey, TMessage>> producer,
    IDlqConsumerFactory<TKey, TMessage> consumerFactory,
    IServiceScopeFactory serviceScopeFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    KafkaWorkerMetrics metrics,
    DlqReprocessSignal<TMessage> reprocessSignal,
    ILogger<DlqConsumer<TKey, TMessage>> logger,
    TimeProvider timeProvider) : BackgroundService where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).FullName);

    private int MaxReprocessAttempts => _kafkaConfig.DeadLetterMaxReprocessAttempts;
    private int ProcessingIntervalMinutes => _kafkaConfig.DeadLetterProcessingIntervalMinutes;
    private string? DeadLetterTopic => _kafkaConfig.DeadLetterTopic;

    private static readonly ResiliencePipeline _produceResiliencePipeline = ProduceResiliencePipeline.Instance;

    /// <summary>How long the per-sweep watermark query may block per partition.</summary>
    private static readonly TimeSpan WatermarkQueryTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long each poll waits for a record before the sweep treats the dead letter topic as quiet
    /// and ends the tick. This is the fallback boundary: a sweep normally ends at the finish line.
    /// </summary>
    private static readonly TimeSpan ConsumePollTimeout = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        LogStarting(logger, DeadLetterTopic, ProcessingIntervalMinutes);

        try
        {
            // Persists across iterations when the timer wins, so a trigger fired
            // between ticks is never lost.
            Task<bool>? triggerRead = null;

            while (!stoppingToken.IsCancellationRequested)
            {
                triggerRead ??= reprocessSignal.Reader.ReadAsync(stoppingToken).AsTask();
                var delay = Task.Delay(TimeSpan.FromMinutes(ProcessingIntervalMinutes), timeProvider, stoppingToken);

                var winner = await Task.WhenAny(delay, triggerRead);
                if (winner == triggerRead)
                {
                    await triggerRead;
                    triggerRead = null;
                    LogSweepTriggered(logger, DeadLetterTopic);
                }
                else
                {
                    await delay;
                }

                // A unique id per sweep, stamped onto whatever this sweep re-enqueues. Purely a
                // diagnostic breadcrumb: the finish line is what bounds the sweep.
                var batchId = Guid.NewGuid().ToString();

                LogSweepStarting(logger, batchId, DeadLetterTopic);

                await SweepDeadLetterQueueAsync(batchId, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            LogShuttingDown(logger);
        }
        catch (Exception ex)
        {
            LogFatalError(logger, ex);
            throw;
        }
    }

    /// <summary>
    /// Subscribes to the dead letter topic and reprocesses what it finds, committing offsets after
    /// each message is handled. The sweep ends when every assigned partition reaches the finish
    /// line snapshotted at the start, when the topic goes quiet, or when the cancellation token is
    /// triggered.
    /// </summary>
    internal async Task SweepDeadLetterQueueAsync(string batchId, CancellationToken stoppingToken)
    {
        using var consumer = consumerFactory.Create();

        try
        {
            consumer.Subscribe(DeadLetterTopic);
            LogSubscribedToDlq(logger, DeadLetterTopic);

            // The end of the log for each assigned partition as it stood when this sweep began.
            // Captured once, lazily, after the first record proves the assignment is settled — and
            // always before any message is handled, so this sweep's own re-enqueues land beyond it.
            Dictionary<TopicPartition, long>? finishLine = null;
            var paused = new HashSet<TopicPartition>();

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<TKey, TMessage> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(ConsumePollTimeout);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    var record = ex.ConsumerRecord;
                    if (record is null || record.Offset == Offset.Unset)
                    {
                        // No record offset to skip past — end the sweep and retry on the next tick.
                        LogDlqConsumeError(logger, ex, DeadLetterTopic);
                        break;
                    }

                    // An undeserializable DLQ record would otherwise abort every sweep at the same
                    // offset and wedge the DLQ permanently. Skip it and commit past it. Records the
                    // main consumer captured as raw bytes (deserialization-failed header) are
                    // expected to be undeserializable — they await manual redrive, so skip quietly.
                    if (DlqRecordClassifier.IsCapturedPoison(record.Message?.Headers))
                    {
                        LogCapturedPoisonSkipped(logger, DeadLetterTopic, record.Partition.Value, record.Offset.Value);
                    }
                    else
                    {
                        LogDlqPoisonMessageSkipped(logger, ex, DeadLetterTopic, record.Partition.Value, record.Offset.Value);
                    }

                    metrics.DlqSkipped.Add(1,
                        new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic),
                        new KeyValuePair<string, object?>("reason", "deserialization_failed"));

                    // StoreOffset(TopicPartitionOffset) stores the given offset verbatim, so +1 to move past the failed record.
                    consumer.StoreOffset(new TopicPartitionOffset(record.TopicPartition, record.Offset + 1));
                    CommitStoredOffsets(consumer);
                    continue;
                }

                if (consumeResult == null)
                {
                    break;
                }

                if (consumeResult.IsPartitionEOF)
                {
                    // An informational marker rather than a record: nothing to hand the handler and
                    // no offset to commit. It is never emitted today (the DLQ consumer does not set
                    // EnablePartitionEof) and terminating the sweep on it would be wrong anyway,
                    // since EOF tracks the *current* end of the log — which includes this sweep's
                    // own re-enqueues. Bounding the sweep is the finish line's job.
                    continue;
                }

                finishLine ??= SnapshotFinishLine(consumer);
                if (finishLine.Count == 0)
                {
                    // Nothing to bound the sweep against. Draining unbounded would risk consuming
                    // this sweep's own re-enqueues, so leave the work for the next tick.
                    LogNoFinishLine(logger, DeadLetterTopic);
                    break;
                }

                // Checked before the tombstone branch: a record at or beyond the finish line was
                // appended during this sweep and must not be committed, tombstone or not.
                if (TryFinishPartition(consumer, consumeResult, finishLine, paused))
                {
                    // Membership, not a count: a partition assigned mid-sweep by a rebalance is
                    // paused too but is absent from the snapshot, so counting would let it stand in
                    // for a snapshotted partition that has not finished yet.
                    if (finishLine.Keys.All(paused.Contains))
                    {
                        break;
                    }

                    continue;
                }

                // One classification drives every skip decision from here on. The rules live in
                // DlqRecordClassifier so this sweep and IDlqInspector cannot drift apart.
                var message = consumeResult.Message;
                var state = DlqRecordClassifier.Classify(
                    message?.Value is not null,
                    message?.Headers,
                    MaxReprocessAttempts);

                // A null message classifies as a tombstone already; naming it here as well is what
                // tells the compiler the record is non-null for the rest of the loop.
                if (message is null || state == DlqEntryState.Tombstone)
                {
                    // Tombstone (null value): commit past it and keep processing — treating it as
                    // the end of the sweep would leave the offset behind it and wedge the DLQ forever.
                    LogDlqTombstoneSkipped(logger, DeadLetterTopic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    consumer.StoreOffset(consumeResult);
                    CommitStoredOffsets(consumer);
                    continue;
                }

                var success = await HandleMessageAsync(consumeResult, state, batchId, stoppingToken);

                if (!success)
                {
                    LogSweepStopping(logger, message.Key);
                    break;
                }

                consumer.StoreOffset(consumeResult);
                CommitStoredOffsets(consumer);
            }

            LogSweepFinished(logger, DeadLetterTopic);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogSweepError(logger, ex, DeadLetterTopic);
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Commits the stored offset, tolerating the commit failures that are routine rather than fatal.
    /// </summary>
    /// <remarks>
    /// A rebalance mid-sweep revokes the partition and the commit is rejected. That is ordinary: the
    /// next tick re-reads whatever was not committed, and in-place reprocessing is required to be
    /// idempotent anyway. Letting it reach the sweep's catch-all would log a routine event at
    /// Critical and abandon the partitions that are still draining.
    /// </remarks>
    private void CommitStoredOffsets(IConsumer<TKey, TMessage> consumer)
    {
        try
        {
            consumer.Commit();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_NoOffset)
        {
            // Nothing stored to commit - not an error.
        }
        catch (KafkaException ex)
        {
            LogDlqCommitFailed(logger, ex, DeadLetterTopic);
        }
    }

    /// <summary>
    /// Captures the end of the log for every assigned partition, fixing the finish line for this
    /// sweep before any message is reprocessed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The DLQ consumer appends to the very topic it is draining, so the end of the log runs away
    /// from it as it works. Snapshotting up front means this sweep's own re-enqueues land beyond the
    /// finish line and are structurally unreachable until the next tick — the job the
    /// <c>batch-id</c> header used to do by tagging and recognising our own output.
    /// </para>
    /// <para>
    /// Queried rather than read from the client's cache: <c>GetWatermarkOffsets</c> only has a value
    /// for partitions that have already delivered a message set, so a partition not yet fetched
    /// would silently get no finish line and would then consume its own re-enqueues. One blocking
    /// call per partition is irrelevant on a sweep that runs on an hourly interval by default.
    /// </para>
    /// <para>
    /// Taken exactly once per sweep. Re-reading it later would pick up messages appended in the
    /// meantime and reintroduce the moving finish line this exists to escape.
    /// </para>
    /// </remarks>
    private Dictionary<TopicPartition, long> SnapshotFinishLine(IConsumer<TKey, TMessage> consumer)
    {
        var finishLine = new Dictionary<TopicPartition, long>();
        foreach (var topicPartition in consumer.Assignment)
        {
            var watermarks = consumer.QueryWatermarkOffsets(topicPartition, WatermarkQueryTimeout);
            finishLine[topicPartition] = watermarks.High.Value;
            LogFinishLine(logger, DeadLetterTopic, topicPartition.Partition.Value, watermarks.High.Value);
        }

        return finishLine;
    }

    /// <summary>
    /// Returns <c>true</c> when the record lies at or beyond its partition's finish line, meaning the
    /// partition is done for this sweep. The partition is paused on the first such record so the
    /// remaining partitions keep draining, and recorded in <paramref name="paused"/> so it is not
    /// paused again.
    /// </summary>
    /// <remarks>
    /// Pausing rather than ending the whole sweep is what makes a multi-partition dead letter topic
    /// safe: stopping everything the moment one partition is exhausted abandons unprocessed messages
    /// on the others. The record itself is deliberately left uncommitted so the next tick picks it up.
    /// </remarks>
    private bool TryFinishPartition(
        IConsumer<TKey, TMessage> consumer,
        ConsumeResult<TKey, TMessage> consumeResult,
        Dictionary<TopicPartition, long> finishLine,
        HashSet<TopicPartition> paused)
    {
        var topicPartition = consumeResult.TopicPartition;

        // A partition absent from the snapshot was assigned mid-sweep by a rebalance and has no
        // finish line of its own; leave it entirely to the next tick rather than draining it
        // unbounded, which could reach this sweep's re-enqueues.
        var reachedEnd = !finishLine.TryGetValue(topicPartition, out var stopAt)
            || consumeResult.Offset.Value >= stopAt;

        if (!reachedEnd)
        {
            return false;
        }

        if (paused.Add(topicPartition))
        {
            consumer.Pause(new[] { topicPartition });
            LogPartitionFinished(logger, DeadLetterTopic, topicPartition.Partition.Value, consumeResult.Offset.Value);
        }

        return true;
    }

    /// <summary>
    /// Returns true if the message was handled (reprocessed or intentionally skipped) and its
    /// offset should be committed. Returns false if a produce operation failed — the sweep should stop.
    /// </summary>
    /// <remarks>
    /// The message is reprocessed in place by invoking the registered message handler. On success the
    /// offset is committed. A permanent failure (<see cref="InvalidMessageException"/>) is skipped. Any
    /// other failure re-enqueues the message to the dead letter topic with an incremented attempt count
    /// so it is retried on a future tick (bounded by <see cref="KafkaWorkerConfig.DeadLetterMaxReprocessAttempts"/>).
    /// </remarks>
    private async Task<bool> HandleMessageAsync(
        ConsumeResult<TKey, TMessage> consumeResult,
        DlqEntryState state,
        string batchId,
        CancellationToken stoppingToken)
    {
        switch (state)
        {
            // Invalid messages should not be reprocessed - they will always fail
            case DlqEntryState.Invalid:
                LogSkippingInvalidMessage(logger, consumeResult.Message.Key);
                metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "invalid"));
                await NotifyTerminalFailureAsync(consumeResult, TerminalFailureReason.InvalidMessage, error: null, stoppingToken);
                return true;

            case DlqEntryState.AttemptsExhausted:
                LogExceededMaxReprocessAttempts(logger, MaxReprocessAttempts, consumeResult.Message.Key);
                metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "max_attempts"));
                await NotifyTerminalFailureAsync(consumeResult, TerminalFailureReason.MaxReprocessAttemptsExceeded, error: null, stoppingToken);
                return true;

            // Raw bytes captured from a failed deserialization that happen to deserialize here
            // anyway. The header means the record awaits manual redrive, so honour it rather than
            // handing the handler a message the main consumer could not read. Matches the quiet
            // skip the ConsumeException path above gives the records that do not deserialize, and
            // like that path notifies no sink: a captured poison record never fires the typed one.
            case DlqEntryState.Undeserializable:
                LogCapturedPoisonSkipped(logger, DeadLetterTopic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "deserialization_failed"));
                return true;
        }

        try
        {
            using var scope = serviceScopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();
            await handler.HandleMessageAsync(consumeResult.Message.Value, stoppingToken);

            LogSuccessfullyReprocessed(logger, consumeResult.Message.Key);
            metrics.DlqReprocessed.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic));
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (InvalidMessageException ex)
        {
            // Permanent failure - the message will never succeed, so skip it (commit and move on).
            LogInvalidMessageInPlace(logger, ex, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "invalid"));
            await NotifyTerminalFailureAsync(consumeResult, TerminalFailureReason.InvalidMessage, ex.Message, stoppingToken);
            return true;
        }
        catch (Exception ex)
        {
            // Reprocessing failed again - re-enqueue to the DLQ for a future tick with an incremented attempt.
            LogInPlaceReprocessFailed(logger, ex, consumeResult.Message.Key);
            return await ReEnqueueToDeadLetterAsync(consumeResult, batchId, ex, stoppingToken);
        }
    }

    /// <summary>
    /// Invokes the optional <see cref="ITerminalFailureSink{TMessage}"/> when a DLQ message is
    /// permanently skipped. Best-effort: sink failures are logged and never affect the sweep or the
    /// offset commit. When <paramref name="error"/> is null, the message's <c>error-message</c>
    /// header (the last recorded failure) is used.
    /// </summary>
    private async Task NotifyTerminalFailureAsync(
        ConsumeResult<TKey, TMessage> consumeResult,
        TerminalFailureReason reason,
        string? error,
        CancellationToken stoppingToken)
    {
        try
        {
            using var scope = serviceScopeFactory.CreateScope();
            var sink = scope.ServiceProvider.GetService<ITerminalFailureSink<TMessage>>();
            if (sink is null)
            {
                return;
            }

            await sink.HandleAsync(new TerminalFailure<TMessage>
            {
                Message = consumeResult.Message.Value,
                MessageKey = consumeResult.Message.Key,
                SourceTopic = consumeResult.Message.Headers.GetValue(KafkaHeaders.OriginalTopic) ?? _kafkaConfig.Topic,
                Reason = reason,
                Error = error ?? consumeResult.Message.Headers.GetValue(KafkaHeaders.ErrorMessage),
                ReprocessAttempts = consumeResult.Message.Headers.GetReprocessAttemptCount(),
                Headers = consumeResult.Message.Headers
            }, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogTerminalFailureSinkFailed(logger, ex, consumeResult.Message.Key);
        }
    }

    /// <summary>
    /// Re-enqueues a message that failed in-place reprocessing back to the dead letter topic with an
    /// incremented reprocess-attempt count and the current sweep id. The re-enqueued record lands
    /// beyond this sweep's finish line, so it is naturally left for a future tick; the id is carried
    /// purely as a diagnostic breadcrumb, under the historical <c>batch-id</c> header name. Returns
    /// false if the produce fails so the sweep stops without committing.
    /// </summary>
    private async Task<bool> ReEnqueueToDeadLetterAsync(
        ConsumeResult<TKey, TMessage> consumeResult,
        string batchId,
        Exception failure,
        CancellationToken stoppingToken)
    {
        var nextAttempt = consumeResult.Message.Headers.GetReprocessAttemptCount() + 1;

        try
        {
            var overrideHeaders = new Headers();
            overrideHeaders.AddUtf8(KafkaHeaders.BatchId, batchId);
            overrideHeaders.AddUtf8(KafkaHeaders.ReprocessedAttempt, nextAttempt.ToString());
            overrideHeaders.AddUtf8(KafkaHeaders.ErrorMessage, failure.Message);

            await DeadLetterPublisher.PublishAsync(
                producer.Value,
                DeadLetterTopic!,
                consumeResult.Message.Key,
                consumeResult.Message.Value,
                overrideHeaders,
                consumeResult.Message.Headers,
                _produceResiliencePipeline,
                stoppingToken);

            LogReEnqueuedToDeadLetter(logger, consumeResult.Message.Key, nextAttempt);
            metrics.DlqPublished.Add(1,
                new KeyValuePair<string, object?>("topic", _kafkaConfig.Topic),
                new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic),
                new KeyValuePair<string, object?>("reason", "reprocess_failed"));
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogFailedToReEnqueue(logger, ex, consumeResult.Message.Key);
            return false;
        }
    }

    [LoggerMessage(EventId = 200, Level = LogLevel.Information, Message = "Starting KafkaDlqConsumer for topic: {DeadLetterTopic} with processing interval: {IntervalMinutes} minutes")]
    private static partial void LogStarting(ILogger logger, string? deadLetterTopic, int intervalMinutes);

    [LoggerMessage(EventId = 201, Level = LogLevel.Information, Message = "Starting dead letter queue sweep {BatchId} of topic: {DeadLetterTopic}")]
    private static partial void LogSweepStarting(ILogger logger, string batchId, string? deadLetterTopic);

    [LoggerMessage(EventId = 202, Level = LogLevel.Warning, Message = "KafkaDlqConsumer shutting down.")]
    private static partial void LogShuttingDown(ILogger logger);

    [LoggerMessage(EventId = 203, Level = LogLevel.Critical, Message = "Fatal error in KafkaDlqConsumer")]
    private static partial void LogFatalError(ILogger logger, Exception ex);

    [LoggerMessage(EventId = 204, Level = LogLevel.Information, Message = "Subscribed to dead letter topic: {DeadLetterTopic}")]
    private static partial void LogSubscribedToDlq(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 205, Level = LogLevel.Warning, Message = "Stopping the sweep due to failed reprocess. Will retry on next tick. Key: {MessageKey}")]
    private static partial void LogSweepStopping(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 206, Level = LogLevel.Information, Message = "Finished the dead letter queue sweep of topic: {DeadLetterTopic}")]
    private static partial void LogSweepFinished(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 207, Level = LogLevel.Critical, Message = "Error during the dead letter queue sweep of topic: {DeadLetterTopic}")]
    private static partial void LogSweepError(ILogger logger, Exception ex, string? deadLetterTopic);

    [LoggerMessage(EventId = 208, Level = LogLevel.Warning, Message = "Skipping invalid message (will not succeed on retry). Key: {MessageKey}")]
    private static partial void LogSkippingInvalidMessage(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 209, Level = LogLevel.Warning, Message = "Message has exceeded max reprocess attempts ({MaxAttempts}). Skipping. Key: {MessageKey}")]
    private static partial void LogExceededMaxReprocessAttempts(ILogger logger, int maxAttempts, TKey messageKey);

    [LoggerMessage(EventId = 210, Level = LogLevel.Information, Message = "Successfully reprocessed dead letter message in place. Key: {MessageKey}")]
    private static partial void LogSuccessfullyReprocessed(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 214, Level = LogLevel.Warning, Message = "Invalid message detected during in-place reprocessing (will not succeed on retry). Skipping. Key: {MessageKey}")]
    private static partial void LogInvalidMessageInPlace(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 215, Level = LogLevel.Warning, Message = "In-place reprocessing failed. Re-enqueuing to dead letter topic. Key: {MessageKey}")]
    private static partial void LogInPlaceReprocessFailed(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 216, Level = LogLevel.Information, Message = "Re-enqueued message to dead letter topic for future reprocessing. Key: {MessageKey}, Attempt: {Attempt}")]
    private static partial void LogReEnqueuedToDeadLetter(ILogger logger, TKey messageKey, int attempt);

    [LoggerMessage(EventId = 217, Level = LogLevel.Error, Message = "Failed to re-enqueue message to dead letter topic. Key: {MessageKey}")]
    private static partial void LogFailedToReEnqueue(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 218, Level = LogLevel.Critical, Message = "Skipping DLQ message that failed to deserialize. DLQ Topic: {DeadLetterTopic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogDlqPoisonMessageSkipped(ILogger logger, Exception ex, string? deadLetterTopic, int partition, long offset);

    [LoggerMessage(EventId = 219, Level = LogLevel.Error, Message = "Consume error on DLQ topic {DeadLetterTopic}; no record offset available. Ending the sweep.")]
    private static partial void LogDlqConsumeError(ILogger logger, Exception ex, string? deadLetterTopic);

    [LoggerMessage(EventId = 220, Level = LogLevel.Debug, Message = "Skipping tombstone (null value) DLQ message and committing offset. DLQ Topic: {DeadLetterTopic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogDlqTombstoneSkipped(ILogger logger, string? deadLetterTopic, int partition, long offset);

    [LoggerMessage(EventId = 221, Level = LogLevel.Information, Message = "On-demand reprocess trigger received. Running an immediate sweep of dead letter topic: {DeadLetterTopic}")]
    private static partial void LogSweepTriggered(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 222, Level = LogLevel.Error, Message = "Terminal failure sink threw; the terminal failure record was not persisted. Key: {MessageKey}")]
    private static partial void LogTerminalFailureSinkFailed(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 224, Level = LogLevel.Debug, Message = "DLQ sweep finish line for {DeadLetterTopic} partition {Partition}: offset {HighWatermark}")]
    private static partial void LogFinishLine(ILogger logger, string? deadLetterTopic, int partition, long highWatermark);

    [LoggerMessage(EventId = 225, Level = LogLevel.Debug, Message = "Reached the finish line for {DeadLetterTopic} partition {Partition} at offset {Offset}; pausing it for the rest of this sweep")]
    private static partial void LogPartitionFinished(ILogger logger, string? deadLetterTopic, int partition, long offset);

    [LoggerMessage(EventId = 227, Level = LogLevel.Error, Message = "Failed to commit offsets during the dead letter queue sweep of {DeadLetterTopic}. The affected messages are re-read on the next tick.")]
    private static partial void LogDlqCommitFailed(ILogger logger, Exception ex, string? deadLetterTopic);

    [LoggerMessage(EventId = 226, Level = LogLevel.Warning, Message = "No partitions assigned when starting the sweep of {DeadLetterTopic}; skipping this tick")]
    private static partial void LogNoFinishLine(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 223, Level = LogLevel.Debug, Message = "Skipping captured raw poison record (awaiting manual redrive). DLQ Topic: {DeadLetterTopic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogCapturedPoisonSkipped(ILogger logger, string? deadLetterTopic, int partition, long offset);
}
