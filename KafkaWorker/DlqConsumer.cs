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
/// DLQ messages in each batch. Messages marked as invalid (via <see cref="InvalidMessageException"/>)
/// or that have exceeded the maximum reprocess attempts are skipped.
/// <para>
/// Messages are reprocessed in place by invoking the registered <see cref="IMessageHandler{TMessage}"/>.
/// A message that fails again is re-enqueued to the dead letter topic with an incremented attempt for a
/// future tick, so failed messages never reappear on the original topic.
/// </para>
/// <para>
/// For optimal performance, the dead letter topic should be configured with a single partition.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The type of message key being consumed.</typeparam>
/// <typeparam name="TMessage">The type of message being consumed.</typeparam>
internal sealed partial class DlqConsumer<TKey, TMessage>(
    IProducer<TKey, TMessage> producer,
    IDlqConsumerFactory<TKey, TMessage> consumerFactory,
    IServiceScopeFactory serviceScopeFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    KafkaWorkerMetrics metrics,
    ILogger<DlqConsumer<TKey, TMessage>> logger,
    TimeProvider timeProvider) : BackgroundService where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).FullName);

    private int MaxReprocessAttempts => _kafkaConfig.DeadLetterMaxReprocessAttempts;
    private int ProcessingIntervalMinutes => _kafkaConfig.DeadLetterProcessingIntervalMinutes;
    private string? DeadLetterTopic => _kafkaConfig.DeadLetterTopic;

    private static readonly ResiliencePipeline _produceResiliencePipeline = ProduceResiliencePipeline.Instance;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        LogStarting(logger, DeadLetterTopic, ProcessingIntervalMinutes);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMinutes(ProcessingIntervalMinutes), timeProvider, stoppingToken);

                // Each iteration generate a unique guid as an identifier for the batch. This allows us to track which messages have been processed in this batch
                var batchId = Guid.NewGuid().ToString();

                LogProcessingBatch(logger, batchId, DeadLetterTopic);

                await ProcessDeadLetterQueueBatchAsync(batchId, stoppingToken);
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
    /// This method subscribes to the dead letter topic and processes new messages, committing
    /// offsets after each message is handled. Processing stops when the cancellation token is triggered or when there
    /// are no more messages
    /// </summary>
    internal async Task ProcessDeadLetterQueueBatchAsync(string batchId, CancellationToken stoppingToken)
    {
        using var consumer = consumerFactory.Create();

        try
        {
            consumer.Subscribe(DeadLetterTopic);
            LogSubscribedToDlq(logger, DeadLetterTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<TKey, TMessage> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    var record = ex.ConsumerRecord;
                    if (record is null || record.Offset == Offset.Unset)
                    {
                        // No record offset to skip past — end the batch and retry on the next tick.
                        LogDlqConsumeError(logger, ex, DeadLetterTopic);
                        break;
                    }

                    // An undeserializable DLQ record would otherwise abort every batch at the same
                    // offset and wedge the DLQ permanently. Skip it and commit past it.
                    LogDlqPoisonMessageSkipped(logger, ex, DeadLetterTopic, record.Partition.Value, record.Offset.Value);
                    metrics.DlqSkipped.Add(1,
                        new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic),
                        new KeyValuePair<string, object?>("reason", "deserialization_failed"));

                    // StoreOffset(TopicPartitionOffset) stores the given offset verbatim, so +1 to move past the failed record.
                    consumer.StoreOffset(new TopicPartitionOffset(record.TopicPartition, record.Offset + 1));
                    consumer.Commit();
                    continue;
                }

                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    break;
                }

                if (consumeResult.Message?.Value == null)
                {
                    // Tombstone (null value): commit past it and keep processing — treating it as
                    // batch end would leave the offset behind it and wedge the DLQ forever.
                    LogDlqTombstoneSkipped(logger, DeadLetterTopic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    consumer.StoreOffset(consumeResult);
                    consumer.Commit();
                    continue;
                }

                var messageBatchId = consumeResult.Message.Headers.GetBatchId();
                // If the message's batch ID matches the current batch ID, we stop processing
                // because it indicates we've looped back to already processed messages for this batch.
                if (!string.IsNullOrEmpty(messageBatchId) && messageBatchId == batchId)
                {
                    break;
                }

                var success = await HandleMessageAsync(consumeResult, batchId, stoppingToken);

                if (!success)
                {
                    LogStoppingBatch(logger, consumeResult.Message.Key);
                    break;
                }

                consumer.StoreOffset(consumeResult);
                consumer.Commit();
            }

            LogFinishedBatch(logger, DeadLetterTopic);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogBatchError(logger, ex, DeadLetterTopic);
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Returns true if the message was handled (reprocessed or intentionally skipped) and its
    /// offset should be committed. Returns false if a produce operation failed — the batch should stop.
    /// </summary>
    /// <remarks>
    /// The message is reprocessed in place by invoking the registered message handler. On success the
    /// offset is committed. A permanent failure (<see cref="InvalidMessageException"/>) is skipped. Any
    /// other failure re-enqueues the message to the dead letter topic with an incremented attempt count
    /// so it is retried on a future tick (bounded by <see cref="KafkaWorkerConfig.DeadLetterMaxReprocessAttempts"/>).
    /// </remarks>
    private async Task<bool> HandleMessageAsync(
        ConsumeResult<TKey, TMessage> consumeResult,
        string batchId,
        CancellationToken stoppingToken)
    {
        // Invalid messages should not be reprocessed - they will always fail
        if (consumeResult.Message.Headers.IsInvalidMessage())
        {
            LogSkippingInvalidMessage(logger, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "invalid"));
            return true;
        }

        if (consumeResult.Message.Headers.GetReprocessAttemptCount() >= MaxReprocessAttempts)
        {
            LogExceededMaxReprocessAttempts(logger, MaxReprocessAttempts, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "max_attempts"));
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
    /// Re-enqueues a message that failed in-place reprocessing back to the dead letter topic with an
    /// incremented reprocess-attempt count and the current batch id (so the current batch's loop
    /// detection stops before reprocessing it again). Returns false if the produce fails so the batch
    /// stops without committing.
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
                producer,
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

    [LoggerMessage(EventId = 201, Level = LogLevel.Information, Message = "Processing dead letter queue batch {BatchId} for topic: {DeadLetterTopic}")]
    private static partial void LogProcessingBatch(ILogger logger, string batchId, string? deadLetterTopic);

    [LoggerMessage(EventId = 202, Level = LogLevel.Warning, Message = "KafkaDlqConsumer shutting down.")]
    private static partial void LogShuttingDown(ILogger logger);

    [LoggerMessage(EventId = 203, Level = LogLevel.Critical, Message = "Fatal error in KafkaDlqConsumer")]
    private static partial void LogFatalError(ILogger logger, Exception ex);

    [LoggerMessage(EventId = 204, Level = LogLevel.Information, Message = "Subscribed to dead letter topic: {DeadLetterTopic}")]
    private static partial void LogSubscribedToDlq(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 205, Level = LogLevel.Warning, Message = "Stopping batch due to failed reprocess. Will retry on next tick. Key: {MessageKey}")]
    private static partial void LogStoppingBatch(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 206, Level = LogLevel.Information, Message = "Finished processing dead letter queue batch for topic: {DeadLetterTopic}")]
    private static partial void LogFinishedBatch(ILogger logger, string? deadLetterTopic);

    [LoggerMessage(EventId = 207, Level = LogLevel.Critical, Message = "Error processing dead letter queue batch for topic: {DeadLetterTopic}")]
    private static partial void LogBatchError(ILogger logger, Exception ex, string? deadLetterTopic);

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

    [LoggerMessage(EventId = 219, Level = LogLevel.Error, Message = "Consume error on DLQ topic {DeadLetterTopic}; no record offset available. Ending batch.")]
    private static partial void LogDlqConsumeError(ILogger logger, Exception ex, string? deadLetterTopic);

    [LoggerMessage(EventId = 220, Level = LogLevel.Debug, Message = "Skipping tombstone (null value) DLQ message and committing offset. DLQ Topic: {DeadLetterTopic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogDlqTombstoneSkipped(ILogger logger, string? deadLetterTopic, int partition, long offset);
}
