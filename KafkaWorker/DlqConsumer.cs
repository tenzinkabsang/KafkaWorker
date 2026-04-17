using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;

namespace KafkaWorker;

/// <summary>
/// Periodically processes messages from the dead letter queue and reprocesses them by sending
/// back to the original topic for retry.
/// </summary>
/// <remarks>
/// The consumer runs on a configurable interval (default: 60 minutes) and processes all pending
/// DLQ messages in each batch. Messages marked as invalid (via <see cref="InvalidMessageException"/>)
/// or that have exceeded the maximum reprocess attempts are skipped.
/// <para>
/// For optimal performance, the dead letter topic should be configured with a single partition.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The type of message key being consumed.</typeparam>
/// <typeparam name="TMessage">The type of message being consumed.</typeparam>
internal sealed partial class DlqConsumer<TKey, TMessage>(
    IProducer<TKey, TMessage> producer,
    IDlqConsumerFactory<TKey, TMessage> consumerFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    KafkaWorkerMetrics metrics,
    ILogger<DlqConsumer<TKey, TMessage>> logger,
    TimeProvider timeProvider) : BackgroundService where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).Name);

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
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult?.Message?.Value == null || consumeResult.IsPartitionEOF)
                {
                    break;
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
    /// Returns true if the message was handled (produced or intentionally skipped).
    /// Returns false if the produce to the original topic failed — the batch should stop.
    /// </summary>
    private async Task<bool> HandleMessageAsync(
        ConsumeResult<TKey, TMessage> consumeResult,
        string batchId,
        CancellationToken stoppingToken)
    {
        // Invalid messages should not be reprocessed - they will always fail
        if (consumeResult.Message.Headers.IsInvalidMessage())
        {
            LogSkippingInvalidMessage(logger, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "invalid"));
            return true;
        }

        if (consumeResult.Message.Headers.GetReprocessAttemptCount() >= MaxReprocessAttempts)
        {
            LogExceededMaxReprocessAttempts(logger, MaxReprocessAttempts, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "max_attempts"));
            return true;
        }

        var originalTopic = consumeResult.Message.Headers.GetOriginalTopic();

        if (string.IsNullOrEmpty(originalTopic))
        {
            LogMissingOriginalTopic(logger, consumeResult.Message.Key);
            metrics.DlqSkipped.Add(1, new KeyValuePair<string, object?>("topic", DeadLetterTopic), new KeyValuePair<string, object?>("reason", "missing_topic"));
            return true;
        }

        try
        {
            var reprocessMessage = CreateReprocessMessage(consumeResult, batchId);

            await _produceResiliencePipeline.ExecuteAsync(
                async token => await producer.ProduceAsync(originalTopic, reprocessMessage, token),
                stoppingToken);

            LogSuccessfullyReprocessed(logger, consumeResult.Message.Key, originalTopic);
            metrics.DlqReprocessed.Add(1, new KeyValuePair<string, object?>("topic", originalTopic), new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic));
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogFailedToReprocess(logger, ex, consumeResult.Message.Key);
            return false;
        }
    }

    private static Message<TKey, TMessage> CreateReprocessMessage(ConsumeResult<TKey, TMessage> consumeResult, string batchId)
    {
        var currentAttempt = consumeResult.Message.Headers.GetReprocessAttemptCount();

        // Note: We don't need to preserve original-topic header here because the main consumer
        // always adds it when sending to the DLQ. Only batch-id and reprocess-attempt are needed.
        var headers = new Headers();
        headers.AddUtf8(KafkaHeaders.BatchId, batchId);
        headers.AddUtf8(KafkaHeaders.ReprocessedAttempt, (currentAttempt + 1).ToString());

        var failedGroupId = consumeResult.Message.Headers.GetFailedConsumerGroupId();
        if (!string.IsNullOrEmpty(failedGroupId))
        {
            headers.AddUtf8(KafkaHeaders.FailedConsumerGroupId, failedGroupId);
        }

        return new Message<TKey, TMessage>
        {
            Key = consumeResult.Message.Key,
            Value = consumeResult.Message.Value,
            Headers = headers
        };
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

    [LoggerMessage(EventId = 210, Level = LogLevel.Warning, Message = "Original topic header missing. Cannot reprocess message. Key: {MessageKey}")]
    private static partial void LogMissingOriginalTopic(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 211, Level = LogLevel.Information, Message = "Successfully reprocessed message. Key: {MessageKey}, OriginalTopic: {OriginalTopic}")]
    private static partial void LogSuccessfullyReprocessed(ILogger logger, TKey messageKey, string originalTopic);

    [LoggerMessage(EventId = 212, Level = LogLevel.Error, Message = "Failed to reprocess dead letter message. Key: {MessageKey}")]
    private static partial void LogFailedToReprocess(ILogger logger, Exception ex, TKey messageKey);
}
