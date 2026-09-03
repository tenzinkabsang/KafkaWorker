using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace KafkaWorker;

/// <summary>
/// Consumes messages from a specified Kafka topic, processes them using a provided message handler, and handles
/// failures with configurable retry and dead letter queue support.
/// </summary>
/// <remarks>The consumer automatically retries failed message processing up to a configurable maximum number of
/// attempts. If a dead letter topic is configured, messages that cannot be processed after
/// all retries are sent to the dead letter queue; otherwise, they are logged and skipped. Offsets are stored only
/// after successful processing or after a message is sent to the dead letter topic, and are committed to the broker
/// by the client's background auto-commit (periodically, on rebalance, and on close).
/// The consumer continues processing subsequent messages even if individual messages fail.</remarks>
internal sealed partial class Consumer<TKey, TMessage>(
    IConsumer<TKey, TMessage> consumer,
    Lazy<IProducer<TKey, TMessage>> deadLetterProducer,
    RawDeadLetterProducer<TMessage> rawDeadLetterProducer,
    IServiceScopeFactory serviceScopeFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    KafkaWorkerMetrics metrics,
    ILogger<Consumer<TKey, TMessage>> logger) : BackgroundService where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).FullName);

    /// <summary>
    /// Gets the maximum number of retry attempts for processing a message.
    /// </summary>
    private int MaxRetries => _kafkaConfig.MaxRetries;

    /// <summary>
    /// Gets the Kafka topic name to consume messages from.
    /// </summary>
    private string Topic => _kafkaConfig.Topic;

    /// <summary>
    /// Gets the dead letter topic name for failed messages.
    /// If null, failed messages will be logged but not published to a dead letter queue.
    /// </summary>
    private string? DeadLetterTopic => _kafkaConfig.DeadLetterTopic;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        try
        {
            consumer.Subscribe(Topic);

            LogSubscribed(logger, Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<TKey, TMessage> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    // A message that cannot be deserialized would otherwise crash the host and
                    // then be re-consumed on restart, forever. Capture its raw bytes to the DLQ
                    // when possible, then skip past it.
                    await SkipPoisonMessageAsync(ex, stoppingToken);
                    continue;
                }

                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }

                if (consumeResult.Message?.Value == null)
                {
                    // Tombstone (null value): nothing to process, but store the offset so the consumer advances.
                    LogTombstoneSkipped(logger, Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    consumer.StoreOffset(consumeResult);
                    continue;
                }

                await ProcessMessageWithRetryAsync(consumeResult, stoppingToken);

                // Storing (not committing) is deliberate: the client's background auto-commit flushes
                // stored offsets periodically, on rebalance, and on Close() — no per-message round trip.
                consumer.StoreOffset(consumeResult);
            }

            LogFinishedExecuting(logger, Topic);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            LogShuttingDown(logger, Topic);
        }
        catch (Exception ex)
        {
            LogFatalError(logger, ex, Topic);
            throw;
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task ProcessMessageWithRetryAsync(
        ConsumeResult<TKey, TMessage> consumerResult,
        CancellationToken stoppingToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            await _messageProcessingResiliencePipeline.ExecuteAsync(
                async token =>
                {
                    LogProcessingMessage(logger, consumerResult.Message.Key);
                    using var scope = serviceScopeFactory.CreateScope();
                    var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();
                    await handler.HandleMessageAsync(consumerResult.Message.Value, token);
                },
                stoppingToken);

            LogSuccessfullyProcessed(logger, consumerResult.Message.Key);
            metrics.MessagesProcessed.Add(1, new KeyValuePair<string, object?>("topic", Topic), new KeyValuePair<string, object?>("status", "success"));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw; // Let it propagate to ExecuteAsync's catch — don't DLQ or commit
        }
        catch (InvalidMessageException ex)
        {
            // Invalid message - no point retrying, send directly to DLQ with invalid-message flag
            LogInvalidMessageDetected(logger, ex, consumerResult.Message.Key);
            metrics.MessagesProcessed.Add(1, new KeyValuePair<string, object?>("topic", Topic), new KeyValuePair<string, object?>("status", "invalid"));

            await PublishToDeadLetterAsync(consumerResult, ex, isInvalidMessage: true, stoppingToken);
        }
        catch (Exception ex)
        {
            LogFailedToProcess(logger, ex, MaxRetries, consumerResult.Message.Key);
            metrics.MessagesProcessed.Add(1, new KeyValuePair<string, object?>("topic", Topic), new KeyValuePair<string, object?>("status", "failed"));

            await PublishToDeadLetterAsync(consumerResult, ex, isInvalidMessage: false, stoppingToken);
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            metrics.ProcessingDuration.Record(elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("topic", Topic));
        }
    }

    /// <summary>
    /// Publishes a failed message to the dead letter topic with retry.
    /// Best-effort: if all retry attempts fail, the failure is logged at Critical level
    /// and the offset is still committed. The main consumer is never crashed by a DLQ publish failure.
    /// </summary>
    private async Task PublishToDeadLetterAsync(
        ConsumeResult<TKey, TMessage> consumerResult,
        Exception exception,
        bool isInvalidMessage,
        CancellationToken stoppingToken)
    {
        // If no dead letter topic is configured, the message is terminal right here — notify the
        // optional sink (its last chance to be persisted anywhere), log, and return (offset advances)
        if (string.IsNullOrWhiteSpace(DeadLetterTopic))
        {
            LogNoDeadLetterTopicConfigured(logger, consumerResult.Message.Key);
            await NotifyTerminalFailureAsync(
                consumerResult, TerminalFailureReason.NoDeadLetterTopicConfigured, exception, stoppingToken);
            return;
        }

        try
        {
            var overrideHeaders = new Headers();
            overrideHeaders.AddUtf8(KafkaHeaders.OriginalTopic, Topic);
            overrideHeaders.AddUtf8(KafkaHeaders.ErrorMessage, exception.Message);

            if (isInvalidMessage)
            {
                overrideHeaders.AddUtf8(KafkaHeaders.InvalidMessage, "true");
            }

            // Override headers win; remaining original headers are copied for DLQ processing.
            await DeadLetterPublisher.PublishAsync(
                deadLetterProducer.Value,
                DeadLetterTopic,
                consumerResult.Message.Key,
                consumerResult.Message.Value,
                overrideHeaders,
                consumerResult.Message.Headers,
                ProduceResiliencePipeline.Instance,
                stoppingToken);

            LogSentToDeadLetter(logger, DeadLetterTopic, consumerResult.Message.Key);
            metrics.DlqPublished.Add(1,
                new KeyValuePair<string, object?>("topic", Topic),
                new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic),
                new KeyValuePair<string, object?>("reason", isInvalidMessage ? "invalid" : "processing_failed"));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutdown mid-publish: propagate so the offset is NOT stored and the message is
            // redelivered on restart, instead of being committed past without reaching the DLQ.
            throw;
        }
        catch (Exception ex)
        {
            LogFailedToPublishToDeadLetter(logger, ex, DeadLetterTopic, consumerResult.Message.Key);
            await NotifyTerminalFailureAsync(
                consumerResult, TerminalFailureReason.DeadLetterPublishFailed, exception, stoppingToken);
        }
    }

    /// <summary>
    /// Invokes the optional <see cref="ITerminalFailureSink{TMessage}"/> when the library permanently
    /// gives up on a message. Best-effort: sink failures are logged and never affect the consume
    /// loop or offset storage.
    /// </summary>
    private async Task NotifyTerminalFailureAsync(
        ConsumeResult<TKey, TMessage> consumerResult,
        TerminalFailureReason reason,
        Exception exception,
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
                Message = consumerResult.Message.Value,
                MessageKey = consumerResult.Message.Key,
                SourceTopic = Topic,
                Reason = reason,
                Error = exception.Message,
                Headers = consumerResult.Message.Headers
            }, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogTerminalFailureSinkFailed(logger, ex, consumerResult.Message.Key);
        }
    }

    /// <summary>
    /// Handles a message that failed to deserialize (poison message). The raw payload cannot be
    /// represented as <typeparamref name="TMessage"/>, so it cannot be retried or reprocessed; when
    /// a DLQ is configured its raw bytes are captured there (best-effort) for manual inspection and
    /// redrive, otherwise it is logged at Critical and lost. Either way the offset is stored past it
    /// so the consumer survives. Consume errors without a record offset (e.g. broker errors) are
    /// logged and not stored.
    /// </summary>
    private async Task SkipPoisonMessageAsync(ConsumeException ex, CancellationToken stoppingToken)
    {
        var record = ex.ConsumerRecord;
        if (record is null || record.Offset == Offset.Unset)
        {
            LogConsumeError(logger, ex, Topic);
            return;
        }

        metrics.MessagesProcessed.Add(1,
            new KeyValuePair<string, object?>("topic", Topic),
            new KeyValuePair<string, object?>("status", "deserialization_failed"));

        if (!string.IsNullOrWhiteSpace(DeadLetterTopic) && record.Message is not null)
        {
            try
            {
                var overrideHeaders = new Headers();
                overrideHeaders.AddUtf8(KafkaHeaders.OriginalTopic, Topic);
                overrideHeaders.AddUtf8(KafkaHeaders.ErrorMessage, ex.Error.Reason);
                overrideHeaders.AddUtf8(KafkaHeaders.DeserializationFailed, "true");

                await DeadLetterPublisher.PublishAsync(
                    rawDeadLetterProducer.Value,
                    DeadLetterTopic,
                    record.Message.Key,
                    record.Message.Value,
                    overrideHeaders,
                    record.Message.Headers,
                    ProduceResiliencePipeline.Instance,
                    stoppingToken);

                LogPoisonMessageCaptured(logger, ex, record.Topic, record.Partition.Value, record.Offset.Value, DeadLetterTopic);
                metrics.DlqPublished.Add(1,
                    new KeyValuePair<string, object?>("topic", Topic),
                    new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic),
                    new KeyValuePair<string, object?>("reason", "deserialization_failed"));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown mid-capture: propagate so the offset is NOT stored and the record is
                // re-consumed (and re-captured) on restart.
                throw;
            }
            catch (Exception publishEx)
            {
                LogPoisonMessageSkipped(logger, publishEx, record.Topic, record.Partition.Value, record.Offset.Value);
            }
        }
        else
        {
            LogPoisonMessageSkipped(logger, ex, record.Topic, record.Partition.Value, record.Offset.Value);
        }

        // StoreOffset(TopicPartitionOffset) stores the given offset verbatim, so +1 to move past the failed record.
        consumer.StoreOffset(new TopicPartitionOffset(record.TopicPartition, record.Offset + 1));
    }

    // Configure the resilience pipeline for message processing with retry based on the MaxRetries configuration.
    private readonly ResiliencePipeline _messageProcessingResiliencePipeline = BuildRetryPipeline(
        kafkaConfigMonitor.Get(typeof(TMessage).FullName).MaxRetries);

    private static ResiliencePipeline BuildRetryPipeline(int maxRetries) => maxRetries > 0
        ? new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = maxRetries,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => ex is not InvalidMessageException)
            })
            .Build()
        : ResiliencePipeline.Empty;

    [LoggerMessage(EventId = 100, Level = LogLevel.Information, Message = "Subscribed to kafka topic: {Topic}")]
    private static partial void LogSubscribed(ILogger logger, string topic);

    [LoggerMessage(EventId = 101, Level = LogLevel.Information, Message = "Finished executing KafkaConsumer for topic: {Topic}")]
    private static partial void LogFinishedExecuting(ILogger logger, string topic);

    [LoggerMessage(EventId = 102, Level = LogLevel.Warning, Message = "KafkaConsumer shutting down for topic: {Topic}")]
    private static partial void LogShuttingDown(ILogger logger, string topic);

    [LoggerMessage(EventId = 103, Level = LogLevel.Critical, Message = "Fatal error in KafkaConsumer for topic: {Topic}")]
    private static partial void LogFatalError(ILogger logger, Exception ex, string topic);

    [LoggerMessage(EventId = 104, Level = LogLevel.Debug, Message = "Processing message. Key: {MessageKey}")]
    private static partial void LogProcessingMessage(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 105, Level = LogLevel.Debug, Message = "Successfully processed message. Key: {MessageKey}")]
    private static partial void LogSuccessfullyProcessed(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 106, Level = LogLevel.Warning, Message = "Invalid message detected, attempting to route to DLQ if configured. Key: {MessageKey}")]
    private static partial void LogInvalidMessageDetected(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 107, Level = LogLevel.Error, Message = "Failed to process message after {MaxRetries} retries. Key: {MessageKey}")]
    private static partial void LogFailedToProcess(ILogger logger, Exception ex, int maxRetries, TKey messageKey);

    [LoggerMessage(EventId = 108, Level = LogLevel.Warning, Message = "No dead letter topic configured. Message will be lost. Key: {MessageKey}")]
    private static partial void LogNoDeadLetterTopicConfigured(ILogger logger, TKey messageKey);

    [LoggerMessage(EventId = 109, Level = LogLevel.Information, Message = "Message sent to dead letter topic: {DeadLetterTopic}. Original Key: {MessageKey}")]
    private static partial void LogSentToDeadLetter(ILogger logger, string? deadLetterTopic, TKey messageKey);

    [LoggerMessage(EventId = 110, Level = LogLevel.Critical, Message = "Failed to publish message to dead letter topic: {DeadLetterTopic}. Message Key: {MessageKey}")]
    private static partial void LogFailedToPublishToDeadLetter(ILogger logger, Exception ex, string? deadLetterTopic, TKey messageKey);

    [LoggerMessage(EventId = 111, Level = LogLevel.Critical, Message = "Skipping message that failed to deserialize; it could NOT be captured to the DLQ and will be lost. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogPoisonMessageSkipped(ILogger logger, Exception ex, string topic, int partition, long offset);

    [LoggerMessage(EventId = 114, Level = LogLevel.Error, Message = "Message failed to deserialize; its raw bytes were captured to dead letter topic {DeadLetterTopic} for manual inspection. It will NOT be auto-reprocessed. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogPoisonMessageCaptured(ILogger logger, Exception ex, string topic, int partition, long offset, string? deadLetterTopic);

    [LoggerMessage(EventId = 115, Level = LogLevel.Error, Message = "Terminal failure sink threw; the terminal failure record was not persisted. Key: {MessageKey}")]
    private static partial void LogTerminalFailureSinkFailed(ILogger logger, Exception ex, TKey messageKey);

    [LoggerMessage(EventId = 112, Level = LogLevel.Error, Message = "Consume error on topic {Topic}; no record offset available, continuing without commit")]
    private static partial void LogConsumeError(ILogger logger, Exception ex, string topic);

    [LoggerMessage(EventId = 113, Level = LogLevel.Debug, Message = "Skipping tombstone (null value) message and storing its offset. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}")]
    private static partial void LogTombstoneSkipped(ILogger logger, string topic, int partition, long offset);
}
