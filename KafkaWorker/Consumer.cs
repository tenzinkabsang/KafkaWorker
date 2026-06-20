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
/// all retries are sent to the dead letter queue; otherwise, they are logged and skipped. Offsets are committed only
/// after successful processing or after a message is sent to the dead letter topic.
/// The consumer continues processing subsequent messages even if individual messages fail.</remarks>
internal sealed partial class Consumer<TKey, TMessage>(
    IConsumer<TKey, TMessage> consumer,
    IProducer<TKey, TMessage> deadLetterProducer,
    IServiceScopeFactory serviceScopeFactory,
    IOptionsMonitor<KafkaWorkerConfig> kafkaConfigMonitor,
    KafkaWorkerMetrics metrics,
    ILogger<Consumer<TKey, TMessage>> logger) : BackgroundService where TMessage : class
{
    private readonly KafkaWorkerConfig _kafkaConfig = kafkaConfigMonitor.Get(typeof(TMessage).Name);

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
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult?.Message?.Value == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }

                await ProcessMessageWithRetryAsync(consumeResult, stoppingToken);

                CommitOffset(consumeResult);
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
        // If no dead letter topic is configured, log a warning and return (offset will still be committed)
        if (string.IsNullOrWhiteSpace(DeadLetterTopic))
        {
            LogNoDeadLetterTopicConfigured(logger, consumerResult.Message.Key);
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
                deadLetterProducer,
                DeadLetterTopic,
                consumerResult.Message.Key,
                consumerResult.Message.Value,
                overrideHeaders,
                consumerResult.Message.Headers,
                ProduceResiliencePipeline.Instance,
                stoppingToken);

            LogSentToDeadLetter(logger, DeadLetterTopic, consumerResult.Message.Key);
            metrics.DlqPublished.Add(1, new KeyValuePair<string, object?>("topic", Topic), new KeyValuePair<string, object?>("dlq_topic", DeadLetterTopic));
        }
        catch (Exception ex)
        {
            LogFailedToPublishToDeadLetter(logger, ex, DeadLetterTopic, consumerResult.Message.Key);
        }
    }

    private void CommitOffset(ConsumeResult<TKey, TMessage> consumeResult)
    {
        consumer.StoreOffset(consumeResult);
        consumer.Commit();
    }

    // Configure the resilience pipeline for message processing with retry based on the MaxRetries configuration.
    private readonly ResiliencePipeline _messageProcessingResiliencePipeline = BuildRetryPipeline(
        kafkaConfigMonitor.Get(typeof(TMessage).Name).MaxRetries);

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
}
