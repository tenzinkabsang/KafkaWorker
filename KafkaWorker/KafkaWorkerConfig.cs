using System.ComponentModel.DataAnnotations;

namespace KafkaWorker;

/// <summary>
/// Represents the configuration settings required for a Kafka worker, including consumer group, topic, retry policies,
/// and dead letter handling options.
/// </summary>
/// <remarks>This record is typically used to bind configuration values from application settings for Kafka-based
/// background processing. All required properties must be set for correct operation. The static <see cref="Section"/>
/// field specifies the configuration section name expected in the application's configuration source.</remarks>
public record KafkaWorkerConfig
{
    /// <summary>
    /// The Kafka consumer group ID. All instances sharing the same group ID coordinate to split partitions.
    /// </summary>
    /// <example><c>"xyz-order-processor"</c></example>
    [Required]
    public required string GroupId { get; init; }

    /// <summary>
    /// The Kafka topic to consume messages from.
    /// </summary>
    /// <example><c>"xyz.orders.v1"</c></example>
    [Required]
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the maximum number of retry attempts allowed for an operation.
    /// </summary>
    /// /// <value>
    /// The default value is 3. Set to 0 to disable retries entirely. Max allowed 5.
    /// </value>
    /// <remarks>
    /// Use this property to control how many times an operation will be retried after a failure before giving up. 
    /// Setting a higher value may increase the time taken to complete the operation in case of repeated failures.
    /// </remarks>
    [Range(0, RetryConstants.MaxRetryCount)]
    public int MaxRetries { get; init; } = RetryConstants.DefaultRetryCount;

    /// <summary>
    /// The maximum number of messages handed to an <see cref="IBatchMessageHandler{TMessage}"/> in
    /// a single call. Ignored unless the consumer was registered with <c>AddKafkaWorkerBatch</c>.
    /// </summary>
    /// <value>The default value is 100. Acceptable values are (1 - 10000).</value>
    /// <remarks>
    /// Keep <c>MaxBatchSize</c> multiplied by the per-message processing time comfortably inside the
    /// client's <c>MaxPollIntervalMs</c> (default 5 minutes), or the group evicts the consumer
    /// mid-batch. This value also bounds redelivery after a hard crash: batch consumers commit at
    /// each batch boundary, so at most one batch is reprocessed.
    /// </remarks>
    [Range(1, 10_000)]
    public int MaxBatchSize { get; init; } = 100;

    /// <summary>
    /// How long (in milliseconds) to keep accumulating messages into a batch after the first one
    /// arrives, before handing whatever has been collected to the batch handler. Ignored unless the
    /// consumer was registered with <c>AddKafkaWorkerBatch</c>.
    /// </summary>
    /// <value>The default value is 500. Acceptable values are (0 - 60000).</value>
    /// <remarks>
    /// This is a latency control, not a throughput one. Under load a batch fills from the client's
    /// local pre-fetch queue almost instantly and never waits; on a quiet topic the consumer waits
    /// this long and then processes the handful of messages it has. Set to 0 to never wait, taking
    /// only what is already buffered locally.
    /// </remarks>
    [Range(0, 60_000)]
    public int BatchLingerMs { get; init; } = 500;

    /// <summary>
    /// The DLQ topic where failed messages are published after all retries are exhausted.
    /// Leave <c>null</c> to disable DLQ publishing (failed messages are logged and skipped).
    /// </summary>
    /// <example><c>"xyz.orders.v1.dlq"</c></example>
    public string? DeadLetterTopic { get; init; }

    /// <summary>
    /// Gets the maximum number of times a message in the dead-letter queue can be reprocessed before it is permanently
    /// discarded.
    /// </summary>
    /// <value>
    /// The default value is 3. Acceptable value is (1 - 5)
    /// </value>
    /// <remarks>Use this property to limit the number of reprocessing attempts for messages that have failed
    /// and been moved to the dead-letter queue. Once the specified number of attempts is reached, the message will not
    /// be retried again.</remarks>
    [Range(1, RetryConstants.MaxRetryCount)]
    public int DeadLetterMaxReprocessAttempts { get; init; } = RetryConstants.DefaultRetryCount;

    /// <summary>
    /// How often (in minutes) the DLQ consumer checks for and reprocesses failed messages.
    /// </summary>
    /// <value>Default: 60 minutes.</value>
    [Range(1, int.MaxValue)]
    public int DeadLetterProcessingIntervalMinutes { get; init; } = 60;

    /// <summary>
    /// Optional UTC timestamp from which the dead letter consumer should start processing messages
    /// on partitions that have no committed offset for its consumer group. Use this when enabling
    /// the DLQ consumer after the system has been running to avoid reprocessing old messages.
    /// </summary>
    /// <remarks>
    /// When set, the DLQ consumer uses Kafka's <c>OffsetsForTimes</c> API to seek each partition
    /// without a committed offset to the first message at or after this timestamp; partitions with
    /// a committed offset resume from it and are unaffected. If the timestamp is newer than every
    /// message in a partition, that partition starts at the end (new messages only). When
    /// <c>null</c> (default), partitions without a committed offset start from the earliest
    /// available message (<c>AutoOffsetReset.Earliest</c>).
    /// The value must include a UTC offset (e.g., suffix with <c>Z</c> or <c>+00:00</c>).
    /// Values without an explicit offset will be interpreted using the server's local timezone.
    /// </remarks>
    /// <example><c>"2025-06-01T00:00:00Z"</c></example>
    public DateTimeOffset? DeadLetterStartFrom { get; init; }

    /// <summary>
    /// Specifies the configuration section name used for KafkaWorker consumer settings.
    /// </summary>
    public const string Section = "KafkaWorker:Consumer";
}
