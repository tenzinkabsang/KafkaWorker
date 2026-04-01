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
    /// when no committed offsets exist for its consumer group. Use this when enabling the DLQ
    /// consumer after the system has been running to avoid reprocessing old messages.
    /// </summary>
    /// <remarks>
    /// When set, the DLQ consumer uses Kafka's <c>OffsetsForTimes</c> API to seek to the first
    /// message at or after this timestamp on first startup. Once offsets are committed, this
    /// setting has no effect. When <c>null</c> (default), the consumer starts from the earliest
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
