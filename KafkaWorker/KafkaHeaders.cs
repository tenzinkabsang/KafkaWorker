namespace KafkaWorker;

/// <summary>
/// Constants for Kafka message headers used throughout the consumer library.
/// </summary>
internal static class KafkaHeaders
{
    /// <summary>
    /// The original topic the message was consumed from before being sent to the DLQ.
    /// </summary>
    public const string OriginalTopic = "original-topic";

    /// <summary>
    /// The error message describing why the message failed processing.
    /// </summary>
    public const string ErrorMessage = "error-message";

    /// <summary>
    /// Indicates the message is an invalid message that should not be reprocessed.
    /// Value should be "true" if present.
    /// </summary>
    public const string InvalidMessage = "invalid-message";

    /// <summary>
    /// Tracks which DLQ processing batch this message belongs to.
    /// Used to detect when we've looped back to already-processed messages.
    /// </summary>
    public const string BatchId = "batch-id";

    /// <summary>
    /// The number of times this message has been reprocessed from the DLQ.
    /// </summary>
    public const string ReprocessedAttempt = "reprocessed-attempt";
}
