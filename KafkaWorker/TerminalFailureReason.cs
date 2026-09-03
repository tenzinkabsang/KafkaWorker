namespace KafkaWorker;

/// <summary>
/// Why the library permanently stopped retrying a message.
/// </summary>
public enum TerminalFailureReason
{
    /// <summary>
    /// The handler threw <see cref="InvalidMessageException"/> — the message will never succeed
    /// and is permanently skipped.
    /// </summary>
    InvalidMessage,

    /// <summary>
    /// The message exceeded <see cref="KafkaWorkerConfig.DeadLetterMaxReprocessAttempts"/> and is
    /// permanently skipped by the DLQ consumer. It remains in the DLQ topic until retention expires.
    /// </summary>
    MaxReprocessAttemptsExceeded,

    /// <summary>
    /// The best-effort DLQ publish failed after all retries. The message is NOT in the DLQ topic —
    /// the sink is its last chance to be persisted anywhere.
    /// </summary>
    DeadLetterPublishFailed,

    /// <summary>
    /// Processing failed and no <see cref="KafkaWorkerConfig.DeadLetterTopic"/> is configured, so
    /// there is nowhere to dead-letter the message. The sink is its last chance to be persisted.
    /// </summary>
    NoDeadLetterTopicConfigured
}
