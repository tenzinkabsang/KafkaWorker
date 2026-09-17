namespace KafkaWorker;

/// <summary>
/// What the DLQ consumer will do with a dead letter record on its next sweep. This is the question
/// a generic Kafka topic browser cannot answer: whether a message is still on its way back or is
/// stuck where it is.
/// </summary>
public enum DlqEntryState
{
    /// <summary>
    /// Will be reprocessed in place on the next sweep.
    /// </summary>
    Retryable,

    /// <summary>
    /// Carries the <c>invalid-message</c> header. Skipped on every sweep, forever: the handler
    /// declared it will never succeed. Fires <see cref="ITerminalFailureSink{TMessage}"/> each time
    /// a sweep reaches it.
    /// </summary>
    Invalid,

    /// <summary>
    /// Has been reprocessed <see cref="KafkaWorkerConfig.DeadLetterMaxReprocessAttempts"/> times and
    /// is skipped from here on. Raising that limit brings the message back into play.
    /// </summary>
    AttemptsExhausted,

    /// <summary>
    /// Carries the <c>deserialization-failed</c> header, or failed to deserialize when read: the
    /// record's value is raw bytes the consumer cannot turn into a message.
    /// Never auto-reprocessed; it awaits a manual redrive.
    /// </summary>
    Undeserializable,

    /// <summary>
    /// A null-valued record. Committed past rather than reprocessed, so it never blocks the sweep.
    /// </summary>
    Tombstone
}
