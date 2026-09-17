using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// The single source of truth for what the DLQ sweep will do with a dead letter record.
/// </summary>
/// <remarks>
/// <para>
/// Two code paths need this answer: <see cref="DlqConsumer{TKey, TMessage}"/> acts on it, and
/// <see cref="DlqInspector{TKey, TMessage}"/> reports it. They must agree — an inspector that
/// disagrees with the sweep does not fail loudly, it quietly tells an operator a message is coming
/// back when it is actually stuck. Keeping the rules here means a change to sweep behaviour cannot
/// be made without the inspector following it.
/// </para>
/// <para>
/// The ordering below is the ordering the sweep applies, and each step earns its position: a
/// tombstone carries no value to look at, captured poison is never handed to a handler whatever its
/// headers say, and invalid beats attempt-counting because it is permanent regardless of how many
/// attempts remain.
/// </para>
/// </remarks>
internal static class DlqRecordClassifier
{
    /// <summary>
    /// Classifies a dead letter record from its value and tracking headers.
    /// </summary>
    /// <param name="hasValue">False for a tombstone — a record whose value (or message) is null.</param>
    /// <param name="headers">The record's Kafka headers. Null is treated as no tracking headers.</param>
    /// <param name="maxReprocessAttempts">
    /// <see cref="KafkaWorkerConfig.DeadLetterMaxReprocessAttempts"/> for the message type.
    /// </param>
    /// <returns>What the next sweep will do with the record.</returns>
    public static DlqEntryState Classify(bool hasValue, Headers? headers, int maxReprocessAttempts)
    {
        if (!hasValue)
        {
            return DlqEntryState.Tombstone;
        }

        if (IsCapturedPoison(headers))
        {
            return DlqEntryState.Undeserializable;
        }

        if (headers.IsInvalidMessage())
        {
            return DlqEntryState.Invalid;
        }

        return headers.GetReprocessAttemptCount() >= maxReprocessAttempts
            ? DlqEntryState.AttemptsExhausted
            : DlqEntryState.Retryable;
    }

    /// <summary>
    /// True when the record's value is the raw bytes of a message that failed deserialization on the
    /// main topic, captured for manual redrive. Checked on its own by the sweep's
    /// <see cref="ConsumeException"/> path, where the record never deserializes far enough to classify.
    /// </summary>
    public static bool IsCapturedPoison(Headers? headers)
        => string.Equals(
            headers.GetValue(KafkaHeaders.DeserializationFailed),
            "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// How many reprocess attempts a record has left. Zero for every state other than
    /// <see cref="DlqEntryState.Retryable"/>: a record the sweep will never hand to a handler again
    /// has no attempts remaining, however few it used.
    /// </summary>
    public static int RemainingAttempts(DlqEntryState state, int attempts, int maxReprocessAttempts)
        => state == DlqEntryState.Retryable ? Math.Max(0, maxReprocessAttempts - attempts) : 0;
}
