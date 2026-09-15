using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// One record sitting in the dead letter topic, as reported by <see cref="IDlqInspector{TMessage}"/>,
/// with the library's tracking headers already interpreted.
/// </summary>
/// <typeparam name="TMessage">The message type being consumed.</typeparam>
public sealed record DlqEntry<TMessage> where TMessage : class
{
    /// <summary>The dead letter topic partition the record sits in.</summary>
    public required int Partition { get; init; }

    /// <summary>The record's offset within <see cref="Partition"/>.</summary>
    public required long Offset { get; init; }

    /// <summary>When the record was written to the dead letter topic.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The Kafka message key. Typed as <see cref="object"/> because the inspector is generic over the
    /// message type only; with the default registration overloads this is a <see cref="string"/>.
    /// </summary>
    public object? MessageKey { get; init; }

    /// <summary>
    /// The deserialized value. Null when <see cref="State"/> is
    /// <see cref="DlqEntryState.Tombstone"/> or <see cref="DlqEntryState.Undeserializable"/> —
    /// inspect <see cref="Headers"/> and <see cref="Error"/> for those.
    /// </summary>
    public TMessage? Message { get; init; }

    /// <summary>The topic the message was originally consumed from (the <c>original-topic</c> header).</summary>
    public string? SourceTopic { get; init; }

    /// <summary>
    /// Why the message is here: the <c>error-message</c> header, or the deserialization error when
    /// the record could not be read.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// How many times the DLQ consumer has already reprocessed this message
    /// (the <c>reprocessed-attempt</c> header).
    /// </summary>
    public int ReprocessAttempts { get; init; }

    /// <summary>
    /// How many reprocess attempts are left before the message is skipped for good. Zero for every
    /// state other than <see cref="DlqEntryState.Retryable"/>.
    /// </summary>
    public int RemainingAttempts { get; init; }

    /// <summary>What the next sweep will do with this record.</summary>
    public required DlqEntryState State { get; init; }

    /// <summary>The record's Kafka headers, including the library's tracking headers.</summary>
    public Headers? Headers { get; init; }
}
