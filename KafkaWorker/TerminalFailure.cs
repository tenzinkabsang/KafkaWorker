using Confluent.Kafka;

namespace KafkaWorker;

/// <summary>
/// Everything the library knows about a message at the moment it permanently gives up on it.
/// Passed to <see cref="ITerminalFailureSink{TMessage}"/>.
/// </summary>
/// <typeparam name="TMessage">The message type being consumed.</typeparam>
public sealed record TerminalFailure<TMessage> where TMessage : class
{
    /// <summary>The deserialized message value. Never null.</summary>
    public required TMessage Message { get; init; }

    /// <summary>
    /// The Kafka message key. Typed as <see cref="object"/> because the sink is generic over the
    /// message type only; with the default registration overloads this is a <see cref="string"/>.
    /// </summary>
    public object? MessageKey { get; init; }

    /// <summary>
    /// The topic the message was originally consumed from. For failures during DLQ reprocessing
    /// this is taken from the <c>original-topic</c> header when present.
    /// </summary>
    public required string SourceTopic { get; init; }

    /// <summary>Why the library stopped retrying this message.</summary>
    public required TerminalFailureReason Reason { get; init; }

    /// <summary>The failure description — the exception message, or the <c>error-message</c> header value.</summary>
    public string? Error { get; init; }

    /// <summary>
    /// How many times the DLQ consumer reprocessed this message (from the <c>reprocessed-attempt</c>
    /// header). Zero for failures on the main consumer path.
    /// </summary>
    public int ReprocessAttempts { get; init; }

    /// <summary>The Kafka headers of the failed message, including the library's tracking headers.</summary>
    public Headers? Headers { get; init; }
}
