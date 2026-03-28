namespace KafkaWorker;

/// <summary>
/// Throw this exception when a message cannot be processed and should not be retried.
/// The message will be sent directly to the dead letter queue without any retry attempts.
/// </summary>
/// <remarks>
/// Use this for permanent failures such as:
/// <list type="bullet">
///   <item>Validation errors (malformed data that will never pass validation)</item>
///   <item>Business rule violations that cannot be resolved by retrying</item>
///   <item>Schema mismatches or deserialization errors</item>
///   <item>Missing required fields or invalid data formats</item>
/// </list>
/// </remarks>
/// <example>
/// <code>
/// public async Task HandleMessageAsync(OrderMessage message, CancellationToken token)
/// {
///     if (string.IsNullOrEmpty(message.OrderId))
///     {
///         throw new InvalidMessageException("OrderId is required", message);
///     }
///     
///     // Process the message...
/// }
/// </code>
/// </example>
public sealed class InvalidMessageException : Exception
{
    /// <summary>
    /// Gets the message payload that caused the failure, if available.
    /// </summary>
    public object? MessagePayload { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidMessageException"/> class.
    /// </summary>
    /// <param name="message">The error message describing why the message is invalid.</param>
    public InvalidMessageException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidMessageException"/> class.
    /// </summary>
    /// <param name="message">The error message describing why the message is invalid.</param>
    /// <param name="messagePayload">The message payload that caused the failure.</param>
    public InvalidMessageException(string message, object? messagePayload) : base(message)
    {
        MessagePayload = messagePayload;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidMessageException"/> class.
    /// </summary>
    /// <param name="message">The error message describing why the message is invalid.</param>
    /// <param name="innerException">The exception that caused this failure.</param>
    public InvalidMessageException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidMessageException"/> class.
    /// </summary>
    /// <param name="message">The error message describing why the message is invalid.</param>
    /// <param name="messagePayload">The message payload that caused the failure.</param>
    /// <param name="innerException">The exception that caused this failure.</param>
    public InvalidMessageException(string message, object? messagePayload, Exception innerException) : base(message, innerException)
    {
        MessagePayload = messagePayload;
    }
}
