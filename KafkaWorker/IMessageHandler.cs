namespace KafkaWorker;

/// <summary>
/// Implement this interface to define how messages from your Kafka topic are handled.
/// The library handles consuming, retrying, offset commits, and dead letter queue routing —
/// you only write the business logic.
/// </summary>
/// <typeparam name="TMessage">The deserialized message type from your Kafka topic.</typeparam>
/// <remarks>
/// <para>
/// The handler is registered as a <b>scoped</b> service by the library's extension methods.
/// A new DI scope is created for each message, so scoped dependencies (e.g., EF Core <c>DbContext</c>)
/// can be injected directly via the constructor.
/// </para>
/// <para>
/// <b>Error handling rules:</b>
/// <list type="bullet">
///   <item>Throw any <see cref="System.Exception"/> for transient failures — the library retries automatically with exponential backoff.</item>
///   <item>Throw <see cref="InvalidMessageException"/> for permanent failures (bad data, validation errors) — the message bypasses retry and goes directly to the DLQ.</item>
///   <item>Do not catch <see cref="System.OperationCanceledException"/> — let it propagate so the consumer can shut down cleanly.</item>
/// </list>
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class OrderMessageHandler(IOrderService orderService) : IMessageHandler&lt;OrderMessage&gt;
/// {
///     public async Task HandleMessageAsync(OrderMessage message, CancellationToken stoppingToken)
///     {
///         if (string.IsNullOrEmpty(message.OrderId))
///             throw new InvalidMessageException("OrderId is required", message);
///
///         await orderService.ProcessAsync(message, stoppingToken);
///     }
/// }
/// </code>
/// </example>
public interface IMessageHandler<TMessage> where TMessage : class
{
    /// <summary>
    /// Handles a single Kafka message. The library calls this for each consumed message.
    /// </summary>
    /// <param name="message">The deserialized message value from Kafka. Never null.</param>
    /// <param name="stoppingToken">
    /// Cancellation token triggered when the host is shutting down.
    /// Pass this to any async operations. Do not catch <see cref="System.OperationCanceledException"/>.
    /// </param>
    /// <returns>A task that completes when the message has been successfully processed.</returns>
    /// <exception cref="InvalidMessageException">
    /// Throw for messages that will never succeed (validation errors, malformed data).
    /// The message bypasses retry and is sent directly to the dead letter queue.
    /// </exception>
    /// <exception cref="System.Exception">
    /// Any other exception triggers automatic retry with exponential backoff.
    /// After all retries are exhausted, the message is sent to the dead letter queue (if configured).
    /// </exception>
    Task HandleMessageAsync(TMessage message, CancellationToken stoppingToken);
}
