namespace KafkaWorker;

/// <summary>
/// Implement this interface to process messages from your Kafka topic in batches instead of one
/// at a time. The library handles consuming, batching, retrying, offset commits, and dead letter
/// queue routing — you only write the business logic.
/// </summary>
/// <typeparam name="TMessage">The deserialized message type from your Kafka topic.</typeparam>
/// <remarks>
/// <para>
/// <b>When batching helps.</b> Reading from Kafka is already effectively free: the client keeps a
/// local pre-fetched queue, so consuming a message costs roughly a microsecond and involves no
/// broker round trip. Batching does not reduce broker calls — it exists solely to amortize the
/// work your handler does <i>downstream</i>. It pays off when the batch can collapse into a single
/// operation (one bulk insert, one transaction, one multi-row upsert, one batch API call). A
/// handler that simply loops over the batch issuing one call per message gains nothing.
/// </para>
/// <para>
/// <b>The handler must be idempotent.</b> If <see cref="HandleBatchAsync"/> throws, the library
/// cannot tell which message failed, so it re-runs every message in that batch individually
/// through the single-message path (retry, then dead letter queue). Messages that already
/// succeeded inside the failed batch are therefore processed a second time.
/// </para>
/// <para>
/// The handler is registered as a <b>scoped</b> service by <c>AddKafkaWorkerBatch</c>. A new DI
/// scope is created for each batch, so scoped dependencies (e.g., EF Core <c>DbContext</c>) can be
/// injected directly via the constructor and are shared by every message in the batch.
/// </para>
/// <para>
/// <b>Error handling rules:</b>
/// <list type="bullet">
///   <item>Throw any <see cref="System.Exception"/> to signal the batch failed — each message is
///   then retried and dead lettered individually, exactly as in single-message mode.</item>
///   <item>Throw <see cref="InvalidMessageException"/> only from a single-message batch; from a
///   larger batch it is indistinguishable from any other failure and simply triggers the
///   per-message fallback, where it regains its usual meaning.</item>
///   <item>Do not catch <see cref="System.OperationCanceledException"/> — let it propagate so the
///   consumer can shut down cleanly. The batch is not committed and is redelivered on restart.</item>
/// </list>
/// </para>
/// <para>
/// Batch size and latency are controlled by <see cref="KafkaWorkerConfig.MaxBatchSize"/> and
/// <see cref="KafkaWorkerConfig.BatchLingerMs"/>. Keep
/// <c>MaxBatchSize x per-message processing time</c> comfortably inside the client's
/// <c>MaxPollIntervalMs</c>, or the group will evict the consumer mid-batch.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class OrderBatchHandler(OrderDbContext db) : IBatchMessageHandler&lt;OrderMessage&gt;
/// {
///     public async Task HandleBatchAsync(IReadOnlyList&lt;OrderMessage&gt; messages, CancellationToken stoppingToken)
///     {
///         db.Orders.AddRange(messages.Select(Order.From));
///         await db.SaveChangesAsync(stoppingToken); // one round trip for the whole batch
///     }
/// }
/// </code>
/// </example>
public interface IBatchMessageHandler<TMessage> where TMessage : class
{
    /// <summary>
    /// Handles a batch of Kafka messages. The library calls this once per drained batch.
    /// </summary>
    /// <param name="messages">
    /// The deserialized message values, in the order they were consumed. Never null, never empty,
    /// and never longer than <see cref="KafkaWorkerConfig.MaxBatchSize"/>. Tombstones (null values)
    /// are filtered out before the handler sees them. A batch may span partitions, so no ordering
    /// is guaranteed across messages from different partitions.
    /// </param>
    /// <param name="stoppingToken">
    /// Cancellation token triggered when the host is shutting down.
    /// Pass this to any async operations. Do not catch <see cref="System.OperationCanceledException"/>.
    /// </param>
    /// <returns>A task that completes when every message in the batch has been processed.</returns>
    /// <exception cref="System.Exception">
    /// Any exception marks the whole batch as failed. The library then re-processes these same
    /// messages one at a time through the single-message path, so each one is retried and dead
    /// lettered on its own merits.
    /// </exception>
    Task HandleBatchAsync(IReadOnlyList<TMessage> messages, CancellationToken stoppingToken);
}
