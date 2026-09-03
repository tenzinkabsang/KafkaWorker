namespace KafkaWorker;

/// <summary>
/// Optional extension point invoked when the library permanently gives up on a message — the
/// message will never be retried again by KafkaWorker. Implement it to persist terminal failures
/// somewhere durable and queryable (a database table, blob storage, an alerting system) instead of
/// relying on the DLQ topic's retention as the only record.
/// </summary>
/// <remarks>
/// <para>
/// Register any implementation in DI (any lifetime — it is resolved from a fresh scope per call,
/// so scoped dependencies like an EF Core <c>DbContext</c> work naturally):
/// </para>
/// <code>
/// builder.Services.AddScoped&lt;ITerminalFailureSink&lt;OrderMessage&gt;, PostgresFailureSink&gt;();
/// </code>
/// <para>
/// The sink fires exactly when the library stops tracking a message
/// (<see cref="TerminalFailureReason"/> lists the cases):
/// </para>
/// <list type="bullet">
///   <item>The DLQ consumer permanently skips a message — marked invalid, rejected with
///   <see cref="InvalidMessageException"/> during reprocessing, or over the reprocess-attempt cap.</item>
///   <item>The main consumer loses a message — the best-effort DLQ publish failed, or no
///   <see cref="KafkaWorkerConfig.DeadLetterTopic"/> is configured. For these the sink is the
///   message's last chance to be persisted anywhere.</item>
/// </list>
/// <para>
/// A message that reaches the DLQ successfully does <b>not</b> fire the sink until it later becomes
/// terminal there, so with the DLQ consumer registered each terminal message fires the sink once.
/// Records that failed deserialization never fire the typed sink — they are captured to the DLQ as
/// raw bytes instead.
/// </para>
/// <para>
/// The sink is best-effort: an exception it throws is logged at <c>Error</c> and never crashes the
/// consumer, blocks the batch, or prevents the offset from advancing. If the sink write itself must
/// never be lost, make the sink internally durable (e.g., write-ahead or retry within the sink).
/// </para>
/// </remarks>
/// <typeparam name="TMessage">The message type being consumed.</typeparam>
public interface ITerminalFailureSink<TMessage> where TMessage : class
{
    /// <summary>
    /// Called once when a message becomes terminal. Exceptions are logged and swallowed by the library.
    /// </summary>
    /// <param name="failure">The message and the context of why it became terminal.</param>
    /// <param name="cancellationToken">Triggered when the host is shutting down.</param>
    Task HandleAsync(TerminalFailure<TMessage> failure, CancellationToken cancellationToken);
}
