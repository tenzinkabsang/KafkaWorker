namespace KafkaWorker;

/// <summary>
/// Triggers an immediate dead letter queue reprocessing batch without waiting for the next
/// scheduled tick (<see cref="KafkaWorkerConfig.DeadLetterProcessingIntervalMinutes"/>).
/// </summary>
/// <remarks>
/// <para>
/// Registered by <c>AddKafkaWorkerDeadLetter</c>. Inject it anywhere — an admin endpoint, a
/// console command, a health-check remediation — and call <see cref="Trigger"/> when you want
/// failed messages retried right away, for example after a downstream outage is resolved:
/// </para>
/// <code>
/// app.MapPost("/admin/dlq/reprocess", (IDlqReprocessTrigger&lt;OrderMessage&gt; trigger) =>
/// {
///     trigger.Trigger();
///     return Results.Accepted();
/// });
/// </code>
/// <para>
/// The regular schedule is unaffected — triggering simply wakes the DLQ consumer early for one batch.
/// </para>
/// </remarks>
/// <typeparam name="TMessage">The message type whose DLQ consumer should run.</typeparam>
public interface IDlqReprocessTrigger<TMessage> where TMessage : class
{
    /// <summary>
    /// Wakes the DLQ consumer to run a reprocessing batch immediately. Safe to call at any time
    /// and from any thread; calls made while a trigger is already pending coalesce into a single
    /// batch. If a batch is currently running, another one runs right after it completes.
    /// </summary>
    void Trigger();
}
