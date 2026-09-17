namespace KafkaWorker;

/// <summary>
/// Reads the dead letter topic without consuming it: no offset is stored and nothing is committed,
/// so inspecting never changes what the DLQ consumer will reprocess.
/// </summary>
/// <remarks>
/// <para>
/// Registered by <c>AddKafkaWorkerDeadLetter</c> alongside <see cref="IDlqReprocessTrigger{TMessage}"/>.
/// Inject it into a health check, an admin endpoint, or a support tool:
/// </para>
/// <code>
/// app.MapGet("/admin/dlq/orders", async (IDlqInspector&lt;OrderMessage&gt; inspector, CancellationToken ct) =>
/// {
///     var entries = await inspector.PeekAsync(new DlqPeekOptions { MaxMessages = 100 }, ct);
///     return Results.Ok(entries);
/// }).RequireAuthorization("admin");
/// </code>
/// <para>
/// Both methods read every partition of the dead letter topic, not just the ones this process's DLQ
/// consumer is assigned, so any replica gives a complete answer. They talk to the broker on each
/// call — cache <c>GetDepthAsync</c> rather than calling it from a health probe on a short interval.
/// </para>
/// <para>
/// The inspector is read-only by design. To act on what it shows, call
/// <see cref="IDlqReprocessTrigger{TMessage}.Trigger"/> to run a sweep now.
/// </para>
/// </remarks>
/// <typeparam name="TMessage">The message type whose dead letter topic should be read.</typeparam>
public interface IDlqInspector<TMessage> where TMessage : class
{
    /// <summary>
    /// Reports how much is waiting in the dead letter topic, per partition and in total. Reads
    /// offsets and metadata only — no message is fetched or deserialized, so this is cheap enough
    /// to poll on a slow interval and is the right thing to alert on.
    /// </summary>
    /// <param name="cancellationToken">Cancels the broker round trips.</param>
    /// <returns>The depth of every partition of the dead letter topic.</returns>
    Task<DlqDepth> GetDepthAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads entries from the dead letter topic, starting where the next sweep would, with each
    /// record's tracking headers interpreted into a <see cref="DlqEntryState"/>. Records that cannot
    /// be deserialized are reported rather than thrown — surfacing them is the point.
    /// </summary>
    /// <param name="options">Bounds the read. Null uses the defaults: 50 entries, 10 seconds, every partition.</param>
    /// <param name="cancellationToken">Cancels the read, returning nothing.</param>
    /// <returns>The entries read, in partition and offset order.</returns>
    /// <exception cref="ArgumentException">
    /// <see cref="DlqPeekOptions.FromOffset"/> was set without <see cref="DlqPeekOptions.Partition"/>.
    /// </exception>
    Task<IReadOnlyList<DlqEntry<TMessage>>> PeekAsync(
        DlqPeekOptions? options = null,
        CancellationToken cancellationToken = default);
}
